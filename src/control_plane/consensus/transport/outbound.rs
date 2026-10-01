use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::time::Duration;

use anyhow::Context;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::control_plane::NodeId;
use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::control_plane::consensus::messages::{
    InboundRaftRpc, OutboundRaftPacket, RaftTransportCommand, WireRaftMessage,
};
use crate::control_plane::consensus::raft::states::security::AclRecord;
use crate::control_plane::membership::actor::SwimSender;
use crate::control_plane::metadata::AclResource;
use crate::impl_from_variant;
use crate::net::{TransportTcpStream, TransportWriteHalf};
use crate::security::NodeTransportSecurity;

use super::before_deadline;
use super::inbound::{AcceptedRaftConnection, ClusterMessageReader};
use super::protocol::{
    AclSnapshotRequest, AclSnapshotResponse, ClusterRequest, MAX_CLUSTER_FRAME_SIZE, encode_frame,
};

const CONNECT_BACKOFF: Duration = Duration::from_secs(2);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const IDENTITY_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
const RAFT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// Bounds aggregate dial tasks and their per-peer message buffers. Overflow is
/// safe to drop because Raft re-drives unacknowledged RPCs.
const MAX_CONNECTING_PEERS: usize = 16;
/// Upper bounds on work buffered per peer while its connection is in flight;
/// overflow is dropped (Raft retries by timer).
const CONNECTING_PEER_BUFFER_CAP: usize = 256;
const CONNECTING_PEER_BUFFER_BYTE_CAP: usize = MAX_CLUSTER_FRAME_SIZE * 2;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConnectionOrigin {
    Local,
    Remote,
}

/// A writer and its matching reader task. Dropping the slot always closes both
/// halves, including when a newer authenticated connection replaces it.
struct ActiveConnection {
    generation: u64,
    origin: ConnectionOrigin,
    writer: TransportWriteHalf,
    reader_task: JoinHandle<()>,
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.reader_task.abort();
    }
}

impl ActiveConnection {
    async fn write_messages_before(
        &mut self,
        messages: &[WireRaftMessage],
        deadline: Instant,
    ) -> io::Result<()> {
        if deadline <= Instant::now() {
            return Err(io::ErrorKind::TimedOut.into());
        }

        for message in messages {
            let frame = encode_frame(message).map_err(io::Error::other)?;
            before_deadline(deadline, self.writer.write_all(&frame))
                .await
                .ok_or(io::ErrorKind::TimedOut)??;
        }
        before_deadline(deadline, self.writer.flush())
            .await
            .ok_or(io::ErrorKind::TimedOut)?
    }
}

struct ConnectingPeer {
    generation: u64,
    messages: Vec<WireRaftMessage>,
    serialized_bytes: usize,
    task: JoinHandle<()>,
}

impl ConnectingPeer {
    fn extend(&mut self, messages: impl IntoIterator<Item = WireRaftMessage>) {
        for message in messages {
            if self.messages.len() == CONNECTING_PEER_BUFFER_CAP {
                break;
            }
            let Ok(message_bytes) = borsh::object_length(&message) else {
                break;
            };
            let serialized_bytes = self
                .serialized_bytes
                .saturating_add(message_bytes)
                .saturating_add(std::mem::size_of::<u32>());
            if serialized_bytes > CONNECTING_PEER_BUFFER_BYTE_CAP {
                break;
            }
            self.messages.push(message);
            self.serialized_bytes = serialized_bytes;
        }
    }
}

impl Drop for ConnectingPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A background connection attempt completed.
pub(super) struct ConnectionAttemptCompleted {
    target: NodeId,
    generation: u64,
    result: anyhow::Result<OutboundClusterConnection>,
}

/// A particular generation's reader stopped.
pub(super) struct ConnectionReaderClosed {
    peer_id: NodeId,
    generation: u64,
}

pub(super) enum ConnectionEvent {
    AttemptCompleted(ConnectionAttemptCompleted),
    ReaderClosed(ConnectionReaderClosed),
}

impl_from_variant!(
    ConnectionEvent,
    AttemptCompleted(ConnectionAttemptCompleted),
    ReaderClosed(ConnectionReaderClosed),
);

/// Manages peer connections, address resolution, and dead-peer tracking.
///
/// On simultaneous connect, the connection initiated by the lower `NodeId`
/// wins. A newer authenticated connection with the same origin replaces the
/// older generation.
pub(super) struct RaftRpcDispatcher {
    node_id: NodeId,
    connections: HashMap<NodeId, ActiveConnection>,
    next_generation: u64,
    next_send_start: usize,
    /// Peers explicitly disconnected via DisconnectPeer. Outbound RPCs to
    /// these peers are dropped until an authenticated connection is accepted.
    dead_peers: HashSet<NodeId>,
    connect_backoffs: HashMap<NodeId, Instant>,
    connecting_peers: HashMap<NodeId, ConnectingPeer>,
    connection_event_tx: mpsc::Sender<ConnectionEvent>,
    node_transport: NodeTransportSecurity,
}

impl RaftRpcDispatcher {
    pub(super) fn new(
        node_id: NodeId,
        connection_event_tx: mpsc::Sender<ConnectionEvent>,
        node_transport: NodeTransportSecurity,
    ) -> Self {
        Self {
            node_id,
            connections: HashMap::new(),
            next_generation: 0,
            next_send_start: 0,
            dead_peers: HashSet::new(),
            connect_backoffs: HashMap::new(),
            connecting_peers: HashMap::new(),
            connection_event_tx,
            node_transport,
        }
    }

    pub(super) fn accept(&mut self, connection: AcceptedRaftConnection, raft_tx: &MutlRaftSender) {
        let AcceptedRaftConnection {
            initial_message,
            reader,
            writer,
        } = connection;
        let peer_id = initial_message.peer_id.clone();

        // Authentication is positive evidence that a formerly-dead peer is
        // back. Do this even when the deterministic connection tie-break keeps
        // an already-installed connection.
        self.dead_peers.remove(&peer_id);
        self.connect_backoffs.remove(&peer_id);

        if self.existing_connection_wins(&peer_id, ConnectionOrigin::Remote) {
            // The local node is the lower initiator, so its connection wins.
            return;
        }

        let initial_rpc = InboundRaftRpc {
            shard_group_id: initial_message.shard_group_id,
            peer_id: peer_id.clone(),
            rpc: initial_message.rpc,
        };
        self.install_connection(
            peer_id,
            ConnectionOrigin::Remote,
            reader,
            writer,
            Some(initial_rpc),
            raft_tx,
        );
    }

    pub(super) async fn handle_commands(
        &mut self,
        batch: Box<[RaftTransportCommand]>,
        swim_tx: &SwimSender,
    ) {
        // Apply disconnects before sending so packets in the same batch already
        // skip removed peers.
        let mut packets = Vec::new();
        for command in batch {
            match command {
                RaftTransportCommand::Send(outbound) => packets.extend(outbound),
                RaftTransportCommand::DisconnectPeer(peer_id) => self.disconnect(peer_id),
            }
        }
        if !packets.is_empty() {
            self.send(packets, swim_tx).await;
        }
    }

    pub(super) async fn send(&mut self, packets: Vec<OutboundRaftPacket>, swim_tx: &SwimSender) {
        let batch_write_deadline = Instant::now() + RAFT_WRITE_TIMEOUT;
        for (target_id, messages) in self.group_packets(packets) {
            if batch_write_deadline <= Instant::now() {
                break;
            }
            self.send_to_target(target_id, messages, swim_tx, batch_write_deadline)
                .await;
        }
    }

    fn group_packets(
        &mut self,
        packets: Vec<OutboundRaftPacket>,
    ) -> Vec<(NodeId, Vec<WireRaftMessage>)> {
        let mut by_target: BTreeMap<NodeId, Vec<WireRaftMessage>> = BTreeMap::new();
        for packet in packets {
            if self.dead_peers.contains(&packet.target) {
                continue;
            }
            by_target
                .entry(packet.target)
                .or_default()
                .push(WireRaftMessage {
                    shard_group_id: packet.shard_group_id,
                    peer_id: self.node_id.clone(),
                    rpc: packet.rpc,
                });
        }
        let mut groups: Vec<_> = by_target.into_iter().collect();
        if !groups.is_empty() {
            let start = self.next_send_start % groups.len();
            groups.rotate_left(start);
            self.next_send_start = self.next_send_start.wrapping_add(1);
        }
        groups
    }

    async fn send_to_target(
        &mut self,
        target_id: NodeId,
        mut messages: Vec<WireRaftMessage>,
        swim_tx: &SwimSender,
        batch_write_deadline: Instant,
    ) {
        if let Some(&failed_at) = self.connect_backoffs.get(&target_id) {
            if failed_at.elapsed() < CONNECT_BACKOFF {
                return;
            }
            self.connect_backoffs.remove(&target_id);
        }
        if self.connections.contains_key(&target_id)
            && self
                .write_messages_to_before(&target_id, &messages, batch_write_deadline)
                .await
                .is_ok()
        {
            return;
        }
        if batch_write_deadline <= Instant::now() {
            return;
        }
        if let Some(connecting) = self.connecting_peers.get_mut(&target_id) {
            connecting.extend(messages);
            return;
        }
        if self.connecting_peers.len() == MAX_CONNECTING_PEERS {
            tracing::debug!(
                peer = %target_id,
                "Raft connection attempt dropped: global attempt limit reached"
            );
            return;
        }

        let initial_raft_message = messages.remove(0);
        let generation = self.allocate_generation();
        let peer_id = target_id.clone();
        let local_node_id = self.node_id.clone();
        let swim_tx = swim_tx.clone();
        let node_transport = self.node_transport.clone();
        let event_tx = self.connection_event_tx.clone();
        let task = tokio::spawn(async move {
            let result = OutboundClusterConnection::connect_raft(
                &peer_id,
                &local_node_id,
                &swim_tx,
                &node_transport,
                initial_raft_message,
            )
            .await;
            let _ = event_tx
                .send(
                    ConnectionAttemptCompleted {
                        target: peer_id,
                        generation,
                        result,
                    }
                    .into(),
                )
                .await;
        });
        let mut connecting = ConnectingPeer {
            generation,
            messages: Vec::new(),
            serialized_bytes: 0,
            task,
        };
        connecting.extend(messages);
        self.connecting_peers.insert(target_id, connecting);
    }

    pub(super) async fn on_connection_event(
        &mut self,
        event: ConnectionEvent,
        raft_tx: &MutlRaftSender,
    ) {
        match event {
            ConnectionEvent::AttemptCompleted(completed) => {
                self.on_connection_attempt_completed(completed, raft_tx)
                    .await;
            }
            ConnectionEvent::ReaderClosed(closed) => {
                let is_current = self
                    .connections
                    .get(&closed.peer_id)
                    .is_some_and(|connection| connection.generation == closed.generation);
                if is_current {
                    self.connections.remove(&closed.peer_id);
                }
            }
        }
    }

    async fn on_connection_attempt_completed(
        &mut self,
        completed: ConnectionAttemptCompleted,
        raft_tx: &MutlRaftSender,
    ) {
        let ConnectionAttemptCompleted {
            target,
            generation,
            result,
        } = completed;
        let Some(attempt) = self
            .connecting_peers
            .get_mut(&target)
            .filter(|attempt| attempt.generation == generation)
        else {
            tracing::debug!(peer = %target, generation, "ignoring stale connection attempt");
            return;
        };
        let buffered = std::mem::take(&mut attempt.messages);
        self.connecting_peers.remove(&target);
        let established = match result {
            Ok(established) => established,
            Err(error) => {
                tracing::warn!(peer = %target, "connection attempt failed: {error}");
                if self.connections.contains_key(&target) {
                    self.flush_buffered_messages(&target, &buffered).await;
                } else {
                    self.connect_backoffs.insert(target, Instant::now());
                }
                return;
            }
        };

        if self.dead_peers.contains(&target) {
            return;
        }
        if self.existing_connection_wins(&target, ConnectionOrigin::Local) {
            tracing::debug!(
                peer = %target,
                buffered = buffered.len(),
                "simultaneous connect: peer's lower-NodeId connection wins",
            );
            self.flush_buffered_messages(&target, &buffered).await;
            return;
        }

        let (reader, writer) = established.into_parts();
        self.install_connection(
            target.clone(),
            ConnectionOrigin::Local,
            reader,
            writer,
            None,
            raft_tx,
        );
        self.flush_buffered_messages(&target, &buffered).await;
    }

    /// The connection initiated by the lower NodeId wins. A newer connection
    /// with the same origin replaces the older generation.
    fn existing_connection_wins(
        &self,
        peer_id: &NodeId,
        incoming_origin: ConnectionOrigin,
    ) -> bool {
        self.connections.get(peer_id).is_some_and(|existing| {
            !existing.reader_task.is_finished()
                && match (existing.origin, incoming_origin) {
                    (ConnectionOrigin::Local, ConnectionOrigin::Remote) => &self.node_id < peer_id,
                    (ConnectionOrigin::Remote, ConnectionOrigin::Local) => peer_id < &self.node_id,
                    (ConnectionOrigin::Local, ConnectionOrigin::Local)
                    | (ConnectionOrigin::Remote, ConnectionOrigin::Remote) => false,
                }
        })
    }

    async fn flush_buffered_messages(&mut self, target: &NodeId, messages: &[WireRaftMessage]) {
        if !messages.is_empty()
            && let Err(error) = self
                .write_messages_to_before(target, messages, Instant::now() + RAFT_WRITE_TIMEOUT)
                .await
        {
            tracing::debug!(
                peer = %target,
                "failed to flush buffered Raft messages: {error}"
            );
        }
    }

    fn allocate_generation(&mut self) -> u64 {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);
        generation
    }

    fn install_connection(
        &mut self,
        peer_id: NodeId,
        origin: ConnectionOrigin,
        reader: ClusterMessageReader,
        writer: TransportWriteHalf,
        initial_rpc: Option<InboundRaftRpc>,
        raft_tx: &MutlRaftSender,
    ) {
        let generation = self.allocate_generation();

        let reader_peer_id = peer_id.clone();
        let raft_tx = raft_tx.clone();
        let event_tx = self.connection_event_tx.clone();
        let reader_task = tokio::spawn(async move {
            let actor_available = match initial_rpc {
                Some(initial_rpc) => raft_tx.send(initial_rpc).await.is_ok(),
                None => true,
            };
            if actor_available {
                reader.run(raft_tx, reader_peer_id.clone()).await;
            }
            let _ = event_tx
                .send(
                    ConnectionReaderClosed {
                        peer_id: reader_peer_id,
                        generation,
                    }
                    .into(),
                )
                .await;
        });

        self.connections.insert(
            peer_id,
            ActiveConnection {
                generation,
                origin,
                writer,
                reader_task,
            },
        );
    }

    pub(super) fn disconnect(&mut self, peer_id: NodeId) {
        self.connections.remove(&peer_id);
        self.connecting_peers.remove(&peer_id);
        tracing::info!("[{}] Disconnected dead peer {:?}", self.node_id, peer_id);
        self.dead_peers.insert(peer_id);
    }

    pub(super) fn cleanup_dead_peers(&mut self) {
        self.dead_peers.clear();
        self.connect_backoffs.clear();
    }

    /// Writes one batch within a single deadline. A timeout may leave a partial
    /// frame on the stream, so any error drops the entire connection.
    async fn write_messages_to_before(
        &mut self,
        target: &NodeId,
        messages: &[WireRaftMessage],
        batch_write_deadline: Instant,
    ) -> io::Result<()> {
        if batch_write_deadline <= Instant::now() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Raft transport batch write deadline elapsed",
            ));
        }

        let result = {
            let connection = self.connections.get_mut(target).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "no writer for target")
            })?;
            connection
                .write_messages_before(messages, batch_write_deadline)
                .await
        };

        if result.is_err() {
            self.connections.remove(target);
        }
        result
    }

    #[cfg(test)]
    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.connections.contains_key(node_id)
    }

    #[cfg(test)]
    pub fn connection_generation(&self, node_id: &NodeId) -> Option<u64> {
        self.connections
            .get(node_id)
            .map(|connection| connection.generation)
    }

    #[cfg(test)]
    pub fn is_dead(&self, node_id: &NodeId) -> bool {
        self.dead_peers.contains(node_id)
    }
}

/// One authenticated outbound cluster stream.
pub(crate) struct OutboundClusterConnection {
    reader: ClusterMessageReader,
    writer: TransportWriteHalf,
}

impl OutboundClusterConnection {
    /// Connects, authenticates, and sends the opening Raft request.
    async fn connect_raft(
        target_id: &NodeId,
        local_node_id: &NodeId,
        swim_tx: &SwimSender,
        node_transport: &NodeTransportSecurity,
        initial_raft_message: WireRaftMessage,
    ) -> anyhow::Result<Self> {
        tokio::time::timeout(CONNECTION_ATTEMPT_TIMEOUT, async {
            let Some(address) = swim_tx.resolve_address(target_id.clone()).await? else {
                anyhow::bail!("cannot resolve address for {target_id}");
            };

            let stream = tokio::time::timeout(
                CONNECT_TIMEOUT,
                TransportTcpStream::connect_node(address.cluster_addr(), node_transport),
            )
            .await??;
            tokio::time::timeout(IDENTITY_HANDSHAKE_TIMEOUT, async {
                let mut connection =
                    Self::new(stream, target_id, local_node_id, node_transport).await?;
                connection.send_request(initial_raft_message.into()).await?;
                Ok(connection)
            })
            .await?
        })
        .await
        .context("connection attempt timed out")?
    }

    pub(crate) async fn new(
        mut stream: TransportTcpStream,
        expected_peer_id: &NodeId,
        local_node_id: &NodeId,
        node_transport: &NodeTransportSecurity,
    ) -> anyhow::Result<Self> {
        node_transport
            .authenticate_outbound(&mut stream, local_node_id, expected_peer_id)
            .await?;
        let (reader, writer) = stream.into_split();
        Ok(Self {
            reader: ClusterMessageReader::new(reader),
            writer,
        })
    }

    pub(crate) async fn read_acl_snapshot(
        &mut self,
        resource: AclResource,
    ) -> anyhow::Result<AclRecord> {
        self.send_request(AclSnapshotRequest { resource }.into())
            .await?;
        Ok(self
            .reader
            .read_frame::<AclSnapshotResponse>(MAX_CLUSTER_FRAME_SIZE, "ACL snapshot response")
            .await?
            .snapshot)
    }

    pub(super) async fn send_request(&mut self, request: ClusterRequest) -> anyhow::Result<()> {
        self.writer.write_all(&encode_frame(&request)?).await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub(super) fn into_parts(self) -> (ClusterMessageReader, TransportWriteHalf) {
        (self.reader, self.writer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::consensus::actor::MultiRaftActor;
    use crate::control_plane::consensus::messages::{InstallSnapshot, RaftRpc};
    use crate::control_plane::consensus::raft::storage::RaftSnapshotHeader;
    use crate::control_plane::membership::ShardGroupId;
    use crate::control_plane::membership::actor::SwimActor;
    use crate::net::TcpStream;
    use turmoil::Builder;

    fn snapshot_message(offset: u64, payload_len: usize) -> WireRaftMessage {
        WireRaftMessage {
            shard_group_id: ShardGroupId(1),
            peer_id: NodeId::new("peer"),
            rpc: InstallSnapshot {
                term: 1,
                leader_id: NodeId::new("peer"),
                header: RaftSnapshotHeader {
                    last_included_index: 1,
                    last_included_term: 1,
                    checksum: 0,
                    size_bytes: payload_len as u64,
                },
                offset,
                data: vec![0; payload_len].into_boxed_slice(),
                done: false,
            }
            .into(),
        }
    }

    fn test_dispatcher(local_node_id: NodeId) -> (RaftRpcDispatcher, MutlRaftSender) {
        let (raft_tx, _raft_mailbox) = MultiRaftActor::channel(1);
        let (event_tx, _event_rx) = mpsc::channel(1);
        (
            RaftRpcDispatcher::new(
                local_node_id,
                event_tx,
                NodeTransportSecurity::TrustedDevelopment,
            ),
            raft_tx,
        )
    }

    #[tokio::test]
    async fn pending_messages_preserve_prefix_at_byte_cap() {
        let payload_len = CONNECTING_PEER_BUFFER_BYTE_CAP / 3;
        let mut pending = ConnectingPeer {
            generation: 0,
            messages: Vec::new(),
            serialized_bytes: 0,
            task: tokio::spawn(std::future::pending()),
        };

        pending.extend((0..3).map(|offset| snapshot_message(offset, payload_len)));

        assert_eq!(pending.messages.len(), 2);
        assert!(pending.serialized_bytes <= CONNECTING_PEER_BUFFER_BYTE_CAP);
        let offsets: Vec<_> = pending
            .messages
            .iter()
            .map(|message| {
                let RaftRpc::InstallSnapshot(snapshot) = &message.rpc else {
                    panic!("test constructed a non-snapshot message")
                };
                snapshot.offset
            })
            .collect();
        assert_eq!(offsets, [0, 1]);
    }

    #[tokio::test]
    async fn batch_start_rotates_across_peers() {
        let (mut dispatcher, _raft_tx) = test_dispatcher(NodeId::new("local"));
        let targets = ["a", "b", "c"];

        for expected in targets.into_iter().cycle().take(6) {
            let packets = targets
                .iter()
                .map(|target| {
                    let message = snapshot_message(0, 0);
                    OutboundRaftPacket::new(
                        message.shard_group_id,
                        NodeId::new(*target),
                        message.rpc,
                    )
                })
                .collect();
            let groups = dispatcher.group_packets(packets);

            assert_eq!(groups[0].0, NodeId::new(expected));
        }
    }

    #[tokio::test]
    async fn stale_attempt_completion_keeps_the_replacement_attempt() {
        let local_node_id = NodeId::new("local");
        let target = NodeId::new("peer");
        let (mut dispatcher, raft_tx) = test_dispatcher(local_node_id);
        dispatcher.connecting_peers.insert(
            target.clone(),
            ConnectingPeer {
                generation: 2,
                messages: Vec::new(),
                serialized_bytes: 0,
                task: tokio::spawn(std::future::pending()),
            },
        );

        dispatcher
            .on_connection_attempt_completed(
                ConnectionAttemptCompleted {
                    target: target.clone(),
                    generation: 1,
                    result: Err(anyhow::anyhow!("stale attempt")),
                },
                &raft_tx,
            )
            .await;

        assert_eq!(dispatcher.connecting_peers[&target].generation, 2);
    }

    #[test]
    fn expired_write_budget_does_not_remove_live_connection() -> turmoil::Result {
        let mut simulation = Builder::new()
            .simulation_duration(Duration::from_secs(5))
            .build();

        simulation.host("server", || async {
            let listener = crate::net::TcpListener::bind("0.0.0.0:9000").await?;
            let (_healthy, _) = listener.accept().await?;
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(())
        });

        simulation.host("client", || async {
            let target = NodeId::new("peer");
            let (mut dispatcher, _raft_tx) = test_dispatcher(NodeId::new("local"));
            let address = (turmoil::lookup("server"), 9000);

            let healthy = TcpStream::connect(address).await?;
            let (_healthy_reader, healthy_writer) = healthy.into_split();
            let healthy_generation = dispatcher.allocate_generation();
            dispatcher.connections.insert(
                target.clone(),
                ActiveConnection {
                    generation: healthy_generation,
                    origin: ConnectionOrigin::Local,
                    writer: healthy_writer.into(),
                    reader_task: tokio::spawn(std::future::pending()),
                },
            );
            assert!(
                dispatcher
                    .write_messages_to_before(&target, &[], Instant::now())
                    .await
                    .is_err()
            );
            assert_eq!(
                dispatcher.connection_generation(&target),
                Some(healthy_generation)
            );
            assert!(dispatcher.existing_connection_wins(&target, ConnectionOrigin::Remote));

            let connection = dispatcher.connections.get_mut(&target).unwrap();
            connection.reader_task.abort();
            assert!(
                (&mut connection.reader_task)
                    .await
                    .unwrap_err()
                    .is_cancelled()
            );
            assert!(!dispatcher.existing_connection_wins(&target, ConnectionOrigin::Remote));

            Ok(())
        });

        simulation.run()
    }

    #[test]
    fn stalled_address_resolution_completes_with_timeout() -> turmoil::Result {
        let mut simulation = Builder::new()
            .simulation_duration(Duration::from_secs(35))
            .build();

        simulation.host("node", || async {
            let local_node_id = NodeId::new("local");
            let target = NodeId::new("stalled-peer");
            let (swim_tx, swim_mailbox) = SwimActor::channel(1);
            let _swim_mailbox = swim_mailbox;
            let result = OutboundClusterConnection::connect_raft(
                &target,
                &local_node_id,
                &swim_tx,
                &NodeTransportSecurity::TrustedDevelopment,
                snapshot_message(0, 0),
            )
            .await;

            let Err(error) = result else {
                panic!("stalled resolution must time out")
            };
            assert_eq!(error.to_string(), "connection attempt timed out");
            Ok(())
        });

        simulation.run()
    }
}
