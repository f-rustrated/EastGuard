use std::collections::{BTreeMap, HashMap, HashSet};

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::control_plane::consensus::actor::MutlRaftSender;

use crate::control_plane::consensus::messages::{
    InboundRaftRpc, OutboundRaftPacket, WireRaftMessage,
};

use crate::control_plane::NodeId;
use crate::control_plane::consensus::transport::ClusterMessageReader;
use crate::control_plane::membership::actor::SwimSender;
use crate::net::{TransportTcpStream, TransportWriteHalf};
use crate::security::NodeTransportSecurity;

use super::protocol::{
    AclSnapshotRequest, AclSnapshotResponse, InitialClusterMessage, encode_frame,
};

const CONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);
/// Upper bound on messages buffered per peer while its dial is in flight;
/// overflow is dropped (raft retries by timer).
const PENDING_DIAL_BUFFER_CAP: usize = 256;

/// Manages peer connections, address resolution, and dead-peer tracking.
///
/// On simultaneous connect, the connection initiated by the **lower `NodeId`**
/// wins; the other is dropped.
///
/// A connection begins with its first Raft message, which identifies the peer.
/// The acceptor uses that sender to key the writer slot and resolve simultaneous
/// connection conflicts.
pub(super) struct RaftRpcDispatcher {
    node_id: NodeId,
    writers: HashMap<NodeId, TransportWriteHalf>,
    /// Peers explicitly disconnected via DisconnectPeer. Outbound RPCs
    /// to these peers are silently dropped until a new connection is
    /// accepted (peer restart with new UUID won't hit this — different NodeId).
    dead_peers: HashSet<NodeId>,
    /// Tracks when the last connect attempt to a peer failed. Prevents
    /// rapid reconnect storms to dead/unreachable peers that would block
    /// the transport's select loop and stall flush_events in MultiRaftActor.
    connect_backoffs: HashMap<NodeId, Instant>,
    /// Messages buffered for peers whose connection is being established on a
    /// background task; flushed (or dropped on failure) in `on_dial_result`.
    pending_dials: HashMap<NodeId, Vec<WireRaftMessage>>,
    dial_tx: mpsc::Sender<DialOutcome>,
    security: NodeTransportSecurity,
}

/// Result of a background dial attempt, delivered back to the transport loop.
pub(super) struct DialOutcome {
    target: NodeId,
    outcome: anyhow::Result<(ClusterMessageReader, TransportWriteHalf)>,
}

impl RaftRpcDispatcher {
    pub(super) fn new(
        node_id: NodeId,
        dial_tx: mpsc::Sender<DialOutcome>,
        security: NodeTransportSecurity,
    ) -> Self {
        Self {
            node_id,
            writers: HashMap::new(),
            dead_peers: HashSet::new(),
            connect_backoffs: HashMap::new(),
            pending_dials: HashMap::new(),
            dial_tx,
            security,
        }
    }

    pub(super) async fn accept(&mut self, stream: TransportTcpStream, raft_tx: &MutlRaftSender) {
        let transport_identity = stream.peer_identity();
        let (read_half, write_half) = stream.into_split();
        let mut reader = ClusterMessageReader::new(read_half, transport_identity);

        let Ok(initial_message) = reader.read_initial_message().await else {
            tracing::debug!("cluster connection closed before its initial message");
            return;
        };

        match initial_message {
            InitialClusterMessage::AclSnapshot(request) => {
                let peer_id = request.requester_node_id.clone();
                tokio::spawn(serve_acl_snapshot_request(
                    peer_id,
                    request,
                    raft_tx.clone(),
                    write_half,
                ));
            }
            InitialClusterMessage::Raft(initial_raft_message) => {
                let initial_rpc = InboundRaftRpc {
                    shard_group_id: initial_raft_message.shard_group_id,
                    peer_id: initial_raft_message.sender,
                    rpc: initial_raft_message.rpc,
                };
                if self.writers.contains_key(&initial_rpc.peer_id)
                    && initial_rpc.peer_id > self.node_id
                {
                    // simultaneous connect: dropping accepted connection
                    return;
                }
                self.writers.insert(initial_rpc.peer_id.clone(), write_half);
                let raft_tx = raft_tx.clone();
                tokio::spawn(async move {
                    let peer_id = initial_rpc.peer_id.clone();
                    let _ = raft_tx.send(initial_rpc).await;
                    reader.run(raft_tx, peer_id).await;
                });
            }
        }
    }

    pub(super) async fn send(&mut self, packets: Vec<OutboundRaftPacket>, swim_tx: &SwimSender) {
        for (target_id, msgs) in self.group_packets(packets) {
            self.send_to_target(target_id, msgs, swim_tx).await;
        }
    }

    fn group_packets(
        &self,
        packets: Vec<OutboundRaftPacket>,
    ) -> BTreeMap<NodeId, Vec<WireRaftMessage>> {
        let mut by_target: BTreeMap<NodeId, Vec<WireRaftMessage>> = BTreeMap::new();
        for pkt in packets {
            if self.dead_peers.contains(&pkt.target) {
                continue;
            }
            by_target
                .entry(pkt.target)
                .or_default()
                .push(WireRaftMessage {
                    shard_group_id: pkt.shard_group_id,
                    sender: self.node_id.clone(),
                    rpc: pkt.rpc,
                });
        }
        by_target
    }

    async fn send_to_target(
        &mut self,
        target_id: NodeId,
        mut msgs: Vec<WireRaftMessage>,
        swim_tx: &SwimSender,
    ) {
        if let Some(&failed_at) = self.connect_backoffs.get(&target_id) {
            if failed_at.elapsed() < CONNECT_BACKOFF {
                return;
            }
            self.connect_backoffs.remove(&target_id);
        }
        if self.writers.contains_key(&target_id)
            && self.write_messages_to(&target_id, &msgs).await.is_ok()
        {
            return;
        }
        // No usable writer: hand the messages to the in-flight dial (if any)
        // or start one on a background task. Dials must never run inline — a
        // hung connect (crashed peer; acceptor starved because *its* loop is
        // mid-dial) blocks this select loop for the full connect timeout,
        // stalling every queued batch and the accept arm with it (#133).
        if let Some(buffered) = self.pending_dials.get_mut(&target_id) {
            if buffered.len() + msgs.len() <= PENDING_DIAL_BUFFER_CAP {
                buffered.extend(msgs);
            }
            return;
        }
        let initial_raft_message = msgs.remove(0);
        self.pending_dials.insert(target_id.clone(), msgs);
        let dial_task = dial(
            target_id.clone(),
            swim_tx.clone(),
            self.security.clone(),
            initial_raft_message,
        );

        let dial_tx = self.dial_tx.clone();
        tokio::spawn(async move {
            let outcome = dial_task.await;
            let _ = dial_tx
                .send(DialOutcome {
                    target: target_id,
                    outcome,
                })
                .await;
        });
    }

    /// Installs (or discards, per the NodeId tie-break) a completed dial and
    /// flushes any messages buffered while it was in flight.
    pub(super) async fn on_dial_result(&mut self, result: DialOutcome, raft_tx: &MutlRaftSender) {
        let DialOutcome { target, outcome } = result;
        let buffered = self.pending_dials.remove(&target).unwrap_or_default();

        let Ok((reader, write_half)) = outcome.inspect_err(|err| {
            tracing::warn!(peer = %target, "dial failed: {err}");
        }) else {
            self.connect_backoffs.insert(target, Instant::now());
            return;
        };

        if self.dead_peers.contains(&target) {
            return;
        }
        // Mirror `accept`'s tie-break: the connection initiated by
        // the lower NodeId wins, and we initiated this one. Losing the
        // tie-break must not lose the buffered messages — deliver them
        // over the surviving (accepted) connection instead;
        if self.writers.contains_key(&target) && self.node_id > target {
            tracing::debug!(
                peer = %target,
                buffered = buffered.len(),
                "simultaneous connect: discarding our dial, peer's connection \
                 wins the tie-break (lower NodeId); rerouting buffered messages",
            );
            if !buffered.is_empty() {
                let _ = self.write_messages_to(&target, &buffered).await;
            }
            return;
        }
        self.writers.insert(target.clone(), write_half);
        tokio::spawn(reader.run(raft_tx.clone(), target.clone()));

        if !buffered.is_empty() {
            let _ = self.write_messages_to(&target, &buffered).await;
        }
    }

    pub(super) fn disconnect(&mut self, peer_id: NodeId) {
        self.writers.remove(&peer_id);
        self.pending_dials.remove(&peer_id);
        tracing::info!("[{}] Disconnected dead peer {:?}", self.node_id, peer_id);
        self.dead_peers.insert(peer_id);
    }

    pub(super) fn cleanup_dead_peers(&mut self) {
        self.dead_peers.clear();
        self.connect_backoffs.clear();
    }

    // --- Wire helpers ---
    // On error, writer is removed from the map so subsequent calls reconnect.
    /// Encode all messages into a single buffer, write to target's connection.
    async fn write_messages_to(
        &mut self,
        target: &NodeId,
        msgs: &[WireRaftMessage],
    ) -> std::io::Result<()> {
        let writer = self.writers.get_mut(target).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "no writer for target")
        })?;
        let mut buf = Vec::new();
        for msg in msgs {
            let frame = encode_frame(msg).map_err(std::io::Error::other)?;
            buf.extend_from_slice(&frame);
        }
        let result = writer.write_all(&buf).await;
        if result.is_err() {
            self.writers.remove(target);
        }
        result
    }

    #[cfg(test)]
    pub fn contains(&self, node_id: &NodeId) -> bool {
        self.writers.contains_key(node_id)
    }
}

/// Resolve, connect (3secs cap), and send the opening Raft message on a spawned
/// task, so a hung connect can never block the transport select loop. The loop
/// installs the writer and flushes buffered messages in `on_dial_result`.
// ! never inline this. Actor Model should only do work whose duration it controls.
// ! Anything whose latency the outside actor controls must not be awaited in the handler.
async fn dial(
    target_id: NodeId,
    swim_tx: SwimSender,
    security: NodeTransportSecurity,
    initial_raft_message: WireRaftMessage,
) -> anyhow::Result<(ClusterMessageReader, TransportWriteHalf)> {
    let Some(addr) = swim_tx.resolve_address(target_id.clone()).await? else {
        anyhow::bail!("cannot resolve address for {target_id}");
    };

    let stream = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        TransportTcpStream::connect_node(addr.cluster_addr(), &security),
    )
    .await??;

    let transport_identity = stream.peer_identity();
    let (read_half, mut write_half) = stream.into_split();
    write_half
        .write_all(&encode_frame(&InitialClusterMessage::Raft(
            initial_raft_message,
        ))?)
        .await?;
    Ok((
        ClusterMessageReader::new(read_half, transport_identity),
        write_half,
    ))
}

async fn serve_acl_snapshot_request(
    peer_id: NodeId,
    request: AclSnapshotRequest,
    raft_tx: MutlRaftSender,
    mut writer: TransportWriteHalf,
) {
    let snapshot = raft_tx
        .get_acl_snapshot(request.shard_group_id, request.resource)
        .await;
    let Ok(frame) = encode_frame(&AclSnapshotResponse { snapshot }) else {
        tracing::debug!(peer = %peer_id, "failed to encode ACL snapshot response");
        return;
    };
    if let Err(error) = writer.write_all(&frame).await {
        tracing::debug!(peer = %peer_id, "failed to send ACL snapshot response: {error}");
    }
}
