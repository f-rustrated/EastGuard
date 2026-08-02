use std::collections::{BTreeMap, HashMap, HashSet};

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::control_plane::consensus::actor::MutlRaftSender;

use crate::control_plane::consensus::messages::{
    InboundRaftRpc, OutboundRaftPacket, WireRaftMessage,
};

use crate::control_plane::NodeId;
use crate::control_plane::consensus::transport::{AcceptedRaftConnection, ClusterMessageReader};
use crate::control_plane::membership::actor::SwimSender;
use crate::net::{TransportTcpStream, TransportWriteHalf};
use crate::security::{AdmissionProof, CertificatePrincipal};

use super::protocol::{AdmissionRequest, ClusterRequest, InitialClusterMessage, encode_frame};
use crate::security::SecurityHandle;

const CONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);
const ADMISSION_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);
/// Upper bound on messages buffered per peer while its connection is in flight;
/// overflow is dropped (raft retries by timer).
const CONNECTING_PEER_BUFFER_CAP: usize = 256;

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
    /// background task; flushed (or dropped on failure) in `on_connection_result`.
    connecting_peers: HashMap<NodeId, Vec<WireRaftMessage>>,
    connection_result_tx: mpsc::Sender<ConnectionAttemptResult>,
    security: SecurityHandle,
}

/// Result of a background connection attempt, delivered to the transport loop.
pub(super) struct ConnectionAttemptResult {
    target: NodeId,
    result: anyhow::Result<(ClusterMessageReader, TransportWriteHalf)>,
}

impl RaftRpcDispatcher {
    pub(super) fn new(
        node_id: NodeId,
        connection_result_tx: mpsc::Sender<ConnectionAttemptResult>,
        security: SecurityHandle,
    ) -> Self {
        Self {
            node_id,
            writers: HashMap::new(),
            dead_peers: HashSet::new(),
            connect_backoffs: HashMap::new(),
            connecting_peers: HashMap::new(),
            connection_result_tx,
            security,
        }
    }

    pub(super) fn accept(&mut self, connection: AcceptedRaftConnection, raft_tx: &MutlRaftSender) {
        let AcceptedRaftConnection {
            peer_id: connection_peer_id,
            initial_message,
            reader,
            writer,
        } = connection;

        let initial_rpc = InboundRaftRpc {
            shard_group_id: initial_message.shard_group_id,
            peer_id: connection_peer_id,
            rpc: initial_message.rpc,
        };
        if self.writers.contains_key(&initial_rpc.peer_id) && initial_rpc.peer_id > self.node_id {
            // simultaneous connect: dropping accepted connection
            return;
        }
        self.writers.insert(initial_rpc.peer_id.clone(), writer);
        let raft_tx = raft_tx.clone();
        tokio::spawn(async move {
            let reader_peer_id = initial_rpc.peer_id.clone();
            let _ = raft_tx.send(initial_rpc).await;
            reader.run(raft_tx, reader_peer_id).await;
        });
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
                    peer_id: self.node_id.clone(),
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
        // No usable writer: hand the messages to the in-flight connection (if
        // any) or start one on a background task. Connection attempts must
        // never run inline: a hung connect blocks this select loop for the full
        // timeout, stalling every queued batch and the accept arm with it (#133).
        if let Some(buffered) = self.connecting_peers.get_mut(&target_id) {
            if buffered.len() + msgs.len() <= CONNECTING_PEER_BUFFER_CAP {
                buffered.extend(msgs);
            }
            return;
        }
        let initial_raft_message = msgs.remove(0);
        self.connecting_peers.insert(target_id.clone(), msgs);

        tokio::spawn(connect_peer(
            target_id,
            swim_tx.clone(),
            self.security.clone(),
            initial_raft_message,
            self.connection_result_tx.clone(),
        ));
    }

    /// Installs (or discards, per the NodeId tie-break) a completed connection
    /// and flushes any messages buffered while it was in flight.
    pub(super) async fn on_connection_result(
        &mut self,
        attempt: ConnectionAttemptResult,
        raft_tx: &MutlRaftSender,
    ) {
        let ConnectionAttemptResult { target, result } = attempt;
        let buffered = self.connecting_peers.remove(&target).unwrap_or_default();

        let Ok((reader, write_half)) = result.inspect_err(|err| {
            tracing::warn!(peer = %target, "connection attempt failed: {err}");
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
                "simultaneous connect: discarding our connection, peer's connection \
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
        self.connecting_peers.remove(&peer_id);
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

/// Connects, performs the opening Raft exchange, and reports the result.
async fn connect_peer(
    target_id: NodeId,
    swim_tx: SwimSender,
    security: SecurityHandle,
    initial_raft_message: WireRaftMessage,
    connection_result_tx: mpsc::Sender<ConnectionAttemptResult>,
) {
    let result = async {
        let Some(addr) = swim_tx.resolve_address(target_id.clone()).await? else {
            anyhow::bail!("cannot resolve address for {target_id}");
        };

        let stream = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            TransportTcpStream::connect_node(addr.cluster_addr(), security.node_transport()),
        )
        .await??;

        let mut connection = OutboundClusterConnection::new(stream, target_id.clone())?;
        tokio::time::timeout(
            ADMISSION_HANDSHAKE_TIMEOUT,
            connection.send_initial_request(&security, ClusterRequest::Raft(initial_raft_message)),
        )
        .await??;
        Ok(connection.into_parts())
    }
    .await;

    let _ = connection_result_tx
        .send(ConnectionAttemptResult {
            target: target_id,
            result,
        })
        .await;
}

/// One outbound cluster stream before its first Raft or ACL message.
///
/// It owns the expected peer identity and both stream halves so callers cannot
/// accidentally perform only one side of the mutual admission exchange.
pub(crate) struct OutboundClusterConnection {
    pub(crate) reader: ClusterMessageReader,
    writer: TransportWriteHalf,
    expected_peer_id: NodeId,
    /// Present only for a TLS stream; trusted-development streams skip
    /// admission entirely.
    tls_peer: Option<(CertificatePrincipal, [u8; 32])>,
}

impl OutboundClusterConnection {
    pub(crate) fn new(
        stream: TransportTcpStream,
        expected_peer_id: NodeId,
    ) -> anyhow::Result<Self> {
        let certificate_principal = stream.peer_principal();
        let tls_peer = match &certificate_principal {
            Some(principal) => Some((principal.clone(), stream.admission_binding()?)),
            None => None,
        };

        let (read_half, writer) = stream.into_split();
        Ok(Self {
            reader: ClusterMessageReader::new(read_half, certificate_principal),
            writer,
            expected_peer_id,
            tls_peer,
        })
    }

    pub(crate) async fn send_initial_request(
        &mut self,
        security: &SecurityHandle,
        request: ClusterRequest,
    ) -> anyhow::Result<()> {
        let Some((peer_principal, tls_session_binding)) = self.tls_peer.as_ref() else {
            let initial = InitialClusterMessage::Request(request);
            self.writer.write_all(&encode_frame(&initial)?).await?;
            return Ok(());
        };
        let local_proof = security.create_admission_proof(tls_session_binding)?;
        let process_admission = InitialClusterMessage::ProcessAdmission(AdmissionRequest {
            proof: local_proof,
            request,
        });
        self.writer
            .write_all(&encode_frame(&process_admission)?)
            .await?;
        let peer_admission = security.lookup_admission(peer_principal).await?;
        let peer_proof = self
            .reader
            .read_frame::<AdmissionProof>(4 * 1024, "admission proof")
            .await?;
        let admitted_peer =
            peer_proof.verify_admission(&peer_admission, peer_principal, tls_session_binding)?;
        anyhow::ensure!(
            admitted_peer == self.expected_peer_id,
            "connected broker differs from the expected admitted process"
        );
        Ok(())
    }

    fn into_parts(self) -> (ClusterMessageReader, TransportWriteHalf) {
        (self.reader, self.writer)
    }
}
