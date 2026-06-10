use std::collections::{BTreeMap, HashMap, HashSet};

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::control_plane::consensus::actor::MutlRaftSender;

use crate::control_plane::consensus::messages::{OutboundRaftPacket, WireRaftMessage};

use crate::control_plane::consensus::transport::RaftRpcListener;
use crate::control_plane::membership::actor::SwimSender;
use crate::control_plane::{BINCODE_CONFIG, NodeId};
use crate::net::{OwnedWriteHalf, TcpStream};

const CONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);
/// Upper bound on messages buffered per peer while its dial is in flight;
/// overflow is dropped (raft retries by timer).
const PENDING_DIAL_BUFFER_CAP: usize = 256;

/// Manages peer connections, address resolution, and dead-peer tracking.
///
/// On simultaneous connect, the connection initiated by the **lower `NodeId`**
/// wins; the other is dropped.
///
/// Handshake: after connecting, the initiator sends its `NodeId`. The acceptor
/// reads it, and if a connection to that peer already exists (from our own
/// outbound connect), the tie is broken by NodeId ordering.
pub(super) struct RaftRpcDispatcher {
    node_id: NodeId,
    writers: HashMap<NodeId, OwnedWriteHalf>,
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
}

/// Result of a background dial attempt, delivered back to the transport loop.
pub(super) struct DialOutcome {
    target: NodeId,
    outcome: anyhow::Result<(RaftRpcListener, OwnedWriteHalf)>,
}

impl RaftRpcDispatcher {
    pub(super) fn new(node_id: NodeId, dial_tx: mpsc::Sender<DialOutcome>) -> Self {
        Self {
            node_id,
            writers: HashMap::new(),
            dead_peers: HashSet::new(),
            connect_backoffs: HashMap::new(),
            pending_dials: HashMap::new(),
            dial_tx,
        }
    }

    pub(super) async fn accept(&mut self, stream: TcpStream, raft_tx: &MutlRaftSender) {
        let (read_half, write_half) = stream.into_split();
        let mut reader = RaftRpcListener(read_half);

        let Ok(peer_id) = reader.read_node_id().await else {
            tracing::error!("Failed to read peer NodeId during accept");
            return;
        };

        if self.writers.contains_key(&peer_id) && peer_id > self.node_id {
            return;
        }

        self.writers.insert(peer_id, write_half);
        tokio::spawn(reader.run(raft_tx.clone()));
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
        msgs: Vec<WireRaftMessage>,
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
        self.pending_dials.insert(target_id.clone(), msgs);
        let dial_task = dial(self.node_id.clone(), target_id.clone(), swim_tx.clone());
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
            if !buffered.is_empty() {
                let _ = self.write_messages_to(&target, &buffered).await;
            }
            return;
        }
        self.writers.insert(target.clone(), write_half);
        tokio::spawn(reader.run(raft_tx.clone()));

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
            let bytes = bincode::encode_to_vec(msg, BINCODE_CONFIG)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let len = bytes.len() as u32;
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(&bytes);
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

/// Resolve, connect (3s cap), and handshake — on a spawned task, so a hung
/// connect can never block the transport select loop. The loop
/// installs the writer and flushes buffered messages in `on_dial_result`.
// ! never inline this. Actor Model should onkly do work whose duration it controls.
// ! Anything whose latency the outside actor controls must not be awaited in the handler.
async fn dial(
    node_id: NodeId,
    target_id: NodeId,
    swim_tx: SwimSender,
) -> anyhow::Result<(RaftRpcListener, OwnedWriteHalf)> {
    let Some(addr) = swim_tx.resolve_address(target_id.clone()).await? else {
        anyhow::bail!("[{}] Cannot resolve address for {:?}", node_id, target_id);
    };

    let stream = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        TcpStream::connect(addr.cluster_addr()),
    )
    .await??;

    let (read_half, mut write_half) = stream.into_split();
    let bytes = bincode::encode_to_vec(&node_id, BINCODE_CONFIG)
        .map_err(|e| anyhow::anyhow!("[{}] Handshake encode failed: {e}", node_id))?;
    let len = bytes.len() as u32;
    write_half.write_all(&len.to_be_bytes()).await?;
    write_half.write_all(&bytes).await?;
    Ok((RaftRpcListener(read_half), write_half))
}
