use std::collections::{HashMap, HashSet};

use tokio::io::AsyncWriteExt;
use tokio::time::Instant;

use crate::control_plane::consensus::actor::MutlRaftSender;

use crate::control_plane::consensus::messages::{OutboundRaftPacket, WireRaftMessage};

use crate::control_plane::consensus::transport::RaftRpcListener;
use crate::control_plane::membership::actor::SwimSender;
use crate::control_plane::{BINCODE_CONFIG, NodeAddress, NodeId};
use crate::net::{OwnedWriteHalf, TcpStream};

const CONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

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
}

impl RaftRpcDispatcher {
    pub(super) fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            writers: HashMap::new(),
            dead_peers: HashSet::new(),
            connect_backoffs: HashMap::new(),
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

    pub(super) async fn send(
        &mut self,
        packets: Vec<OutboundRaftPacket>,
        raft_tx: &MutlRaftSender,
        swim_tx: &SwimSender,
    ) {
        let by_target = self.group_packets(packets);
        for (target_id, msgs) in by_target {
            self.send_to_target(target_id, msgs, raft_tx, swim_tx).await;
        }
    }

    fn group_packets(
        &self,
        packets: Vec<OutboundRaftPacket>,
    ) -> HashMap<NodeId, Vec<WireRaftMessage>> {
        let mut by_target: HashMap<NodeId, Vec<WireRaftMessage>> = HashMap::new();
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
        raft_tx: &MutlRaftSender,
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
        if !self
            .connect_and_send(target_id.clone(), msgs, raft_tx, swim_tx)
            .await
        {
            self.connect_backoffs.insert(target_id, Instant::now());
        }
    }

    async fn connect_and_send(
        &mut self,
        target_id: NodeId,
        msgs: Vec<WireRaftMessage>,
        raft_tx: &MutlRaftSender,
        swim_tx: &SwimSender,
    ) -> bool {
        let Some(node_addr) = self.resolve_address(&target_id, swim_tx).await else {
            tracing::warn!(
                "[{}] Cannot resolve address for {:?}",
                self.node_id,
                target_id
            );
            return false;
        };
        // Timeout required: in turmoil (and on dead hosts generally), TcpStream::connect
        // never returns an error — it hangs forever, stalling the select loop.
        let Some(stream) = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            TcpStream::connect(node_addr.cluster_addr),
        )
        .await
        .ok()
        .and_then(|r| r.ok()) else {
            tracing::warn!(
                "[{}] Failed to connect to {} ({:?})",
                self.node_id,
                node_addr.cluster_addr,
                target_id
            );
            return false;
        };
        let (read_half, write_half) = stream.into_split();
        self.writers.insert(target_id.clone(), write_half);
        if let Err(e) = self.handshake(&target_id).await {
            tracing::warn!(
                "[{}] Handshake with {:?} failed: {e}",
                self.node_id,
                target_id
            );
            return false;
        }
        if let Err(e) = self.write_messages_to(&target_id, &msgs).await {
            tracing::warn!(
                "[{}] Write {} msgs to {:?} failed: {e}",
                self.node_id,
                msgs.len(),
                target_id
            );
            return false;
        }
        tokio::spawn(RaftRpcListener(read_half).run(raft_tx.clone()));
        true
    }

    pub(super) fn disconnect(&mut self, peer_id: NodeId) {
        self.writers.remove(&peer_id);
        tracing::info!("[{}] Disconnected dead peer {:?}", self.node_id, peer_id);
        self.dead_peers.insert(peer_id);
    }

    pub(super) fn cleanup_dead_peers(&mut self) {
        self.dead_peers.clear();
        self.connect_backoffs.clear();
    }

    async fn resolve_address(&self, node_id: &NodeId, swim_tx: &SwimSender) -> Option<NodeAddress> {
        match swim_tx.resolve_address(node_id.clone()).await {
            Ok(addr) => addr,
            Err(e) => {
                tracing::debug!("Resolve address for {:?} failed: {e}", node_id);
                None
            }
        }
    }

    // --- Wire helpers ---
    // On error, writer is removed from the map so subsequent calls reconnect.

    /// Send handshake (our NodeId) to a writer already in the map.
    async fn handshake(&mut self, target: &NodeId) -> std::io::Result<()> {
        let writer = self.writers.get_mut(target).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "no writer for target")
        })?;
        let bytes = bincode::encode_to_vec(&self.node_id, BINCODE_CONFIG)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let len = bytes.len() as u32;
        let result = async {
            writer.write_all(&len.to_be_bytes()).await?;
            writer.write_all(&bytes).await
        }
        .await;

        if result.is_err() {
            self.writers.remove(target);
        }
        result
    }

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
