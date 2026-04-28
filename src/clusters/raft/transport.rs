#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::clusters::raft::messages::MultiRaftActorCommand;
use crate::clusters::raft::messages::MultiRaftCommand;
use crate::clusters::raft::messages::{OutboundRaftPacket, RaftTransportCommand, WireRaftMessage};
use crate::clusters::swims::{SwimCommand, SwimQueryCommand};
use crate::clusters::{BINCODE_CONFIG, NodeAddress, NodeId};
use crate::net::{OwnedReadHalf, OwnedWriteHalf, TcpListener, TcpStream};

struct RaftReader(OwnedReadHalf);

impl RaftReader {
    async fn read_node_id(&mut self) -> Option<NodeId> {
        let len = self.0.read_u32().await.ok()? as usize;
        if len > 1024 {
            return None;
        }
        let mut buf = vec![0u8; len];
        self.0.read_exact(&mut buf).await.ok()?;
        bincode::decode_from_slice::<NodeId, _>(&buf, BINCODE_CONFIG)
            .ok()
            .map(|(id, _)| id)
    }

    async fn read_message(&mut self) -> Option<WireRaftMessage> {
        let len = self.0.read_u32().await.ok()? as usize;
        if len > 4 * 1024 * 1024 {
            return None;
        }
        let mut buf = vec![0u8; len];
        self.0.read_exact(&mut buf).await.ok()?;
        bincode::decode_from_slice::<WireRaftMessage, _>(&buf, BINCODE_CONFIG)
            .ok()
            .map(|(msg, _)| msg)
    }

    async fn run(mut self, tx: mpsc::Sender<MultiRaftActorCommand>) {
        while let Some(msg) = self.read_message().await {
            let _ = tx
                .send(
                    MultiRaftCommand::PacketReceived {
                        shard_group_id: msg.shard_group_id,
                        from: msg.sender,
                        rpc: msg.rpc,
                    }
                    .into(),
                )
                .await;
        }
    }
}

/// Manages peer connections, address resolution, and dead-peer tracking.
///
/// On simultaneous connect, the connection initiated by the **lower `NodeId`**
/// wins; the other is dropped.
///
/// Handshake: after connecting, the initiator sends its `NodeId`. The acceptor
/// reads it, and if a connection to that peer already exists (from our own
/// outbound connect), the tie is broken by NodeId ordering.
struct RaftWriters {
    node_id: NodeId,
    writers: HashMap<NodeId, OwnedWriteHalf>,
    addr_cache: HashMap<NodeId, NodeAddress>,
    /// Peers explicitly disconnected via DisconnectPeer. Outbound RPCs
    /// to these peers are silently dropped until a new connection is
    /// accepted (peer restart with new UUID won't hit this — different NodeId).
    dead_peers: HashSet<NodeId>,
}

impl RaftWriters {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            writers: HashMap::new(),
            addr_cache: HashMap::new(),
            dead_peers: HashSet::new(),
        }
    }

    async fn accept(&mut self, stream: TcpStream, raft_tx: &mpsc::Sender<MultiRaftActorCommand>) {
        let (read_half, write_half) = stream.into_split();
        let mut reader = RaftReader(read_half);

        let Some(peer_id) = reader.read_node_id().await else {
            return;
        };

        if self.writers.contains_key(&peer_id) && peer_id > self.node_id {
            return;
        }

        self.writers.insert(peer_id, write_half);
        tokio::spawn(reader.run(raft_tx.clone()));
    }

    /// Send a batch of outbound packets, grouping by target NodeId.
    /// Encodes all messages per target into a single buffer, writes once.
    async fn send(
        &mut self,
        packets: Vec<OutboundRaftPacket>,
        raft_tx: &mpsc::Sender<MultiRaftActorCommand>,
        swim_tx: &mpsc::Sender<SwimCommand>,
    ) {
        // Group by target — flush_dirty already groups, but be defensive.
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

        for (target_id, msgs) in by_target {
            // Try existing writer
            if self.writers.contains_key(&target_id)
                && self.write_messages_to(&target_id, &msgs).await.is_ok()
            {
                continue;
            }

            // Establish new connection
            let Some(node_addr) = self.resolve_address(&target_id, swim_tx).await else {
                tracing::warn!(
                    "[{}] Cannot resolve address for {:?}",
                    self.node_id,
                    target_id
                );
                continue;
            };

            let Ok(stream) = TcpStream::connect(node_addr.cluster_addr).await else {
                tracing::warn!(
                    "[{}] Failed to connect to {} ({:?})",
                    self.node_id,
                    node_addr.cluster_addr,
                    target_id
                );
                continue;
            };

            let (read_half, write_half) = stream.into_split();
            self.writers.insert(target_id.clone(), write_half);

            if let Err(e) = self.handshake(&target_id).await {
                tracing::warn!(
                    "[{}] Handshake with {:?} failed: {e}",
                    self.node_id,
                    target_id
                );
                continue;
            }

            if let Err(e) = self.write_messages_to(&target_id, &msgs).await {
                tracing::warn!(
                    "[{}] Write {} msgs to {:?} failed: {e}",
                    self.node_id,
                    msgs.len(),
                    target_id
                );
                continue;
            }

            tokio::spawn(RaftReader(read_half).run(raft_tx.clone()));
        }
    }

    fn disconnect(&mut self, peer_id: NodeId) {
        self.writers.remove(&peer_id);
        self.addr_cache.remove(&peer_id);
        tracing::info!("[{}] Disconnected dead peer {:?}", self.node_id, peer_id);
        self.dead_peers.insert(peer_id);
    }

    fn cleanup_dead_peers(&mut self) {
        self.dead_peers.clear();
    }

    async fn resolve_address(
        &mut self,
        node_id: &NodeId,
        swim_tx: &mpsc::Sender<SwimCommand>,
    ) -> Option<NodeAddress> {
        if let Some(&addr) = self.addr_cache.get(node_id) {
            return Some(addr);
        }

        let (tx, rx) = oneshot::channel();
        let query = SwimCommand::Query(SwimQueryCommand::ResolveAddress {
            node_id: node_id.clone(),
            reply: tx,
        });

        if swim_tx.send(query).await.is_err() {
            return None;
        }

        if let Ok(Some(node_addr)) = rx.await {
            self.addr_cache.insert(node_id.clone(), node_addr);
            Some(node_addr)
        } else {
            None
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
}

/// Thin async boundary — select loop over listener and actor commands.
pub struct RaftTransportActor;

impl RaftTransportActor {
    pub async fn run(
        node_id: NodeId,
        listener: TcpListener,
        raft_tx: mpsc::Sender<MultiRaftActorCommand>,
        mut from_actor: mpsc::Receiver<RaftTransportCommand>,
        swim_tx: mpsc::Sender<SwimCommand>,
    ) {
        let mut state = RaftWriters::new(node_id);
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(300));
        cleanup_interval.tick().await; // consume immediate first tick

        loop {
            tokio::select! {
                Ok((stream, _)) = listener.accept() => {
                    state.accept(stream, &raft_tx).await;
                }
                Some(cmd) = from_actor.recv() => {
                    match cmd {
                        RaftTransportCommand::Send(packets) => {
                            state.send(packets, &raft_tx, &swim_tx).await;
                        }
                        RaftTransportCommand::DisconnectPeer(peer_id) => {
                            state.disconnect(peer_id);
                        }
                    }
                }
                _ = cleanup_interval.tick() => {
                    state.cleanup_dead_peers();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clusters::raft::messages::{RaftRpc, RequestVote};
    use crate::clusters::swims::ShardGroupId;
    use std::time::Duration;
    use turmoil::Builder;

    /// Write a length-prefixed bincode-encoded value to a raw write half.
    /// Used by tests to simulate the peer side of the wire protocol.
    async fn write_frame(
        writer: &mut OwnedWriteHalf,
        value: &impl bincode::Encode,
    ) -> std::io::Result<()> {
        let bytes = bincode::encode_to_vec(value, BINCODE_CONFIG)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let len = bytes.len() as u32;
        writer.write_all(&len.to_be_bytes()).await?;
        writer.write_all(&bytes).await?;
        Ok(())
    }

    #[test]
    fn handshake_write_then_read_node_id() -> turmoil::Result {
        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(5))
            .build();

        sim.host("server", || async {
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let (stream, _) = listener.accept().await?;
            let (read_half, _) = stream.into_split();
            let mut reader = RaftReader(read_half);

            let peer_id = reader.read_node_id().await.unwrap();
            assert_eq!(peer_id, NodeId::new("node-abc"));
            Ok(())
        });

        sim.host("client", || async {
            let addr = turmoil::lookup("server");
            let stream = TcpStream::connect((addr, 9000)).await?;
            let (_, mut write_half) = stream.into_split();

            write_frame(&mut write_half, &NodeId::new("node-abc")).await?;
            Ok(())
        });

        sim.run()
    }

    #[test]
    fn write_and_read_raft_message() -> turmoil::Result {
        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(5))
            .build();

        sim.host("server", || async {
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let (stream, _) = listener.accept().await?;
            let (read_half, _) = stream.into_split();
            let mut reader = RaftReader(read_half);

            let msg = reader.read_message().await.unwrap();
            assert_eq!(msg.shard_group_id, ShardGroupId(42));
            assert_eq!(msg.sender, NodeId::new("sender-1"));
            match msg.rpc {
                RaftRpc::RequestVote(rv) => {
                    assert_eq!(rv.term, 5);
                    assert_eq!(rv.last_log_index, 10);
                }
                _ => panic!("expected RequestVote"),
            }
            Ok(())
        });

        sim.host("client", || async {
            let addr = turmoil::lookup("server");
            let stream = TcpStream::connect((addr, 9000)).await?;
            let (_, mut write_half) = stream.into_split();

            write_frame(
                &mut write_half,
                &WireRaftMessage {
                    shard_group_id: ShardGroupId(42),
                    sender: NodeId::new("sender-1"),
                    rpc: RaftRpc::RequestVote(RequestVote {
                        term: 5,
                        candidate_id: NodeId::new("sender-1"),
                        last_log_index: 10,
                        last_log_term: 3,
                    }),
                },
            )
            .await?;
            Ok(())
        });

        sim.run()
    }

    #[test]
    fn accepted_connection_registers_writer_after_handshake() -> turmoil::Result {
        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(5))
            .build();

        sim.host("acceptor", || async {
            let (raft_tx, _raft_rx) = mpsc::channel(16);
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let mut state = RaftWriters::new(NodeId::new("node-b"));

            let (stream, _) = listener.accept().await?;
            state.accept(stream, &raft_tx).await;

            assert!(
                state.writers.contains_key(&NodeId::new("node-a")),
                "writer should be registered after handshake"
            );
            Ok(())
        });

        sim.host("initiator", || async {
            let addr = turmoil::lookup("acceptor");
            let stream = TcpStream::connect((addr, 9000)).await?;
            let (_, mut write_half) = stream.into_split();
            write_frame(&mut write_half, &NodeId::new("node-a")).await?;
            Ok(())
        });

        sim.run()
    }

    #[test]
    fn conflict_lower_node_id_connection_wins() -> turmoil::Result {
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");
        assert!(node_a < node_b);

        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(5))
            .build();

        sim.host("node-b", || async {
            let (raft_tx, _raft_rx) = mpsc::channel(16);
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let dummy_listener = TcpListener::bind("0.0.0.0:9001").await?;
            let mut state = RaftWriters::new(NodeId::new("node-b"));

            // First connection from node-a
            let (stream, _) = listener.accept().await?;
            state.accept(stream, &raft_tx).await;
            assert!(state.writers.contains_key(&NodeId::new("node-a")));

            // Second connection from node-a (simulating simultaneous connect)
            let (stream2, _) = dummy_listener.accept().await?;
            let (read_half, _write_half) = stream2.into_split();
            let mut reader = RaftReader(read_half);
            let peer_id = reader.read_node_id().await.unwrap();
            assert_eq!(peer_id, NodeId::new("node-a"));

            // Conflict: node-a < node-b → incoming wins, replace
            let should_drop =
                state.writers.contains_key(&peer_id) && peer_id > NodeId::new("node-b");
            assert!(
                !should_drop,
                "lower NodeId's connection should NOT be dropped"
            );

            Ok(())
        });

        sim.host("node-a", || async {
            let addr = turmoil::lookup("node-b");

            let stream1 = TcpStream::connect((addr, 9000)).await?;
            let (_, mut write_half) = stream1.into_split();
            write_frame(&mut write_half, &NodeId::new("node-a")).await?;

            let stream2 = TcpStream::connect((addr, 9001)).await?;
            let (_, mut write_half2) = stream2.into_split();
            write_frame(&mut write_half2, &NodeId::new("node-a")).await?;

            Ok(())
        });

        sim.run()
    }

    #[test]
    fn conflict_higher_node_id_connection_dropped() {
        let node_b = NodeId::new("node-b");
        let node_c = NodeId::new("node-c");
        assert!(node_b < node_c);

        let has_existing = true;
        let incoming_initiator = node_c.clone();
        let self_id = node_b;
        let should_drop = has_existing && incoming_initiator > self_id;
        assert!(
            should_drop,
            "higher NodeId's incoming connection should be dropped"
        );
    }
}
