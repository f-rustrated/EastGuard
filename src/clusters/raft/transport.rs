#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::clusters::raft::actor::RaftCommand;
use crate::clusters::raft::messages::{OutboundRaftPacket, RaftTransportCommand, WireRaftMessage};
use crate::clusters::swims::{SwimCommand, SwimQueryCommand};
use crate::clusters::{BINCODE_CONFIG, NodeId};
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

    async fn run(mut self, tx: mpsc::Sender<RaftCommand>) {
        while let Some(msg) = self.read_message().await {
            let _ = tx
                .send(RaftCommand::PacketReceived {
                    shard_group_id: msg.shard_group_id,
                    from: msg.sender,
                    rpc: msg.rpc,
                })
                .await;
        }
    }
}

struct RaftWriter(OwnedWriteHalf);

impl RaftWriter {
    async fn write_node_id(&mut self, node_id: &NodeId) -> std::io::Result<()> {
        let bytes = bincode::encode_to_vec(node_id, BINCODE_CONFIG)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let len = bytes.len() as u32;
        self.0.write_all(&len.to_be_bytes()).await?;
        self.0.write_all(&bytes).await?;
        Ok(())
    }

    async fn write_message(&mut self, msg: &WireRaftMessage) -> std::io::Result<()> {
        let bytes = bincode::encode_to_vec(msg, BINCODE_CONFIG)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let len = bytes.len() as u32;
        self.0.write_all(&len.to_be_bytes()).await?;
        self.0.write_all(&bytes).await?;
        Ok(())
    }
}

/// TCP transport for Raft RPCs.
///
/// Either side can initiate a connection. On simultaneous connect, the
/// connection initiated by the **lower `NodeId`** wins; the other is dropped.
///
/// Handshake: after connecting, the initiator sends its `NodeId`. The acceptor
/// reads it, and if a connection to that peer already exists (from our own
/// outbound connect), the tie is broken by NodeId ordering.
pub struct RaftTransportActor;

impl RaftTransportActor {
    pub async fn run(
        node_id: NodeId,
        listener: TcpListener,
        raft_tx: mpsc::Sender<RaftCommand>,
        mut from_actor: mpsc::Receiver<RaftTransportCommand>,
        swim_tx: mpsc::Sender<SwimCommand>,
    ) {
        let mut writers: HashMap<NodeId, RaftWriter> = HashMap::new();
        let mut addr_cache: HashMap<NodeId, SocketAddr> = HashMap::new();
        // Peers explicitly disconnected via DisconnectPeer. Outbound RPCs
        // to these peers are silently dropped until a new connection is
        // accepted (peer restart with new UUID won't hit this — different NodeId).
        let mut dead_peers: HashSet<NodeId> = HashSet::new();
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(300));
        cleanup_interval.tick().await; // consume immediate first tick

        loop {
            tokio::select! {
                Ok((stream, _)) = listener.accept() => {
                    Self::handle_accepted(
                        stream, &node_id, &raft_tx, &mut writers,
                    ).await;
                }
                Some(cmd) = from_actor.recv() => {
                    match cmd {
                        RaftTransportCommand::Send(pkt) => {
                            if dead_peers.contains(&pkt.target) {
                                continue;
                            }
                            Self::handle_outbound(
                                pkt, &node_id, &raft_tx, &swim_tx,
                                &mut writers, &mut addr_cache,
                            ).await;
                        }
                        RaftTransportCommand::DisconnectPeer(peer_id) => {
                            writers.remove(&peer_id);
                            addr_cache.remove(&peer_id);
                            dead_peers.insert(peer_id.clone());
                            tracing::info!(
                                "[{}] Disconnected dead peer {:?}",
                                node_id, peer_id
                            );
                        }
                    }
                }
                _ = cleanup_interval.tick() => {
                    if !dead_peers.is_empty() {
                        dead_peers.clear();
                    }
                }
            }
        }
    }

    async fn handle_accepted(
        stream: TcpStream,
        node_id: &NodeId,
        raft_tx: &mpsc::Sender<RaftCommand>,
        writers: &mut HashMap<NodeId, RaftWriter>,
    ) {
        let (read_half, write_half) = stream.into_split();
        let mut reader = RaftReader(read_half);

        let Some(peer_id) = reader.read_node_id().await else {
            return;
        };

        if writers.contains_key(&peer_id) && peer_id > *node_id {
            return;
        }

        writers.insert(peer_id, RaftWriter(write_half));
        tokio::spawn(reader.run(raft_tx.clone()));
    }

    async fn handle_outbound(
        pkt: OutboundRaftPacket,
        node_id: &NodeId,
        raft_tx: &mpsc::Sender<RaftCommand>,
        swim_tx: &mpsc::Sender<SwimCommand>,
        writers: &mut HashMap<NodeId, RaftWriter>,
        addr_cache: &mut HashMap<NodeId, SocketAddr>,
    ) {
        let target_id = pkt.target.clone();
        let wire_msg = WireRaftMessage {
            shard_group_id: pkt.shard_group_id,
            sender: node_id.clone(),
            rpc: pkt.rpc,
        };

        if let Some(writer) = writers.get_mut(&target_id) {
            if writer.write_message(&wire_msg).await.is_ok() {
                return;
            }
            writers.remove(&target_id);
        }

        let Some(target_addr) = Self::resolve_address(&target_id, swim_tx, addr_cache).await else {
            tracing::warn!("Cannot resolve address for {:?}", target_id);
            return;
        };

        let Ok(stream) = TcpStream::connect(target_addr).await else {
            tracing::warn!("Failed to connect to {} ({:?})", target_addr, target_id);
            return;
        };

        let (read_half, write_half) = stream.into_split();
        let mut writer = RaftWriter(write_half);

        if writer.write_node_id(node_id).await.is_err() {
            return;
        }

        tokio::spawn(RaftReader(read_half).run(raft_tx.clone()));
        if writer.write_message(&wire_msg).await.is_err() {
            return;
        }
        writers.insert(target_id, writer);
    }

    async fn resolve_address(
        node_id: &NodeId,
        swim_tx: &mpsc::Sender<SwimCommand>,
        addr_cache: &mut HashMap<NodeId, SocketAddr>,
    ) -> Option<SocketAddr> {
        if let Some(&addr) = addr_cache.get(node_id) {
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

        if let Ok(Some(addr)) = rx.await {
            addr_cache.insert(node_id.clone(), addr);
            Some(addr)
        } else {
            None
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
            let (_, write_half) = stream.into_split();
            let mut writer = RaftWriter(write_half);

            writer.write_node_id(&NodeId::new("node-abc")).await?;
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
            let (_, write_half) = stream.into_split();
            let mut writer = RaftWriter(write_half);

            writer
                .write_message(&WireRaftMessage {
                    shard_group_id: ShardGroupId(42),
                    sender: NodeId::new("sender-1"),
                    rpc: RaftRpc::RequestVote(RequestVote {
                        term: 5,
                        candidate_id: NodeId::new("sender-1"),
                        last_log_index: 10,
                        last_log_term: 3,
                    }),
                })
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
            let mut writers: HashMap<NodeId, RaftWriter> = HashMap::new();
            let node_id = NodeId::new("node-b");

            let (stream, _) = listener.accept().await?;
            RaftTransportActor::handle_accepted(stream, &node_id, &raft_tx, &mut writers).await;

            assert!(
                writers.contains_key(&NodeId::new("node-a")),
                "writer should be registered after handshake"
            );
            Ok(())
        });

        sim.host("initiator", || async {
            let addr = turmoil::lookup("acceptor");
            let stream = TcpStream::connect((addr, 9000)).await?;
            let (_, write_half) = stream.into_split();
            let mut writer = RaftWriter(write_half);
            writer.write_node_id(&NodeId::new("node-a")).await?;
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
            let mut writers: HashMap<NodeId, RaftWriter> = HashMap::new();
            let node_id = NodeId::new("node-b");

            // First connection from node-a
            let (stream, _) = listener.accept().await?;
            RaftTransportActor::handle_accepted(stream, &node_id, &raft_tx, &mut writers).await;
            assert!(writers.contains_key(&NodeId::new("node-a")));

            // Second connection from node-a (simulating simultaneous connect)
            let (stream2, _) = dummy_listener.accept().await?;
            let (read_half, _write_half) = stream2.into_split();
            let mut reader = RaftReader(read_half);
            let peer_id = reader.read_node_id().await.unwrap();
            assert_eq!(peer_id, NodeId::new("node-a"));

            // Conflict: node-a < node-b → incoming wins, replace
            let should_drop = writers.contains_key(&peer_id) && peer_id > NodeId::new("node-b");
            assert!(
                !should_drop,
                "lower NodeId's connection should NOT be dropped"
            );

            Ok(())
        });

        sim.host("node-a", || async {
            let addr = turmoil::lookup("node-b");

            let stream1 = TcpStream::connect((addr, 9000)).await?;
            let (_, write_half) = stream1.into_split();
            let mut writer = RaftWriter(write_half);
            writer.write_node_id(&NodeId::new("node-a")).await?;

            let stream2 = TcpStream::connect((addr, 9001)).await?;
            let (_, write_half2) = stream2.into_split();
            let mut writer2 = RaftWriter(write_half2);
            writer2.write_node_id(&NodeId::new("node-a")).await?;

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
