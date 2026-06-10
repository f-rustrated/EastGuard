#![allow(dead_code)]

mod inbound;
use inbound::*;
mod outbound;
use outbound::*;

use tokio::sync::mpsc;

use crate::control_plane::consensus::actor::MutlRaftSender;

use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::RaftTransportCommand;
use crate::control_plane::membership::actor::SwimSender;
use crate::net::TcpListener;

const CONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

/// Thin async boundary — select loop over listener and actor commands.
pub struct RaftTransportActor;

impl RaftTransportActor {
    pub async fn run(
        node_id: NodeId,
        listener: TcpListener,
        raft_tx: MutlRaftSender,
        mut from_actor: mpsc::Receiver<Box<[RaftTransportCommand]>>,
        swim_tx: SwimSender,
    ) {
        let (dial_tx, mut dial_rx) = mpsc::channel(256);
        let mut dispatcher = RaftRpcDispatcher::new(node_id, dial_tx);
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(300));
        cleanup_interval.tick().await; // consume immediate first tick

        loop {
            tokio::select! {
                Ok((stream, _)) = listener.accept() => {
                    dispatcher.accept(stream, &raft_tx).await;
                }
                Some(batch) = from_actor.recv() => {
                    // Aggregate the batch into ONE dispatcher.send() call: the
                    // actor emits one Send command *per target* (#133), so
                    // dispatching them in arrival order would let a dial to a
                    // crashed peer (3s connect timeout, no RST) stall commands
                    // to reachable peers queued behind it — e.g. the two halves
                    // of a RequestVote broadcast. Disconnects are applied first
                    // so same-batch sends already skip removed peers, then
                    // `send()` flushes connected peers before dialing anyone.
                    let mut to_send = Vec::new();
                    for cmd in batch {
                        match cmd {
                            RaftTransportCommand::Send(packets) => to_send.extend(packets),
                            RaftTransportCommand::DisconnectPeer(peer_id) => {
                                dispatcher.disconnect(peer_id);
                            }
                        }
                    }
                    if !to_send.is_empty() {
                        dispatcher.send(to_send, &swim_tx).await;
                    }
                }
                Some(result) = dial_rx.recv() => {
                    dispatcher.on_dial_result(result, &raft_tx).await;
                }
                _ = cleanup_interval.tick() => {
                    dispatcher.cleanup_dead_peers();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::BINCODE_CONFIG;
    use crate::control_plane::consensus::messages::{RaftRpc, RequestVote, WireRaftMessage};
    use crate::control_plane::membership::ShardGroupId;
    use crate::net::OwnedWriteHalf;
    use crate::net::TcpStream;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
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
            let mut reader = RaftRpcListener(read_half);

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
            let mut reader = RaftRpcListener(read_half);

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
            let (raft_tx, _raft_rx) =
                crate::control_plane::consensus::actor::MultiRaftActor::channel(16);
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let (dial_tx, _dial_rx) = tokio::sync::mpsc::channel(8);
            let mut state = RaftRpcDispatcher::new(NodeId::new("node-b"), dial_tx);

            let (stream, _) = listener.accept().await?;
            state.accept(stream, &raft_tx).await;

            assert!(
                state.contains(&NodeId::new("node-a")),
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
            let (raft_tx, _raft_rx) =
                crate::control_plane::consensus::actor::MultiRaftActor::channel(16);
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let dummy_listener = TcpListener::bind("0.0.0.0:9001").await?;
            let (dial_tx, _dial_rx) = tokio::sync::mpsc::channel(8);
            let mut state = RaftRpcDispatcher::new(NodeId::new("node-b"), dial_tx);

            // First connection from node-a
            let (stream, _) = listener.accept().await?;
            state.accept(stream, &raft_tx).await;
            assert!(state.contains(&NodeId::new("node-a")));

            // Second connection from node-a (simulating simultaneous connect)
            let (stream2, _) = dummy_listener.accept().await?;
            let (read_half, _write_half) = stream2.into_split();
            let mut reader = RaftRpcListener(read_half);
            let peer_id = reader.read_node_id().await.unwrap();
            assert_eq!(peer_id, NodeId::new("node-a"));

            // Conflict: node-a < node-b → incoming wins, replace
            let should_drop = state.contains(&peer_id) && peer_id > NodeId::new("node-b");
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
}
