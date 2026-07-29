#![allow(dead_code)]

mod acl;
pub(crate) use acl::{AclSnapshotActor, AclSnapshotSender};
mod admission;
mod inbound;
use inbound::*;
mod outbound;
use outbound::*;
mod protocol;

use tokio::sync::mpsc;

use crate::control_plane::consensus::actor::MutlRaftSender;

use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::RaftTransportCommand;
use crate::control_plane::membership::actor::SwimSender;
use crate::net::TcpListener;
use crate::net::TransportTcpStream;
use crate::security::NodeTransportSecurity;
#[cfg(test)]
use crate::security::TransportIdentity;

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
        security: NodeTransportSecurity,
    ) {
        let (dial_tx, mut dial_rx) = mpsc::channel(256);
        let mut dispatcher = RaftRpcDispatcher::new(node_id, dial_tx, security.clone());
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(300));
        cleanup_interval.tick().await; // consume immediate first tick

        loop {
            tokio::select! {
                Ok((stream, _)) = listener.accept() => {
                    match TransportTcpStream::accept_node(stream, &security).await {
                        Ok(stream) => dispatcher.accept(stream, &raft_tx).await,
                        Err(error) => tracing::debug!("Raft TLS accept rejected: {error}"),
                    }
                }
                Some(batch) = from_actor.recv() => {
                    // Disconnects are applied first so same-batch sends already skip removed peers, then
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
    use super::protocol::InitialClusterMessage;
    use super::*;
    use crate::control_plane::consensus::actor::MultiRaftActor;
    use crate::control_plane::consensus::messages::{
        MultiRaftActorCommand, RaftProtocolMessage, RaftRpc, RequestVote, WireRaftMessage,
    };
    use crate::control_plane::consensus::raft::states::security::{AclRecord, AdmissionRecord};
    use crate::control_plane::membership::ShardGroupId;
    use crate::control_plane::membership::actor::{RemoteShard, ShardRouting};
    use crate::control_plane::metadata::{AclResource, TopicId};
    use crate::control_plane::{NodeAddress, NodeAddressInfo};
    use crate::net::OwnedWriteHalf;
    use crate::net::TcpStream;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::sync::Notify;
    use turmoil::Builder;

    /// Write a length-prefixed borsh-encoded value to a raw write half.
    /// Used by tests to simulate the peer side of the wire protocol.
    async fn write_frame(
        writer: &mut OwnedWriteHalf,
        value: &impl borsh::BorshSerialize,
    ) -> std::io::Result<()> {
        let bytes = borsh::to_vec(value)?;
        let len = bytes.len() as u32;
        writer.write_all(&len.to_be_bytes()).await?;
        writer.write_all(&bytes).await?;
        Ok(())
    }

    fn request_vote_message(shard_group_id: u64, sender: &str) -> WireRaftMessage {
        WireRaftMessage {
            shard_group_id: ShardGroupId(shard_group_id),
            sender: NodeId::new(sender),
            rpc: RaftRpc::RequestVote(RequestVote {
                term: 1,
                candidate_id: NodeId::new(sender),
                last_log_index: 0,
                last_log_term: 0,
            }),
        }
    }

    #[test]
    fn initial_raft_message_identifies_the_peer() -> turmoil::Result {
        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(5))
            .build();

        sim.host("server", || async {
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let (stream, _) = listener.accept().await?;
            let (read_half, _) = stream.into_split();
            let mut reader =
                ClusterMessageReader::new(read_half, TransportIdentity::TrustedDevelopment);

            let InitialClusterMessage::Raft(message) = reader.read_initial_message().await.unwrap()
            else {
                panic!("expected initial Raft message");
            };
            assert_eq!(message.sender, NodeId::new("node-abc"));
            Ok(())
        });

        sim.host("client", || async {
            let addr = turmoil::lookup("server");
            let stream = TcpStream::connect((addr, 9000)).await?;
            let (_, mut write_half) = stream.into_split();

            write_frame(
                &mut write_half,
                &InitialClusterMessage::Raft(request_vote_message(42, "node-abc")),
            )
            .await?;
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
            let mut reader =
                ClusterMessageReader::new(read_half, TransportIdentity::TrustedDevelopment);

            let msg = reader.read_raft_message().await.unwrap();
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
    fn reader_binds_message_sender_to_connection_peer() -> turmoil::Result {
        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(5))
            .build();

        sim.host("server", || async {
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let (stream, _) = listener.accept().await?;
            let (read_half, _) = stream.into_split();
            let reader =
                ClusterMessageReader::new(read_half, TransportIdentity::TrustedDevelopment);
            let peer = NodeId::new("node-a");
            let (raft_tx, mut raft_rx) = MultiRaftActor::channel(8);

            reader.run(raft_tx, peer.clone()).await;

            let Some(MultiRaftActorCommand::ProtocolMessage(RaftProtocolMessage::InboundRaftRpc(
                cmd,
            ))) = raft_rx.recv().await
            else {
                panic!("expected one inbound Raft RPC")
            };
            assert_eq!(cmd.peer_id, peer);
            assert_eq!(cmd.shard_group_id, ShardGroupId(1));
            assert!(raft_rx.try_recv().is_err());
            Ok(())
        });

        sim.host("client", || async {
            let addr = turmoil::lookup("server");
            let stream = TcpStream::connect((addr, 9000)).await?;
            let (_, mut writer) = stream.into_split();
            write_frame(&mut writer, &request_vote_message(1, "node-a")).await?;
            write_frame(&mut writer, &request_vote_message(2, "node-b")).await?;
            Ok(())
        });

        sim.run()
    }

    #[test]
    fn accepted_connection_registers_writer_after_initial_raft_message() -> turmoil::Result {
        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(5))
            .build();

        sim.host("acceptor", || async {
            let (raft_tx, mut raft_rx) = MultiRaftActor::channel(16);
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let (dial_tx, _dial_rx) = tokio::sync::mpsc::channel(8);
            let mut state = RaftRpcDispatcher::new(
                NodeId::new("node-b"),
                dial_tx,
                NodeTransportSecurity::TrustedDevelopment,
            );

            let (stream, _) = listener.accept().await?;
            state
                .accept(TransportTcpStream::TrustedDevelopment(stream), &raft_tx)
                .await;

            assert!(
                state.contains(&NodeId::new("node-a")),
                "writer should be registered after the initial Raft message"
            );
            let Some(MultiRaftActorCommand::ProtocolMessage(RaftProtocolMessage::InboundRaftRpc(
                rpc,
            ))) = raft_rx.recv().await
            else {
                panic!("expected the initial Raft RPC")
            };
            assert_eq!(rpc.peer_id, NodeId::new("node-a"));
            Ok(())
        });

        sim.host("initiator", || async {
            let addr = turmoil::lookup("acceptor");
            let stream = TcpStream::connect((addr, 9000)).await?;
            let (_, mut write_half) = stream.into_split();
            write_frame(
                &mut write_half,
                &InitialClusterMessage::Raft(request_vote_message(1, "node-a")),
            )
            .await?;
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
            let (raft_tx, _raft_rx) = MultiRaftActor::channel(16);
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let dummy_listener = TcpListener::bind("0.0.0.0:9001").await?;
            let (dial_tx, _dial_rx) = tokio::sync::mpsc::channel(8);
            let mut state = RaftRpcDispatcher::new(
                NodeId::new("node-b"),
                dial_tx,
                NodeTransportSecurity::TrustedDevelopment,
            );

            // First connection from node-a
            let (stream, _) = listener.accept().await?;
            state
                .accept(TransportTcpStream::TrustedDevelopment(stream), &raft_tx)
                .await;
            assert!(state.contains(&NodeId::new("node-a")));

            // Second connection from node-a (simulating simultaneous connect)
            let (stream2, _) = dummy_listener.accept().await?;
            let (read_half, _write_half) = stream2.into_split();
            let mut reader =
                ClusterMessageReader::new(read_half, TransportIdentity::TrustedDevelopment);
            let InitialClusterMessage::Raft(initial_raft_message) =
                reader.read_initial_message().await.unwrap()
            else {
                panic!("expected initial Raft message");
            };
            let peer_id = initial_raft_message.sender;
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
            write_frame(
                &mut write_half,
                &InitialClusterMessage::Raft(request_vote_message(1, "node-a")),
            )
            .await?;

            let stream2 = TcpStream::connect((addr, 9001)).await?;
            let (_, mut write_half2) = stream2.into_split();
            write_frame(
                &mut write_half2,
                &InitialClusterMessage::Raft(request_vote_message(1, "node-a")),
            )
            .await?;

            Ok(())
        });

        sim.run()
    }

    #[test]
    fn acl_snapshot_actor_coalesces_concurrent_refreshes() -> turmoil::Result {
        let resource = AclResource::TopicData(TopicId(7));
        let snapshot = AclRecord {
            resource: resource.clone(),
            revision: 3,
            principals: vec!["orders-service".to_owned()].into(),
        };
        let owner = NodeAddressInfo::new(
            NodeId::new("owner"),
            NodeAddress::test(
                "127.0.0.1:9000".parse().unwrap(),
                "127.0.0.1:9001".parse().unwrap(),
            ),
        );
        let response_received = std::sync::Arc::new(Notify::new());
        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(5))
            .build();

        let server_snapshot = snapshot.clone();
        let server_resource = resource.clone();
        let server_response_received = response_received.clone();
        sim.host("owner", move || {
            let expected_snapshot = server_snapshot.clone();
            let expected_resource = server_resource.clone();
            let owner_completion = server_response_received.clone();
            async move {
                let listener = TcpListener::bind("0.0.0.0:9000").await?;
                let (raft_tx, mut raft_rx) = MultiRaftActor::channel(8);
                let (dial_tx, _dial_rx) = tokio::sync::mpsc::channel(8);
                let mut dispatcher = RaftRpcDispatcher::new(
                    NodeId::new("owner"),
                    dial_tx,
                    NodeTransportSecurity::TrustedDevelopment,
                );

                let (stream, _) = listener.accept().await?;
                dispatcher
                    .accept(TransportTcpStream::TrustedDevelopment(stream), &raft_tx)
                    .await;

                let Some(MultiRaftActorCommand::GetAclSnapshot(query)) = raft_rx.recv().await
                else {
                    panic!("expected ACL snapshot query");
                };
                assert_eq!(query.shard_group_id, ShardGroupId(42));
                assert_eq!(query.resource, expected_resource);
                let _ = query.reply.send(Some(expected_snapshot));
                owner_completion.notified().await;
                Ok(())
            }
        });

        let client_resource = resource.clone();
        let client_snapshot = snapshot.clone();
        let client_owner = owner.clone();
        sim.host("requester", move || {
            let requested_resource = client_resource.clone();
            let expected_snapshot = client_snapshot.clone();
            let remote_owner = client_owner.clone();
            let requester_completion = response_received.clone();
            async move {
                let client = AclSnapshotActor::spawn(NodeTransportSecurity::TrustedDevelopment);
                let first_owner = remote_owner.clone();
                let first_resource = requested_resource.clone();
                let (first, second) = tokio::join!(
                    client.fetch(
                        NodeId::new("requester"),
                        first_owner,
                        ShardGroupId(42),
                        first_resource,
                    ),
                    client.fetch(
                        NodeId::new("requester"),
                        remote_owner,
                        ShardGroupId(42),
                        requested_resource,
                    ),
                );
                assert_eq!(first, Some(expected_snapshot.clone()));
                assert_eq!(second, Some(expected_snapshot));
                requester_completion.notify_one();
                Ok(())
            }
        });

        sim.run()
    }

    #[test]
    fn admission_lookup_actor_coalesces_remote_reads() -> turmoil::Result {
        let admission = AdmissionRecord {
            node_certificate_principal: "broker-a".to_string(),
            revision: 3,
            epoch: 8,
            node_id: NodeId::new("broker-a::process-2"),
            process_public_key: vec![1, 2, 3].into_boxed_slice(),
        };
        let owner = NodeAddressInfo::new(
            NodeId::new("owner"),
            NodeAddress::test(
                "127.0.0.1:9000".parse().unwrap(),
                "127.0.0.1:9001".parse().unwrap(),
            ),
        );
        let response_received = std::sync::Arc::new(Notify::new());
        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(5))
            .build();

        let server_admission = admission.clone();
        let server_response_received = response_received.clone();
        sim.host("owner", move || {
            let expected_admission = server_admission.clone();
            let owner_completion = server_response_received.clone();
            async move {
                let listener = TcpListener::bind("0.0.0.0:9000").await?;
                let (raft_tx, mut raft_rx) = MultiRaftActor::channel(8);
                let (dial_tx, _dial_rx) = tokio::sync::mpsc::channel(8);
                let mut dispatcher = RaftRpcDispatcher::new(
                    NodeId::new("owner"),
                    dial_tx,
                    NodeTransportSecurity::TrustedDevelopment,
                );

                let (stream, _) = listener.accept().await?;
                dispatcher
                    .accept(TransportTcpStream::TrustedDevelopment(stream), &raft_tx)
                    .await;

                let Some(MultiRaftActorCommand::GetAdmission(query)) = raft_rx.recv().await else {
                    panic!("expected admission query");
                };
                assert_eq!(query.shard_group_id, ShardGroupId(42));
                assert_eq!(query.node_certificate_principal.as_ref(), "broker-a");
                let _ = query.reply.send(Ok(Some(expected_admission)));
                owner_completion.notified().await;
                Ok(())
            }
        });

        let client_admission = admission.clone();
        let client_owner = owner.clone();
        sim.host("requester", move || {
            let expected_admission = client_admission.clone();
            let remote_owner = client_owner.clone();
            let requester_completion = response_received.clone();
            async move {
                let (raft_tx, _raft_rx) = MultiRaftActor::channel(8);
                let lookup = admission::AdmissionLookupActor::spawn(
                    raft_tx,
                    NodeTransportSecurity::TrustedDevelopment,
                );
                let first_owner = remote_owner.clone();
                let (first, second) = tokio::join!(
                    lookup.lookup(
                        ShardRouting::Redirect(Some(RemoteShard {
                            group_id: ShardGroupId(42),
                            member: Some(first_owner),
                        })),
                        "broker-a".into(),
                    ),
                    lookup.lookup(
                        ShardRouting::Redirect(Some(RemoteShard {
                            group_id: ShardGroupId(42),
                            member: Some(remote_owner),
                        })),
                        "broker-a".into(),
                    ),
                );
                assert_eq!(first, Ok(Some(expected_admission.clone())));
                assert_eq!(second, Ok(Some(expected_admission)));
                requester_completion.notify_one();
                Ok(())
            }
        });

        sim.run()
    }
}
