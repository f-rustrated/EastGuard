mod inbound;
use inbound::*;
mod outbound;
pub(crate) use outbound::OutboundClusterConnection;
use outbound::*;
mod protocol;

use std::time::Duration;

use anyhow::Context;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
#[cfg(test)]
use tokio::time::Instant;

use crate::control_plane::consensus::actor::MutlRaftSender;

use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::RaftTransportCommand;
use crate::control_plane::membership::actor::SwimSender;
use crate::net::{TcpListener, before_deadline};
#[cfg(test)]
use crate::security::CertificatePrincipal;
use crate::security::SecurityHandle;

const MAX_IN_FLIGHT_CLUSTER_HANDSHAKES: usize = 128;
// Covers TLS, the bounded identity exchange, and an optional one-shot ACL read.
const CLUSTER_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(24);

/// Thin async boundary — select loop over listener and actor commands.
pub struct RaftTransportActor;

impl RaftTransportActor {
    pub async fn run(
        node_id: NodeId,
        listener: TcpListener,
        raft_tx: MutlRaftSender,
        mut from_actor: mpsc::Receiver<Box<[RaftTransportCommand]>>,
        swim_tx: SwimSender,
        security: SecurityHandle,
    ) {
        let (connection_event_tx, mut connection_event_rx) = mpsc::channel(256);

        let mut handshakes = JoinSet::new();
        let mut dispatcher = RaftRpcDispatcher::new(node_id, connection_event_tx, security.clone());
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(300));
        cleanup_interval.tick().await; // consume immediate first tick

        loop {
            tokio::select! {
                Ok((stream, _)) = listener.accept() => {
                    if handshakes.len() >= MAX_IN_FLIGHT_CLUSTER_HANDSHAKES {
                        tracing::debug!("cluster connection rejected: handshake limit reached");
                        continue;
                    }
                    let security = security.clone();
                    handshakes.spawn(async move {
                        tokio::time::timeout(
                            CLUSTER_HANDSHAKE_TIMEOUT,
                            accept_cluster_connection(stream, security),
                        ).await.context("cluster handshake timed out")?
                    });
                }
                Some(result) = handshakes.join_next(), if !handshakes.is_empty() => {
                    match result.unwrap_or_else(|error| Err(error.into())) {
                        Ok(Some(connection)) => dispatcher.accept(connection, &raft_tx),
                        Ok(None) => {}
                        Err(error) => tracing::debug!("cluster handshake failed: {error:#}"),
                    }
                }
                Some(batch) = from_actor.recv() => {
                    dispatcher.handle_commands(batch, &swim_tx).await;
                }
                Some(event) = connection_event_rx.recv() => {
                    dispatcher.on_connection_event(event, &raft_tx).await;
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
    use super::protocol::ClusterRequest;
    use super::*;
    use crate::control_plane::consensus::actor::MultiRaftActor;
    use crate::control_plane::consensus::messages::{
        MultiRaftActorCommand, RaftProtocolMessage, RaftRpc, RequestVote, WireRaftMessage,
    };
    use crate::control_plane::consensus::raft::states::security::AclRecord;
    use crate::control_plane::membership::actor::SwimActor;
    use crate::control_plane::membership::{
        QueryCommand, ShardGroup, ShardGroupId, SwimActorCommand, Topology, TopologyConfig,
    };
    use crate::control_plane::metadata::{AclResource, TopicId};
    use crate::control_plane::{NodeAddress, NodeAddressInfo, Replicas};
    use crate::net::OwnedWriteHalf;
    use crate::net::{TcpStream, TransportTcpStream};
    use crate::security::{NodeTransportSecurity, SecurityActor, SecurityHandle};
    use rcgen::string::Ia5String;
    use rcgen::{CertificateParams, KeyPair, SanType};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Notify;
    use turmoil::Builder;

    #[tokio::test]
    async fn expired_deadline_does_not_poll_operation() {
        let started = std::sync::atomic::AtomicBool::new(false);

        let result = before_deadline(Instant::now(), async {
            started.store(true, std::sync::atomic::Ordering::Relaxed);
        })
        .await;

        assert!(result.is_none());
        assert!(!started.load(std::sync::atomic::Ordering::Relaxed));
    }

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
            peer_id: NodeId::new(sender),
            rpc: RaftRpc::RequestVote(RequestVote {
                term: 1,
                candidate_id: NodeId::new(sender),
                last_log_index: 0,
                last_log_term: 0,
            }),
        }
    }

    fn test_security(raft_tx: MutlRaftSender) -> SecurityHandle {
        let (swim_tx, _swim_rx) = SwimActor::channel(1);
        let node_id = NodeId::new("test-node");
        let topology = Topology::new(
            [node_id.clone()],
            TopologyConfig {
                vnodes_per_pnode: 1,
                replication_factor: 1,
            },
        )
        .channel()
        .1;
        SecurityActor::spawn(
            node_id,
            swim_tx,
            raft_tx,
            topology,
            NodeTransportSecurity::TrustedDevelopment,
        )
    }

    #[test]
    fn cluster_handshakes_are_bounded_and_do_not_block_healthy_peers() -> turmoil::Result {
        let mut sim = Builder::new()
            .rng_seed(19)
            .tcp_capacity(MAX_IN_FLIGHT_CLUSTER_HANDSHAKES + 1)
            .simulation_duration(Duration::from_secs(30))
            .build();
        sim.client("node", async {
            let (raft_tx, mut raft_rx) = MultiRaftActor::channel(1);
            let security = test_security(raft_tx.clone());
            let (swim_tx, _swim_rx) = SwimActor::channel(1);
            let (_commands, commands_rx) = mpsc::channel(1);
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let actor = tokio::spawn(RaftTransportActor::run(
                NodeId::new("test-node"),
                listener,
                raft_tx,
                commands_rx,
                swim_tx,
                security,
            ));
            let address = (turmoil::lookup("node"), 9000);
            let mut stalled = futures::future::try_join_all(
                (0..MAX_IN_FLIGHT_CLUSTER_HANDSHAKES - 1).map(|_| TcpStream::connect(address)),
            )
            .await?;

            let mut healthy = TcpStream::connect(address).await?;
            healthy
                .write_all(&protocol::encode_frame(&ClusterRequest::Raft(
                    request_vote_message(42, "healthy-peer"),
                ))?)
                .await?;
            let Some(MultiRaftActorCommand::ProtocolMessage(RaftProtocolMessage::InboundRaftRpc(
                rpc,
            ))) = tokio::time::timeout(Duration::from_secs(1), raft_rx.recv()).await?
            else {
                panic!("healthy peer must be served while other handshakes stall");
            };
            assert_eq!(rpc.peer_id, NodeId::new("healthy-peer"));

            stalled.push(TcpStream::connect(address).await?);
            let mut overflow = TcpStream::connect(address).await?;
            let mut byte = [0; 1];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), overflow.read(&mut byte)).await??,
                0,
                "overflow must close instead of waiting for a handshake slot",
            );
            assert_eq!(
                tokio::time::timeout(
                    CLUSTER_HANDSHAKE_TIMEOUT + Duration::from_secs(1),
                    stalled[0].read(&mut byte),
                )
                .await??,
                0,
                "stalled handshake must close at its deadline",
            );
            actor.abort();
            Ok(())
        });
        sim.run()
    }

    struct NodeMaterial {
        certificate: CertificateDer<'static>,
        private_key: PrivateKeyDer<'static>,
    }

    fn node_material(principal: &str) -> NodeMaterial {
        let mut params = CertificateParams::default();
        params.subject_alt_names.push(SanType::URI(
            Ia5String::try_from(format!("urn:eastguard:node:{principal}")).unwrap(),
        ));
        let key = KeyPair::generate().unwrap();
        let certificate = params.self_signed(&key).unwrap();
        NodeMaterial {
            certificate: certificate.der().clone(),
            private_key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        }
    }

    fn load_secure_node(
        material: &NodeMaterial,
        trust_roots: &[CertificateDer<'static>],
    ) -> NodeTransportSecurity {
        NodeTransportSecurity::test_secure(
            vec![material.certificate.clone()],
            material.private_key.clone_key(),
            trust_roots,
        )
        .unwrap()
    }

    fn secure_test_security(node_id: NodeId, transport: NodeTransportSecurity) -> SecurityHandle {
        let (raft_tx, raft_rx) = MultiRaftActor::channel(1);
        drop(raft_rx); // No metadata service is available during these handshakes.
        let (swim_tx, _swim_rx) = SwimActor::channel(1);
        let topology = Topology::new(
            [node_id.clone()],
            TopologyConfig {
                vnodes_per_pnode: 1,
                replication_factor: 1,
            },
        )
        .channel()
        .1;
        SecurityActor::spawn(node_id, swim_tx, raft_tx, topology, transport)
    }

    #[test]
    fn secure_raft_authenticates_without_metadata_and_binds_every_sender() -> turmoil::Result {
        let client_material = node_material("broker-a");
        let server_material = node_material("broker-b");
        let trust_roots = [
            client_material.certificate.clone(),
            server_material.certificate.clone(),
        ];
        for (server_id, target_id, sender, handshake_ok, request_ok) in [
            ("broker-b::1", "broker-b::1", "broker-a::1", true, true),
            ("broker-b::2", "broker-b::2", "broker-a::1", true, true),
            ("broker-b::1", "broker-b::2", "broker-a::1", false, false),
            (
                "broker-b::1",
                "other-broker::1",
                "broker-a::1",
                false,
                false,
            ),
            ("broker-b::1", "broker-b::1", "forged::1", true, false),
        ] {
            let client_transport = load_secure_node(&client_material, &trust_roots);
            let server_transport = load_secure_node(&server_material, &trust_roots);
            let mut sim = Builder::new()
                .rng_seed(17)
                .simulation_duration(Duration::from_secs(80))
                .build();
            sim.host("server", move || {
                let transport = server_transport.clone();
                async move {
                    let listener = TcpListener::bind("0.0.0.0:9000").await?;
                    let security = secure_test_security(NodeId::new(server_id), transport);
                    let (stream, _) = listener.accept().await?;
                    let result = accept_cluster_connection(stream, security).await;
                    if request_ok {
                        let mut accepted = result?.expect("expected authenticated Raft stream");
                        assert_eq!(accepted.initial_message.peer_id, NodeId::new("broker-a::1"));
                        // The connection remains usable beyond the removed admission lease.
                        let message: WireRaftMessage = accepted
                            .reader
                            .read_frame(protocol::MAX_CLUSTER_FRAME_SIZE, "Raft message")
                            .await?;
                        assert_eq!(message.shard_group_id, ShardGroupId(43));
                    } else {
                        assert!(
                            result.is_err(),
                            "forged or mismatched identity must be rejected"
                        );
                    }
                    Ok(())
                }
            });
            sim.host("client", move || {
                let transport = client_transport.clone();
                async move {
                    let security = secure_test_security(NodeId::new("broker-a::1"), transport);
                    let stream = TransportTcpStream::connect_node(
                        (turmoil::lookup("server"), 9000),
                        security.node_transport(),
                    )
                    .await?;
                    let result = OutboundClusterConnection::new(
                        stream,
                        &NodeId::new(target_id),
                        security.local_node_id(),
                        security.node_transport(),
                    )
                    .await;
                    if handshake_ok {
                        let mut connection = result?;
                        connection
                            .send_raft_request(request_vote_message(42, sender))
                            .await?;
                        if request_ok {
                            let (_, mut writer) = connection.into_parts();
                            tokio::time::sleep(Duration::from_secs(61)).await;
                            writer
                                .write_all(&protocol::encode_frame(&request_vote_message(
                                    43,
                                    "broker-a::1",
                                ))?)
                                .await?;
                            writer.flush().await?;
                            assert_eq!(
                                security
                                    .authorize(
                                        Some(&CertificatePrincipal::new("client-a")),
                                        AclResource::Cluster,
                                    )
                                    .await,
                                Err(crate::client::ServerError::Unauthorized)
                            );
                        }
                    } else {
                        assert!(
                            result.is_err(),
                            "wrong destination must fail before application data"
                        );
                    }
                    Ok(())
                }
            });
            sim.run()?;
        }
        Ok(())
    }

    #[test]
    fn secure_receiver_rejects_forged_and_oversized_node_identities() -> turmoil::Result {
        let client_material = node_material("broker-a");
        let server_material = node_material("broker-b");
        let trust_roots = [
            client_material.certificate.clone(),
            server_material.certificate.clone(),
        ];
        for (claimed_id, expected_error) in [
            (Some("broker-b::forged"), "certificate principal"),
            (Some("broker-a::"), "certificate principal"),
            (None, "node identity frame too large"),
        ] {
            let client_transport = load_secure_node(&client_material, &trust_roots);
            let server_transport = load_secure_node(&server_material, &trust_roots);
            let mut sim = Builder::new()
                .rng_seed(19)
                .simulation_duration(Duration::from_secs(5))
                .build();
            sim.host("server", move || {
                let transport = server_transport.clone();
                async move {
                    let listener = TcpListener::bind("0.0.0.0:9000").await?;
                    let security = secure_test_security(NodeId::new("broker-b::1"), transport);
                    let (stream, _) = listener.accept().await?;
                    match accept_cluster_connection(stream, security).await {
                        Err(error) => assert!(error.to_string().contains(expected_error)),
                        Ok(_) => panic!("invalid node identity was accepted"),
                    }
                    Ok(())
                }
            });
            sim.client("client", async move {
                let mut stream = TransportTcpStream::connect_node(
                    (turmoil::lookup("server"), 9000),
                    &client_transport,
                )
                .await?;
                // Bypass the sender's helper to test validation at the receiver.
                if let Some(claimed_id) = claimed_id {
                    let payload = borsh::to_vec(&NodeId::new(claimed_id))?;
                    stream.write_u32(u32::try_from(payload.len())?).await?;
                    stream.write_all(&payload).await?;
                } else {
                    stream
                        .write_u32(crate::security::MAX_SECURITY_ID_BYTES as u32 + 5)
                        .await?;
                }
                stream.flush().await?;
                // Consume the server's own identity, then wait for rejection.
                let mut reader = ClusterMessageReader::new(stream.into_split().0);
                let peer: NodeId = reader.read_frame(128, "test identity").await?;
                assert_eq!(peer, NodeId::new("broker-b::1"));
                assert!(
                    reader
                        .read_frame::<u8>(1, "unexpected payload")
                        .await
                        .is_err()
                );
                Ok(())
            });
            sim.run()?;
        }
        Ok(())
    }

    fn remote_security(
        node_id: NodeId,
        raft_tx: MutlRaftSender,
        owner: NodeAddressInfo,
    ) -> SecurityHandle {
        let (swim_tx, mut swim_rx) = SwimActor::channel(8);
        let group = ShardGroup {
            id: ShardGroupId(42),
            replicas: Replicas::new(vec![owner.node_id.clone()]),
        };
        let topology = Topology::new(
            [owner.node_id.clone()],
            TopologyConfig {
                vnodes_per_pnode: 1,
                replication_factor: 1,
            },
        )
        .channel()
        .1;
        tokio::spawn(async move {
            while let Some(command) = swim_rx.recv().await {
                match command {
                    SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) => {
                        let _ = reply.send(Some(group.clone()));
                    }
                    SwimActorCommand::Query(QueryCommand::ResolveAddress { reply, .. }) => {
                        let _ = reply.send(Some(owner.addr));
                    }
                    SwimActorCommand::Command(_) | SwimActorCommand::Query(_) => {}
                }
            }
        });
        SecurityActor::spawn(
            node_id,
            swim_tx,
            raft_tx,
            topology,
            NodeTransportSecurity::TrustedDevelopment,
        )
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
            let mut reader = ClusterMessageReader::new(read_half);

            let ClusterRequest::Raft(message) = reader
                .read_frame(4 * 1024 * 1024, "cluster handshake")
                .await
                .unwrap()
            else {
                panic!("expected initial Raft message");
            };
            assert_eq!(message.peer_id, NodeId::new("node-abc"));
            Ok(())
        });

        sim.host("client", || async {
            let addr = turmoil::lookup("server");
            let stream = TcpStream::connect((addr, 9000)).await?;
            let (_, mut write_half) = stream.into_split();

            write_frame(
                &mut write_half,
                &ClusterRequest::Raft(request_vote_message(42, "node-abc")),
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
            let mut reader = ClusterMessageReader::new(read_half);

            let msg: WireRaftMessage = reader
                .read_frame(4 * 1024 * 1024, "Raft message")
                .await
                .unwrap();
            assert_eq!(msg.shard_group_id, ShardGroupId(42));
            assert_eq!(msg.peer_id, NodeId::new("sender-1"));
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
                    peer_id: NodeId::new("sender-1"),
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
            let reader = ClusterMessageReader::new(read_half);
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
            let (connection_result_tx, _connection_result_rx) = tokio::sync::mpsc::channel(8);
            let mut state = RaftRpcDispatcher::new(
                NodeId::new("node-b"),
                connection_result_tx,
                test_security(raft_tx.clone()),
            );
            let (stream, _) = listener.accept().await?;
            let connection = accept_cluster_connection(stream, test_security(raft_tx.clone()))
                .await?
                .expect("expected Raft stream");
            state.accept(connection, &raft_tx);

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
                &ClusterRequest::Raft(request_vote_message(1, "node-a")),
            )
            .await?;
            Ok(())
        });

        sim.run()
    }

    #[test]
    fn replacement_aborts_old_connection_and_accept_clears_dead_state() -> turmoil::Result {
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");
        assert!(node_a < node_b);

        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(5))
            .build();

        sim.host("node-b", || async {
            let (raft_tx, _raft_rx) = MultiRaftActor::channel(16);
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let (connection_result_tx, _connection_result_rx) = tokio::sync::mpsc::channel(8);
            let security = test_security(raft_tx.clone());
            let mut state = RaftRpcDispatcher::new(
                NodeId::new("node-b"),
                connection_result_tx,
                security.clone(),
            );

            let (first_stream, _) = listener.accept().await?;
            let connection = accept_cluster_connection(first_stream, security.clone())
                .await?
                .expect("expected Raft stream");
            state.accept(connection, &raft_tx);
            let peer_id = NodeId::new("node-a");
            let first_generation = state
                .connection_generation(&peer_id)
                .expect("first connection should be installed");

            let (replacement_stream, _) = listener.accept().await?;
            let replacement = accept_cluster_connection(replacement_stream, security.clone())
                .await?
                .expect("expected replacement Raft stream");
            state.accept(replacement, &raft_tx);
            let replacement_generation = state
                .connection_generation(&peer_id)
                .expect("replacement connection should be installed");
            assert_ne!(replacement_generation, first_generation);

            state.disconnect(peer_id.clone());
            assert!(state.is_dead(&peer_id));

            let (restarted_stream, _) = listener.accept().await?;
            let restarted = accept_cluster_connection(restarted_stream, security)
                .await?
                .expect("expected restarted peer's Raft stream");
            state.accept(restarted, &raft_tx);
            assert!(state.contains(&peer_id));
            assert!(!state.is_dead(&peer_id));

            Ok(())
        });

        sim.host("node-a", || async {
            let addr = turmoil::lookup("node-b");

            let stream1 = TcpStream::connect((addr, 9000)).await?;
            let (mut first_reader, mut write_half) = stream1.into_split();
            write_frame(
                &mut write_half,
                &ClusterRequest::Raft(request_vote_message(1, "node-a")),
            )
            .await?;

            let stream2 = TcpStream::connect((addr, 9000)).await?;
            let (_, mut write_half2) = stream2.into_split();
            write_frame(
                &mut write_half2,
                &ClusterRequest::Raft(request_vote_message(1, "node-a")),
            )
            .await?;

            let mut byte = [0; 1];
            let bytes_read =
                tokio::time::timeout(Duration::from_secs(1), first_reader.read(&mut byte))
                    .await??;
            assert_eq!(bytes_read, 0, "replacement must close the old connection");

            let stream3 = TcpStream::connect((addr, 9000)).await?;
            let (_, mut write_half3) = stream3.into_split();
            write_frame(
                &mut write_half3,
                &ClusterRequest::Raft(request_vote_message(1, "node-a")),
            )
            .await?;

            Ok(())
        });

        sim.run()
    }

    #[test]
    fn security_actor_coalesces_concurrent_acl_reads() -> turmoil::Result {
        let resource = AclResource::TopicData(TopicId(7));
        let snapshot = AclRecord {
            resource: resource.clone(),
            revision: 3,
            principals: vec!["orders-service".to_owned()].into(),
        };
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
                let responder = tokio::spawn(async move {
                    let Some(MultiRaftActorCommand::GetAclSnapshot(query)) = raft_rx.recv().await
                    else {
                        panic!("expected ACL snapshot query");
                    };
                    assert_eq!(query.resource, expected_resource);
                    let _ = query.reply.send(Ok(expected_snapshot));
                });
                let (stream, _) = listener.accept().await?;
                assert!(
                    accept_cluster_connection(stream, test_security(raft_tx))
                        .await?
                        .is_none()
                );
                responder.await?;
                owner_completion.notified().await;
                Ok(())
            }
        });

        let client_resource = resource.clone();
        sim.host("requester", move || {
            let requested_resource = client_resource.clone();
            let requester_completion = response_received.clone();
            async move {
                let owner_addr = turmoil::lookup("owner");
                let owner = NodeAddressInfo::new(
                    NodeId::new("owner"),
                    NodeAddress::test((owner_addr, 9000).into(), (owner_addr, 9001).into()),
                );
                let (raft_tx, _raft_rx) = MultiRaftActor::channel(8);
                let security = remote_security(NodeId::new("requester"), raft_tx, owner);
                let principal = CertificatePrincipal::new("orders-service");
                let first_resource = requested_resource.clone();
                let (first, second) = tokio::join!(
                    security.authorize(Some(&principal), first_resource),
                    security.authorize(Some(&principal), requested_resource),
                );
                assert_eq!(first, Ok(()));
                assert_eq!(second, Ok(()));
                requester_completion.notify_one();
                Ok(())
            }
        });

        sim.run()
    }
}
