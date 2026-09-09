pub(crate) mod command;
mod connection;
mod reader;
mod writers;

use anyhow::Context;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::control_plane::NodeId;
use crate::control_plane::membership::TopologyReader;
use crate::control_plane::membership::actor::SwimSender;
use crate::data_plane::actor::DataPlaneSender;

use crate::net::TcpListener;
use crate::security::SecurityHandle;

use command::DataTransportCommand;
use connection::DataConnection;
use writers::TransportState;

const MAX_HANDSHAKES: usize = 128;
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(16);

pub struct DataTransportActor;

impl DataTransportActor {
    pub async fn run(
        node_id: NodeId,
        listener: TcpListener,
        data_plane_tx: DataPlaneSender,
        mut from_actor: mpsc::Receiver<Box<[DataTransportCommand]>>,
        swim_tx: SwimSender,
        topology: TopologyReader,
        security: SecurityHandle,
    ) {
        let mut state = TransportState::new(node_id, security.clone());
        let mut handshakes = JoinSet::new();
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(300));
        cleanup_interval.tick().await;

        // Reader tasks report here when their connection ends, so the writer for
        // that peer is evicted and the next send reconnects (see DataReader::run).
        let (disconnect_tx, mut disconnect_rx) = mpsc::channel(64);

        loop {
            tokio::select! {
                Some(batch) = from_actor.recv() => {
                    for cmd in batch {
                        match cmd {
                            DataTransportCommand::SendToTargets(cmd) => {
                                state.send(&cmd.targets, &cmd.message, &swim_tx, &data_plane_tx, &disconnect_tx).await;
                            }
                            DataTransportCommand::SendToCoordinator(cmd) => {
                                // Resolve the coordinator from the lock-free topology snapshot — no
                                // actor round-trip. The shard-leader map is a cache; on a miss
                                // (MAP-EMPTY, #135) fall back to the ring's members and broadcast —
                                // the Raft leader acts, followers no-op (propose → NotLeader). The
                                // ring is the durable group membership; the map only accelerates the
                                // common case.
                                if let Some(entry) = topology.shard_leader(cmd.shard_group_id) {
                                    state.send(&[entry.leader.node_id], &cmd.message, &swim_tx, &data_plane_tx, &disconnect_tx).await;
                                } else if let Some(members) = topology.group_ring_members(cmd.shard_group_id)
                                    && !members.is_empty()
                                {
                                    state.send(&members, &cmd.message, &swim_tx, &data_plane_tx, &disconnect_tx).await;
                                } else {
                                    tracing::debug!("coordinator unresolved for {:?} (no leader, no ring members); will retry via timeout", cmd.shard_group_id);
                                }
                            }
                            DataTransportCommand::DisconnectPeer(peer_id) => {
                                state.disconnect(peer_id);
                            }
                        }
                    }
                }

                Ok((stream, _)) = listener.accept() => {
                    if handshakes.len() >= MAX_HANDSHAKES {
                        tracing::debug!("data handshake limit reached");
                        continue;
                    }
                    let security = security.clone();
                    handshakes.spawn(async move {
                        tokio::time::timeout(HANDSHAKE_TIMEOUT, DataConnection::accept(stream, &security)).await
                            .context("data handshake timed out")?
                    });
                }

                Some(result) = handshakes.join_next(), if !handshakes.is_empty() => {
                    match result.unwrap_or_else(|error| Err(error.into())) {
                        Ok(connection) => {
                            if let Err(error) = state.accept(connection, &data_plane_tx, &disconnect_tx) {
                                tracing::debug!("data connection rejected: {error}");
                            }
                        }
                        Err(error) => tracing::debug!("data handshake failed: {error:#}"),
                    }
                }

                Some(peer) = disconnect_rx.recv() => {
                    state.evict_writer(peer);
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
    use crate::control_plane::consensus::actor::MultiRaftActor;
    use crate::control_plane::membership::actor::SwimActor;
    use crate::control_plane::membership::{Topology, TopologyConfig};
    use crate::data_plane::messages::DataPlaneMessage;
    use crate::data_plane::messages::command::{
        DataPlaneCommand, DeleteSegments, ReceivePeerMessage,
    };
    use crate::net::TcpStream;
    use crate::security::{NodeTransportSecurity, SecurityActor};

    #[test]
    fn stalled_data_handshake_does_not_block_another_peer() -> turmoil::Result {
        let mut sim = turmoil::Builder::new()
            .rng_seed(19)
            .simulation_duration(std::time::Duration::from_secs(5))
            .build();
        sim.client("node", async {
            let local = NodeId::new("node");
            let (swim, _swim_rx) = SwimActor::channel(1);
            let (raft, _raft_rx) = MultiRaftActor::channel(1);
            let topology = Topology::new(
                [local.clone()],
                TopologyConfig {
                    vnodes_per_pnode: 1,
                    replication_factor: 1,
                },
            )
            .channel()
            .1;
            let security = SecurityActor::spawn(
                local.clone(),
                swim.clone(),
                raft,
                topology.clone(),
                NodeTransportSecurity::TrustedDevelopment,
            );
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let (data, mailbox) = flume::bounded(1);
            let (_commands, commands_rx) = mpsc::channel(1);
            let actor = tokio::spawn(DataTransportActor::run(
                local,
                listener,
                DataPlaneSender(data),
                commands_rx,
                swim,
                topology,
                security,
            ));
            let address = (turmoil::lookup("node"), 9000);
            let _stalled = TcpStream::connect(address).await?;
            let mut peer = TcpStream::connect(address).await?;
            connection::write_frame(&mut peer, &NodeId::new("peer"), 1024).await?;
            let message = ReceivePeerMessage {
                from: NodeId::new("peer"),
                message: Box::new(
                    DeleteSegments {
                        segment_keys: Box::new([]),
                    }
                    .into(),
                ),
            };
            connection::write_frame(&mut peer, &message, connection::DATA_FRAME_MAX).await?;
            let received =
                tokio::time::timeout(std::time::Duration::from_secs(1), mailbox.recv_async())
                    .await??;
            let DataPlaneMessage::Command(DataPlaneCommand::ReceivePeerMessage(received)) =
                received
            else {
                panic!("expected admitted peer message");
            };
            assert_eq!(received.from, NodeId::new("peer"));
            actor.abort();
            Ok(())
        });
        sim.run()
    }
}
