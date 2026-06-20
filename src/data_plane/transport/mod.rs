pub(crate) mod command;
mod reader;
mod writers;

use tokio::sync::mpsc;

use crate::control_plane::NodeId;
use crate::control_plane::membership::TopologyReader;
use crate::control_plane::membership::actor::SwimSender;
use crate::data_plane::actor::DataPlaneSender;

use crate::net::TcpListener;

use command::DataTransportCommand;
use writers::TransportState;

pub struct DataTransportActor;

impl DataTransportActor {
    pub async fn run(
        node_id: NodeId,
        listener: TcpListener,
        data_plane_tx: DataPlaneSender,
        mut from_actor: mpsc::Receiver<Box<[DataTransportCommand]>>,
        swim_tx: SwimSender,
        topology: TopologyReader,
    ) {
        let mut state = TransportState::new(node_id);
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(300));
        cleanup_interval.tick().await;

        // Reader tasks report here when their connection ends, so the writer for
        // that peer is evicted and the next send reconnects (see DataReader::run).
        let (disconnect_tx, mut disconnect_rx) = mpsc::channel::<NodeId>(64);

        loop {
            tokio::select! {
                biased;
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
                                    state.send(&[entry.leader_node_id], &cmd.message, &swim_tx, &data_plane_tx, &disconnect_tx).await;
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
                    match state.accept(stream).await {
                        Ok((peer, reader)) => {
                            tokio::spawn(reader.run(data_plane_tx.clone(), peer, disconnect_tx.clone()));
                        }
                        Err(e) => {
                            tracing::debug!("Data accept rejected: {e}");
                        }
                    }
                }

                Some(peer) = disconnect_rx.recv() => {
                    state.evict_writer(&peer);
                }

                _ = cleanup_interval.tick() => {
                    state.cleanup_dead_peers();
                }
            }
        }
    }
}
