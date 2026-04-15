use super::*;

use crate::clusters::raft::actor::RaftCommand;
use crate::clusters::swims::swim::Swim;
use crate::schedulers::ticker_message::TickerCommand;

use tokio::sync::mpsc;

// ==========================================
// PROTOCOL LAYER (SWIM Actor)
// ==========================================

pub struct SwimActor;

impl SwimActor {
    pub async fn run(
        mut mailbox: mpsc::Receiver<SwimCommand>,
        mut state: Swim,
        transport_tx: mpsc::Sender<OutboundPacket>,
        scheduler_tx: mpsc::Sender<TickerCommand<SwimTimer>>,
        raft_tx: mpsc::Sender<RaftCommand>,
    ) {
        tracing::info!("[{}] SwimActor started.", state.node_id);
        Self::flush(&mut state, &transport_tx, &scheduler_tx, &raft_tx).await;

        while let Some(event) = mailbox.recv().await {
            match event {
                SwimCommand::PacketReceived { src, packet } => {
                    state.step(src, packet);
                }

                SwimCommand::Timeout(tick_event) => {
                    state.handle_timeout(tick_event);
                }
                SwimCommand::Query(command) => state.handle_query(command),
            }

            Self::flush(&mut state, &transport_tx, &scheduler_tx, &raft_tx).await;
        }
    }

    async fn flush(
        state: &mut Swim,
        transport_tx: &mpsc::Sender<OutboundPacket>,
        scheduler_tx: &mpsc::Sender<TickerCommand<SwimTimer>>,
        raft_tx: &mpsc::Sender<RaftCommand>,
    ) {
        let timer_commands = state.take_timer_commands();
        let outbound_packets = state.take_outbound();
        let membership_events = state.take_membership_events();

        // Translate membership events into RaftActor commands.
        // Topology is available synchronously via state.topology — no async roundtrip.
        // Skip events about our own node — self-registration at startup is not a
        // membership change that Raft needs to act on.
        let raft_commands: Vec<RaftCommand> = membership_events
            .into_iter()
            .filter(|event| match event {
                MembershipEvent::NodeAlive { node_id, .. }
                | MembershipEvent::NodeDead { node_id } => *node_id != state.node_id,
            })
            .filter_map(|event| match event {
                MembershipEvent::NodeDead { node_id } => Some(RaftCommand::HandleNodeDeath {
                    dead_node_id: node_id,
                }),
                MembershipEvent::NodeAlive { node_id, .. } => {
                    let affected_groups = state.topology.shard_groups_for_node(&node_id);
                    if affected_groups.is_empty() {
                        return None;
                    }
                    Some(RaftCommand::HandleNodeJoin {
                        new_node_id: node_id,
                        affected_groups,
                    })
                }
            })
            .collect();

        tokio::join!(
            async {
                for cmd in timer_commands {
                    tracing::debug!("[TIMER] {}", cmd);
                    let _ = scheduler_tx.send(cmd.into()).await;
                }
            },
            async {
                for pkt in outbound_packets {
                    tracing::debug!("[PACKET] {}", pkt);
                    let _ = transport_tx.send(pkt).await;
                }
            },
            async {
                for cmd in raft_commands {
                    let _ = raft_tx.send(cmd).await;
                }
            }
        );
    }
}
