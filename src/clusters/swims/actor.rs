use super::*;

use crate::clusters::raft::actor::RaftCommand;
use crate::clusters::swims::swim::Swim;
use crate::schedulers::ticker_message::TickerCommand;

use tokio::sync::mpsc;

// ==========================================
// PROTOCOL LAYER (SWIM Actor)
// ==========================================

pub struct SwimActor {
    mailbox: mpsc::Receiver<SwimCommand>,
    transport_tx: mpsc::Sender<OutboundPacket>,
    scheduler_tx: mpsc::Sender<TickerCommand<SwimTimer>>,
    raft_tx: mpsc::Sender<RaftCommand>,
}

impl SwimActor {
    pub fn new(
        mailbox: mpsc::Receiver<SwimCommand>,
        transport_tx: mpsc::Sender<OutboundPacket>,
        ticker_tx: mpsc::Sender<TickerCommand<SwimTimer>>,
        raft_tx: mpsc::Sender<RaftCommand>,
    ) -> Self {
        Self {
            mailbox,
            transport_tx,
            scheduler_tx: ticker_tx,
            raft_tx,
        }
    }

    pub async fn run(mut self, mut state: Swim) {
        tracing::info!("[{}] SwimActor started.", state.node_id);
        self.flush_outbound_commands(&mut state).await;

        while let Some(event) = self.mailbox.recv().await {
            match event {
                SwimCommand::PacketReceived { src, packet } => {
                    state.step(src, packet);
                }

                SwimCommand::Timeout(tick_event) => {
                    state.handle_timeout(tick_event);
                }
                SwimCommand::Query(command) => state.handle_query(command),
            }

            self.flush_outbound_commands(&mut state).await;
        }
    }

    async fn flush_outbound_commands(&mut self, state: &mut Swim) {
        let timer_commands = state.take_timer_commands();
        let outbound_packets = state.take_outbound();
        let membership_events = state.take_membership_events();

        // Translate membership events into RaftActor commands.
        // Topology is available synchronously via state.topology — no async roundtrip.
        let raft_commands: Vec<RaftCommand> = membership_events
            .into_iter()
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
                    let _ = self.scheduler_tx.send(cmd.into()).await;
                }
            },
            async {
                for pkt in outbound_packets {
                    tracing::debug!("[PACKET] {}", pkt);
                    let _ = self.transport_tx.send(pkt).await;
                }
            },
            async {
                for cmd in raft_commands {
                    let _ = self.raft_tx.send(cmd).await;
                }
            }
        );
    }
}
