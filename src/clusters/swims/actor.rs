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

        let mut buf = Vec::with_capacity(64);
        loop {
            if mailbox.recv_many(&mut buf, 64).await == 0 {
                break;
            }
            for event in buf.drain(..) {
                state.process(event);
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
        for event in state.take_events() {
            match event {
                SwimEvent::Packet(pkt) => {
                    let _ = transport_tx.send(pkt).await;
                }
                SwimEvent::Timer(cmd) => {
                    let _ = scheduler_tx.send(cmd.into()).await;
                }
                SwimEvent::Membership(m) => {
                    if let Some(cmd) = Self::to_raft_command(state, m) {
                        let _ = raft_tx.send(cmd).await;
                    }
                }
            }
        }
    }

    /// Translate a membership event into a MultiRaftActor command.
    /// Returns `None` for self-events and nodes with no shard groups.
    fn to_raft_command(state: &Swim, event: MembershipEvent) -> Option<RaftCommand> {
        if *event.node_id() == state.node_id {
            return None;
        }

        match event {
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
        }
    }
}
