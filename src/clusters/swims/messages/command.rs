use std::net::SocketAddr;

use crate::clusters::NodeId;
use crate::clusters::raft::messages::{
    HandleNodeDeath, HandleNodeJoin, LeaderChange, MultiRaftActorCommand,
};
use crate::clusters::swims::peer_discovery::JoinAttempt;
use crate::clusters::swims::topology::Topology;
use crate::schedulers::ticker_message::TimerCommand;

use super::packet::{OutboundPacket, SwimPacket};
use super::timer::SwimTimer;

/// Inputs to the Swim state machine (tokio-free, WASM-safe).
#[derive(Debug)]
pub(crate) enum SwimCommand {
    PacketReceived { src: SocketAddr, packet: SwimPacket },
    Timeout(SwimTimeOutCallback),
    AnnounceShardLeader(LeaderChange),
}

impl From<SwimTimeOutCallback> for SwimCommand {
    fn from(value: SwimTimeOutCallback) -> Self {
        SwimCommand::Timeout(value)
    }
}

#[derive(Debug, Default)]
pub(crate) enum SwimTimeOutCallback {
    #[default]
    ProtocolPeriodElapsed,
    TimedOut {
        seq: u64,
        target_node_id: Option<NodeId>,
        phase: SwimTimerKind,
    },
}

#[derive(Debug)]
pub(crate) enum SwimTimerKind {
    DirectProbe,
    IndirectProbe,
    Suspect,
    JoinTry(JoinAttempt),
    ProxyPing,
}

/// Unified side-effect type emitted by the Swim state machine.
/// The actor layer drains these and routes each variant to the
/// appropriate channel (transport / scheduler / raft).
#[derive(Debug)]
pub enum SwimEvent {
    Packet(OutboundPacket),
    Timer(TimerCommand<SwimTimer>),
    Membership(MembershipEvent),
}

/// Membership change events emitted by the SWIM state machine.
/// Consumed by the MultiRaftActor to drive shard group lifecycle.
#[derive(Debug)]
#[allow(dead_code)] // NodeAlive fields used when node join handling is added
pub enum MembershipEvent {
    NodeAlive { node_id: NodeId, addr: SocketAddr },
    NodeDead { node_id: NodeId },
}

impl MembershipEvent {
    pub(crate) fn node_id(&self) -> &NodeId {
        match self {
            MembershipEvent::NodeAlive { node_id, .. } => node_id,
            MembershipEvent::NodeDead { node_id } => node_id,
        }
    }

    pub(crate) fn into_raft_command(
        self,
        local_node_id: &NodeId,
        topology: &Topology,
    ) -> Option<MultiRaftActorCommand> {
        if self.node_id() == local_node_id {
            return None;
        }
        match self {
            MembershipEvent::NodeDead { node_id } => Some(
                HandleNodeDeath {
                    dead_node_id: node_id,
                }
                .into(),
            ),
            MembershipEvent::NodeAlive { node_id, .. } => {
                let affected_groups: Vec<_> = topology
                    .shard_groups_for_node(&node_id)
                    .into_iter()
                    .cloned()
                    .collect();
                if affected_groups.is_empty() {
                    return None;
                }
                Some(
                    HandleNodeJoin {
                        new_node_id: node_id,
                        affected_groups,
                    }
                    .into(),
                )
            }
        }
    }
}
