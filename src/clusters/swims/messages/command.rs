use std::net::SocketAddr;

use crate::clusters::raft::messages::LeaderChange;
use crate::clusters::swims::peer_discovery::JoinAttempt;
use crate::clusters::{NodeId, SwimNode};
use crate::schedulers::ticker_message::TimerCommand;

use super::packet::{OutboundPacket, SwimPacket};
use super::timer::SwimTimer;

/// Internal Events (Actor Logic)
#[derive(Debug)]
pub enum SwimCommand {
    // From Transport
    PacketReceived { src: SocketAddr, packet: SwimPacket },
    // From Ticker
    Timeout(SwimTimeOutCallback),
    Query(SwimQueryCommand),
    // From RaftActor — leader election completed for a shard group
    AnnounceShardLeader(LeaderChange),
}

#[derive(Debug)]
pub enum SwimQueryCommand {
    GetMembers {
        reply: tokio::sync::oneshot::Sender<Vec<SwimNode>>,
    },
    ResolveAddress {
        node_id: NodeId,
        reply: tokio::sync::oneshot::Sender<Option<SocketAddr>>,
    },
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
        seq: u32,
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
/// Consumed by the RaftActor to drive shard group lifecycle.
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
}
