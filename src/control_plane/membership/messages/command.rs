use std::net::SocketAddr;

use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::LeaderChange;
use crate::control_plane::membership::peer_discovery::JoinAttempt;
use crate::control_plane::membership::{MembershipEvent, NodeAlive, NodeDead};
use crate::schedulers::ticker_message::TimerCommand;
use crate::{impl_from_variant, impl_from_variant_via};

use super::packet::{OutboundPacket, SwimPacket};
use super::timer::SwimTimer;

/// Inputs to the Swim state machine (tokio-free, WASM-safe).
#[derive(Debug)]
pub(crate) enum SwimCommand {
    InboundRaftRpc { src: SocketAddr, packet: SwimPacket },
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

#[derive(Debug, Clone)]
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
    MembershipEvent(MembershipEvent),
}

impl_from_variant!(
    SwimEvent,
    Timer(TimerCommand<SwimTimer>),
    MembershipEvent(MembershipEvent)
);

impl_from_variant_via!(SwimEvent, MembershipEvent, NodeAlive, NodeDead);
