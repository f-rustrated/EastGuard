use std::net::SocketAddr;

use crate::clusters::{JoinAttempt, NodeId, SwimNode};
use crate::schedulers::{
    ticker::{DIRECT_ACK_TIMEOUT_TICKS, INDIRECT_ACK_TIMEOUT_TICKS, SUSPECT_TIMEOUT_TICKS},
    timer::TTimer,
};
use bincode::{Decode, Encode};

/// The Wire Format (What goes over UDP)
#[derive(Clone, Debug, Encode, Decode)]
pub enum SwimPacket {
    Ping {
        seq: u32,
        source_node_id: NodeId,
        source_incarnation: u64,
        gossip: Vec<SwimNode>,
    },
    Ack {
        seq: u32,
        source_node_id: NodeId,
        source_incarnation: u64,
        gossip: Vec<SwimNode>,
    },
    PingReq {
        seq: u32,
        target: SocketAddr,
        source_node_id: NodeId,
        source_incarnation: u64,
        gossip: Vec<SwimNode>,
    },
}

/// Internal Events (Actor Logic)
#[derive(Debug)]
pub enum SwimCommand {
    // From Transport
    PacketReceived {
        src: SocketAddr,
        packet: SwimPacket,
    },
    // From Ticker
    Timeout(SwimTimeOutCallback),
    #[cfg(test)]
    Test(SwimTestCommand),
}

#[cfg(test)]
#[derive(Debug)]
pub enum SwimTestCommand {
    TopologyValidationCount {
        reply: tokio::sync::oneshot::Sender<usize>,
    },
    TopologyIncludesNode {
        node_id: NodeId,
        reply: tokio::sync::oneshot::Sender<bool>,
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
}

/// Outbound Commands (Logic -> Transport)
#[derive(Debug)]
pub struct OutboundPacket {
    pub target: SocketAddr,
    packet: SwimPacket,
}

impl OutboundPacket {
    pub(crate) fn new(target: SocketAddr, packet: SwimPacket) -> Self {
        OutboundPacket { target, packet }
    }

    pub fn packet(&self) -> &SwimPacket {
        &self.packet
    }
}

#[derive(Debug)]
pub(crate) struct SwimTimer {
    target_node_id: Option<NodeId>,
    kind: SwimTimerKind,
    ticks_remaining: u32,
}

impl TTimer for SwimTimer {
    type Callback = SwimTimeOutCallback;

    fn tick(&mut self) -> u32 {
        self.ticks_remaining -= 1;
        self.ticks_remaining
    }

    fn to_timeout_callback(self, seq: u32) -> SwimTimeOutCallback {
        SwimTimeOutCallback::TimedOut {
            seq: seq,
            target_node_id: self.target_node_id,
            phase: self.kind,
        }
    }

    #[cfg(test)]
    fn target_node_id(&self) -> Option<NodeId> {
        self.target_node_id.clone()
    }
}
impl SwimTimer {
    pub(crate) fn direct_probe(target: NodeId) -> Self {
        Self {
            target_node_id: Some(target),
            kind: SwimTimerKind::DirectProbe,
            ticks_remaining: DIRECT_ACK_TIMEOUT_TICKS,
        }
    }

    pub(crate) fn indirect_probe(target: NodeId) -> Self {
        Self {
            target_node_id: Some(target),
            kind: SwimTimerKind::IndirectProbe,
            ticks_remaining: INDIRECT_ACK_TIMEOUT_TICKS,
        }
    }

    pub(crate) fn suspect_timer(target: NodeId) -> Self {
        Self {
            target_node_id: Some(target),
            kind: SwimTimerKind::Suspect,
            ticks_remaining: SUSPECT_TIMEOUT_TICKS,
        }
    }

    pub(crate) fn join_try(join_try: JoinAttempt) -> Self {
        Self {
            target_node_id: None,
            ticks_remaining: join_try.ticks_for_wait,
            kind: SwimTimerKind::JoinTry(join_try),
        }
    }
}
