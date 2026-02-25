use std::net::SocketAddr;

use bincode::{Decode, Encode};

use crate::clusters::{
    NodeId, SwimNode,
    tickers::{
        ticker::{DIRECT_ACK_TIMEOUT_TICKS, INDIRECT_ACK_TIMEOUT_TICKS, SUSPECT_TIMEOUT_TICKS},
        timer::TTimer,
    },
};

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
    PacketReceived { src: SocketAddr, packet: SwimPacket },
    // From Ticker
    Timeout(TimeoutEvent),
}

impl From<TimeoutEvent> for SwimCommand {
    fn from(value: TimeoutEvent) -> Self {
        SwimCommand::Timeout(value)
    }
}

#[derive(Debug)]
pub(crate) enum TimeoutEvent {
    ProtocolPeriodElapsed,
    DirectProbeTimedOut { seq: u32, target_node_id: NodeId },
    IndirectProbeTimedOut { seq: u32, target_node_id: NodeId },
    SuspectTimedOut { node_id: NodeId },
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
pub(crate) struct SwimTimeOutSchedule {
    target_node_id: NodeId,
    phase: ProbePhase,
    ticks_remaining: u32,
}

impl TTimer for SwimTimeOutSchedule {
    fn tick(&mut self) -> u32 {
        self.ticks_remaining -= 1;
        self.ticks_remaining
    }

    fn to_timeout_event(self: Box<Self>, seq: u32) -> TimeoutEvent {
        match self.phase {
            ProbePhase::Direct => TimeoutEvent::DirectProbeTimedOut {
                seq: seq,
                target_node_id: self.target_node_id,
            },
            ProbePhase::Indirect => TimeoutEvent::IndirectProbeTimedOut {
                seq: seq,
                target_node_id: self.target_node_id,
            },
            ProbePhase::Suspect => TimeoutEvent::SuspectTimedOut {
                node_id: self.target_node_id,
            },
        }
    }

    #[cfg(test)]
    fn target_node_id(&self) -> NodeId {
        self.target_node_id.clone()
    }
}
impl SwimTimeOutSchedule {
    pub(crate) fn direct_probe(target: NodeId) -> Self {
        Self {
            target_node_id: target,
            phase: ProbePhase::Direct,
            ticks_remaining: DIRECT_ACK_TIMEOUT_TICKS,
        }
    }

    pub(crate) fn indirect_probe(target: NodeId) -> Self {
        Self {
            target_node_id: target,
            phase: ProbePhase::Indirect,
            ticks_remaining: INDIRECT_ACK_TIMEOUT_TICKS,
        }
    }

    pub(crate) fn suspect_timer(target: NodeId) -> Self {
        Self {
            target_node_id: target,
            phase: ProbePhase::Suspect,
            ticks_remaining: SUSPECT_TIMEOUT_TICKS,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbePhase {
    Direct,
    Indirect,
    Suspect,
}
