use std::net::SocketAddr;

use bincode::{Decode, Encode};

use crate::clusters::{NodeId, SwimNode};

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

    Tick(TickEvent),
}

impl From<TickEvent> for SwimCommand {
    fn from(value: TickEvent) -> Self {
        SwimCommand::Tick(value)
    }
}

#[derive(Debug)]
pub(crate) enum TickEvent {
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
