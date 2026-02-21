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
pub enum ActorEvent {
    // From Transport
    PacketReceived { src: SocketAddr, packet: SwimPacket },

    // Internal Timers
    ProtocolTick,
    DirectProbeTimeout { target_node_id: NodeId, seq: u32 },
    IndirectProbeTimeout { target_node_id: NodeId },
    SuspectTimeout { target_node_id: NodeId },
}

/// Outbound Commands (Logic -> Transport)
#[derive(Debug)]
pub struct OutboundPacket {
    pub target: SocketAddr,
    pub packet: SwimPacket,
}
