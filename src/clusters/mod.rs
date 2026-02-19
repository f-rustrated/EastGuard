// ==========================================
// MESSAGES & DATA STRUCTURES
// ==========================================

use bincode::{Decode, Encode};
use std::net::SocketAddr;
mod livenode_tracker;
pub(crate) mod swim;
pub(crate) mod topology;
pub(crate) mod transport;

#[cfg(test)]
pub mod tests;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

type NodeId = String;

/// The Wire Format (What goes over UDP)
#[derive(Clone, Debug, Encode, Decode)]
pub enum SwimPacket {
    Ping {
        seq: u32,
        source_node_id: NodeId,
        source_incarnation: u64,
        gossip: Vec<Member>,
    },
    Ack {
        seq: u32,
        source_node_id: NodeId,
        source_incarnation: u64,
        gossip: Vec<Member>,
    },
    PingReq {
        seq: u32,
        target: SocketAddr,
        source_node_id: NodeId,
        source_incarnation: u64,
        gossip: Vec<Member>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub enum NodeState {
    Alive,
    Suspect,
    Dead,
}

// Used to decide what to say. You must include Dead/Suspect nodes in your messages so that other nodes learn about these failures.
#[derive(Debug, Clone, Encode, Decode)]
pub struct Member {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub state: NodeState,
    pub incarnation: u64,
}
