// ==========================================
// MESSAGES & DATA STRUCTURES
// ==========================================

use bincode::{Decode, Encode};
use std::net::SocketAddr;
pub(crate) mod swim;

pub(crate) mod transport;

#[cfg(test)]
pub mod tests;
pub mod topology;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[derive(Hash, Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Encode, Decode)]
pub struct PhysicalNodeId(pub String);

impl From<&str> for PhysicalNodeId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for PhysicalNodeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// TODO: Let users define their own NodeId using config
impl From<SocketAddr> for PhysicalNodeId {
    fn from(addr: SocketAddr) -> Self {
        Self(addr.to_string())
    }
}

/// Membership events from SWIM; may fire repeatedly for the same node — listeners must be idempotent.
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    NodeAlive {
        id: PhysicalNodeId,
        addr: SocketAddr,
    },
    NodeDead {
        id: PhysicalNodeId,
    },
}

/// The Wire Format (What goes over UDP)
#[derive(Clone, Debug, Encode, Decode)]
pub enum SwimPacket {
    Ping {
        seq: u32,
        source_incarnation: u64,
        gossip: Vec<Member>,
    },
    Ack {
        seq: u32,
        source_incarnation: u64,
        gossip: Vec<Member>,
    },
    PingReq {
        seq: u32,
        target: SocketAddr,
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
    DirectProbeTimeout { target: SocketAddr, seq: u32 },
    IndirectProbeTimeout { target: SocketAddr },
    SuspectTimeout { target: SocketAddr },
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

#[derive(Debug, Clone, Encode, Decode)]
pub struct Member {
    pub addr: SocketAddr,
    pub state: NodeState,
    pub incarnation: u64,
}
