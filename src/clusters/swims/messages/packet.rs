use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

use bincode::{Decode, Encode};

use crate::clusters::{NodeId, SwimNode};

use super::dissemination_buffer::ShardLeaderInfo;

#[derive(Clone, Debug, Encode, Decode)]
pub struct SwimHeader {
    pub seq: u32,
    pub source_node_id: NodeId,
    pub source_incarnation: u64,
    pub gossip: Vec<SwimNode>,
    pub shard_leaders: Vec<ShardLeaderInfo>,
}

/// The Wire Format (What goes over UDP)
#[derive(Clone, Debug, Encode, Decode)]
pub enum SwimPacket {
    Ping(SwimHeader),
    Ack(SwimHeader),
    PingReq {
        target: SocketAddr,
        header: SwimHeader,
    },
}

impl SwimPacket {
    pub(crate) fn gossip(&self) -> &[SwimNode] {
        match self {
            SwimPacket::Ping(swim_header) => &swim_header.gossip,
            SwimPacket::Ack(swim_header) => &swim_header.gossip,
            SwimPacket::PingReq { header, .. } => &header.gossip,
        }
    }

    pub(crate) fn shard_leaders(&self) -> &[ShardLeaderInfo] {
        match self {
            SwimPacket::Ping(h) => &h.shard_leaders,
            SwimPacket::Ack(h) => &h.shard_leaders,
            SwimPacket::PingReq { header, .. } => &header.shard_leaders,
        }
    }
}

impl Display for SwimPacket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (kind, header, target) = match self {
            Self::Ping(h) => ("PING", h, None),
            Self::Ack(h) => ("ACK", h, None),
            Self::PingReq { target, header } => ("PINGREQ", header, Some(target)),
        };

        write!(f, "[{}] seq: {}, ", kind, header.seq)?;

        if let Some(t) = target {
            write!(f, "target: {}, ", t)?;
        }

        write!(
            f,
            "source: {} (inc:{}), gossip: {}",
            header.source_node_id,
            header.source_incarnation,
            header.gossip.len()
        )
    }
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

impl Display for OutboundPacket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Target: {}, Packet: {}", self.target, self.packet)
    }
}
