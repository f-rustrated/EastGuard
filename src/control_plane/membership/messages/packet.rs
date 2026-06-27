use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::{NodeAddress, NodeId, SwimNode};

use super::dissemination_buffer::ShardLeaderInfo;

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct SwimHeader {
    pub seq: u64,
    pub source_node_id: NodeId,
    pub source_addr: NodeAddress,
    pub source_incarnation: u64,
    pub gossip: Vec<SwimNode>,
    pub shard_leaders: Vec<ShardLeaderInfo>,
}

/// The Wire Format (What goes over UDP)
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
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

impl SwimPacket {
    fn kind_header_target(&self) -> (&str, &SwimHeader, Option<&SocketAddr>) {
        match self {
            Self::Ping(h) => ("PING", h, None),
            Self::Ack(h) => ("ACK", h, None),
            Self::PingReq { target, header } => ("PINGREQ", header, Some(target)),
        }
    }
}

impl Display for SwimPacket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (kind, header, target) = self.kind_header_target();
        let target_str = target
            .map(|t| format!(", target: {}", t))
            .unwrap_or_default();
        write!(
            f,
            "[{}] seq: {}{}, source: {} (inc:{}), gossip: {}",
            kind,
            header.seq,
            target_str,
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
