use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

use crate::clusters::swims::Topology;
use crate::clusters::swims::peer_discovery::JoinAttempt;
use crate::clusters::{NodeId, SwimNode};
use crate::schedulers::{
    ticker::{
        DIRECT_ACK_TIMEOUT_TICKS, INDIRECT_ACK_TIMEOUT_TICKS, SUSPECT_TIMEOUT_TICKS,
        TOMBSTONE_TIMEOUT_TICKS,
    },
    timer::TTimer,
};
use bincode::{Decode, Encode};

#[derive(Clone, Debug, Encode, Decode)]
pub struct SwimHeader {
    pub seq: u32,
    pub source_node_id: NodeId,
    pub source_incarnation: u64,
    pub gossip: Vec<SwimNode>,
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

/// Internal Events (Actor Logic)
#[derive(Debug)]
pub enum SwimCommand {
    // From Transport
    PacketReceived { src: SocketAddr, packet: SwimPacket },
    // From Ticker
    Timeout(SwimTimeOutCallback),
    Query(SwimQueryCommand),
}

#[derive(Debug)]
pub enum SwimQueryCommand {
    GetMembers {
        reply: tokio::sync::oneshot::Sender<Vec<SwimNode>>,
    },
    GetTopology {
        reply: tokio::sync::oneshot::Sender<Topology>,
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
    Tombstone,
    JoinTry(JoinAttempt),
    ProxyPing,
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

    pub(crate) fn proxy_ping() -> Self {
        Self {
            target_node_id: None,
            kind: SwimTimerKind::ProxyPing,
            ticks_remaining: DIRECT_ACK_TIMEOUT_TICKS,
        }
    }

    pub(crate) fn join_try(join_try: JoinAttempt) -> Self {
        Self {
            target_node_id: None,
            ticks_remaining: join_try.ticks_for_wait,
            kind: SwimTimerKind::JoinTry(join_try),
        }
    }

    pub(crate) fn tombstone(target: NodeId) -> Self {
        Self {
            target_node_id: Some(target),
            kind: SwimTimerKind::Tombstone,
            ticks_remaining: TOMBSTONE_TIMEOUT_TICKS,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ProxyPing {
    pub(crate) requester_addr: SocketAddr,
    pub(crate) request_seq: u32,
}
