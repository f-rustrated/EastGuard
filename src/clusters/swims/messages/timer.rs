use crate::clusters::NodeId;
use crate::clusters::swims::peer_discovery::JoinAttempt;
use crate::schedulers::timer::TTimer;

use super::command::{SwimTimeOutCallback, SwimTimerKind};

pub(crate) const DIRECT_ACK_TIMEOUT_TICKS: u32 = 3; // 3 × 100ms = 300ms
pub(crate) const INDIRECT_ACK_TIMEOUT_TICKS: u32 = 6; // 6 × 100ms = 600ms (must cover 4-hop RTT through intermediary, so it should be at least * 2 of DIRECT_ACK_TIMEOUT_TICKS)
pub(crate) const SUSPECT_TIMEOUT_TICKS: u32 = 50; // 50 × 100ms = 5s

#[derive(Debug)]
pub(crate) struct SwimTimer {
    target_node_id: Option<NodeId>,
    kind: SwimTimerKind,
    ticks_remaining: u32,
}

impl TTimer for SwimTimer {
    type Callback = SwimTimeOutCallback;

    fn tick(&mut self) -> u32 {
        self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        self.ticks_remaining
    }

    fn to_timeout_callback(self, seq: u32) -> SwimTimeOutCallback {
        SwimTimeOutCallback::TimedOut {
            seq,
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
}

#[derive(Debug)]
pub(crate) struct ProxyPing {
    pub(crate) requester_addr: std::net::SocketAddr,
    pub(crate) request_seq: u32,
}
