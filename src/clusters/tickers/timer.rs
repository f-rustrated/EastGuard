use crate::clusters::tickers::ticker::INDIRECT_ACK_TIMEOUT_TICKS;
use crate::clusters::{NodeId, TickEvent, tickers::ticker::DIRECT_ACK_TIMEOUT_TICKS};

#[derive(Debug)]
pub(crate) struct ProbeTimer {
    pub(crate) target_node_id: NodeId,
    pub(crate) phase: ProbePhase,
    pub(crate) ticks_remaining: u32,
}
impl ProbeTimer {
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
    pub(crate) fn to_timeout_event(self, seq: u32) -> TickEvent {
        match self.phase {
            ProbePhase::Direct => TickEvent::DirectProbeTimedOut {
                seq: seq,
                target_node_id: self.target_node_id,
            },
            ProbePhase::Indirect => TickEvent::IndirectProbeTimedOut {
                seq: seq,
                target_node_id: self.target_node_id,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbePhase {
    Direct,
    Indirect,
}
