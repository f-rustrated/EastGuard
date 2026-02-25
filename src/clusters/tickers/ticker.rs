use crate::clusters::tickers::timer::TTimer;

use crate::clusters::TimeoutEvent;
use crate::clusters::types::ticker_message::TimerCommand;
use std::collections::HashMap;

pub(crate) const PROBE_INTERVAL_TICKS: u32 = 10; // 10 × 100ms = 1s
pub(crate) const DIRECT_ACK_TIMEOUT_TICKS: u32 = 3; // 3 × 100ms = 300ms
pub(crate) const INDIRECT_ACK_TIMEOUT_TICKS: u32 = 3; // 3 × 100ms = 300ms
pub(crate) const SUSPECT_TIMEOUT_TICKS: u32 = 50; // 50 × 100ms = 5s

#[derive(Default, Debug)]
pub(crate) struct Ticker {
    protocol_elapsed: u32,
    timers: HashMap<u32, Box<dyn TTimer>>,
}

impl Ticker {
    pub(crate) fn apply(&mut self, cmd: TimerCommand) {
        match cmd {
            TimerCommand::SetSchedule { seq, timer } => {
                self.timers.insert(seq, timer);
            }

            TimerCommand::CancelSchedule { seq } => {
                self.timers.remove(&seq);
            }
        }
    }

    pub fn advance_clock(&mut self) -> Vec<TimeoutEvent> {
        let mut events = Vec::new();

        // 1. Age every in-flight probe
        let mut timeout_seqs: Vec<u32> = vec![];
        for (seq, probe) in self.timers.iter_mut() {
            if probe.tick() == 0 {
                timeout_seqs.push(*seq);
            }
        }

        for seq in timeout_seqs {
            let probe = self.timers.remove(&seq).unwrap();
            events.push(probe.to_timeout_event(seq));
        }

        // 2. Advance the protocol clock
        self.protocol_elapsed += 1;
        if self.protocol_elapsed >= PROBE_INTERVAL_TICKS {
            self.protocol_elapsed = 0;
            events.push(TimeoutEvent::ProtocolPeriodElapsed);
        }

        events
    }

    #[cfg(test)]
    pub fn probe_seq_for(&self, node_id: &str) -> Option<u32> {
        self.timers
            .iter()
            .find(|(_, probe)| &*probe.target_node_id() == node_id)
            .map(|(&seq, _)| seq)
    }

    #[cfg(test)]
    pub fn has_timer(&self, seq: u32) -> bool {
        self.timers.contains_key(&seq)
    }
}

#[cfg(test)]
mod tests {

    use crate::clusters::SwimTimeOutSchedule;

    use super::*;

    #[test]
    fn no_protocol_period_before_interval_elapses() {
        let mut ticker = Ticker::default();
        for _ in 0..PROBE_INTERVAL_TICKS - 1 {
            let events = ticker.advance_clock();
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, TimeoutEvent::ProtocolPeriodElapsed)),
            );
        }
    }

    #[test]
    fn protocol_period_fires_at_interval() {
        let mut ticker = Ticker::default();
        for _ in 0..PROBE_INTERVAL_TICKS - 1 {
            ticker.advance_clock();
        }
        let events = ticker.advance_clock();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, TimeoutEvent::ProtocolPeriodElapsed))
        );
    }

    #[test]
    fn direct_probe_timeout() {
        let mut ticker = Ticker::default();
        ticker.apply(TimerCommand::SetSchedule {
            seq: 1,
            timer: Box::new(SwimTimeOutSchedule::direct_probe("node-b".into())),
        });

        for _ in 0..DIRECT_ACK_TIMEOUT_TICKS - 1 {
            let events = ticker.advance_clock();
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, TimeoutEvent::DirectProbeTimedOut { .. }))
            );
        }

        let events = ticker.advance_clock();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, TimeoutEvent::DirectProbeTimedOut { seq: 1, .. }))
        );
        assert!(!ticker.has_timer(1));
    }

    #[test]
    fn indirect_probe_timeout() {
        let mut ticker = Ticker::default();
        ticker.apply(TimerCommand::SetSchedule {
            seq: 2,
            timer: Box::new(SwimTimeOutSchedule::indirect_probe("node-c".into())),
        });

        for _ in 0..INDIRECT_ACK_TIMEOUT_TICKS - 1 {
            ticker.advance_clock();
        }

        let events = ticker.advance_clock();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, TimeoutEvent::IndirectProbeTimedOut { seq: 2, .. }))
        );
    }

    #[test]
    fn cancel_probe_prevents_timeout() {
        let mut ticker = Ticker::default();
        ticker.apply(TimerCommand::SetSchedule {
            seq: 1,
            timer: Box::new(SwimTimeOutSchedule::direct_probe("node-b".into())),
        });
        ticker.apply(TimerCommand::CancelSchedule { seq: 1 });

        for _ in 0..DIRECT_ACK_TIMEOUT_TICKS + 1 {
            let events = ticker.advance_clock();
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, TimeoutEvent::DirectProbeTimedOut { .. }))
            );
        }
    }

    #[test]
    fn suspect_timer_fires_after_timeout() {
        let mut ticker = Ticker::default();
        let node_id = "node-b";
        let seq = 1;
        ticker.apply(TimerCommand::SetSchedule {
            timer: Box::new(SwimTimeOutSchedule::suspect_timer(node_id.into())),
            seq,
        });

        for _ in 0..SUSPECT_TIMEOUT_TICKS - 1 {
            let events = ticker.advance_clock();
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, TimeoutEvent::SuspectTimedOut { .. }))
            );
        }

        let events = ticker.advance_clock();

        assert!(
            events
                .iter()
                .any(|e| matches!(e, TimeoutEvent::SuspectTimedOut { .. }))
        );
        assert!(
            !ticker.has_timer(seq),
            "There shouldn't be any existing suspect"
        );
    }
}
