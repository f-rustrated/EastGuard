use crate::schedulers::{ticker_message::TimerCommand, timer::TTimer};
use std::collections::HashMap;

pub const TICK_PERIOD_100_MS: u64 = 100;
#[allow(dead_code)]
pub const TICK_PERIOD_10_MS: u64 = 10;
pub(crate) const PROBE_INTERVAL_TICKS: u64 = 10; // 10 × 100ms = 1s

#[derive(Debug)]
pub(crate) struct Ticker<T> {
    protocol_elapsed: u64,
    protocol_interval_ticks: Option<u64>,
    timers: HashMap<u64, T>,
}

impl<T> Ticker<T>
where
    T: TTimer,
{
    pub(crate) fn new(protocol_interval_ticks: Option<u64>) -> Self {
        Self {
            protocol_elapsed: 0,
            protocol_interval_ticks,
            timers: Default::default(),
        }
    }
    pub(crate) fn apply(&mut self, cmd: TimerCommand<T>) {
        match cmd {
            TimerCommand::SetSchedule { seq, timer } => {
                self.timers.entry(seq).or_insert(timer);
            }
            TimerCommand::CancelSchedule { seq } => {
                self.timers.remove(&seq);
            }
        }
    }

    pub fn advance_clock(&mut self, now: u64) -> Vec<T::Callback> {
        let mut events = Vec::new();

        // 1. Age every in-flight timer
        let mut timeout_seqs: Vec<u64> = vec![];
        for (seq, probe) in self.timers.iter_mut() {
            if probe.tick() == 0 {
                timeout_seqs.push(*seq);
            }
        }

        for seq in timeout_seqs {
            let probe = self.timers.remove(&seq).unwrap();
            events.push(probe.to_timeout_callback(seq, now));
        }

        // 2. Advance the protocol clock (if enabled)
        if let Some(interval) = self.protocol_interval_ticks {
            self.protocol_elapsed += 1;
            if self.protocol_elapsed >= interval {
                self.protocol_elapsed = 0;
                events.push(Default::default());
            }
        }

        events
    }

    #[cfg(test)]
    pub fn probe_seq_for(&self, node_id: &str) -> Option<u64> {
        self.timers
            .iter()
            .find(|(_, probe)| probe.target_node_id().as_deref() == Some(node_id))
            .map(|(&seq, _)| seq)
    }

    #[cfg(test)]
    pub fn has_timer(&self, seq: u64) -> bool {
        self.timers.contains_key(&seq)
    }
}

#[cfg(test)]
mod tests {

    use crate::control_plane::{
        NodeId,
        membership::{
            DIRECT_ACK_TIMEOUT_TICKS, INDIRECT_ACK_TIMEOUT_TICKS, SUSPECT_TIMEOUT_TICKS,
            SwimTimeOutCallback, SwimTimer, SwimTimerKind,
        },
    };

    use super::*;

    #[test]
    fn no_protocol_period_before_interval_elapses() {
        let mut ticker = Ticker::<SwimTimer>::new(Some(PROBE_INTERVAL_TICKS));
        for _ in 0..PROBE_INTERVAL_TICKS - 1 {
            let events = ticker.advance_clock(0);
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, SwimTimeOutCallback::ProtocolPeriodElapsed)),
            );
        }
    }

    #[test]
    fn protocol_period_fires_at_interval() {
        let mut ticker = Ticker::<SwimTimer>::new(Some(PROBE_INTERVAL_TICKS));
        for _ in 0..PROBE_INTERVAL_TICKS - 1 {
            ticker.advance_clock(0);
        }
        let events = ticker.advance_clock(0);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, SwimTimeOutCallback::ProtocolPeriodElapsed))
        );
    }

    #[test]
    fn direct_probe_timeout() {
        let mut ticker = Ticker::<SwimTimer>::new(Some(PROBE_INTERVAL_TICKS));
        ticker.apply(TimerCommand::SetSchedule {
            seq: 1,
            timer: SwimTimer::direct_probe(NodeId::new("node-b")),
        });

        for _ in 0..DIRECT_ACK_TIMEOUT_TICKS - 1 {
            let events = ticker.advance_clock(0);
            assert!(!events.iter().any(|e| matches!(
                e,
                SwimTimeOutCallback::TimedOut {
                    phase: SwimTimerKind::DirectProbe,
                    ..
                }
            )));
        }

        let events = ticker.advance_clock(0);
        assert!(events.iter().any(|e| matches!(
            e,
            SwimTimeOutCallback::TimedOut {
                phase: SwimTimerKind::DirectProbe,
                seq: 1,
                ..
            }
        )));
        assert!(!ticker.has_timer(1));
    }

    #[test]
    fn indirect_probe_timeout() {
        let mut ticker = Ticker::<SwimTimer>::new(Some(PROBE_INTERVAL_TICKS));
        ticker.apply(TimerCommand::SetSchedule {
            seq: 2,
            timer: SwimTimer::indirect_probe(NodeId::new("node-c")),
        });

        for _ in 0..INDIRECT_ACK_TIMEOUT_TICKS - 1 {
            ticker.advance_clock(0);
        }

        let events = ticker.advance_clock(0);
        assert!(events.iter().any(|e| matches!(
            e,
            SwimTimeOutCallback::TimedOut {
                phase: SwimTimerKind::IndirectProbe,
                seq: 2,
                ..
            }
        )));
    }

    #[test]
    fn cancel_probe_prevents_timeout() {
        let mut ticker = Ticker::<SwimTimer>::new(Some(PROBE_INTERVAL_TICKS));
        ticker.apply(TimerCommand::SetSchedule {
            seq: 1,
            timer: SwimTimer::direct_probe(NodeId::new("node-b")),
        });
        ticker.apply(TimerCommand::CancelSchedule { seq: 1 });

        for _ in 0..DIRECT_ACK_TIMEOUT_TICKS + 1 {
            let events = ticker.advance_clock(0);
            assert!(!events.iter().any(|e| matches!(
                e,
                SwimTimeOutCallback::TimedOut {
                    phase: SwimTimerKind::DirectProbe,
                    ..
                }
            )));
        }
    }

    #[test]
    fn suspect_timer_fires_after_timeout() {
        let mut ticker = Ticker::<SwimTimer>::new(Some(PROBE_INTERVAL_TICKS));
        let node_id = "node-b";
        let seq = 1;
        ticker.apply(TimerCommand::SetSchedule {
            timer: SwimTimer::suspect_timer(NodeId::new(node_id)),
            seq,
        });

        for _ in 0..SUSPECT_TIMEOUT_TICKS - 1 {
            let events = ticker.advance_clock(0);
            assert!(!events.iter().any(|e| matches!(
                e,
                SwimTimeOutCallback::TimedOut {
                    phase: SwimTimerKind::Suspect,
                    ..
                }
            )));
        }

        let events = ticker.advance_clock(0);

        assert!(events.iter().any(|e| matches!(
            e,
            SwimTimeOutCallback::TimedOut {
                phase: SwimTimerKind::Suspect,
                ..
            }
        )));
        assert!(
            !ticker.has_timer(seq),
            "There shouldn't be any existing suspect"
        );
    }
}
