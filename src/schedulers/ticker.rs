use crate::schedulers::{ticker_message::TimerCommand, timer::TTimer};
use std::collections::HashMap;

pub(crate) const PROBE_INTERVAL_TICKS: u32 = 10; // 10 × 100ms = 1s
pub(crate) const DIRECT_ACK_TIMEOUT_TICKS: u32 = 3; // 3 × 100ms = 300ms
pub(crate) const INDIRECT_ACK_TIMEOUT_TICKS: u32 = 3; // 3 × 100ms = 300ms
pub(crate) const SUSPECT_TIMEOUT_TICKS: u32 = 50; // 50 × 100ms = 5s

#[derive(Debug)]
pub(crate) struct Ticker<T> {
    protocol_elapsed: u32,
    timers: HashMap<u32, T>,
}

impl<T> Ticker<T>
where
    T: TTimer,
{
    pub(crate) fn new() -> Self {
        Self {
            protocol_elapsed: 0,
            timers: Default::default(),
        }
    }
    pub(crate) fn apply(&mut self, cmd: TimerCommand<T>) {
        match cmd {
            TimerCommand::SetSchedule { seq, timer } => {
                self.timers.insert(seq, timer);
            }

            TimerCommand::CancelSchedule { seq } => {
                self.timers.remove(&seq);
            }
        }
    }

    pub fn advance_clock(&mut self) -> Vec<T::Callback> {
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
            events.push(probe.to_timeout_callback(seq));
        }

        // 2. Advance the protocol clock
        self.protocol_elapsed += 1;
        if self.protocol_elapsed >= PROBE_INTERVAL_TICKS {
            self.protocol_elapsed = 0;
            events.push(Default::default());
        }

        events
    }

    #[cfg(test)]
    pub fn probe_seq_for(&self, node_id: &str) -> Option<u32> {
        self.timers
            .iter()
            .find(|(_, probe)| probe.target_node_id().as_deref() == Some(node_id))
            .map(|(&seq, _)| seq)
    }

    #[cfg(test)]
    pub fn has_timer(&self, seq: u32) -> bool {
        self.timers.contains_key(&seq)
    }
}

#[cfg(test)]
mod tests {

    use crate::clusters::swims::{ProbePhase, SwimTimeOutCallback, SwimTimer};

    use super::*;

    #[test]
    fn no_protocol_period_before_interval_elapses() {
        let mut ticker = Ticker::<SwimTimer>::new();
        for _ in 0..PROBE_INTERVAL_TICKS - 1 {
            let events = ticker.advance_clock();
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, SwimTimeOutCallback::ProtocolPeriodElapsed)),
            );
        }
    }

    #[test]
    fn protocol_period_fires_at_interval() {
        let mut ticker = Ticker::<SwimTimer>::new();
        for _ in 0..PROBE_INTERVAL_TICKS - 1 {
            ticker.advance_clock();
        }
        let events = ticker.advance_clock();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, SwimTimeOutCallback::ProtocolPeriodElapsed))
        );
    }

    #[test]
    fn direct_probe_timeout() {
        let mut ticker = Ticker::<SwimTimer>::new();
        ticker.apply(TimerCommand::SetSchedule {
            seq: 1,
            timer: SwimTimer::direct_probe("node-b".into()),
        });

        for _ in 0..DIRECT_ACK_TIMEOUT_TICKS - 1 {
            let events = ticker.advance_clock();
            assert!(!events.iter().any(|e| matches!(
                e,
                SwimTimeOutCallback::TimedOut {
                    phase: ProbePhase::Direct,
                    ..
                }
            )));
        }

        let events = ticker.advance_clock();
        assert!(events.iter().any(|e| matches!(
            e,
            SwimTimeOutCallback::TimedOut {
                phase: ProbePhase::Direct,
                seq: 1,
                ..
            }
        )));
        assert!(!ticker.has_timer(1));
    }

    #[test]
    fn indirect_probe_timeout() {
        let mut ticker = Ticker::<SwimTimer>::new();
        ticker.apply(TimerCommand::SetSchedule {
            seq: 2,
            timer: SwimTimer::indirect_probe("node-c".into()),
        });

        for _ in 0..INDIRECT_ACK_TIMEOUT_TICKS - 1 {
            ticker.advance_clock();
        }

        let events = ticker.advance_clock();
        assert!(events.iter().any(|e| matches!(
            e,
            SwimTimeOutCallback::TimedOut {
                phase: ProbePhase::Indirect,
                seq: 2,
                ..
            }
        )));
    }

    #[test]
    fn cancel_probe_prevents_timeout() {
        let mut ticker = Ticker::<SwimTimer>::new();
        ticker.apply(TimerCommand::SetSchedule {
            seq: 1,
            timer: SwimTimer::direct_probe("node-b".into()),
        });
        ticker.apply(TimerCommand::CancelSchedule { seq: 1 });

        for _ in 0..DIRECT_ACK_TIMEOUT_TICKS + 1 {
            let events = ticker.advance_clock();
            assert!(!events.iter().any(|e| matches!(
                e,
                SwimTimeOutCallback::TimedOut {
                    phase: ProbePhase::Direct,
                    ..
                }
            )));
        }
    }

    #[test]
    fn suspect_timer_fires_after_timeout() {
        let mut ticker = Ticker::<SwimTimer>::new();
        let node_id = "node-b";
        let seq = 1;
        ticker.apply(TimerCommand::SetSchedule {
            timer: SwimTimer::suspect_timer(node_id.into()),
            seq,
        });

        for _ in 0..SUSPECT_TIMEOUT_TICKS - 1 {
            let events = ticker.advance_clock();
            assert!(!events.iter().any(|e| matches!(
                e,
                SwimTimeOutCallback::TimedOut {
                    phase: ProbePhase::Suspect,
                    ..
                }
            )));
        }

        let events = ticker.advance_clock();

        assert!(events.iter().any(|e| matches!(
            e,
            SwimTimeOutCallback::TimedOut {
                phase: ProbePhase::Suspect,
                ..
            }
        )));
        assert!(
            !ticker.has_timer(seq),
            "There shouldn't be any existing suspect"
        );
    }
}
