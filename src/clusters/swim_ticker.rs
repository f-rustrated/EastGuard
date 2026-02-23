use crate::clusters::{ActorEvent, NodeId, TickEvent};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;

/// One real-time tick drives one logical tick in SwimTicker.
/// PROTOCOL_PERIOD_TICKS (10) × TICK_PERIOD (100 ms) = 1 s per probe round.
const TICK_PERIOD: Duration = Duration::from_millis(100);

pub(crate) const PROBE_INTERVAL_TICKS: u32 = 10; // 10 × 100ms = 1s
pub(crate) const DIRECT_ACK_TIMEOUT_TICKS: u32 = 3; // 3 × 100ms = 300ms
pub(crate) const INDIRECT_ACK_TIMEOUT_TICKS: u32 = 3; // 3 × 100ms = 300ms
pub(crate) const SUSPECT_TIMEOUT_TICKS: u32 = 50; // 50 × 100ms = 5s

#[derive(Default)]
pub(crate) struct SwimTicker {
    protocol_elapsed: u32,
    probe_timers: HashMap<u32, ProbeTimer>,
    suspect_timers: HashMap<NodeId, u32>,
}

#[derive(Debug)]
pub(crate) struct ProbeTimer {
    target_node_id: NodeId,
    phase: ProbePhase,
    ticks_remaining: u32,
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

#[derive(Debug)]
pub(crate) enum TickerCommand {
    Probe(TimerCommand),
    #[cfg(test)]
    ForceTick,
}

#[derive(Debug)]
pub(crate) enum TimerCommand {
    SetProbe { seq: u32, timer: ProbeTimer },
    CancelProbe { seq: u32 },
    SetSuspectTimer { node_id: NodeId },
}
impl From<TimerCommand> for TickerCommand {
    fn from(value: TimerCommand) -> Self {
        TickerCommand::Probe(value)
    }
}

impl SwimTicker {
    pub(crate) fn apply(&mut self, cmd: TimerCommand) {
        match cmd {
            TimerCommand::SetProbe { seq, timer } => {
                self.probe_timers.insert(seq, timer);
            }
            TimerCommand::SetSuspectTimer { node_id } => {
                self.suspect_timers.insert(node_id, SUSPECT_TIMEOUT_TICKS);
            }
            TimerCommand::CancelProbe { seq } => {
                self.probe_timers.remove(&seq);
            }
        }
    }

    pub fn tick(&mut self) -> Vec<TickEvent> {
        let mut events = Vec::new();

        // 1. Age every in-flight probe
        let mut timeout_seqs: Vec<u32> = vec![];
        for (seq, probe) in self.probe_timers.iter_mut() {
            probe.ticks_remaining -= 1;
            if probe.ticks_remaining == 0 {
                timeout_seqs.push(*seq);
            }
        }

        for seq in timeout_seqs {
            let probe = self.probe_timers.remove(&seq).unwrap();
            events.push(probe.to_timeout_event(seq));
        }

        // 2. Age suspect timers, collect the ones that expired
        let expired: Vec<NodeId> = self
            .suspect_timers
            .iter_mut()
            .filter_map(|(id, ticks)| {
                *ticks -= 1;
                (*ticks == 0).then(|| id.clone())
            })
            .collect();

        for node_id in expired {
            self.suspect_timers.remove(&node_id);
            events.push(TickEvent::SuspectTimedOut { node_id });
        }

        // 3. Advance the protocol clock
        self.protocol_elapsed += 1;
        if self.protocol_elapsed >= PROBE_INTERVAL_TICKS {
            self.protocol_elapsed = 0;
            events.push(TickEvent::ProtocolPeriodElapsed);
        }

        events
    }

    #[cfg(test)]
    pub fn probe_seq_for(&self, node_id: &str) -> Option<u32> {
        self.probe_timers
            .iter()
            .find(|(_, probe)| &*probe.target_node_id == node_id)
            .map(|(&seq, _)| seq)
    }

    #[cfg(test)]
    pub fn has_probe(&self, seq: u32) -> bool {
        self.probe_timers.contains_key(&seq)
    }

    #[cfg(test)]
    pub fn has_suspect_timer(&self, node_id: &str) -> bool {
        self.suspect_timers.contains_key(node_id)
    }

    #[cfg(test)]
    pub fn probe_phase(&self, seq: u32) -> Option<ProbePhase> {
        self.probe_timers.get(&seq).map(|p| p.phase)
    }
}

pub(crate) struct TickerActor {
    ticker: SwimTicker,
    commands: mpsc::Receiver<TickerCommand>,
    swim_sender: mpsc::Sender<ActorEvent>,
}

impl TickerActor {
    pub fn new(
        mailbox: mpsc::Receiver<TickerCommand>,
        swim_sender: mpsc::Sender<ActorEvent>,
    ) -> Self {
        Self {
            ticker: SwimTicker::default(),
            commands: mailbox,
            swim_sender,
        }
    }

    pub async fn run(mut self) {
        let mut interval = time::interval(TICK_PERIOD);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.do_tick().await;
                }
                Some(cmd) = self.commands.recv() => {
                    match cmd {
                        #[cfg(test)]
                        TickerCommand::ForceTick => {
                            self.do_tick().await;
                        }
                        TickerCommand::Probe(probe_cmd)=>{
                            self.ticker.apply(probe_cmd);
                        }
                    }
                }

            }
        }
    }

    async fn do_tick(&mut self) {
        for event in self.ticker.tick() {
            let _ = self.swim_sender.send(event.into()).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_protocol_period_before_interval_elapses() {
        let mut ticker = SwimTicker::default();
        for _ in 0..PROBE_INTERVAL_TICKS - 1 {
            let events = ticker.tick();
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, TickEvent::ProtocolPeriodElapsed)),
            );
        }
    }

    #[test]
    fn protocol_period_fires_at_interval() {
        let mut ticker = SwimTicker::default();
        for _ in 0..PROBE_INTERVAL_TICKS - 1 {
            ticker.tick();
        }
        let events = ticker.tick();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, TickEvent::ProtocolPeriodElapsed))
        );
    }

    #[test]
    fn direct_probe_timeout() {
        let mut ticker = SwimTicker::default();
        ticker.apply(TimerCommand::SetProbe {
            seq: 1,
            timer: ProbeTimer::direct_probe("node-b".into()),
        });

        for _ in 0..DIRECT_ACK_TIMEOUT_TICKS - 1 {
            let events = ticker.tick();
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, TickEvent::DirectProbeTimedOut { .. }))
            );
        }

        let events = ticker.tick();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, TickEvent::DirectProbeTimedOut { seq: 1, .. }))
        );
        assert!(!ticker.has_probe(1));
    }

    #[test]
    fn indirect_probe_timeout() {
        let mut ticker = SwimTicker::default();
        ticker.apply(TimerCommand::SetProbe {
            seq: 2,
            timer: ProbeTimer::indirect_probe("node-c".into()),
        });

        for _ in 0..INDIRECT_ACK_TIMEOUT_TICKS - 1 {
            ticker.tick();
        }

        let events = ticker.tick();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, TickEvent::IndirectProbeTimedOut { seq: 2, .. }))
        );
    }

    #[test]
    fn cancel_probe_prevents_timeout() {
        let mut ticker = SwimTicker::default();
        ticker.apply(TimerCommand::SetProbe {
            seq: 1,
            timer: ProbeTimer::direct_probe("node-b".into()),
        });
        ticker.apply(TimerCommand::CancelProbe { seq: 1 });

        for _ in 0..DIRECT_ACK_TIMEOUT_TICKS + 1 {
            let events = ticker.tick();
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, TickEvent::DirectProbeTimedOut { .. }))
            );
        }
    }

    #[test]
    fn suspect_timer_fires_after_timeout() {
        let mut ticker = SwimTicker::default();
        ticker.apply(TimerCommand::SetSuspectTimer {
            node_id: "node-b".into(),
        });

        for _ in 0..SUSPECT_TIMEOUT_TICKS - 1 {
            let events = ticker.tick();
            assert!(
                !events
                    .iter()
                    .any(|e| matches!(e, TickEvent::SuspectTimedOut { .. }))
            );
        }

        let events = ticker.tick();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, TickEvent::SuspectTimedOut { .. }))
        );
        assert!(!ticker.has_suspect_timer("node-b"));
    }
}
