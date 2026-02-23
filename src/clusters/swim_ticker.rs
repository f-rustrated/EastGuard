use crate::clusters::{ActorEvent, NodeId};
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

pub(crate) struct SwimTicker {
    protocol_elapsed: u32,
    probe_timers: HashMap<u32, ProbeTimer>,
    suspect_timers: HashMap<NodeId, u32>,
}

struct ProbeTimer {
    target_node_id: NodeId,
    phase: ProbePhase,
    ticks_remaining: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbePhase {
    Direct,
    Indirect,
}

#[derive(Debug)]
pub(crate) enum TickEvent {
    ProtocolPeriodElapsed,
    DirectProbeTimedOut { seq: u32, target_node_id: NodeId },
    IndirectProbeTimedOut { seq: u32, target_node_id: NodeId },
    SuspectTimedOut { node_id: NodeId },
}

impl From<TickEvent> for ActorEvent {
    fn from(value: TickEvent) -> Self {
        ActorEvent::Tick(value)
    }
}

#[derive(Debug)]
pub(crate) enum TimerCommand {
    SetDirectProbe { seq: u32, target_node_id: NodeId },
    SetIndirectProbe { seq: u32, target_node_id: NodeId },
    SetSuspectTimer { node_id: NodeId },
    CancelProbe { seq: u32 },
}

impl SwimTicker {
    pub fn new() -> Self {
        Self {
            protocol_elapsed: 0,
            probe_timers: HashMap::new(),
            suspect_timers: HashMap::new(),
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
            match probe.phase {
                ProbePhase::Direct => {
                    events.push(TickEvent::DirectProbeTimedOut {
                        seq,
                        target_node_id: probe.target_node_id,
                    });
                }
                ProbePhase::Indirect => {
                    events.push(TickEvent::IndirectProbeTimedOut {
                        seq,
                        target_node_id: probe.target_node_id,
                    });
                }
            }
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

    pub fn apply(&mut self, cmd: TimerCommand) {
        match cmd {
            TimerCommand::SetDirectProbe {
                seq,
                target_node_id,
            } => {
                self.probe_timers.insert(
                    seq,
                    ProbeTimer {
                        target_node_id,
                        phase: ProbePhase::Direct,
                        ticks_remaining: DIRECT_ACK_TIMEOUT_TICKS,
                    },
                );
            }
            TimerCommand::SetIndirectProbe {
                seq,
                target_node_id,
            } => {
                self.probe_timers.insert(
                    seq,
                    ProbeTimer {
                        target_node_id,
                        phase: ProbePhase::Indirect,
                        ticks_remaining: INDIRECT_ACK_TIMEOUT_TICKS,
                    },
                );
            }
            TimerCommand::SetSuspectTimer { node_id } => {
                self.suspect_timers.insert(node_id, SUSPECT_TIMEOUT_TICKS);
            }
            TimerCommand::CancelProbe { seq } => {
                self.probe_timers.remove(&seq);
            }
        }
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

// ==========================================
// TickerActor — async wrapper around SwimTicker
// ==========================================

#[derive(Debug)]
pub(crate) enum TickerCommand {
    Apply(TimerCommand),
    ForceTick,
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
            ticker: SwimTicker::new(),
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
                        TickerCommand::Apply(timer_cmd) => {
                            self.ticker.apply(timer_cmd);
                        }
                        TickerCommand::ForceTick => {
                            self.do_tick().await;
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
