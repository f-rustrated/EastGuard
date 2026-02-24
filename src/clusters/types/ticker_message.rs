use crate::clusters::{NodeId, tickers::timer::ProbeTimer};

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
