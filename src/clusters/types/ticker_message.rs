use crate::clusters::{
    NodeId, TimeoutEvent,
    tickers::{
        ticker::{DIRECT_ACK_TIMEOUT_TICKS, INDIRECT_ACK_TIMEOUT_TICKS, SUSPECT_TIMEOUT_TICKS},
        timer::TTimer,
    },
};

#[derive(Debug)]
pub(crate) enum TickerCommand {
    Probe(TimerCommand),
    #[cfg(test)]
    ForceTick,
}

#[derive(Debug)]
pub(crate) enum TimerCommand {
    SetProbe { seq: u32, timer: Box<dyn TTimer> },
    CancelProbe { seq: u32 },
    SetSuspectTimer { seq: u32, timer: Box<dyn TTimer> },
}
impl From<TimerCommand> for TickerCommand {
    fn from(value: TimerCommand) -> Self {
        TickerCommand::Probe(value)
    }
}
