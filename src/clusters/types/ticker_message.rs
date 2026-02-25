use crate::clusters::tickers::timer::TTimer;

#[derive(Debug)]
pub(crate) enum TickerCommand {
    Schedule(TimerCommand),
    #[cfg(test)]
    ForceTick,
}

#[derive(Debug)]
pub(crate) enum TimerCommand {
    SetSchedule { seq: u32, timer: Box<dyn TTimer> },
    CancelSchedule { seq: u32 },
}
impl From<TimerCommand> for TickerCommand {
    fn from(value: TimerCommand) -> Self {
        TickerCommand::Schedule(value)
    }
}
