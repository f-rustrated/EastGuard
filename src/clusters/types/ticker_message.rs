use crate::clusters::tickers::timer::TTimer;

#[derive(Debug)]
pub(crate) enum TickerCommand<T> {
    Schedule(TimerCommand<T>),
    #[cfg(test)]
    ForceTick,
}

#[derive(Debug)]
pub(crate) enum TimerCommand<T> {
    SetSchedule { seq: u32, timer: T },
    CancelSchedule { seq: u32 },
}
impl<T> From<TimerCommand<T>> for TickerCommand<T> {
    fn from(value: TimerCommand<T>) -> Self {
        TickerCommand::Schedule(value)
    }
}
