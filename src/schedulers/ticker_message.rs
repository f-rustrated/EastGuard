use std::fmt::{Display, Formatter};

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

impl<T> Display for TimerCommand<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimerCommand::SetSchedule { seq, timer: _timer } => {
                write!(f, "seq: {}", seq)
            }
            TimerCommand::CancelSchedule { seq } => {
                write!(f, "seq: {}", seq)
            }
        }
    }
}

impl<T> From<TimerCommand<T>> for TickerCommand<T> {
    fn from(value: TimerCommand<T>) -> Self {
        TickerCommand::Schedule(value)
    }
}
