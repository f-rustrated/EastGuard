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

impl<T> TimerCommand<T> {
    pub(crate) fn seq(&self) -> u32 {
        match self {
            TimerCommand::SetSchedule { seq, .. } | TimerCommand::CancelSchedule { seq } => *seq,
        }
    }
    pub(crate) fn set_seq(mut self, new_seq: u32) -> Self {
        match &mut self {
            TimerCommand::SetSchedule { seq, .. } | TimerCommand::CancelSchedule { seq } => {
                *seq = new_seq;
            }
        }
        self
    }
}

impl<T> Display for TimerCommand<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimerCommand::SetSchedule { seq, timer: _timer } => {
                write!(f, "[SetSchedule] seq: {}", seq)
            }
            TimerCommand::CancelSchedule { seq } => {
                write!(f, "[CancelSchedule] seq: {}", seq)
            }
        }
    }
}

impl<T> From<TimerCommand<T>> for TickerCommand<T> {
    fn from(value: TimerCommand<T>) -> Self {
        TickerCommand::Schedule(value)
    }
}
