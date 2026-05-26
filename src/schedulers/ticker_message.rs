use std::fmt::{Display, Formatter};

use tokio::sync::mpsc as tokio_mpsc;

use crate::channels::BatchSender;

#[derive(Debug)]
pub(crate) enum TickerCommand<T> {
    Schedule(TimerCommand<T>),
    #[cfg(test)]
    ForceTick,
}

pub(crate) struct SchedulerSender<T>(BatchSender<TickerCommand<T>>);

impl<T> From<tokio_mpsc::Sender<Box<[TickerCommand<T>]>>> for SchedulerSender<T> {
    fn from(tx: tokio_mpsc::Sender<Box<[TickerCommand<T>]>>) -> Self {
        Self(tx.into())
    }
}

impl<T> SchedulerSender<T> {
    pub(crate) fn schedule(&self, seq: u64, timer: impl Into<T>) {
        let cmd: TickerCommand<T> = TimerCommand::SetSchedule {
            seq,
            timer: timer.into(),
        }
        .into();
        self.0.blocking_send(Box::new([cmd]));
    }

    pub(crate) fn send(&self, cmd: TimerCommand<T>) {
        self.0.blocking_send(Box::new([cmd.into()]));
    }

    pub(crate) async fn send_batch(&self, cmds: Vec<TickerCommand<T>>) {
        self.0.send_batch(cmds).await;
    }
}

#[derive(Debug)]
pub(crate) enum TimerCommand<T> {
    SetSchedule { seq: u64, timer: T },
    CancelSchedule { seq: u64 },
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
