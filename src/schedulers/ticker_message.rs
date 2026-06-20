use std::fmt::{Display, Formatter};

use tokio::sync::mpsc as tokio_mpsc;

use crate::channels::BatchSender;

#[derive(Clone, Debug)]
pub(crate) enum TickerCommand<T> {
    Schedule(TimerCommand<T>),
    #[cfg(test)]
    ForceTick,
}

#[derive(Clone)]
pub(crate) struct SchedulerSender<T>(BatchSender<TickerCommand<T>>);

impl<T> From<tokio_mpsc::Sender<Box<[TickerCommand<T>]>>> for SchedulerSender<T> {
    fn from(tx: tokio_mpsc::Sender<Box<[TickerCommand<T>]>>) -> Self {
        Self(tx.into())
    }
}

pub(crate) type SchedulerReceiver<T> = tokio_mpsc::Receiver<Box<[TickerCommand<T>]>>;

impl<T> SchedulerSender<T> {
    pub(crate) fn channel(capacity: usize) -> (SchedulerSender<T>, SchedulerReceiver<T>) {
        let (tx, rx) = tokio_mpsc::channel(capacity);

        (tx.into(), rx)
    }

    pub(crate) async fn send_batch(&self, cmds: Box<[TickerCommand<T>]>) {
        self.0.send_batch(cmds).await;
    }

    pub(crate) async fn send_timer_batch(&self, cmds: Box<[TimerCommand<T>]>) {
        self.0
            .send_batch(cmds.into_iter().map(Into::into).collect())
            .await;
    }

    #[cfg(test)]
    pub(crate) async fn force_tick(&self) {
        self.0
            .send_batch(Box::new([TickerCommand::ForceTick]))
            .await;
    }
}

#[derive(Debug, Clone)]
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
