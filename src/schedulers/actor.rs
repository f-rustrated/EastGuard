use crate::schedulers::ticker::{TICK_PERIOD_MS, Ticker};
use crate::schedulers::ticker_message::TickerCommand;
use crate::schedulers::timer::TTimer;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;

pub async fn run_scheduling_actor<T>(
    sender: mpsc::Sender<impl From<T::Callback>>,
    mut mailbox: mpsc::Receiver<TickerCommand<T>>,
) where
    T: TTimer,
{
    let mut interval = time::interval(Duration::from_millis(TICK_PERIOD_MS));
    let mut ticker = Ticker::<T>::new();

    loop {
        tokio::select! {
            biased;
            _ = interval.tick() => {
                for event in ticker.advance_clock(now_ms()) {
                    let _ = sender.send(event.into()).await;
                }
            }

            Some(cmd) = mailbox.recv() => {
                match cmd {
                    #[cfg(test)]
                    TickerCommand::ForceTick => {
                        for event in ticker.advance_clock(now_ms()) {
                            let _ = sender.send(event.into()).await;
                        }
                    }
                    TickerCommand::Schedule(probe_cmd)=>{
                        ticker.apply(probe_cmd);
                    }
                }
            }

        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
