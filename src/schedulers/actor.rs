use crate::schedulers::ticker::Ticker;
use crate::schedulers::ticker_message::TickerCommand;
use crate::schedulers::timer::TTimer;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;

/// One real-time tick = 100 ms.
/// PROTOCOL_PERIOD_TICKS (10) × TICK_PERIOD_MS (100 ms) = 1 s per probe round.
pub const TICK_PERIOD_MS: u64 = 100;

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
                for event in ticker.advance_clock() {
                    let _ = sender.send(event.into()).await;
                }
            }

            // What if scheduling actor consistantly gets mailbox and ticker never gets picked in select?
            Some(cmd) = mailbox.recv() => {
                match cmd {
                    #[cfg(test)]
                    TickerCommand::ForceTick => {
                        for event in ticker.advance_clock() {
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
