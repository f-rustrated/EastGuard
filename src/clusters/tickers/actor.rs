use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time;

use crate::clusters::TimeoutEvent;
use crate::clusters::tickers::ticker::Ticker;
use crate::clusters::tickers::timer::TTimer;
use crate::clusters::types::ticker_message::TickerCommand;

/// One real-time tick drives one logical tick in SwimTicker.
/// PROTOCOL_PERIOD_TICKS (10) × TICK_PERIOD (100 ms) = 1 s per probe round.
const TICK_PERIOD: Duration = Duration::from_millis(100);

pub(crate) struct SchedullingActor<T, U> {
    ticker: Ticker<U>,
    mailbox: mpsc::Receiver<TickerCommand<U>>,
    sender: mpsc::Sender<T>,
}

impl<T, U> SchedullingActor<T, U>
where
    T: From<TimeoutEvent>,
    U: TTimer,
{
    pub fn new(mailbox: mpsc::Receiver<TickerCommand<U>>, swim_sender: mpsc::Sender<T>) -> Self {
        Self {
            ticker: Ticker::<U>::new(),
            mailbox,
            sender: swim_sender,
        }
    }

    pub async fn run(mut self) {
        let mut interval = time::interval(TICK_PERIOD);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.run_tick().await;
                }

                // What if schedulling actor consistantly gets mailbox and ticker never gets picked in select?
                Some(cmd) = self.mailbox.recv() => {
                    match cmd {
                        #[cfg(test)]
                        TickerCommand::ForceTick => {
                            self.run_tick().await;
                        }
                        TickerCommand::Schedule(probe_cmd)=>{
                            self.ticker.apply(probe_cmd);
                        }
                    }
                }

            }
        }
    }

    async fn run_tick(&mut self) {
        for event in self.ticker.advance_clock() {
            let _ = self.sender.send(event.into()).await;
        }
    }
}
