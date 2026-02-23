use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time;

use crate::clusters::ActorEvent;
use crate::clusters::tickers::ticker::Ticker;
use crate::clusters::types::ticker_message::TickerCommand;

/// One real-time tick drives one logical tick in SwimTicker.
/// PROTOCOL_PERIOD_TICKS (10) × TICK_PERIOD (100 ms) = 1 s per probe round.
const TICK_PERIOD: Duration = Duration::from_millis(100);

pub(crate) struct TickerActor {
    ticker: Ticker,
    commands: mpsc::Receiver<TickerCommand>,
    swim_sender: mpsc::Sender<ActorEvent>,
}

impl TickerActor {
    pub fn new(
        mailbox: mpsc::Receiver<TickerCommand>,
        swim_sender: mpsc::Sender<ActorEvent>,
    ) -> Self {
        Self {
            ticker: Ticker::default(),
            commands: mailbox,
            swim_sender,
        }
    }

    pub async fn run(mut self) {
        let mut interval = time::interval(TICK_PERIOD);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.run_tick().await;
                }
                Some(cmd) = self.commands.recv() => {
                    match cmd {
                        #[cfg(test)]
                        TickerCommand::ForceTick => {
                            self.run_tick().await;
                        }
                        TickerCommand::Probe(probe_cmd)=>{
                            self.ticker.apply(probe_cmd);
                        }
                    }
                }

            }
        }
    }

    async fn run_tick(&mut self) {
        for event in self.ticker.tick() {
            let _ = self.swim_sender.send(event.into()).await;
        }
    }
}
