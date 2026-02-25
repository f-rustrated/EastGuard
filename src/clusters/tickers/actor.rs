use std::marker::PhantomData;
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

pub async fn run_scheduling_actor(
    sender: mpsc::Sender<impl From<TimeoutEvent>>,
    mut mailbox: mpsc::Receiver<TickerCommand<impl TTimer>>,
) {
    let mut interval = time::interval(TICK_PERIOD);
    let mut ticker = Ticker::new();

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
