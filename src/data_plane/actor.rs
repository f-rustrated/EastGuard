use std::path::PathBuf;
use std::thread;

use crossbeam_channel::Sender;
use tokio::sync::mpsc as tokio_mpsc;

use crate::data_plane::messages::command::DataPlaneCommand;
use crate::data_plane::messages::event::DataPlaneEvent;
use crate::schedulers::ticker_message::TickerCommand;

use super::checkpoint::CheckpointJob;
use super::state::DataPlane;
use super::timer::DataPlaneTimer;
use super::wal::WalWriter;

pub struct DataPlaneActor;

impl DataPlaneActor {
    pub fn spawn(
        data_dir: PathBuf,
        checkpoint_tx: Sender<CheckpointJob>,
        scheduler_tx: tokio_mpsc::Sender<TickerCommand<DataPlaneTimer>>,
    ) -> Sender<DataPlaneCommand> {
        let (tx, mailbox) = crossbeam_channel::bounded(4096);

        thread::Builder::new()
            .name("data-plane-worker".into())
            .spawn(move || {
                let wal = match WalWriter::new(data_dir.clone()) {
                    Ok(w) => w,
                    Err(e) => {
                        tracing::error!("Failed to initialize WAL: {e}");
                        return;
                    }
                };
                let mut state = DataPlane::new(wal, data_dir);

                while let Ok(cmd) = mailbox.recv() {
                    state.process(cmd);
                    while let Ok(next) = mailbox.try_recv() {
                        state.process(next);
                    }
                    Self::drain_events(&mut state, &checkpoint_tx, &scheduler_tx);
                }
            })
            .expect("failed to spawn data-plane thread");

        tx
    }

    // Doesn't have 'async' color but communication is real
    fn drain_events(
        state: &mut DataPlane<WalWriter>,
        checkpoint_tx: &Sender<CheckpointJob>,
        scheduler_tx: &tokio_mpsc::Sender<TickerCommand<DataPlaneTimer>>,
    ) {
        for event in state.take_events() {
            use DataPlaneEvent::*;

            match event {
                SubmitCheckpoint(job) => {
                    let _ = checkpoint_tx.send(job);
                }
                ProduceAck { reply, result } => {
                    let _ = reply.send(result);
                }
                Timer(cmd) => {
                    let _ = scheduler_tx.blocking_send(cmd.into());
                }
            }
        }
    }
}
