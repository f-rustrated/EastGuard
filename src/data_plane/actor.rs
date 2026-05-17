use std::path::PathBuf;
use std::thread;

use crossbeam_channel::Sender;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::sync::oneshot;

use crate::data_plane::messages::command::{DataPlaneCommand, ProduceAck};
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
                let mut pending_replies: Vec<oneshot::Sender<ProduceAck>> = Vec::new();

                while let Ok(cmd) = mailbox.recv() {
                    state.process(cmd);

                    while let Ok(next) = mailbox.try_recv() {
                        state.process(next);
                    }

                    Self::handle_events(
                        &mut pending_replies,
                        &checkpoint_tx,
                        &scheduler_tx,
                        state.take_events(),
                    );
                }
            })
            .expect("failed to spawn data-plane thread");

        tx
    }

    fn handle_events(
        pending_replies: &mut Vec<oneshot::Sender<ProduceAck>>,
        checkpoint_tx: &Sender<CheckpointJob>,
        scheduler_tx: &tokio_mpsc::Sender<TickerCommand<DataPlaneTimer>>,
        events: Vec<DataPlaneEvent>,
    ) {
        for event in events {
            match event {
                DataPlaneEvent::SubmitCheckpoint(job) => {
                    let _ = checkpoint_tx.send(job);
                }
                DataPlaneEvent::ProducePending(reply) => {
                    pending_replies.push(reply);
                    // D2: initiate replication after WalBatchComplete
                }
                DataPlaneEvent::WalBatchComplete { lsn: _ } => {
                    // D1: no replication, ACK immediately
                    for reply in pending_replies.drain(..) {
                        let _ = reply.send(ProduceAck::Ok);
                    }
                }
                DataPlaneEvent::WalBatchFailed(e) => {
                    for reply in pending_replies.drain(..) {
                        let _ = reply.send(ProduceAck::Err(e.clone()));
                    }
                }
                DataPlaneEvent::Timer(cmd) => {
                    let _ = scheduler_tx.blocking_send(cmd.into());
                }
            }
        }
    }
}
