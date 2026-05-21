use std::path::PathBuf;
use std::thread;

use crossbeam_channel::Sender;
use tokio::sync::mpsc as tokio_mpsc;

use super::checkpoint::CheckpointJob;
use super::state::DataPlane;
use super::timer::DataPlaneTimer;
use super::wal::WalWriter;
use crate::clusters::NodeId;
use crate::data_plane::messages::command::DataPlaneCommand;
use crate::data_plane::transport::command::DataTransportCommand;
use crate::schedulers::ticker_message::TickerCommand;

pub struct DataPlaneActor;

impl DataPlaneActor {
    pub fn spawn(
        node_id: NodeId,
        data_dir: PathBuf,
        checkpoint_tx: Sender<CheckpointJob>,
        scheduler_tx: tokio_mpsc::Sender<TickerCommand<DataPlaneTimer>>,
        data_transport_tx: tokio_mpsc::Sender<DataTransportCommand>,
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
                let mut state = DataPlane::new(node_id, wal, data_dir);

                while let Ok(cmd) = mailbox.recv() {
                    state.handle_command(cmd);

                    while let Ok(next) = mailbox.try_recv() {
                        state.handle_command(next);
                    }

                    if state.should_flush() {
                        state.flush_batch();
                    }

                    state.handle_events(&checkpoint_tx, &scheduler_tx, &data_transport_tx);
                }
            })
            .expect("failed to spawn data-plane thread");

        tx
    }
}
