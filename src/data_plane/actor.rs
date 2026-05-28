use std::path::PathBuf;
use std::thread;

use crossbeam_channel::Sender;

use super::checkpoint::CheckpointJob;
use super::state::DataPlane;
use super::timer::{BatchFlushTimer, ReplicationTimer};
use super::wal::WalWriter;
use crate::channels::BatchSender;
use crate::config::DataNodeConfig;
use crate::control_plane::NodeId;
use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::data_plane::messages::command::DataPlaneCommand;
use crate::data_plane::transport::command::DataTransportCommand;
use crate::schedulers::ticker_message::SchedulerSender;

pub struct DataPlaneActor;

impl DataPlaneActor {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        node_id: NodeId,
        data_dir: PathBuf,
        config: DataNodeConfig,
        checkpoint_tx: Sender<CheckpointJob>,
        batch_scheduler_tx: SchedulerSender<BatchFlushTimer>,
        repl_scheduler_tx: SchedulerSender<ReplicationTimer>,
        data_transport_tx: BatchSender<DataTransportCommand>,
        coordinator_tx: MutlRaftSender,
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
                let mut state = DataPlane::new(node_id, config, wal, data_dir);

                while let Ok(cmd) = mailbox.recv() {
                    state.handle_command(cmd);

                    while let Ok(next) = mailbox.try_recv() {
                        state.handle_command(next);
                    }

                    state.flush_and_dispatch(
                        &checkpoint_tx,
                        &batch_scheduler_tx,
                        &repl_scheduler_tx,
                        &data_transport_tx,
                        &coordinator_tx,
                    );
                }
            })
            .expect("failed to spawn data-plane thread");

        tx
    }
}
