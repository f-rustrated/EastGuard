use super::checkpoint::CheckpointTask;
use super::cold_read::ColdReadPool;
use super::messages::pending::DataPlaneOutputs;
use super::recovery::inventory::RecoveryOutput;
use super::sparse_index::SparseIndex;
use super::state::DataPlane;
use super::timer::SegmentAgeTimer;
use super::wal::WalWriter;
use crate::channels::BatchSender;
use crate::config::DataNodeConfig;
use crate::control_plane::NodeId;
use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::data_plane::cold_read::DEFAULT_POOL_SIZE;
use crate::data_plane::messages::DataPlaneMessage;
use crate::data_plane::messages::command::DataPlaneCommand;
use crate::data_plane::transport::command::DataTransportCommand;
use crate::schedulers::actor::spawn_scheduling_actor;
use crate::schedulers::ticker::{TICK_PERIOD_10_MS, TICK_PERIOD_100_MS};

use flume::Sender;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct DataPlaneActor;

impl DataPlaneActor {
    /// Spawn the data-plane worker thread together with its three schedulers
    /// (batch-flush, replication, segment-age) and the bridge that forwards
    /// scheduler callbacks (tokio mpsc) into the worker's flume mailbox.
    pub fn spawn(
        node_id: NodeId,
        config: DataNodeConfig,
        checkpoint_tx: Sender<Box<[CheckpointTask]>>,
        data_transport_tx: BatchSender<DataTransportCommand>,
        coordinator_tx: MutlRaftSender,
        sparse_index: Arc<dyn SparseIndex>,
        recovery: RecoveryOutput,
    ) -> DataPlaneSender {
        let (timer_tx, mut timer_rx) = mpsc::channel::<DataPlaneCommand>(64);

        // Cold-read pool: serves fetches that land on sealed segment files.
        // Shares the same sparse index the checkpoint worker populates.
        let cold_read_tx = ColdReadPool::spawn(DEFAULT_POOL_SIZE, sparse_index);

        let batch_scheduler_tx =
            spawn_scheduling_actor(timer_tx.clone(), 64, TICK_PERIOD_10_MS, None);
        let repl_scheduler_tx =
            spawn_scheduling_actor(timer_tx.clone(), 64, TICK_PERIOD_10_MS, None);

        let _age_scheduler_tx = spawn_scheduling_actor::<SegmentAgeTimer, DataPlaneCommand>(
            timer_tx,
            64,
            TICK_PERIOD_100_MS,
            Some(config.age_check_ticks()),
        );

        let (tx, mailbox) = flume::bounded::<DataPlaneMessage>(4096);

        // Timer bridge: schedulers send via tokio mpsc, bridge forwards to the mailbox.
        let bridge_tx = tx.clone();
        tokio::spawn(async move {
            while let Some(cmd) = timer_rx.recv().await {
                if bridge_tx.send(DataPlaneMessage::Command(cmd)).is_err() {
                    break;
                }
            }
        });

        let self_tx = DataPlaneSender(tx.clone());

        // Built in the worker context: recovery ran in bootstrap (before this node joined the cluster),
        // leaving the WAL dir empty with a verified inventory; open the fresh WAL in that dir.
        let RecoveryOutput {
            inventory,
            data_dir,
        } = recovery;
        // A node that can't open its WAL cannot serve the data plane — fail hard
        // rather than silently leaving the worker dead.
        let wal = WalWriter::new(data_dir).expect("data-plane WAL init failed");
        let out = DataPlaneOutputs::new(
            checkpoint_tx,
            batch_scheduler_tx,
            repl_scheduler_tx,
            data_transport_tx,
            coordinator_tx,
        );

        // The worker loop is async — its dispatch awaits channel capacity rather than
        // blocking (tokio's `blocking_send` panics inside a runtime).
        let worker = async move {
            let mut state =
                DataPlane::new(node_id, config, wal, cold_read_tx, self_tx, out, inventory);
            while let Ok(msg) = mailbox.recv_async().await {
                state.process(msg);
                while let Ok(next) = mailbox.try_recv() {
                    state.process(next);
                }
                state.flush_and_dispatch().await;
            }
        };

        // Production: a dedicated OS thread with its own current-thread runtime, so
        // the data plane's blocking I/O (WAL/fsync) never stalls the main runtime.
        #[cfg(not(test))]
        std::thread::Builder::new()
            .name("data-plane-worker".into())
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("data-plane worker runtime")
                    .block_on(worker);
            })
            .expect("failed to spawn data-plane thread");

        // Test: run the worker as a task on turmoil's runtime so it shares the sim's
        // virtual clock and cooperative scheduling — a real OS thread runs in real
        // wall-time outside turmoil and races virtual-time assertions
        #[cfg(test)]
        tokio::spawn(worker);
        DataPlaneSender(tx)
    }
}

#[derive(Clone)]
pub(crate) struct DataPlaneSender(pub flume::Sender<DataPlaneMessage>);

impl DataPlaneSender {
    pub fn send(
        &self,
        msg: impl Into<DataPlaneMessage>,
    ) -> Result<(), flume::SendError<DataPlaneMessage>> {
        self.0.send(msg.into())
    }
}
