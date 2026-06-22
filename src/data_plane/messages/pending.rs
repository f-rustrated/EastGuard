use flume::Sender;
use tokio::sync::oneshot;

use crate::channels::BatchSender;
use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::control_plane::consensus::messages::MultiRaftActorCommand;
use crate::data_plane::SegmentKey;
use crate::data_plane::checkpoint::{CheckpointJob, CheckpointTask};
use crate::data_plane::messages::command::ProduceAck;
use crate::data_plane::sparse_index::SparseEntry;
use crate::data_plane::timer::{BatchFlushTimer, ReplicationTimer};
use crate::data_plane::transport::command::DataTransportCommand;
use crate::schedulers::ticker_message::{SchedulerSender, TimerCommand};

pub(crate) struct DataPlaneOutputs {
    checkpoint_tx: Sender<Box<[CheckpointTask]>>,
    batch_scheduler: SchedulerSender<BatchFlushTimer>,
    repl_scheduler: SchedulerSender<ReplicationTimer>,
    transport_tx: BatchSender<DataTransportCommand>,
    coordinator_tx: MutlRaftSender,

    pub(crate) transport_cmds: Vec<DataTransportCommand>,
    pub(crate) checkpoint_tasks: Vec<CheckpointTask>,
    pub(crate) batch_timer_cmds: Vec<TimerCommand<BatchFlushTimer>>,
    pub(crate) repl_schedules: Vec<(u64, ReplicationTimer)>,
    pub(crate) coordinator_cmds: Vec<MultiRaftActorCommand>,
    /// `(entry_id, reply)` — the committed offset is paired with each waiting
    /// producer so the `ProduceAck::Ok` carries it back to the client.
    pub(crate) produce_replies: Vec<(u64, oneshot::Sender<ProduceAck>)>,
}

impl DataPlaneOutputs {
    pub(crate) fn new(
        checkpoint_tx: Sender<Box<[CheckpointTask]>>,
        batch_scheduler: SchedulerSender<BatchFlushTimer>,
        repl_scheduler: SchedulerSender<ReplicationTimer>,
        transport_tx: BatchSender<DataTransportCommand>,
        coordinator_tx: MutlRaftSender,
    ) -> Self {
        Self {
            checkpoint_tx,
            batch_scheduler,
            repl_scheduler,
            transport_tx,
            coordinator_tx,
            transport_cmds: Vec::new(),
            checkpoint_tasks: Vec::new(),
            batch_timer_cmds: Vec::new(),
            repl_schedules: Vec::new(),
            coordinator_cmds: Vec::new(),
            produce_replies: Vec::new(),
        }
    }

    pub(crate) async fn flush(&mut self) {
        for cmd in self.coordinator_cmds.drain(..) {
            let _ = self.coordinator_tx.try_send(cmd);
        }
        for (entry_id, reply) in self.produce_replies.drain(..) {
            let _ = reply.send(ProduceAck::Ok { entry_id });
        }

        let checkpoint_tasks = std::mem::take(&mut self.checkpoint_tasks);
        if !checkpoint_tasks.is_empty() {
            let _ = self.checkpoint_tx.send(checkpoint_tasks.into_boxed_slice());
        }
        self.batch_scheduler
            .send_timer_batch(self.batch_timer_cmds.drain(..).collect())
            .await;

        let repl_cmds: Box<[TimerCommand<ReplicationTimer>]> = self
            .repl_schedules
            .drain(..)
            .map(|(seq, timer)| TimerCommand::SetSchedule { seq, timer })
            .collect();

        self.repl_scheduler.send_timer_batch(repl_cmds).await;
        self.transport_tx
            .send_batch(std::mem::take(&mut self.transport_cmds).into_boxed_slice())
            .await;
    }

    pub(crate) fn store_transport_cmd(&mut self, cmd: impl Into<DataTransportCommand>) {
        self.transport_cmds.push(cmd.into());
    }

    pub(crate) fn store_coordinator_cmd(&mut self, cmd: impl Into<MultiRaftActorCommand>) {
        self.coordinator_cmds.push(cmd.into())
    }

    pub(crate) fn store_batch_produce_timer(&mut self, timer: TimerCommand<BatchFlushTimer>) {
        self.batch_timer_cmds.push(timer);
    }
    pub(crate) fn store_checkpoint(&mut self, job: CheckpointJob) {
        self.checkpoint_tasks.push(CheckpointTask::Checkpoint(job));
    }

    pub(crate) fn store_put_anchors(&mut self, anchors: Vec<SparseEntry>) {
        self.checkpoint_tasks
            .push(CheckpointTask::PutAnchors(anchors.into_boxed_slice()));
    }

    pub(crate) fn store_delete_segment_index(&mut self, segment_key: SegmentKey) {
        self.checkpoint_tasks
            .push(CheckpointTask::DeleteSegmentIndex(segment_key));
    }

    #[cfg(test)]
    pub(crate) fn test() -> Self {
        use crate::control_plane::consensus::actor::MultiRaftActor;
        let (checkpoint_tx, _) = flume::bounded(1);
        let (batch_tx, _) = tokio::sync::mpsc::channel(1);
        let (repl_tx, _) = tokio::sync::mpsc::channel(1);
        let (transport_tx, _) = tokio::sync::mpsc::channel(1);
        let (coordinator_tx, _) = MultiRaftActor::channel(1);
        Self::new(
            checkpoint_tx,
            batch_tx.into(),
            repl_tx.into(),
            transport_tx.into(),
            coordinator_tx,
        )
    }
}
