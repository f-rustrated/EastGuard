use std::sync::Arc;

use crate::{
    control_plane::NodeId,
    data_plane::{
        SegmentKey,
        checkpoint::CheckpointJob,
        messages::command::{DataPlaneInterNodeCommand, ReplicaAppend},
        states::segment::cache::CachedEntry,
        timer::BatchFlushTimer,
    },
    impl_from_variant,
    schedulers::ticker_message::TimerCommand,
};

pub(crate) struct PendingReplicationBatch {
    pub segment_key: SegmentKey,
    pub entry: Arc<CachedEntry>,
    pub replica_set: Vec<NodeId>,
    pub followers: Vec<NodeId>,
}

impl PendingReplicationBatch {
    pub(crate) fn into_replica_append(self) -> (Vec<NodeId>, ReplicaAppend) {
        let targets = self.followers;
        let message = ReplicaAppend {
            segment_key: self.segment_key,
            replica_set: self.replica_set,
            data: self.entry.data.clone(),
            record_count: self.entry.record_count,
            entry_id: self.entry.entry_id,
        };
        (targets, message)
    }
}

pub(crate) struct BatchPublished {
    pub lsn: u64,
    pub segment_batches: Vec<PendingReplicationBatch>,
}

pub(crate) struct ReplicaAckReceived {
    pub segment_key: SegmentKey,
    pub entry_id: u64,
    pub from: NodeId,
}

pub(crate) struct InterNodeCommandQueued {
    pub targets: Vec<NodeId>,
    pub message: DataPlaneInterNodeCommand,
}
impl InterNodeCommandQueued {
    pub fn new(targets: Vec<NodeId>, message: impl Into<DataPlaneInterNodeCommand>) -> Self {
        Self {
            targets,
            message: message.into(),
        }
    }
}

pub(crate) struct ReplicationTimedOut {
    pub segment_key: SegmentKey,
    pub committed_entry_id: u64,
}

pub(crate) enum DataPlaneEvent {
    CheckpointRequired(CheckpointJob),
    BatchFlushTimerScheduled(TimerCommand<BatchFlushTimer>),
    BatchPublished(BatchPublished),
    ReplicaAckReceived(ReplicaAckReceived),
    InterNodeCommandQueued(InterNodeCommandQueued),
    ReplicationTimedOut(ReplicationTimedOut),
}

impl_from_variant!(
    DataPlaneEvent,
    CheckpointRequired(CheckpointJob),
    BatchFlushTimerScheduled(TimerCommand<BatchFlushTimer>),
    BatchPublished,
    ReplicaAckReceived,
    InterNodeCommandQueued,
    ReplicationTimedOut,
);
