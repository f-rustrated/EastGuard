use std::sync::Arc;

use crate::{
    clusters::NodeId,
    data_plane::{
        SegmentKey, checkpoint::CheckpointJob, messages::command::DataPlaneInterNodeCommand,
        states::segment::cache::CachedBatch, timer::DataPlaneTimer,
    },
    impl_from_variant,
    schedulers::ticker_message::TimerCommand,
};

pub(crate) struct PendingReplicationBatch {
    pub segment_key: SegmentKey,
    pub batch: Arc<CachedBatch>,
    pub replica_set: Vec<NodeId>,
    pub followers: Vec<NodeId>,
}

pub(crate) struct BatchPublished {
    pub lsn: u64,
    pub segment_batches: Vec<PendingReplicationBatch>,
}

pub(crate) struct ReplicaAckReceived {
    pub segment_key: SegmentKey,
    pub end_offset: u64,
    pub from: NodeId,
}

pub(crate) struct InterNodeCommandQueued {
    pub targets: Vec<NodeId>,
    pub message: DataPlaneInterNodeCommand,
}

pub(crate) struct ReplicationTimedOut {
    pub segment_key: SegmentKey,
    pub committed_end_offset: u64,
}

pub(crate) enum DataPlaneEvent {
    CheckpointRequired(CheckpointJob),
    TimerScheduled(TimerCommand<DataPlaneTimer>),
    BatchPublished(BatchPublished),
    ReplicaAckReceived(ReplicaAckReceived),
    InterNodeCommandQueued(InterNodeCommandQueued),
    ReplicationTimedOut(ReplicationTimedOut),
}

impl_from_variant!(
    DataPlaneEvent,
    CheckpointRequired(CheckpointJob),
    TimerScheduled(TimerCommand<DataPlaneTimer>),
    BatchPublished,
    ReplicaAckReceived,
    InterNodeCommandQueued,
    ReplicationTimedOut,
);
