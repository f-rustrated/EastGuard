use std::sync::Arc;

use crate::{
    clusters::NodeId,
    data_plane::{
        SegmentKey, checkpoint::CheckpointJob, states::segment::cache::CachedBatch,
        timer::DataPlaneTimer,
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

pub(crate) struct ReplicationReady {
    pub lsn: u64,
    pub segment_batches: Vec<PendingReplicationBatch>,
}

pub(crate) struct ReplicaAckReady {
    pub leader: NodeId,
    pub from: NodeId,
    pub segment_key: SegmentKey,
    pub end_offset: u64,
}

pub(crate) struct ReplicaAckReceived {
    pub segment_key: SegmentKey,
    pub end_offset: u64,
    pub from: NodeId,
}

pub(crate) struct SendCommitAdvance {
    pub segment_key: SegmentKey,
    pub committed_end_offset: u64,
    pub followers: Vec<NodeId>,
}

pub(crate) struct SendSealRequest {
    pub segment_key: SegmentKey,
    pub failed_nodes: Vec<NodeId>,
    pub end_offset: u64,
}

pub(crate) struct SendSegmentSealed {
    pub segment_key: SegmentKey,
    pub followers: Vec<NodeId>,
}

pub(crate) struct ReplicationTimedOut {
    pub segment_key: SegmentKey,
    pub committed_end_offset: u64,
}

pub(crate) enum DataPlaneEvent {
    SubmitCheckpoint(CheckpointJob),
    Timer(TimerCommand<DataPlaneTimer>),
    ReplicationReady(ReplicationReady),
    ReplicaAckReady(ReplicaAckReady),
    ReplicaAckReceived(ReplicaAckReceived),
    SendCommitAdvance(SendCommitAdvance),
    SendSealRequest(SendSealRequest),
    SendSegmentSealed(SendSegmentSealed),
    ReplicationTimedOut(ReplicationTimedOut),
}

impl_from_variant!(
    DataPlaneEvent,
    ReplicationReady,
    ReplicaAckReady,
    ReplicaAckReceived,
    SendCommitAdvance,
    SendSealRequest,
    SendSegmentSealed,
    ReplicationTimedOut,
);
