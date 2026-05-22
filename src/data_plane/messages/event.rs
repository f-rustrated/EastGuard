use std::sync::Arc;

use tokio::sync::oneshot;

use crate::{
    clusters::NodeId,
    data_plane::{
        SegmentKey,
        checkpoint::CheckpointJob,
        messages::ProduceAck,
        states::segment::cache::CachedBatch,
        timer::DataPlaneTimer,
    },
    schedulers::ticker_message::TimerCommand,
};

pub(crate) struct PendingReplicationBatch {
    pub segment_key: SegmentKey,
    pub batch: Arc<CachedBatch>,
    pub replica_set: Vec<NodeId>,
    pub followers: Vec<NodeId>,
}

pub(crate) enum DataPlaneEvent {
    SubmitCheckpoint(CheckpointJob),
    ProducePending {
        segment_key: SegmentKey,
        reply: oneshot::Sender<ProduceAck>,
    },
    WalBatchComplete {
        lsn: u64,
    },
    WalBatchFailed(String),
    Timer(TimerCommand<DataPlaneTimer>),
    ReplicationReady {
        lsn: u64,
        segment_batches: Vec<PendingReplicationBatch>,
    },
    ReplicaAckReady {
        leader: NodeId,
        from: NodeId,
        segment_key: SegmentKey,
        end_offset: u64,
    },
    ReplicaAckReceived {
        segment_key: SegmentKey,
        end_offset: u64,
        from: NodeId,
    },
    SendCommitAdvance {
        segment_key: SegmentKey,
        committed_end_offset: u64,
        followers: Vec<NodeId>,
    },
    MigrateReplies {
        old_segment_key: SegmentKey,
        new_segment_key: SegmentKey,
    },
    SendSealRequest {
        segment_key: SegmentKey,
        failed_nodes: Vec<NodeId>,
        end_offset: u64,
    },
    SendSegmentSealed {
        segment_key: SegmentKey,
        followers: Vec<NodeId>,
    },
    ReplicationTimedOut {
        segment_key: SegmentKey,
        committed_end_offset: u64,
    },
}
