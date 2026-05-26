use crate::impl_from_variant;
use bincode::{Decode, Encode};
use tokio::sync::oneshot;

use crate::{
    control_plane::NodeId,
    control_plane::metadata::SegmentId,
    control_plane::membership::ShardGroupId,
    data_plane::{EntryPayload, SegmentKey, timer::DataPlaneTimeoutCallback},
};

pub enum DataPlaneCommand {
    Produce(Produce),
    CheckpointComplete(CheckpointComplete),
    Timeout(DataPlaneTimeoutCallback),
    InterNode(DataPlaneInterNodeCommand),
}

pub struct Produce {
    pub segment_key: SegmentKey,
    pub data: EntryPayload,
    pub record_count: u32,
    pub reply: oneshot::Sender<ProduceAck>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SegmentAssignment {
    pub segment_key: SegmentKey,
    pub shard_group_id: ShardGroupId,
    pub replica_set: Vec<NodeId>,
    pub start_entry_id: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ReplicaAppend {
    pub segment_key: SegmentKey,
    pub replica_set: Vec<NodeId>,
    pub data: EntryPayload,
    pub record_count: u32,
    pub entry_id: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ReplicaAck {
    pub segment_key: SegmentKey,
    pub entry_id: u64,
    pub from: NodeId,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct CommitAdvance {
    pub segment_key: SegmentKey,
    pub committed_entry_id: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SealRequest {
    pub from: NodeId,
    pub segment_key: SegmentKey,
    pub failed_nodes: Vec<NodeId>,
    pub end_entry_id: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SealResponse {
    pub old_segment_key: SegmentKey,
    pub new_segment_id: SegmentId,
    pub new_replica_set: Vec<NodeId>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SegmentSealed {
    pub segment_key: SegmentKey,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum DataPlaneInterNodeCommand {
    SegmentAssignment(SegmentAssignment),
    ReplicaAppend(ReplicaAppend),
    ReplicaAck(ReplicaAck),
    CommitAdvance(CommitAdvance),
    SealRequest(SealRequest),
    SealResponse(SealResponse),
    SegmentSealed(SegmentSealed),
}

impl_from_variant!(
    DataPlaneInterNodeCommand,
    SegmentAssignment,
    ReplicaAppend,
    ReplicaAck,
    CommitAdvance,
    SealRequest,
    SealResponse,
    SegmentSealed,
);

#[derive(Debug)]
pub enum ProduceAck {
    Ok,
    Err(String),
}

pub struct CheckpointComplete {
    pub segment_key: SegmentKey,
    pub checkpointed_lsn: u64,
}

impl_from_variant!(
    DataPlaneCommand,
    Produce,
    CheckpointComplete,
    Timeout(DataPlaneTimeoutCallback),
    InterNode(DataPlaneInterNodeCommand),
);
