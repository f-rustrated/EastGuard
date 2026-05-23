use crate::impl_from_variant;
use bincode::{Decode, Encode};
use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{
    clusters::NodeId,
    clusters::metadata::SegmentId,
    data_plane::{SegmentKey, timer::DataPlaneTimeoutCallback},
};

pub enum DataPlaneCommand {
    Produce {
        segment_key: SegmentKey,
        records: Vec<Bytes>,
        reply: oneshot::Sender<ProduceAck>,
    },
    CheckpointComplete(CheckpointComplete),
    Timeout(DataPlaneTimeoutCallback),
    InterNode(DataPlaneInterNodeCommand),
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SegmentAssignment {
    pub segment_key: SegmentKey,
    pub replica_set: Vec<NodeId>,
    pub start_offset: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ReplicaAppend {
    pub segment_key: SegmentKey,
    pub replica_set: Vec<NodeId>,
    pub records: Vec<Vec<u8>>,
    pub start_offset: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ReplicaAck {
    pub segment_key: SegmentKey,
    pub end_offset: u64,
    pub from: NodeId,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct CommitAdvance {
    pub segment_key: SegmentKey,
    pub committed_end_offset: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SealRequest {
    pub segment_key: SegmentKey,
    pub failed_nodes: Vec<NodeId>,
    pub end_offset: u64,
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

impl From<DataPlaneTimeoutCallback> for DataPlaneCommand {
    fn from(cb: DataPlaneTimeoutCallback) -> Self {
        DataPlaneCommand::Timeout(cb)
    }
}

impl From<DataPlaneInterNodeCommand> for DataPlaneCommand {
    fn from(cmd: DataPlaneInterNodeCommand) -> Self {
        DataPlaneCommand::InterNode(cmd)
    }
}
