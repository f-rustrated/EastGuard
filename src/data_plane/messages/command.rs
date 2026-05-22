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
pub enum DataPlaneInterNodeCommand {
    SegmentAssignment {
        segment_key: SegmentKey,
        replica_set: Vec<NodeId>,
        start_offset: u64,
    },
    ReplicaAppend {
        segment_key: SegmentKey,
        replica_set: Vec<NodeId>,
        records: Vec<Vec<u8>>,
        start_offset: u64,
        end_offset: u64,
    },
    ReplicaAck {
        segment_key: SegmentKey,
        end_offset: u64,
        from: NodeId,
    },
    CommitAdvance {
        segment_key: SegmentKey,
        committed_end_offset: u64,
    },
    SealRequest {
        segment_key: SegmentKey,
        failed_nodes: Vec<NodeId>,
        end_offset: u64,
    },
    SealResponse {
        old_segment_key: SegmentKey,
        new_segment_id: SegmentId,
        new_replica_set: Vec<NodeId>,
    },
    SegmentSealed {
        segment_key: SegmentKey,
    },
}

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
