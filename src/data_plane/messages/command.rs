use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{
    clusters::NodeId,
    data_plane::{record::SegmentKey, timer::DataPlaneTimeoutCallback},
};

pub enum DataPlaneCommand {
    Produce {
        segment_key: SegmentKey,
        records: Vec<Bytes>,
        reply: oneshot::Sender<ProduceAck>,
    },
    SegmentAssignment {
        segment_key: SegmentKey,
        replica_set: Vec<NodeId>,
    },
    SealSegment {
        segment_key: SegmentKey,
    },
    CheckpointComplete(CheckpointComplete),
    Timeout(DataPlaneTimeoutCallback),
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
