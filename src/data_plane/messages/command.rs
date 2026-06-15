use crate::impl_from_variant;
use crate::impl_from_variant_via;
use bincode::{Decode, Encode};
use tokio::sync::oneshot;

use crate::{
    control_plane::NodeId,
    control_plane::membership::ShardGroupId,
    control_plane::metadata::SegmentId,
    data_plane::{EntryPayload, SegmentKey, timer::DataPlaneTimeoutCallback},
};

pub enum DataPlaneCommand {
    Produce(Produce),
    CheckpointComplete(CheckpointComplete),
    DataPlaneTimeoutCallback(DataPlaneTimeoutCallback),
    DataPlaneInterNodeCommand(DataPlaneInterNodeCommand),
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
pub struct SegmentAssignmentAck {
    pub segment_key: SegmentKey,
    pub shard_group_id: ShardGroupId,
    pub from: NodeId,
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

// Catch-up: re-replicate a sealed segment to a newly assigned replica
//
// When a node is assigned a sealed segment it does not yet hold completely (a death-driven reassignment, or recovered data reclaimed via the catch-up lottery),
// it brings its local copy up to the sealed end by fetching the missing suffix from a healthy replica. Four messages, all riding the
// `DataPlaneInterNodeCommand` wire (bounded by `DATA_FRAME_MAX`):
//
//   coordinator ─CatchUpAssignment─▶ replacement  "you own `key`; fetch from `source`"
//   replacement ─CatchUpRequest────▶ source       "I have through `local_end`; send the rest"
//   source      ─CatchUpChunk(s)───▶ replacement  bounded batches of entries
//   source      ─CatchUpDone───────▶ replacement  end of stream; verify, then report complete
//
// This commit defines the types, encoding, and routing only — handlers are
// stubbed; the source side lands in commit 20 and the replacement side
// (inventory-aware, per the implementation plan §25) in commit 21.

#[derive(Debug, Clone, Encode, Decode)]
pub struct CatchUpAssignment {
    pub segment_key: SegmentKey,
    /// Base entry id of the sealed segment — lets the replacement create and
    /// position the segment file when it holds no local copy at all.
    pub start_offset: u64,
    /// Committed end entry id of the sealed segment: the catch-up target.
    pub sealed_end: u64,
    /// A healthy replica to fetch the missing suffix from.
    pub source: NodeId,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct CatchUpRequest {
    pub segment_key: SegmentKey,
    /// Highest entry id already held locally; the source streams entries with
    /// `entry_id > local_end`. `None` means nothing is held locally — stream
    /// from the segment's start (avoids underflow when `start_offset == 0`).
    pub local_end: Option<u64>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct CatchUpEntry {
    pub entry_id: u64,
    pub data: EntryPayload,
    pub record_count: u32,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct CatchUpChunk {
    pub segment_key: SegmentKey,
    /// Contiguous entries in ascending `entry_id` order. The source caps each
    /// chunk so the encoded frame stays under `DATA_FRAME_MAX`.
    pub entries: Vec<CatchUpEntry>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct CatchUpDone {
    pub segment_key: SegmentKey,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum DataPlaneInterNodeCommand {
    SegmentAssignment(SegmentAssignment),
    SegmentAssignmentAck(SegmentAssignmentAck),
    ReplicaAppend(ReplicaAppend),
    ReplicaAck(ReplicaAck),
    CommitAdvance(CommitAdvance),
    SealRequest(SealRequest),
    SealResponse(SealResponse),
    SegmentSealed(SegmentSealed),
    CatchUpAssignment(CatchUpAssignment),
    CatchUpRequest(CatchUpRequest),
    CatchUpChunk(CatchUpChunk),
    CatchUpDone(CatchUpDone),
}

impl_from_variant!(
    DataPlaneInterNodeCommand,
    SegmentAssignment,
    SegmentAssignmentAck,
    ReplicaAppend,
    ReplicaAck,
    CommitAdvance,
    SealRequest,
    SealResponse,
    SegmentSealed,
    CatchUpAssignment,
    CatchUpRequest,
    CatchUpChunk,
    CatchUpDone,
);

#[derive(Debug)]
#[allow(dead_code)]
pub enum ProduceAck {
    /// `entry_id` is the committed offset for this produce. Exact for a producer
    /// with one request in flight at a time (the common case); under pipelining
    /// it is the segment's committed highwater at ack time (an upper bound).
    Ok {
        entry_id: u64,
    },
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
    DataPlaneTimeoutCallback(DataPlaneTimeoutCallback),
    DataPlaneInterNodeCommand(DataPlaneInterNodeCommand),
);

use crate::data_plane::timer::{BatchFlushCallback, ReplicationCallback, SegmentAgeCallback};

impl_from_variant_via!(
    DataPlaneCommand,
    DataPlaneTimeoutCallback,
    BatchFlushCallback
);
impl_from_variant_via!(
    DataPlaneCommand,
    DataPlaneTimeoutCallback,
    ReplicationCallback
);
impl_from_variant_via!(
    DataPlaneCommand,
    DataPlaneTimeoutCallback,
    SegmentAgeCallback
);
