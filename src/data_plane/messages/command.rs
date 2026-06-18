use crate::impl_from_variant;
use crate::impl_from_variant_via;
use bincode::{Decode, Encode};
use std::borrow::Borrow;
use std::sync::Arc;
use tokio::sync::oneshot;

use crate::{
    control_plane::NodeId,
    control_plane::membership::ShardGroupId,
    control_plane::metadata::SegmentId,
    data_plane::states::segment::cache::CachedEntry,
    data_plane::{EntryPayload, SegmentKey, timer::DataPlaneTimeoutCallback},
};

pub enum DataPlaneCommand {
    Produce(Produce),
    CheckpointComplete(CheckpointComplete),
    DataPlaneTimeoutCallback(DataPlaneTimeoutCallback),
    DataPlaneInterNodeCommand(DataPlaneInterNodeCommand),
    /// Internal (not a wire message): the cold-read pool's reply for a catch-up
    /// source read. The worker turns it into `CatchUpChunk`s on the transport.
    CatchUpReadComplete(CatchUpReadComplete),
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
//   coordinator ─CatchUpAssignment─▶ each replica "own `key` [start, sealed_end]; reconcile vs your inventory"
//   replacement ─CatchUpRequest────▶ a peer        "I have through `local_end`; send the rest"
//   source      ─CatchUpChunk(s)───▶ replacement  bounded batches of entries
//   source      ─CatchUpDone───────▶ replacement  end of stream; verify, then report complete
//
// This commit defines the types, encoding, and routing only — handlers are
// stubbed; the source side lands in commit 20 and the replacement side
// (inventory-aware, per the implementation plan §25) in commit 21.

#[derive(Debug, Clone, Encode, Decode)]
pub struct CatchUpAssignment {
    pub segment_key: SegmentKey,
    pub start_entry_id: u64,
    pub sealed_end_entry_id: u64,
    pub replica_set: Vec<NodeId>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct CatchUpRequest {
    pub segment_key: SegmentKey,
    /// The requesting node — the source streams its `CatchUpChunk`s back here.
    pub from: NodeId,
    /// Highest entry id the requester already holds locally; the source streams
    /// `(local_end, sealed_end]`. `None` means nothing is held.
    pub local_end: Option<u64>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct CatchUpChunk {
    pub segment_key: SegmentKey,
    /// Contiguous entries in ascending `entry_id` order.
    pub entries: Box<[CatchUpEntry]>,
}
#[derive(Debug, Clone, Encode, Decode)]
pub struct CatchUpEntry {
    pub entry_id: u64,
    pub data: EntryPayload,
    pub record_count: u32,
}
impl CatchUpEntry {
    pub(crate) fn from_cache(cache: impl Borrow<CachedEntry>) -> Self {
        let cache = cache.borrow();
        CatchUpEntry {
            entry_id: cache.entry_id,
            data: cache.data.clone(),
            record_count: cache.record_count,
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct CatchUpDone {
    pub segment_key: SegmentKey,
}

// Leader-crash boundary recovery: establish a sealed segment's committed end.
//
// When an active segment's write leader crashes, no surviving node knows the
// committed end (the leader was the one tracking it). The coordinator gathers
// each survivor's durable (fsync'd) extent and seals at their `min` — the
// highest offset present on every survivor, hence committed (commit requires
// all-replica ack). Two messages on the `DataPlaneInterNodeCommand` wire:
//
//   coordinator ─SealBoundaryQuery──▶ each survivor  "what's your durable extent for `key`?"
//   survivor    ─SealBoundaryReport─▶ coordinator    durable end (or `None` if it holds nothing)
//
// `shard_group_id` rides the query so the survivor can address the report back
// to the coordinator (a follower's tracker doesn't carry the real group id).
// See `diagrams/data-plane/leader_crash_seal_boundary.md`.
#[derive(Debug, Clone, Encode, Decode)]
pub struct SealBoundaryQuery {
    pub segment_key: SegmentKey,
    pub shard_group_id: ShardGroupId,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SealBoundaryReport {
    pub segment_key: SegmentKey,
    pub from: NodeId,
    pub durable_end: Option<u64>,
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
    SealBoundaryQuery(SealBoundaryQuery),
    SealBoundaryReport(SealBoundaryReport),
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
    SealBoundaryQuery,
    SealBoundaryReport,
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

/// The cold-read pool's reply for a catch-up *source* read.
pub struct CatchUpReadComplete {
    pub requester: NodeId,
    pub segment_key: SegmentKey,
    pub start_offset: u64,
    pub sealed_end: u64,
    pub entries: Vec<Arc<CachedEntry>>,
    pub next_offset: u64,
}

impl_from_variant!(
    DataPlaneCommand,
    Produce,
    CheckpointComplete,
    CatchUpReadComplete,
    DataPlaneTimeoutCallback(DataPlaneTimeoutCallback),
    DataPlaneInterNodeCommand(DataPlaneInterNodeCommand),
);

use crate::data_plane::timer::{BatchFlushCallback, ReplicationCallback, SegmentAgeCallback};

impl_from_variant_via!(
    DataPlaneCommand,
    DataPlaneTimeoutCallback,
    BatchFlushCallback,
    ReplicationCallback,
    SegmentAgeCallback
);
