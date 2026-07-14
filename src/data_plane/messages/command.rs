use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::data_plane::consumer_offset_management::ledger::ConsumerOffsetKey;
use crate::data_plane::consumer_offset_management::ledger::ConsumerOffsetPosition;
use crate::data_plane::consumer_offset_management::ledger::EpochSeal;
use crate::impl_from_variant;
use crate::impl_from_variant_via;
use borsh::{BorshDeserialize, BorshSerialize};
use std::borrow::Borrow;
use std::sync::Arc;
use tokio::sync::oneshot;

use crate::{
    control_plane::NodeId,
    control_plane::membership::ShardGroupId,
    control_plane::metadata::{EntryId, SegmentId},
    data_plane::states::segment::cache::CachedEntry,
    data_plane::{EntryPayload, SegmentKey, timer::DataPlaneTimeoutCallback},
};

pub enum DataPlaneCommand {
    Produce(Produce),
    SegmentCheckpointComplete(SegmentCheckpointComplete),
    OffsetCheckpointComplete(OffsetCheckpointComplete),
    DataPlaneTimeoutCallback(DataPlaneTimeoutCallback),
    DataPlaneInterNodeCommand(DataPlaneInterNodeCommand),
    /// Internal (not a wire message): the cold-read pool's reply for a catch-up
    /// source read. The worker turns it into `CatchUpChunk`s on the transport.
    CatchUpReadComplete(CatchUpReadComplete),
    OrphanGcCheck(OrphanGcCheck),
    CommitConsumerOffset(CommitConsumerOffset),
}

/// What the orphan-GC handler replies to the ticker: keep ticking while strays remain, or
/// stop once `recovered` is drained.
pub enum OrphanGcSignal {
    KeepTicking,
    Stop,
}

/// The ticker's periodic prompt, carrying the mpsc sender (back to the ticker) the handler
/// replies on. Internal — never serialized.
pub struct OrphanGcCheck {
    pub reply: tokio::sync::mpsc::Sender<OrphanGcSignal>,
}

pub struct Produce {
    pub segment_key: SegmentKey,
    pub data: EntryPayload,
    pub record_count: u32,
    pub reply: oneshot::Sender<ProduceAck>,
}

pub struct CommitConsumerOffset {
    pub key: ConsumerOffsetKey,
    pub generation: GenerationId,
    pub position: ConsumerOffsetPosition,
    pub reply: oneshot::Sender<ConsumerOffsetCommitAck>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerOffsetCommitAck {
    Committed,
    NotWriteLeader(Option<NodeId>),
    StaleEpoch { sealed_generation: GenerationId },
    InternalError(String),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SegmentAssignment {
    pub segment_key: SegmentKey,
    pub shard_group_id: ShardGroupId,
    pub replica_set: Vec<NodeId>,
    pub start_entry_id: EntryId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SegmentAssignmentAck {
    pub segment_key: SegmentKey,
    pub shard_group_id: ShardGroupId,
    pub from: NodeId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ReplicaAppend {
    pub segment_key: SegmentKey,
    pub replica_set: Vec<NodeId>,
    pub data: EntryPayload,
    pub record_count: u32,
    pub entry_id: EntryId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ReplicaAck {
    pub segment_key: SegmentKey,
    pub entry_id: EntryId,
    pub from: NodeId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ReplicaOffsetCommit {
    pub operation_id: u64,
    pub leader: NodeId,
    pub replica_set: Vec<NodeId>,
    pub key: ConsumerOffsetKey,
    pub generation: GenerationId,
    pub position: ConsumerOffsetPosition,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ReplicaOffsetStaleEpoch {
    pub sealed_generation: GenerationId,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum ReplicaOffsetAckResult {
    Committed,
    StaleEpoch(ReplicaOffsetStaleEpoch),
}

impl_from_variant!(ReplicaOffsetAckResult, StaleEpoch(ReplicaOffsetStaleEpoch));

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ReplicaOffsetAck {
    pub operation_id: u64,
    pub key: ConsumerOffsetKey,
    pub generation: GenerationId,
    pub position: ConsumerOffsetPosition,
    pub from: NodeId,
    pub result: ReplicaOffsetAckResult,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CommitAdvance {
    pub segment_key: SegmentKey,
    pub committed_entry_id: EntryId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SealRequest {
    pub from: NodeId,
    pub segment_key: SegmentKey,
    pub failed_nodes: Vec<NodeId>,
    pub end_entry_id: EntryId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SealResponse {
    pub old_segment_key: SegmentKey,
    pub new_segment_id: SegmentId,
    pub new_replica_set: Vec<NodeId>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SegmentSealed {
    pub segment_key: SegmentKey,
    pub committed_entry_id: Option<EntryId>,
}

// Catch-up: re-replicate a sealed segment to a newly assigned replica.
//
// A node assigned a sealed segment it doesn't fully hold fetches the missing
// suffix from a healthy peer. Messages on the `DataPlaneInterNodeCommand` wire:
//
//   coordinator ─CatchUpAssignment─▶ each replica  "own `key` [start, sealed_end]"
//   replacement ─CatchUpRequest────▶ a peer         "I have through `local_end`; send the rest"
//   source      ─CatchUpChunk(s)───▶ replacement   batches of entries
//   source      ─CatchUpStreamEnd──▶ replacement   end of stream
//   replacement ─CatchUpAck────────▶ coordinator   "have it through `sealed_end`"
//
// `CatchUpAssignment`/`CatchUpAck` are the coordinator↔replica pair; the rest is
// the replacement↔source transfer. The ack lets the coordinator stop re-driving.

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CatchUpAssignment {
    pub segment_key: SegmentKey,
    /// Echoed into the `CatchUpAck` so the reply reaches this group's coordinator.
    pub shard_group_id: ShardGroupId,
    pub start_entry_id: EntryId,
    pub sealed_end_entry_id: EntryId,
    pub replica_set: Vec<NodeId>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CatchUpRequest {
    pub segment_key: SegmentKey,
    /// The requesting node — the source streams its `CatchUpChunk`s back here.
    pub from: NodeId,
    /// Highest entry id the requester already holds locally; the source streams
    /// `(local_end, sealed_end]`. `None` means nothing is held.
    pub local_end: Option<EntryId>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CatchUpChunk {
    pub segment_key: SegmentKey,
    /// Contiguous entries in ascending `entry_id` order.
    pub entries: Box<[CatchUpEntry]>,
}
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CatchUpEntry {
    pub entry_id: EntryId,
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

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CatchUpStreamEnd {
    pub segment_key: SegmentKey,
}

/// Replacement → coordinator: "I hold this segment through `sealed_end`." Lets the
/// coordinator stop re-driving. Sent after a transfer verifies, and on a
/// zero-transfer full match so a re-drive re-confirms.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CatchUpAck {
    pub segment_key: SegmentKey,
    pub shard_group_id: ShardGroupId,
    pub from: NodeId,
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
// The originating coordinator rides the query so every report returns to the
// same gather even while shard-leader caches disagree during an election.
// See `docs/data-plane/leader_crash_seal_boundary.md`.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SealBoundaryQuery {
    pub segment_key: SegmentKey,
    pub coordinator: NodeId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SealBoundaryReport {
    pub segment_key: SegmentKey,
    pub from: NodeId,
    pub durable_end: Option<EntryId>,
}

/// Retention (D7): the coordinator tells replicas to reclaim sealed segments —
/// delete the file, drop the cache, and remove the sparse-index entries. Sent per
/// distinct `replica_set` (segments in a range can sit on different sets), batching
/// all of that set's expired segments into one message. Idempotent per key: a node
/// that no longer holds one (already gone / never had it) skips it; orphan GC is the
/// backstop.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct DeleteSegments {
    pub segment_keys: Box<[SegmentKey]>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum DataPlaneInterNodeCommand {
    SegmentAssignment(SegmentAssignment),
    SegmentAssignmentAck(SegmentAssignmentAck),
    ReplicaAppend(ReplicaAppend),
    ReplicaAck(ReplicaAck),
    ReplicaOffsetCommit(ReplicaOffsetCommit),
    ReplicaOffsetAck(ReplicaOffsetAck),
    CommitAdvance(CommitAdvance),
    SealRequest(SealRequest),
    SealResponse(SealResponse),
    SegmentSealed(SegmentSealed),
    CatchUpAssignment(CatchUpAssignment),
    CatchUpRequest(CatchUpRequest),
    CatchUpChunk(CatchUpChunk),
    CatchUpStreamEnd(CatchUpStreamEnd),
    CatchUpAck(CatchUpAck),
    SealBoundaryQuery(SealBoundaryQuery),
    SealBoundaryReport(SealBoundaryReport),
    DeleteSegments(DeleteSegments),
    ConsumerGroupEpochSeal(EpochSeal),
}

impl_from_variant!(
    DataPlaneInterNodeCommand,
    SegmentAssignment,
    SegmentAssignmentAck,
    ReplicaAppend,
    ReplicaAck,
    ReplicaOffsetCommit,
    ReplicaOffsetAck,
    CommitAdvance,
    SealRequest,
    SealResponse,
    SegmentSealed,
    CatchUpAssignment,
    CatchUpRequest,
    CatchUpChunk,
    CatchUpStreamEnd,
    CatchUpAck,
    SealBoundaryQuery,
    SealBoundaryReport,
    DeleteSegments,
    ConsumerGroupEpochSeal(EpochSeal),
);

#[derive(Debug)]
pub enum ProduceAck {
    /// `entry_id` is the committed offset for this produce. Exact for a producer
    /// with one request in flight at a time (the common case); under pipelining
    /// it is the segment's committed highwater at ack time (an upper bound).
    Ok {
        entry_id: EntryId,
    },
    Err(String),
}

pub struct SegmentCheckpointComplete {
    pub segment_key: SegmentKey,
    pub checkpointed_lsn: u64,
    pub new_frontier: u64,
    pub checkpointed_bytes: u64,
}

pub struct OffsetCheckpointComplete {
    pub checkpointed_lsn: u64,
}

/// The cold-read pool's reply for a catch-up *source* read.
pub struct CatchUpReadComplete {
    pub requester: NodeId,
    pub segment_key: SegmentKey,
    pub start_offset: EntryId,
    pub sealed_end: EntryId,
    pub entries: Vec<Arc<CachedEntry>>,
    pub next_offset: EntryId,
}

impl_from_variant!(
    DataPlaneCommand,
    Produce,
    SegmentCheckpointComplete,
    OffsetCheckpointComplete,
    CatchUpReadComplete,
    OrphanGcCheck,
    CommitConsumerOffset,
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
