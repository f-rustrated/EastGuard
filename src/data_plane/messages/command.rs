use crate::control_plane::Replicas;
use crate::data_plane::consumer_offset_management::ledger::ConsumerOffsetSnapshot;
use crate::data_plane::consumer_offset_management::ledger::ConsumerOffsetUpdate;
use crate::data_plane::consumer_offset_management::ledger::EpochSeal;
use crate::data_plane::consumer_offset_management::ledger::StaleEpoch;
use crate::data_plane::consumer_offset_management::types::ReplicateConsumerOffset;
use crate::impl_from_variant;
use crate::impl_from_variant_via;
use borsh::{BorshDeserialize, BorshSerialize};
use std::borrow::Borrow;
use std::sync::Arc;
use tokio::sync::oneshot;

use crate::{
    control_plane::NodeId,
    control_plane::membership::ShardGroupId,
    control_plane::metadata::{EntryId, RangeId, SegmentId, TopicId},
    data_plane::states::segment::cache::CachedEntry,
    data_plane::{EntryPayload, SegmentKey, timer::DataPlaneTimeoutCallback},
};

pub enum DataPlaneCommand {
    Produce(Produce),
    SegmentCheckpointComplete(SegmentCheckpointComplete),
    OffsetCheckpointComplete(OffsetCheckpointComplete),
    DataPlaneTimeoutCallback(DataPlaneTimeoutCallback),
    DataPlanePeerMessage(DataPlanePeerMessage),
    /// Internal (not a wire message): the cold-read pool's reply for a catch-up
    /// source read. The worker turns it into `CatchUpEntries`s on the transport.
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
    pub update: ConsumerOffsetUpdate,
    pub reply: oneshot::Sender<ConsumerOffsetCommitAck>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerOffsetCommitAck {
    Committed,
    NotWriteLeader(Option<NodeId>),
    StaleEpoch(StaleEpoch),
    InternalError(String),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct PlaceSegment {
    pub segment_key: SegmentKey,
    pub shard_group_id: ShardGroupId,
    pub replica_set: Replicas,
    pub start_entry_id: EntryId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SegmentPlaced {
    pub segment_key: SegmentKey,
    pub shard_group_id: ShardGroupId,
    pub from: NodeId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct AppendReplicaEntries {
    pub segment_key: SegmentKey,
    pub replicas: Replicas,
    pub data: EntryPayload,
    pub record_count: u32,
    pub entry_id: EntryId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ReplicaEntriesAppended {
    pub segment_key: SegmentKey,
    pub entry_id: EntryId,
    pub from: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum ConsumerOffsetReplicationResult {
    Committed,
    StaleEpoch(StaleEpoch),
}

// impl_from_variant!(ConsumerOffsetReplicationResult, StaleEpoch);

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ConsumerOffsetReplicated {
    pub seq: u64,
    pub update: ConsumerOffsetUpdate,
    pub from: NodeId,
    pub result: ConsumerOffsetReplicationResult,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct InstallConsumerOffsetSnapshot {
    pub segment_key: SegmentKey,
    pub replica_set: Replicas,
    pub entries: Box<[ConsumerOffsetSnapshot]>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct RequestConsumerOffsetSnapshot {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub requester: NodeId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ConsumerOffsetSnapshotInstalled {
    pub segment_key: SegmentKey,
    pub from: NodeId,
    pub leader: NodeId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct AdvanceReplicaCommit {
    pub segment_key: SegmentKey,
    pub committed_entry_id: EntryId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct RequestSegmentRoll {
    pub from: NodeId,
    pub segment_key: SegmentKey,
    pub failed_nodes: Vec<NodeId>,
    pub end_entry_id: EntryId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SegmentRollCommitted {
    pub old_segment_key: SegmentKey,
    pub new_segment_id: SegmentId,
    pub new_replica_set: Replicas,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SegmentSealed {
    pub segment_key: SegmentKey,
    pub committed_entry_id: Option<EntryId>,
}

// Catch-up: re-replicate a sealed segment to a newly assigned replica.
//
// A node assigned a sealed segment it doesn't fully hold fetches the missing
// suffix from a healthy peer. Messages on the `DataPlanePeerMessage` wire:
//
//   coordinator ─AssignSegmentCatchUp───▶ each replica  "own `key` [start, sealed_end]"
//   replacement ─RequestCatchUpEntries──▶ source replica "send after `local_end`"
//   source      ─CatchUpEntries─────────▶ replacement    batches of entries
//   source      ─CatchUpEntriesSent─────▶ replacement    end of stream
//   replacement ─SegmentCaughtUp────────▶ coordinator    "have it through `sealed_end`"
//
// `AssignSegmentCatchUp`/`SegmentCaughtUp` are the coordinator↔replica pair; the rest is
// the replacement↔source transfer. The ack lets the coordinator stop re-driving.

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct AssignSegmentCatchUp {
    pub segment_key: SegmentKey,
    /// Echoed into the `SegmentCaughtUp` so the reply reaches this group's coordinator.
    pub shard_group_id: ShardGroupId,
    pub start_entry_id: EntryId,
    pub sealed_end_entry_id: EntryId,
    pub replica_set: Replicas,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct RequestCatchUpEntries {
    pub segment_key: SegmentKey,
    /// The requesting node — the source streams `CatchUpEntries` back here.
    pub from: NodeId,
    /// Highest entry id the requester already holds locally; the source streams
    /// `(local_end, sealed_end]`. `None` means nothing is held.
    pub local_end: Option<EntryId>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct CatchUpEntries {
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
pub struct CatchUpEntriesSent {
    pub segment_key: SegmentKey,
}

/// Replacement → coordinator: "I hold this segment through `sealed_end`." Lets the
/// coordinator stop re-driving. Sent after a transfer verifies, and on a
/// zero-transfer full match so a re-drive re-confirms.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SegmentCaughtUp {
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
// all-replica ack). Two messages on the `DataPlanePeerMessage` wire:
//
//   coordinator ─RequestDurableSegmentEnd──▶ each survivor  "what's your durable extent for `key`?"
//   survivor    ─DurableSegmentEndReported─▶ coordinator    durable end (or `None` if it holds nothing)
//
// The originating coordinator rides the query so every report returns to the
// same gather even while shard-leader caches disagree during an election.
// See `docs/data-plane/leader_crash_seal_boundary.md`.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct RequestDurableSegmentEnd {
    pub segment_key: SegmentKey,
    pub coordinator: NodeId,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct DurableSegmentEndReported {
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
pub enum DataPlanePeerMessage {
    PlaceSegment(PlaceSegment),
    SegmentPlaced(SegmentPlaced),
    AppendReplicaEntries(AppendReplicaEntries),
    ReplicaEntriesAppended(ReplicaEntriesAppended),
    ReplicateConsumerOffset(ReplicateConsumerOffset),
    ConsumerOffsetReplicated(ConsumerOffsetReplicated),
    InstallConsumerOffsetSnapshot(InstallConsumerOffsetSnapshot),
    RequestConsumerOffsetSnapshot(RequestConsumerOffsetSnapshot),
    ConsumerOffsetSnapshotInstalled(ConsumerOffsetSnapshotInstalled),
    AdvanceReplicaCommit(AdvanceReplicaCommit),
    RequestSegmentRoll(RequestSegmentRoll),
    SegmentRollCommitted(SegmentRollCommitted),
    SegmentSealed(SegmentSealed),
    AssignSegmentCatchUp(AssignSegmentCatchUp),
    RequestCatchUpEntries(RequestCatchUpEntries),
    CatchUpEntries(CatchUpEntries),
    CatchUpEntriesSent(CatchUpEntriesSent),
    SegmentCaughtUp(SegmentCaughtUp),
    RequestDurableSegmentEnd(RequestDurableSegmentEnd),
    DurableSegmentEndReported(DurableSegmentEndReported),
    DeleteSegments(DeleteSegments),
    ConsumerGroupEpochSealed(EpochSeal),
}

impl_from_variant!(
    DataPlanePeerMessage,
    PlaceSegment,
    SegmentPlaced,
    AppendReplicaEntries,
    ReplicaEntriesAppended,
    ReplicateConsumerOffset,
    ConsumerOffsetReplicated,
    InstallConsumerOffsetSnapshot,
    RequestConsumerOffsetSnapshot,
    ConsumerOffsetSnapshotInstalled,
    AdvanceReplicaCommit,
    RequestSegmentRoll,
    SegmentRollCommitted,
    SegmentSealed,
    AssignSegmentCatchUp,
    RequestCatchUpEntries,
    CatchUpEntries,
    CatchUpEntriesSent,
    SegmentCaughtUp,
    RequestDurableSegmentEnd,
    DurableSegmentEndReported,
    DeleteSegments,
    ConsumerGroupEpochSealed(EpochSeal),
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
    DataPlanePeerMessage(DataPlanePeerMessage),
);

use crate::data_plane::timer::{BatchFlushCallback, ReplicationCallback, SegmentAgeCallback};

impl_from_variant_via!(
    DataPlaneCommand,
    DataPlaneTimeoutCallback,
    BatchFlushCallback,
    ReplicationCallback,
    SegmentAgeCallback
);
