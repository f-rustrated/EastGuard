//! Read-only queries served by the data plane.
//!
//! Both are pre-routed by the broker layer — the broker resolves
//! topic_name → topic_id and the range's current `progress_signal` via a
//! metadata query, so by the time the query reaches the data plane there is
//! no remaining I/O to perform.

use std::sync::Arc;

use borsh::{BorshDeserialize, BorshSerialize};
use tokio::sync::oneshot;

use crate::connections::protocol::{ConsumerOffsetGenerationMismatch, RangeProgressSignal};
use crate::control_plane::NodeId;
use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::control_plane::metadata::{EntryId, RangeId, TopicId};
use crate::data_plane::auxiliary_states::consumer_offsets::state::{
    ConsumerOffsetKey, ConsumerOffsetPosition,
};
use crate::data_plane::states::segment::cache::CachedEntry;
use crate::impl_from_variant;

pub enum DataPlaneQuery {
    Fetch(Fetch),
    ListOffsets(ListOffsets),
    ReadConsumerOffset(ReadConsumerOffset),
}

impl_from_variant!(DataPlaneQuery, Fetch, ListOffsets, ReadConsumerOffset);

/// Consume read request from a client. The broker layer (`clients.rs`) resolves
/// `topic_name → topic_id` and the range's current `progress_signal` via a
/// metadata query before dispatching, so the data plane never reaches into the
/// metadata RSM during its sync apply loop.
use crate::client::ServerError;

pub struct Fetch {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub entry_id: EntryId,
    pub max_bytes: u32,
    /// Pre-computed by the broker from the topic's metadata snapshot. The data
    /// plane echoes it back on the response so the consumer sees the seal
    /// in-band on the fetch that delivers the range's final records (see
    /// d4_consumer_range_tracking.md §Range Transitions).
    pub progress_signal: RangeProgressSignal,
    pub reply: oneshot::Sender<Result<FetchedRecords, ServerError>>,
}

#[derive(Debug)]
pub struct FetchedRecords {
    pub(crate) entries: Vec<Arc<CachedEntry>>,
    pub next_entry_id: EntryId,
    pub progress_signal: RangeProgressSignal,
}

/// Offset-bounds query for a range. Returns the start and currently-committed
/// entry IDs the range's active segment (on this node) holds.
pub struct ListOffsets {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub reply: oneshot::Sender<Result<RangeOffsets, ServerError>>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct RangeOffsets {
    pub start_entry_id: EntryId,
    pub next_entry_id: EntryId,
}

pub struct ReadConsumerOffset {
    pub key: ConsumerOffsetKey,
    pub generation: GenerationId,
    pub reply: oneshot::Sender<ReadConsumerOffsetResult>,
}

pub enum ReadConsumerOffsetResult {
    Offset(Option<ConsumerOffsetPosition>),
    GenerationMismatch(ConsumerOffsetGenerationMismatch),
    NotLeader(Option<NodeId>),
}
