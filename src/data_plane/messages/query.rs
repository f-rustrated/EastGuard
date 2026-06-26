//! Read-only queries served by the data plane.
//!
//! Both are pre-routed by the broker layer — the broker resolves
//! topic_name → topic_id and the range's current `progress_signal` via a
//! metadata query, so by the time the query reaches the data plane there is
//! no remaining I/O to perform.

use std::sync::Arc;

use tokio::sync::oneshot;

use crate::connections::protocol::RangeProgressSignal;
use crate::control_plane::metadata::{RangeId, TopicId};
use crate::data_plane::states::segment::cache::CachedEntry;
use crate::impl_from_variant;

pub enum DataPlaneQuery {
    Fetch(Fetch),
    ListOffsets(ListOffsets),
}

impl_from_variant!(DataPlaneQuery, Fetch, ListOffsets);

/// Consume read request from a client. The broker layer (`clients.rs`) resolves
/// `topic_name → topic_id` and the range's current `progress_signal` via a
/// metadata query before dispatching, so the data plane never reaches into the
/// metadata RSM during its sync apply loop.
pub struct Fetch {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub entry_id: u64,
    pub max_bytes: u32,
    /// Pre-computed by the broker from the topic's metadata snapshot. The data
    /// plane echoes it back on the response so the consumer sees the seal
    /// in-band on the fetch that delivers the range's final records (see
    /// d4_consumer_range_tracking.md §Range Transitions).
    pub progress_signal: RangeProgressSignal,
    pub reply: oneshot::Sender<FetchResult>,
}

#[derive(Debug)]
#[allow(dead_code)] // `EntryIdOutOfRange` reserved for the cold-read path
pub enum FetchResult {
    /// Records read from the segment (may be empty if the consumer is at the
    /// tail). `next_entry_id` is what the consumer should request next.
    ///
    /// Carries `Arc<CachedEntry>`s straight from the tail cache — no
    /// intermediate struct construction. The broker layer (wire translation)
    /// does the single `Bytes → Vec<u8>` copy at serialization time; the
    /// `lsn` field rides along through the channel but isn't on the wire.
    Records {
        entries: Vec<Arc<CachedEntry>>,
        next_entry_id: u64,
        progress_signal: RangeProgressSignal,
    },
    /// `entry_id` was past the last committed offset on this node (e.g. caller
    /// asked for an offset that hasn't been produced/committed yet).
    EntryIdOutOfRange,
    /// This node does not host any segment of the requested `(topic, range)`.
    /// The consumer should re-resolve via `DescribeTopic` and retry.
    SegmentNotLocal,
    InternalError(String),
}

/// Offset-bounds query for a range. Returns the start and currently-committed
/// entry IDs the range's active segment (on this node) holds.
pub struct ListOffsets {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub reply: oneshot::Sender<ListOffsetsResult>,
}

#[derive(Debug)]

pub enum ListOffsetsResult {
    RangeOffsets {
        start_entry_id: u64,
        committed_entry_id: u64,
    },
    SegmentNotLocal,
    #[allow(dead_code)] // reserved for future failure modes
    InternalError(String),
}
