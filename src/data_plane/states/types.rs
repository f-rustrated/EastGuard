use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::EntryId;
use crate::data_plane::segment_writer::SegmentAppender;
use crate::data_plane::sparse_index::SparseEntry;

/// An in-progress catch-up receive (replacement side): the open segment-file
/// appender, the sealed bounds being filled toward, and the sparse anchors
/// accumulated as chunks land — flushed to the index once the file verifies.
pub(crate) struct PendingCatchUp {
    pub(crate) appender: SegmentAppender,
    pub(crate) shard_group_id: ShardGroupId,
    pub(crate) start_offset: EntryId,
    pub(crate) sealed_end: EntryId,
    /// Highest entry id written so far; `None` until the first append. A resume
    /// re-requests `(last_written, sealed_end]`.
    pub(crate) last_written: Option<EntryId>,
    /// Re-drives since the last append — the stall detector. Reset on append.
    pub(crate) idle_rounds: u32,
    pub(crate) anchors: Vec<SparseEntry>,
}
