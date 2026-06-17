use std::io;

use crate::data_plane::SegmentKey;

pub struct SparseEntry {
    key: Vec<u8>,
    byte_position: [u8; 8],
}
impl SparseEntry {
    pub(crate) fn new(segment_key: SegmentKey, entry_id: u64, byte_position: [u8; 8]) -> Self {
        let mut key = Vec::with_capacity(32);
        key.extend_from_slice(&segment_key.topic_id.to_be_bytes());
        key.extend_from_slice(&segment_key.range_id.to_be_bytes());
        key.extend_from_slice(&segment_key.segment_id.to_be_bytes());
        key.extend_from_slice(&entry_id.to_be_bytes());
        Self { key, byte_position }
    }
}

/// One index anchor every `INDEX_INTERVAL_ENTRIES` entries. A cold read seeks to
/// the nearest anchor at or below its target and scans forward at most this many
/// entries, trading index size against per-read scan cost.
pub(crate) const INDEX_INTERVAL_ENTRIES: u64 = 64;

/// Whether the batch at byte offset `byte_start` carrying positional `entry_id`
/// gets an index anchor. The segment's first batch (`byte_start == 0`) is always
/// anchored so every in-range read resolves to an anchor at or below its target;
/// otherwise one anchor every [`INDEX_INTERVAL_ENTRIES`] entry ids.
///
/// The live checkpoint writer (`checkpoint.rs`) and the recovery rebuild
/// (`recovery/index_rebuild.rs`) both gate their anchor writes on this single
/// predicate — stateless in absolute position, so the rebuilt index is
/// byte-identical to the live one.
pub(crate) fn is_index_anchor(byte_start: u64, entry_id: u64) -> bool {
    byte_start == 0 || entry_id.is_multiple_of(INDEX_INTERVAL_ENTRIES)
}

pub trait SparseIndex: Send + Sync + 'static {
    fn put_batch(&self, entries: Vec<SparseEntry>) -> io::Result<()>;

    /// Resolves `target_offset` to the nearest index anchor at or below it,
    /// returning `(anchor_entry_id, byte_position)`. The index is sparse, so the
    /// anchor is usually *below* the target: the caller seeks to `byte_position`
    /// and scans forward, counting entry ids up from `anchor_entry_id`. When no
    /// anchor exists at or below the target (only on an unindexed segment — each
    /// segment's first entry is always anchored), falls back to
    /// `(target_offset, 0)`.
    fn seek_index(&self, segment_key: SegmentKey, target_offset: u64) -> (u64, u64);

    /// The byte position of the anchor stored at *exactly* `entry_id`, or `None`
    fn anchor_at(&self, segment_key: SegmentKey, entry_id: u64) -> Option<u64>;
}

impl SparseIndex for rocksdb::DB {
    fn put_batch(&self, entries: Vec<SparseEntry>) -> io::Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        for entry in &entries {
            batch.put(&entry.key, entry.byte_position);
        }
        self.write(batch).map_err(io::Error::other)
    }

    fn seek_index(&self, segment_key: SegmentKey, target_offset: u64) -> (u64, u64) {
        let seek_key = encode_key(segment_key, target_offset);

        let mut iter = self.raw_iterator();
        iter.seek_for_prev(&seek_key);

        if iter.valid()
            && let Some(key) = iter.key()
            && key.len() == 32
            && key[..24] == seek_key[..24]
            && let Some(value) = iter.value()
            && value.len() == 8
        {
            let anchor_id = u64::from_be_bytes(key[24..32].try_into().unwrap());
            let byte_position = u64::from_be_bytes(value.try_into().unwrap());
            return (anchor_id, byte_position);
        }
        (target_offset, 0)
    }

    fn anchor_at(&self, segment_key: SegmentKey, entry_id: u64) -> Option<u64> {
        let key = encode_key(segment_key, entry_id);
        match self.get(&key) {
            Ok(Some(value)) => value.as_slice().try_into().ok().map(u64::from_be_bytes),
            Ok(None) => None,
            Err(e) => {
                // A read error means the index is unhealthy — treat the anchor as
                // absent so recovery rebuilds it (the safe direction).
                tracing::warn!(
                    ?segment_key,
                    entry_id,
                    "sparse index read failed; treating anchor as absent: {e}"
                );
                None
            }
        }
    }
}

#[inline]
fn encode_key(segment_key: SegmentKey, offset: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(&segment_key.topic_id.to_be_bytes());
    key.extend_from_slice(&segment_key.range_id.to_be_bytes());
    key.extend_from_slice(&segment_key.segment_id.to_be_bytes());
    key.extend_from_slice(&offset.to_be_bytes());
    key
}
