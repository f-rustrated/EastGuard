use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arc_swap::ArcSwapOption;
use tokio::sync::Notify;

use crate::control_plane::metadata::EntryId;
use crate::data_plane::{PayloadBytes, ProducerAppendIdentity};

#[derive(Debug, Clone)]
pub(crate) struct CachedEntry {
    pub(crate) data: PayloadBytes,
    pub(crate) record_count: u32,
    pub(crate) entry_id: EntryId,
    pub(crate) lsn: u64,
    pub(crate) producer_identity: Option<ProducerAppendIdentity>,
}

impl CachedEntry {
    pub(crate) fn byte_len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;

const DEFAULT_CAPACITY: usize = 1024;
const EMPTY_SLOT_POSITION: u64 = u64::MAX;

/// Slots use `ArcSwapOption` instead of raw `AtomicPtr` to make load + refcount
/// increment atomic. With `AtomicPtr`, a consumer at the hot/cold boundary
/// (position == eviction_frontier) could load a raw pointer, then get preempted
/// before calling `Arc::increment_strong_count`. If the publisher wraps the
/// entire ring (~1024 publishes) during that window, the slot is overwritten
/// and the old Arc freed — the consumer wakes up holding a dangling pointer.
/// `ArcSwapOption::load_full()` returns `Option<Arc<…>>` with the refcount
/// already incremented, closing this gap with ~2-5ns overhead per read —
/// negligible since batch processing cost dominates.
///
/// Refcount lifecycle:
///   - `store(Some(batch))`: slot takes ownership. If overwriting, old Arc is
///     dropped (refcount -1). Old batch freed only if no readers hold it.
///   - `load_full()`: returns a new Arc clone (refcount +1). Slot keeps its
///     own ref independently. Caller dropping the Arc decrements refcount.
///   - Eviction: checkpoint completion advances the frontier and clears evicted
///     slots that still hold the same cache position. If a slot wrapped and now
///     holds a newer entry, it is left intact.
///
/// Ordering model:
/// - [`slot_positions`](Self::slot_positions) and cursors(write_cursor, read_cursor, and eviction_frontier) are the publication fences.
///   Writers store them with "Release"; readers load them with Acquire before trusting a slot or cursor boundary.
///
/// - [`slot_byte_lengths`](Self::slot_byte_lengths) and [`byte counter`](Self::hot_cache_bytes) are accounting only.
///   They do not  make entry contents visible, so "Relaxed" is enough.
pub(crate) struct SegmentRingBuffer {
    entries: Box<[ArcSwapOption<CachedEntry>]>,
    slot_positions: Box<[AtomicU64]>,
    slot_byte_lengths: Box<[AtomicU64]>,
    capacity: usize,
    write_cursor: AtomicU64,
    read_cursor: AtomicU64,
    eviction_frontier: AtomicU64,
    hot_cache_bytes: AtomicU64,
    checkpointable_bytes: AtomicU64,
    notify: Notify,
}

impl SegmentRingBuffer {
    pub(crate) fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub(super) fn with_capacity(capacity: usize) -> Self {
        // when capacity is a power of two, we can replace the expensive modulo
        // division with bitwise AND — a single-cycle CPU instruction
        assert!(capacity > 0 && capacity.is_power_of_two());

        let entries = std::iter::repeat_with(|| ArcSwapOption::new(None))
            .take(capacity)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let slot_positions = std::iter::repeat_with(|| AtomicU64::new(EMPTY_SLOT_POSITION))
            .take(capacity)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let slot_byte_lengths = std::iter::repeat_with(|| AtomicU64::new(0))
            .take(capacity)
            .collect::<Vec<_>>()
            .into_boxed_slice();

        SegmentRingBuffer {
            entries,
            slot_positions,
            slot_byte_lengths,
            capacity,
            write_cursor: AtomicU64::new(0),
            read_cursor: AtomicU64::new(0),
            eviction_frontier: AtomicU64::new(0),
            hot_cache_bytes: AtomicU64::new(0),
            checkpointable_bytes: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    fn slot_index(&self, position: u64) -> usize {
        (position as usize) & (self.capacity - 1)
    }

    pub(super) fn publish(&self, entry: Arc<CachedEntry>) {
        let tail = self.write_cursor.load(Ordering::Relaxed);
        let idx = self.slot_index(tail);
        self.hot_cache_bytes
            .fetch_add(entry.byte_len() as u64, Ordering::Relaxed);

        self.slot_byte_lengths[idx].store(entry.byte_len() as u64, Ordering::Relaxed);
        self.entries[idx].store(Some(entry));
        self.slot_positions[idx].store(tail, Ordering::Release);
        self.write_cursor.store(tail + 1, Ordering::Release);
    }

    pub(crate) fn advance_read_cursor(&self, new_offset: u64) {
        let old_offset = self.read_cursor.load(Ordering::Relaxed);
        let committed_bytes: u64 = (old_offset..new_offset)
            .map(|pos| self.byte_len_at(pos))
            .sum();
        self.checkpointable_bytes
            .fetch_add(committed_bytes, Ordering::Relaxed);
        self.read_cursor.store(new_offset, Ordering::Release);
        self.notify.notify_waiters();
    }

    // Read path (called by consumer tasks)
    pub(crate) fn load_read_cursor(&self) -> u64 {
        self.read_cursor.load(Ordering::Acquire)
    }

    pub(crate) fn load_write_cursor(&self) -> u64 {
        self.write_cursor.load(Ordering::Acquire)
    }

    pub(crate) fn read_committed(&self, position: u64) -> Option<Arc<CachedEntry>> {
        let commit = self.read_cursor.load(Ordering::Acquire);
        let frontier = self.eviction_frontier.load(Ordering::Acquire);

        if position >= commit || position < frontier {
            return None;
        }

        let idx = self.slot_index(position);
        self.load_slot_at(idx, position)
    }

    /// Internal read: returns any published entry (committed or uncommitted).
    pub(super) fn load_published(&self, position: u64) -> Option<Arc<CachedEntry>> {
        let tail = self.write_cursor.load(Ordering::Acquire);
        let frontier = self.eviction_frontier.load(Ordering::Acquire);

        if position >= tail || position < frontier {
            return None;
        }

        let idx = self.slot_index(position);
        self.load_slot_at(idx, position)
    }

    fn load_slot_at(&self, idx: usize, position: u64) -> Option<Arc<CachedEntry>> {
        // The requested logical position maps to this physical slot, but does this slot still contain that logical position?”
        if self.slot_positions[idx].load(Ordering::Acquire) != position {
            return None;
        }
        self.entries[idx].load_full()
    }

    fn byte_len_at(&self, position: u64) -> u64 {
        let idx = self.slot_index(position);
        if self.slot_positions[idx].load(Ordering::Acquire) != position {
            return 0;
        }
        self.slot_byte_lengths[idx].load(Ordering::Relaxed)
    }

    pub(super) fn load_eviction_frontier(&self) -> u64 {
        self.eviction_frontier.load(Ordering::Acquire)
    }

    pub(crate) fn drain_for_checkpoint(&self) -> CheckpointBatch {
        let frontier = self.eviction_frontier.load(Ordering::Acquire);
        let commit = self.read_cursor.load(Ordering::Acquire);
        debug_assert!(
            frontier <= commit,
            "eviction_frontier ({frontier}) > read_cursor ({commit})"
        );

        let mut entries = Vec::new();
        for pos in frontier..commit {
            let idx: usize = self.slot_index(pos);
            if let Some(entry) = self.load_slot_at(idx, pos) {
                entries.push(entry);
            }
        }

        CheckpointBatch {
            entries,
            new_frontier: commit,
        }
    }

    pub(crate) fn complete_checkpoint(&self, new_frontier: u64, checkpointed_bytes: u64) {
        let old_frontier = self.eviction_frontier.load(Ordering::Acquire);
        if new_frontier <= old_frontier {
            return;
        }
        self.clear_evicted_slots(old_frontier, new_frontier);

        Self::fetch_sub_saturating(&self.hot_cache_bytes, checkpointed_bytes);
        Self::fetch_sub_saturating(&self.checkpointable_bytes, checkpointed_bytes);

        self.eviction_frontier
            .store(new_frontier, Ordering::Release);
    }

    fn fetch_sub_saturating(counter: &AtomicU64, amount: u64) {
        // why not fetch_sub? because such a raw atomic subtraction may underflow for u64.
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(amount))
        });
    }

    fn clear_evicted_slots(&self, old_frontier: u64, new_frontier: u64) {
        for pos in old_frontier..new_frontier {
            let idx = self.slot_index(pos);
            if self.slot_positions[idx].load(Ordering::Acquire) == pos {
                self.entries[idx].store(None);
                self.slot_byte_lengths[idx].store(0, Ordering::Relaxed);
                self.slot_positions[idx].store(EMPTY_SLOT_POSITION, Ordering::Release);
            }
        }
    }

    // Uses eviction_frontier..read_cursor, because checkpoint can only drain
    // committed entries. The checkpoint worker itself uses the same boundary in
    // drain_for_checkpoint().
    pub(super) fn checkpointable_bytes(&self) -> u64 {
        self.checkpointable_bytes.load(Ordering::Relaxed)
    }

    // hot_cache_bytes: budget pressure signal
    // Uses (eviction_frontier..write_cursor), because this is total resident hot cache,
    // including uncommitted published entries.
    pub(crate) fn hot_cache_bytes(&self) -> u64 {
        self.hot_cache_bytes.load(Ordering::Relaxed)
    }
}

pub(crate) struct CheckpointBatch {
    pub entries: Vec<Arc<CachedEntry>>,
    pub new_frontier: u64,
}

impl CheckpointBatch {
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn last_lsn(&self) -> u64 {
        self.entries.last().map(|e| e.lsn).unwrap_or(0)
    }

    pub(crate) fn byte_len(&self) -> u64 {
        self.entries
            .iter()
            .map(|entry| entry.byte_len() as u64)
            .sum()
    }
}

#[cfg(any(test, debug_assertions))]
impl TAssertInvariant for SegmentRingBuffer {
    fn assert_invariants(&self) {
        let frontier = self.eviction_frontier.load(Ordering::Acquire);
        let commit = self.read_cursor.load(Ordering::Acquire);
        let tail = self.write_cursor.load(Ordering::Acquire);

        assert!(
            frontier <= commit,
            "eviction_frontier ({frontier}) > read_cursor ({commit})"
        );
        assert!(commit <= tail, "read_cursor ({commit}) > tail ({tail})");

        let actual_hot: u64 = (frontier..tail)
            .filter_map(|pos| self.load_published(pos))
            .map(|entry| entry.byte_len() as u64)
            .sum();
        assert_eq!(
            self.hot_cache_bytes.load(Ordering::Relaxed),
            actual_hot,
            "hot cache byte counter mismatch"
        );

        let actual_checkpointable: u64 = (frontier..commit)
            .filter_map(|pos| self.load_published(pos))
            .map(|entry| entry.byte_len() as u64)
            .sum();
        assert_eq!(
            self.checkpointable_bytes.load(Ordering::Relaxed),
            actual_checkpointable,
            "checkpointable byte counter mismatch"
        );

        for pos in frontier..tail {
            let idx = self.slot_index(pos);
            assert!(
                self.load_slot_at(idx, pos).is_some(),
                "empty slot at position {pos} (between frontier and tail)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data_plane::PayloadBytes;
    use bytes::Bytes;

    use super::*;
    fn make_entry(entry_id: u64, lsn: u64) -> Arc<CachedEntry> {
        Arc::new(CachedEntry {
            data: PayloadBytes::from(Bytes::from("test")),
            record_count: 1,
            entry_id: EntryId(entry_id),
            lsn,
            producer_identity: None,
        })
    }

    #[test]
    fn publish_and_read() {
        let cache = SegmentRingBuffer::with_capacity(4);

        let entry = make_entry(0, 1);
        cache.publish(entry.clone());
        cache.advance_read_cursor(1);

        let read = cache.read_committed(0).unwrap();
        assert_eq!(read.entry_id, EntryId(0));
    }

    #[test]
    fn read_beyond_commit_returns_none() {
        let cache = SegmentRingBuffer::with_capacity(4);

        cache.publish(make_entry(0, 1));
        assert!(cache.read_committed(0).is_none());
    }

    #[test]
    fn read_behind_eviction_returns_none() {
        let cache = SegmentRingBuffer::with_capacity(4);

        cache.publish(make_entry(0, 1));
        cache.advance_read_cursor(1);
        cache.complete_checkpoint(1, 4);

        assert!(cache.read_committed(0).is_none());
    }

    #[test]
    fn multiple_publishes() {
        let cache = SegmentRingBuffer::with_capacity(4);

        for i in 0..3u64 {
            cache.publish(make_entry(i, i + 1));
        }
        cache.advance_read_cursor(3);

        for i in 0..3u64 {
            let entry = cache.read_committed(i).unwrap();
            assert_eq!(entry.entry_id, EntryId(i));
        }
    }

    #[test]
    fn eviction_frontier_advances() {
        let cache = SegmentRingBuffer::with_capacity(4);

        cache.publish(make_entry(0, 1));
        cache.publish(make_entry(1, 2));
        cache.advance_read_cursor(2);

        cache.complete_checkpoint(1, 4);

        assert!(cache.read_committed(0).is_none());
        assert!(cache.read_committed(1).is_some());
    }

    #[test]
    fn checkpoint_completion_clears_only_evicted_positions() {
        let cache = SegmentRingBuffer::with_capacity(4);

        for i in 0..4u64 {
            cache.publish(make_entry(i, i + 1));
        }
        cache.advance_read_cursor(4);

        cache.complete_checkpoint(2, 8);

        assert!(cache.entries[0].load().is_none());
        assert!(cache.entries[1].load().is_none());
        assert!(cache.entries[2].load().is_some());
        assert!(cache.entries[3].load().is_some());
        assert_eq!(cache.hot_cache_bytes(), 8);
        assert_eq!(cache.checkpointable_bytes(), 8);
    }

    #[test]
    fn checkpoint_completion_does_not_clear_wrapped_newer_slot() {
        let cache = SegmentRingBuffer::with_capacity(4);

        for i in 0..5u64 {
            cache.publish(make_entry(i, i + 1));
            cache.advance_read_cursor(i + 1);
        }

        cache.complete_checkpoint(1, 4);

        let entry = cache.load_published(4).expect("wrapped entry kept");
        assert_eq!(entry.entry_id, EntryId(4));
    }

    #[test]
    fn drain_for_checkpoint() {
        let cache = SegmentRingBuffer::with_capacity(4);

        for i in 0..3u64 {
            cache.publish(make_entry(i, i + 1));
        }
        cache.advance_read_cursor(3);

        let checkpoint = cache.drain_for_checkpoint();
        assert_eq!(checkpoint.entries.len(), 3);
        assert_eq!(checkpoint.new_frontier, 3);
    }

    #[test]
    fn ring_buffer_wraps() {
        let cache = SegmentRingBuffer::with_capacity(4);

        for i in 0..6u64 {
            cache.publish(make_entry(i, i + 1));
            cache.advance_read_cursor(i + 1);
            if i >= 1 {
                cache.complete_checkpoint(i - 1, 4);
            }
        }

        let entry = cache.read_committed(5).unwrap();
        assert_eq!(entry.entry_id, EntryId(5));
    }

    #[test]
    fn cursors_maintain_ordering() {
        let cache = SegmentRingBuffer::with_capacity(8);

        for i in 0..5u64 {
            cache.publish(make_entry(i, i + 1));
        }
        cache.advance_read_cursor(5);
        cache.complete_checkpoint(2, 8);

        let frontier = cache.load_eviction_frontier();
        let commit = cache.load_read_cursor();
        let tail = cache.load_write_cursor();

        assert!(frontier <= commit);
        assert!(commit <= tail);
    }
}
