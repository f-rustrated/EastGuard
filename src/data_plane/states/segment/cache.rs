use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arc_swap::ArcSwapOption;
use tokio::sync::Notify;

use crate::data_plane::EntryPayload;

#[derive(Debug, Clone)]
pub struct CachedEntry {
    pub data: EntryPayload,
    pub record_count: u32,
    pub entry_id: u64,
    pub lsn: u64,
}
#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;

const DEFAULT_CAPACITY: usize = 1024;

// Slots use `ArcSwapOption` instead of raw `AtomicPtr` to make load + refcount
// increment atomic. With `AtomicPtr`, a consumer at the hot/cold boundary
// (position == eviction_frontier) could load a raw pointer, then get preempted
// before calling `Arc::increment_strong_count`. If the publisher wraps the
// entire ring (~1024 publishes) during that window, the slot is overwritten
// and the old Arc freed — the consumer wakes up holding a dangling pointer.
// `ArcSwapOption::load_full()` returns `Option<Arc<…>>` with the refcount
// already incremented, closing this gap with ~2-5ns overhead per read —
// negligible since batch processing cost dominates.
//
// Refcount lifecycle:
//   - `store(Some(batch))`: slot takes ownership. If overwriting, old Arc is
//     dropped (refcount -1). Old batch freed only if no readers hold it.
//   - `load_full()`: returns a new Arc clone (refcount +1). Slot keeps its
//     own ref independently. Caller dropping the Arc decrements refcount.
//   - Eviction: `advance_eviction_frontier` moves the cursor but does NOT
//     clear the slot. The old Arc stays alive until `publish` overwrites it
//     on the next ring wrap. This is delayed cleanup, not a leak — the slot
//     is logically evicted (readers rejected by frontier check) but the
//     memory is reclaimed when the publisher physically reaches that slot.
pub(crate) struct SegmentRingBuffer {
    entries: Box<[ArcSwapOption<CachedEntry>]>,
    capacity: usize,
    write_cursor: AtomicU64,
    read_cursor: AtomicU64,
    eviction_frontier: AtomicU64,
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

        SegmentRingBuffer {
            entries,
            capacity,
            write_cursor: AtomicU64::new(0),
            read_cursor: AtomicU64::new(0),
            eviction_frontier: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    fn slot_index(&self, position: u64) -> usize {
        (position as usize) & (self.capacity - 1)
    }

    // Write path (called by DataPlane)

    pub(super) fn publish(&self, entry: Arc<CachedEntry>) {
        let tail = self.write_cursor.load(Ordering::Acquire);
        let idx = self.slot_index(tail);
        self.entries[idx].store(Some(entry));

        // ! SAFETY: why not self.write_cursor.fetch_add(1)?
        // ! because there is only one writer thread
        self.write_cursor.store(tail + 1, Ordering::Release);
    }

    pub(in crate::data_plane::states::segment) fn advance_read_cursor(&self, new_offset: u64) {
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

    #[allow(dead_code)]
    pub async fn notified(&self) {
        self.notify.notified().await;
    }

    pub(crate) fn read_committed(&self, position: u64) -> Option<Arc<CachedEntry>> {
        let commit = self.read_cursor.load(Ordering::Acquire);
        let frontier = self.eviction_frontier.load(Ordering::Acquire);

        if position >= commit || position < frontier {
            return None;
        }

        let idx = self.slot_index(position);
        self.entries[idx].load_full()
    }

    /// Internal read: returns any published entry (committed or uncommitted).
    pub(super) fn load_published(&self, position: u64) -> Option<Arc<CachedEntry>> {
        let tail = self.write_cursor.load(Ordering::Acquire);
        let frontier = self.eviction_frontier.load(Ordering::Acquire);

        if position >= tail || position < frontier {
            return None;
        }

        let idx = self.slot_index(position);
        self.entries[idx].load_full()
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
            if let Some(entry) = self.entries[idx].load_full() {
                entries.push(entry);
            }
        }

        CheckpointBatch {
            entries,
            new_frontier: commit,
        }
    }

    pub(in crate::data_plane::states::segment) fn advance_eviction_frontier(
        &self,
        new_frontier: u64,
    ) {
        self.eviction_frontier
            .store(new_frontier, Ordering::Release);
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

        for pos in frontier..tail {
            let idx = self.slot_index(pos);
            assert!(
                self.entries[idx].load().is_some(),
                "empty slot at position {pos} (between frontier and tail)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data_plane::EntryPayload;
    use bytes::Bytes;

    use super::*;
    fn make_entry(entry_id: u64, lsn: u64) -> Arc<CachedEntry> {
        Arc::new(CachedEntry {
            data: EntryPayload::from(Bytes::from("test")),
            record_count: 1,
            entry_id,
            lsn,
        })
    }

    #[test]
    fn publish_and_read() {
        let cache = SegmentRingBuffer::with_capacity(4);

        let entry = make_entry(0, 1);
        cache.publish(entry.clone());
        cache.advance_read_cursor(1);

        let read = cache.read_committed(0).unwrap();
        assert_eq!(read.entry_id, 0);
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
        cache.advance_eviction_frontier(1);

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
            assert_eq!(entry.entry_id, i);
        }
    }

    #[test]
    fn eviction_frontier_advances() {
        let cache = SegmentRingBuffer::with_capacity(4);

        cache.publish(make_entry(0, 1));
        cache.publish(make_entry(1, 2));
        cache.advance_read_cursor(2);

        cache.advance_eviction_frontier(1);

        assert!(cache.read_committed(0).is_none());
        assert!(cache.read_committed(1).is_some());
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
            if i >= 1 {
                cache.advance_eviction_frontier(i - 1);
            }
        }
        cache.advance_read_cursor(6);

        let entry = cache.read_committed(5).unwrap();
        assert_eq!(entry.entry_id, 5);
    }

    #[test]
    fn cursors_maintain_ordering() {
        let cache = SegmentRingBuffer::with_capacity(8);

        for i in 0..5u64 {
            cache.publish(make_entry(i, i + 1));
        }
        cache.advance_read_cursor(5);
        cache.advance_eviction_frontier(2);

        let frontier = cache.load_eviction_frontier();
        let commit = cache.load_read_cursor();
        let tail = cache.load_write_cursor();

        assert!(frontier <= commit);
        assert!(commit <= tail);
    }
}
