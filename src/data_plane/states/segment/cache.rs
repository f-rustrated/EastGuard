use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub struct CachedBatch {
    pub records: Vec<Bytes>,
    pub start_offset: u64,
    pub end_offset: u64,
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
    batches: Box<[ArcSwapOption<CachedBatch>]>,
    capacity: usize,
    tail: AtomicU64,
    commit_offset: AtomicU64,
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

        let batches = std::iter::repeat_with(|| ArcSwapOption::new(None))
            .take(capacity)
            .collect::<Vec<_>>()
            .into_boxed_slice();

        SegmentRingBuffer {
            batches,
            capacity,
            tail: AtomicU64::new(0),
            commit_offset: AtomicU64::new(0),
            eviction_frontier: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    fn slot_index(&self, position: u64) -> usize {
        (position as usize) & (self.capacity - 1)
    }

    // Write path (called by DataPlane)

    pub(super) fn publish(&self, batch: Arc<CachedBatch>) {
        let tail = self.tail.load(Ordering::Acquire);
        let idx = self.slot_index(tail);
        self.batches[idx].store(Some(batch));

        // ! SAFETY: why not self.tail.fetch_add(1)?
        // ! because there is only one writer thread
        self.tail.store(tail + 1, Ordering::Release);
    }

    pub(super) fn commit(&self, new_offset: u64) {
        self.commit_offset.store(new_offset, Ordering::Release);
        self.notify.notify_waiters();
    }

    // Read path (called by consumer tasks)

    pub(crate) fn load_commit_offset(&self) -> u64 {
        self.commit_offset.load(Ordering::Acquire)
    }

    pub(crate) fn load_tail(&self) -> u64 {
        self.tail.load(Ordering::Acquire)
    }

    pub async fn notified(&self) {
        self.notify.notified().await;
    }

    /// Consumer read: only returns committed, non-evicted batches.
    pub(super) fn read_committed(&self, position: u64) -> Option<Arc<CachedBatch>> {
        let commit = self.commit_offset.load(Ordering::Acquire);
        let frontier = self.eviction_frontier.load(Ordering::Acquire);

        if position >= commit || position < frontier {
            return None;
        }

        let idx = self.slot_index(position);
        self.batches[idx].load_full()
    }

    /// Internal read: returns any published batch (committed or uncommitted).
    pub(super) fn load_published(&self, position: u64) -> Option<Arc<CachedBatch>> {
        let tail = self.tail.load(Ordering::Acquire);
        let frontier = self.eviction_frontier.load(Ordering::Acquire);

        if position >= tail || position < frontier {
            return None;
        }

        let idx = self.slot_index(position);
        self.batches[idx].load_full()
    }

    pub(super) fn load_eviction_frontier(&self) -> u64 {
        self.eviction_frontier.load(Ordering::Acquire)
    }

    // Checkpoint path (called by checkpoint worker)
    pub(crate) fn drain_for_checkpoint(&self) -> CheckpointBatch {
        let frontier = self.eviction_frontier.load(Ordering::Acquire);
        let commit = self.commit_offset.load(Ordering::Acquire);

        if frontier >= commit {
            return CheckpointBatch {
                batches: Vec::new(),
                new_frontier: frontier,
            };
        }

        let mut batches = Vec::with_capacity((commit - frontier) as usize);
        for pos in frontier..commit {
            let idx = self.slot_index(pos);
            if let Some(batch) = self.batches[idx].load_full() {
                batches.push(batch);
            }
        }

        CheckpointBatch {
            batches,
            new_frontier: commit,
        }
    }

    pub(crate) fn advance_eviction_frontier(&self, new_frontier: u64) {
        self.eviction_frontier
            .store(new_frontier, Ordering::Release);
    }
}

pub(crate) struct CheckpointBatch {
    pub batches: Vec<Arc<CachedBatch>>,
    pub new_frontier: u64,
}

impl CheckpointBatch {
    pub(crate) fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub(crate) fn last_lsn(&self) -> u64 {
        self.batches.last().map(|b| b.lsn).unwrap_or(0)
    }
}

#[cfg(any(test, debug_assertions))]
impl TAssertInvariant for SegmentRingBuffer {
    fn assert_invariants(&self) {
        let frontier = self.eviction_frontier.load(Ordering::Acquire);
        let commit = self.commit_offset.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        assert!(
            frontier <= commit,
            "eviction_frontier ({frontier}) > commit_offset ({commit})"
        );
        assert!(commit <= tail, "commit_offset ({commit}) > tail ({tail})");

        for pos in frontier..tail {
            let idx = self.slot_index(pos);
            assert!(
                self.batches[idx].load().is_some(),
                "empty slot at position {pos} (between frontier and tail)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    fn make_batch(start: u64, end: u64, lsn: u64) -> Arc<CachedBatch> {
        Arc::new(CachedBatch {
            records: vec![Bytes::from("test")],
            start_offset: start,
            end_offset: end,
            lsn,
        })
    }

    #[test]
    fn publish_and_read() {
        let cache = SegmentRingBuffer::with_capacity(4);

        let batch = make_batch(0, 10, 1);
        cache.publish(batch.clone());
        cache.commit(1);

        let read = cache.read_committed(0).unwrap();
        assert_eq!(read.start_offset, 0);
        assert_eq!(read.end_offset, 10);
    }

    #[test]
    fn read_beyond_commit_returns_none() {
        let cache = SegmentRingBuffer::with_capacity(4);

        cache.publish(make_batch(0, 10, 1));
        assert!(cache.read_committed(0).is_none());
    }

    #[test]
    fn read_behind_eviction_returns_none() {
        let cache = SegmentRingBuffer::with_capacity(4);

        cache.publish(make_batch(0, 10, 1));
        cache.commit(1);
        cache.advance_eviction_frontier(1);

        assert!(cache.read_committed(0).is_none());
    }

    #[test]
    fn multiple_publishes() {
        let cache = SegmentRingBuffer::with_capacity(4);

        for i in 0..3u64 {
            cache.publish(make_batch(i * 10, (i + 1) * 10, i + 1));
        }
        cache.commit(3);

        for i in 0..3u64 {
            let batch = cache.read_committed(i).unwrap();
            assert_eq!(batch.start_offset, i * 10);
        }
    }

    #[test]
    fn eviction_frontier_advances() {
        let cache = SegmentRingBuffer::with_capacity(4);

        cache.publish(make_batch(0, 10, 1));
        cache.publish(make_batch(10, 20, 2));
        cache.commit(2);

        cache.advance_eviction_frontier(1);

        assert!(cache.read_committed(0).is_none());
        assert!(cache.read_committed(1).is_some());
    }

    #[test]
    fn drain_for_checkpoint() {
        let cache = SegmentRingBuffer::with_capacity(4);

        for i in 0..3u64 {
            cache.publish(make_batch(i * 10, (i + 1) * 10, i + 1));
        }
        cache.commit(3);

        let checkpoint = cache.drain_for_checkpoint();
        assert_eq!(checkpoint.batches.len(), 3);
        assert_eq!(checkpoint.new_frontier, 3);
    }

    #[test]
    fn ring_buffer_wraps() {
        let cache = SegmentRingBuffer::with_capacity(4);

        for i in 0..6u64 {
            cache.publish(make_batch(i * 10, (i + 1) * 10, i + 1));
            if i >= 1 {
                cache.advance_eviction_frontier(i - 1);
            }
        }
        cache.commit(6);

        let batch = cache.read_committed(5).unwrap();
        assert_eq!(batch.start_offset, 50);
    }

    #[test]
    fn cursors_maintain_ordering() {
        let cache = SegmentRingBuffer::with_capacity(8);

        for i in 0..5u64 {
            cache.publish(make_batch(i, i + 1, i + 1));
        }
        cache.commit(5);
        cache.advance_eviction_frontier(2);

        let frontier = cache.load_eviction_frontier();
        let commit = cache.load_commit_offset();
        let tail = cache.load_tail();

        assert!(frontier <= commit);
        assert!(commit <= tail);
    }
}
