//! `SegmentStore` — the data-plane's segment book of record.
//!
//! Holds every locally-resident segment plus the secondary indexes that turn a
//! `(range, offset)` fetch into a concrete `SegmentKey`. Owns four maps that
//! used to be scattered fields on `DataPlane`:
//!
//! - `by_key` — live `SegmentTracker`s, the source of truth for tail-cache WAL access.
//!
//! - `active_by_range` — for each `(topic, range)`, an ordered index of
//!   currently-active segments by `start_entry_id`. Drives the resolver's
//!   "which active segment covers this offset" lookup.
//!
//! - `sealed_by_range` — same shape, but for segments whose live tracker has
//!   been dropped. Cold reads still work because the sparse index + segment
//!   file are on disk; the stored location carries the `end_entry_id` so the
//!   resolver knows where the segment ends.
//!
//! The store does **not** own the seal-handshake state (`pending_seal_requests`)
//! or replication bookkeeping — those are about coordinator + peer protocols,
//! not about where bytes live. It is intentionally a passive registry: it
//! never originates a side effect, never talks to the network, never emits
//! events. `DataPlane` keeps doing all of that.

use super::segment::tracker::SegmentTracker;
use std::collections::{BTreeMap, HashMap};

use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
use crate::data_plane::SegmentKey;

/// Where a segment's data currently lives once its tracker has been dropped.
/// Returned from the resolver as part of a `Sealed` outcome so the cold-read
/// dispatcher can plan its work without re-querying metadata.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SealedSegmentLocation {
    pub(crate) segment_id: SegmentId,
    /// Last entry id in the segment (inclusive). Set at seal time from the
    /// tracker's `committed_entry_id`.
    #[allow(dead_code)]
    pub(crate) end_entry_id: u64,
}

/// Outcome of `SegmentStore::resolve`. Splits "active" (tail cache + WAL via
/// a live tracker) from "sealed" (cold reads via sparse index + segment file)
/// so the fetch handler can pick the right read path. Named for the fetch
/// path's consumer perspective, not its resolver origin.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum SegmentReadState {
    Active(SegmentKey),
    Sealed {
        key: SegmentKey,
        start_entry_id: u64,
        end_entry_id: u64,
    },
}

pub(crate) struct SegmentStore {
    by_key: HashMap<SegmentKey, SegmentTracker>,
    active_by_range: HashMap<(TopicId, RangeId), BTreeMap<u64, SegmentId>>,
    sealed_by_range: HashMap<(TopicId, RangeId), BTreeMap<u64, SealedSegmentLocation>>,
}

impl SegmentStore {
    pub(crate) fn new() -> Self {
        Self {
            by_key: HashMap::new(),
            active_by_range: HashMap::new(),
            sealed_by_range: HashMap::new(),
        }
    }

    // ── Tracker access ────────────────────────────────────────────────────

    pub(crate) fn contains_key(&self, key: &SegmentKey) -> bool {
        self.by_key.contains_key(key)
    }

    pub(crate) fn get(&self, key: &SegmentKey) -> Option<&SegmentTracker> {
        self.by_key.get(key)
    }

    pub(crate) fn get_mut(&mut self, key: &SegmentKey) -> Option<&mut SegmentTracker> {
        self.by_key.get_mut(key)
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &SegmentTracker> {
        self.by_key.values()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&SegmentKey, &SegmentTracker)> {
        self.by_key.iter()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_key.len()
    }

    /// Insert an active tracker AND its `active_by_range` index entry.
    /// Idempotent — if the key already exists the new tracker is dropped, so
    /// callers can blindly retry a SegmentAssignment without checking first.
    /// The active index keys by `tracker.start_entry_id()` (invariant 2).
    ///
    /// Refuses to resurrect an already-sealed segment. A sealed segment is
    /// immutable and its tracker has been handed off.
    pub(crate) fn insert_active(&mut self, key: SegmentKey, tracker: SegmentTracker) {
        if self.by_key.contains_key(&key) {
            return;
        }
        let already_sealed = self
            .sealed_by_range
            .get(&(key.topic_id, key.range_id))
            .is_some_and(|sealed| sealed.values().any(|loc| loc.segment_id == key.segment_id));
        if already_sealed {
            return;
        }
        let start = tracker.start_entry_id();
        self.by_key.insert(key, tracker);
        self.active_by_range
            .entry((key.topic_id, key.range_id))
            .or_default()
            .insert(start, key.segment_id);
    }

    pub(crate) fn unindex_active(&mut self, key: SegmentKey, start_entry_id: u64) {
        let Some(active) = self.active_by_range.get_mut(&(key.topic_id, key.range_id)) else {
            return;
        };

        // ! a segment sealed before its first commit shares its start offset with its successor,
        // ! so a blind remove could evict that successor.
        let segment_mapped_to_entry_id = active.get(&start_entry_id).copied();
        if segment_mapped_to_entry_id != Some(key.segment_id) {
            return;
        }
        active.remove(&start_entry_id);
        if active.is_empty() {
            self.active_by_range.remove(&(key.topic_id, key.range_id));
        }
    }

    /// Take the tracker out of `by_key` and re-index it: always dropped from the
    /// active index, then added to the sealed index if it has committed offsets
    /// (so cold reads still find it). Caller checkpoints the returned tracker.
    pub(crate) fn take_active_and_seal(&mut self, key: SegmentKey) -> Option<SegmentTracker> {
        let tracker = self.by_key.remove(&key)?;
        let start_entry_id = tracker.start_entry_id();
        self.unindex_active(key, start_entry_id);

        // Committed offsets stay cold-readable via the sealed index; a segment
        // that committed nothing owns none, so unindexing is enough.
        if let Some(end) = tracker.last_committed_entry_id() {
            self.sealed_by_range
                .entry((key.topic_id, key.range_id))
                .or_default()
                .insert(
                    start_entry_id,
                    SealedSegmentLocation {
                        segment_id: key.segment_id,
                        end_entry_id: end,
                    },
                );
        }
        Some(tracker)
    }

    /// Register a sealed segment received whole via catch-up. It was never active
    /// on this node (no tracker to seal), so it goes straight into the sealed
    /// index, where the cold-read resolver and `sealed_bounds` will find it. The
    /// segment must not be active here — caught-up segments never were.
    pub(crate) fn insert_sealed_from_catch_up(
        &mut self,
        key: SegmentKey,
        start_entry_id: u64,
        end_entry_id: u64,
    ) {
        debug_assert!(
            !self.by_key.contains_key(&key),
            "catch-up sealed segment {key:?} must not be active"
        );
        self.sealed_by_range
            .entry((key.topic_id, key.range_id))
            .or_default()
            .insert(
                start_entry_id,
                SealedSegmentLocation {
                    segment_id: key.segment_id,
                    end_entry_id,
                },
            );
    }

    /// Map `(range, offset)` to the segment that holds it. Active segments
    /// win (most fetches land on the write head); falls back to the sealed
    /// table for historical offsets. Returns `None` when no locally-hosted
    /// segment of this range covers the offset — either because the offset
    /// is past the last sealed segment's end, or because this node simply
    /// does not host the relevant segment.
    ///
    /// See d4_consumer_range_tracking.md "Resolving a bare (range, offset)
    /// to the segment that holds it."
    pub(crate) fn resolve(
        &self,
        topic_id: TopicId,
        range_id: RangeId,
        entry_id: u64,
    ) -> Option<SegmentReadState> {
        // ACTIVE PATH
        if let Some(active) = self.active_by_range.get(&(topic_id, range_id))
            && let Some((&start, &segment_id)) = active.range(..=entry_id).next_back()
        {
            let key = SegmentKey::new(topic_id, range_id, segment_id);
            // BTreeMap key == tracker.start_entry_id (invariant 2), so any
            // offset >= start lies in this segment while it is still active
            // (no upper bound yet).
            debug_assert!(entry_id >= start, "BTreeMap lookup violated invariant");
            if self.by_key.contains_key(&key) {
                return Some(SegmentReadState::Active(key));
            }
        }

        // SEALED PATH
        let sealed = self.sealed_by_range.get(&(topic_id, range_id))?;
        let (&start_entry_id, slot) = sealed.range(..=entry_id).next_back()?;

        // Asked entry_id is bigger than selected slot's end entry id
        // meaning, it's not in the active nor in the sealed? How come?
        if entry_id > slot.end_entry_id {
            return None;
        }
        Some(SegmentReadState::Sealed {
            key: SegmentKey::new(topic_id, range_id, slot.segment_id),
            start_entry_id,
            end_entry_id: slot.end_entry_id,
        })
    }

    /// Retention (D7): drop a segment this node holds, returning its `start_entry_id`
    /// (needed to build the on-disk file path) so the caller can delete the file and
    /// its sparse-index entries. Removes the live tracker (active) or the sealed-index
    /// slot, whichever holds it. `None` if this node doesn't track it — already gone
    /// or never had it; the caller no-ops and orphan GC is the backstop.
    pub(crate) fn remove_segment(&mut self, key: &SegmentKey) -> Option<u64> {
        if let Some(tracker) = self.by_key.remove(key) {
            let start = tracker.start_entry_id();
            self.unindex_active(*key, start);
            return Some(start);
        }
        let sealed = self
            .sealed_by_range
            .get_mut(&(key.topic_id, key.range_id))?;
        let start = sealed
            .iter()
            .find_map(|(&start, loc)| (loc.segment_id == key.segment_id).then_some(start))?;
        sealed.remove(&start);
        if sealed.is_empty() {
            self.sealed_by_range.remove(&(key.topic_id, key.range_id));
        }
        Some(start)
    }

    /// Look up a sealed segment's `(start_entry_id, end_entry_id)` by key.
    /// The catch-up source path uses this to stream `(local_end, end]` from its own
    /// committed bounds rather than bounds relayed by the requester — so it always
    /// clips to its own end.
    ///
    /// `None` if this node holds no sealed segment with that id (GC'd, never had it, or a race), in
    /// which case the source declines and the requester re-drives. O(n) in the
    /// range's sealed segments; catch-up is death-driven and rare.
    pub(crate) fn sealed_bounds(&self, key: &SegmentKey) -> Option<(u64, u64)> {
        let sealed = self.sealed_by_range.get(&(key.topic_id, key.range_id))?;
        sealed.iter().find_map(|(&start, loc)| {
            (loc.segment_id == key.segment_id).then_some((start, loc.end_entry_id))
        })
    }

    // ── Invariants ────────────────────────────────────────────────────────

    /// D4 invariants on the (range, offset) → segment resolver indexes.
    /// Called from `DataPlane::assert_invariants` after every state-changing
    /// operation per the project's invariant-checking discipline.
    #[cfg(any(test, debug_assertions))]
    pub(crate) fn assert_invariants(&self) {
        // (1) Every entry in active_by_range references a live tracker in
        //     by_key, and the BTreeMap key matches the tracker's
        //     start_entry_id. The resolver depends on this to map an offset
        //     to the right segment via `range(..=offset).next_back()`.
        for ((topic_id, range_id), by_start) in &self.active_by_range {
            for (&start, &segment_id) in by_start {
                let key = SegmentKey::new(*topic_id, *range_id, segment_id);
                let tracker = self.by_key.get(&key).unwrap_or_else(|| {
                    panic!("active_by_range references missing tracker {key:?}")
                });
                assert_eq!(
                    tracker.start_entry_id(),
                    start,
                    "active_by_range key {start} != tracker.start_entry_id for {key:?}",
                );
            }
        }

        // (2) active_by_range and sealed_by_range are disjoint per
        //     (topic, range) — a segment lives in exactly one. Without this,
        //     a resolver lookup could find the same segment in both indexes
        //     and pick the wrong read path (tail cache vs cold).
        for (range_key, active) in &self.active_by_range {
            if let Some(sealed) = self.sealed_by_range.get(range_key) {
                let active_ids: std::collections::HashSet<_> = active.values().collect();
                for slot in sealed.values() {
                    assert!(
                        !active_ids.contains(&slot.segment_id),
                        "segment {:?} in both active_by_range and sealed_by_range for {range_key:?}",
                        slot.segment_id,
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::membership::ShardGroupId;
    use crate::data_plane::states::segment::tracker::SegmentRole;
    use std::path::PathBuf;

    fn key(topic: u64, range: u64, segment: u64) -> SegmentKey {
        SegmentKey::new(TopicId(topic), RangeId(range), SegmentId(segment))
    }

    fn tracker(start: u64) -> SegmentTracker {
        SegmentTracker::new_with_start_entry_id(
            PathBuf::from("/tmp"),
            SegmentRole::Leader,
            vec![],
            ShardGroupId(1),
            start,
        )
    }

    #[test]
    fn resolve_active_when_one_segment_holds_the_offset() {
        let mut store = SegmentStore::new();
        let k = key(1, 0, 0);
        store.insert_active(k, tracker(0));
        let r = store.resolve(TopicId(1), RangeId(0), 5).unwrap();
        assert!(matches!(r, SegmentReadState::Active(g) if g == k));
    }

    #[test]
    fn resolver_routes_by_start_offset_across_two_active_segments() {
        let mut store = SegmentStore::new();
        let s0 = key(1, 0, 0);
        let s1 = key(1, 0, 1);
        store.insert_active(s0, tracker(0));
        store.insert_active(s1, tracker(100));
        let mid = store.resolve(TopicId(1), RangeId(0), 50).unwrap();
        let edge = store.resolve(TopicId(1), RangeId(0), 99).unwrap();
        let next = store.resolve(TopicId(1), RangeId(0), 100).unwrap();
        assert!(matches!(mid, SegmentReadState::Active(g) if g == s0));
        assert!(matches!(edge, SegmentReadState::Active(g) if g == s0));
        assert!(matches!(next, SegmentReadState::Active(g) if g == s1));
    }

    #[test]
    fn take_active_and_seal_routes_offset_through_sealed_table() {
        let mut store = SegmentStore::new();
        let k = key(1, 0, 0);
        let mut t = tracker(0);
        t.commit_entry(42);
        store.insert_active(k, t);

        let taken = store.take_active_and_seal(k);
        assert!(taken.is_some());

        let r = store.resolve(TopicId(1), RangeId(0), 10).unwrap();
        match r {
            SegmentReadState::Sealed {
                key: g,
                start_entry_id,
                end_entry_id,
            } => {
                assert_eq!(g, k);
                assert_eq!(start_entry_id, 0);
                assert_eq!(end_entry_id, 42);
            }
            other => panic!("expected Sealed, got {other:?}"),
        }

        // Offset past sealed end_entry_id → no segment found.
        assert!(store.resolve(TopicId(1), RangeId(0), 43).is_none());
    }

    /// A segment sealed before its first commit owns no offsets, so it must end
    /// up in neither index — a read for its start offset falls through to the
    /// successor rather than an empty sealed segment.
    #[test]
    fn seal_with_nothing_committed_claims_no_offsets() {
        let mut store = SegmentStore::new();
        let s0 = key(1, 0, 0);
        store.insert_active(s0, tracker(0)); // start 0, nothing committed

        let taken = store.take_active_and_seal(s0);
        assert!(taken.is_some());

        assert!(
            store.resolve(TopicId(1), RangeId(0), 0).is_none(),
            "a segment sealed with nothing committed must not claim offset 0",
        );

        // The successor reuses start 0 and now serves offset 0.
        let s1 = key(1, 0, 1);
        store.insert_active(s1, tracker(0));
        let r = store.resolve(TopicId(1), RangeId(0), 0).unwrap();
        assert!(matches!(r, SegmentReadState::Active(g) if g == s1));
    }

    /// `unindex_active` is segment-id-checked: a segment sealed before its first commit shares its start offset with its successor,
    /// so unindexing the old segment must not evict the successor already holding that slot.
    #[test]
    fn unindex_active_only_removes_the_named_segment() {
        let mut store = SegmentStore::new();
        let successor = key(1, 0, 1);
        store.insert_active(successor, tracker(0)); // successor already at start 0

        store.unindex_active(key(1, 0, 0), 0); // try to unindex the OLD segment id

        let r = store.resolve(TopicId(1), RangeId(0), 0).unwrap();
        assert!(
            matches!(r, SegmentReadState::Active(g) if g == successor),
            "unindexing a different segment id must not evict the successor",
        );
    }

    #[test]
    fn sealed_bounds_finds_start_and_end_by_key() {
        let mut store = SegmentStore::new();
        let k = key(1, 0, 7);
        let mut t = tracker(20);
        t.commit_entry(99);
        store.insert_active(k, t);
        store.take_active_and_seal(k);

        assert_eq!(store.sealed_bounds(&k), Some((20, 99)));
        // Same range, a segment id this node never sealed → None.
        assert_eq!(store.sealed_bounds(&key(1, 0, 8)), None);
        // A range with no sealed segments at all → None.
        assert_eq!(store.sealed_bounds(&key(2, 0, 7)), None);
    }

    #[test]
    fn insert_sealed_from_catch_up_makes_segment_cold_readable() {
        let mut store = SegmentStore::new();
        let k = key(1, 0, 5);
        // Received whole via catch-up — never active here.
        store.insert_sealed_from_catch_up(k, 100, 142);

        assert_eq!(store.sealed_bounds(&k), Some((100, 142)));
        // And the resolver routes an in-range offset to it (cold path).
        match store.resolve(TopicId(1), RangeId(0), 120).unwrap() {
            SegmentReadState::Sealed {
                key: g,
                start_entry_id,
                end_entry_id,
            } => {
                assert_eq!(g, k);
                assert_eq!(start_entry_id, 100);
                assert_eq!(end_entry_id, 142);
            }
            other => panic!("expected Sealed, got {other:?}"),
        }
    }
}
