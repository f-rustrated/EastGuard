//! `CursorSet` — the consumer's set of per-range cursors.
//! Mutated only via `apply_drained`, which takes the range_id the
//! consumer just fully drained and the transition to apply. Returns the
//! newly activated cursors so the caller can spawn fetch tasks for them.
//!
//! Lineage discipline (Ordering Guarantees): a successor range is
//! not read until its sole owning predecessor has been drained to the sealed
//! `end_offset`. The set enforces this by holding only the cursors the
//! consumer should currently be fetching — successors only appear after
//! their predecessor drains.

use super::cursor::RangeCursor;
use super::parked_merges::{ParkedMerge, ParkedMerges};
use crate::connections::protocol::RangeTransition;
use crate::control_plane::metadata::RangeId;
use crate::test_traits::TAssertInvariant;
use std::collections::HashSet;

pub struct RangeCursorSet {
    cursors: Vec<RangeCursor>,
    parked_merges: ParkedMerges,
}

impl RangeCursorSet {
    pub fn new(cursors: Vec<RangeCursor>) -> Self {
        let set = Self {
            cursors,
            parked_merges: ParkedMerges::default(),
        };
        #[cfg(any(test, debug_assertions))]
        set.assert_invariants();
        set
    }

    pub fn cursors(&self) -> &[RangeCursor] {
        &self.cursors
    }

    pub(crate) fn cursors_mut(&mut self) -> &mut [RangeCursor] {
        &mut self.cursors
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.cursors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cursors.is_empty()
    }

    pub fn add_or_update(&mut self, cursor: RangeCursor) {
        if let Some(c) = self
            .cursors
            .iter_mut()
            .find(|c| c.range_id == cursor.range_id)
        {
            c.next_entry_id = cursor.next_entry_id;
        } else {
            self.cursors.push(cursor);
        }
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    pub fn remove(&mut self, range_id: RangeId) -> Option<RangeCursor> {
        if let Some(idx) = self.cursors.iter().position(|c| c.range_id == range_id) {
            return Some(self.cursors.remove(idx));
        }
        None
    }

    /// Apply a lineage transition after a range has been fully drained.
    /// Returns any newly activated cursors that the consumer should now fetch.
    pub fn apply_drained(
        &mut self,
        range_id: RangeId,
        transition: RangeTransition,
    ) -> Box<[RangeCursor]> {
        let Some(drained) = self.remove(range_id) else {
            return Box::new([]);
        };

        let new_cursors = self.apply_transition(drained, transition);
        self.cursors.extend(new_cursors.clone());

        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();

        new_cursors
    }

    fn apply_transition(
        &mut self,
        drained: RangeCursor,
        transition: RangeTransition,
    ) -> Box<[RangeCursor]> {
        match transition {
            RangeTransition::Split {
                left_range_id,
                right_range_id,
                split_point,
            } => drained
                .split(left_range_id, right_range_id, split_point)
                .into(),
            RangeTransition::Merged {
                merged_range_id,
                merged_from,
            } => {
                //(a) Both parents tracked, the other already drained. M was parked
                //    pending its second drain; now both have come in.
                //    Build M with the union keyspace; ship it.
                if let Some(merged) = self
                    .parked_merges
                    .try_complete_merge(merged_range_id, &drained)
                {
                    return Box::new([merged]);
                }

                // - (b) Both parents tracked, the other still tracked. Park M pending
                //       the sibling's drain. M is not yet fetchable — reading it now
                //       would advance past records the consumer owes to keys whose
                //       predecessor (the sibling) hasn't drained.
                if self.has_active_sibling(merged_from, &drained) {
                    self.parked_merges.push(ParkedMerge {
                        merged_id: merged_range_id,
                        drained: vec![drained],
                    });
                    return Box::new([]);
                }

                // - (c) Only this parent ever tracked
                //   Setup:
                //      P1 [a, b)
                //      P2 [b, c) -> M [a, c).
                //  Consumer is reading only [a, b). Their CursorSet has only P1 (P2 was never relevant).
                Box::new([drained.into_merged_cursor(merged_range_id)])
            }
        }
    }

    fn has_active_sibling(&self, merged_from: [RangeId; 2], cursor: &RangeCursor) -> bool {
        let [pa, pb] = merged_from;
        let sibling = if pa == cursor.range_id { pb } else { pa };
        self.cursors.iter().any(|c| c.range_id == sibling)
    }
}

#[cfg(any(test, debug_assertions))]
impl TAssertInvariant for RangeCursorSet {
    fn assert_invariants(&self) {
        // (1) Unique range_id. Two cursors on the same range would either
        //     race each other's offsets or deliver duplicates; the lineage
        //     walker assumes one cursor per active range covering the
        //     consumer's interest.
        let mut seen: HashSet<RangeId> = HashSet::new();
        for cursor in &self.cursors {
            assert!(
                seen.insert(cursor.range_id),
                "duplicate range_id in CursorSet: {:?}",
                cursor.range_id,
            );
        }

        // (2) Pairwise-disjoint keyspaces. Overlap would let the consumer
        //     receive the same key from two different cursors → duplicate
        //     delivery for that key's per-key chain. Half-open intervals
        //     `[start, end)`; touching at a boundary is fine.
        for (i, a) in self.cursors.iter().enumerate() {
            for b in self.cursors.iter().skip(i + 1) {
                let overlap =
                    a.keyspace_start < b.keyspace_end && b.keyspace_start < a.keyspace_end;
                assert!(
                    !overlap,
                    "overlapping keyspaces in CursorSet: {a:?} and {b:?}",
                );
            }
        }

        // (3) Pending consistency. A pending merge represents "M is waiting
        //     for a sibling to drain"; if M were already live (in `cursors`)
        //     or if there were no drained parents recorded, the entry would
        //     be a bug.
        for pending in &self.parked_merges.0 {
            assert!(
                !self.cursors.iter().any(|c| c.range_id == pending.merged_id),
                "pending merge {:?} also present as live cursor",
                pending.merged_id,
            );
            assert!(
                !pending.drained.is_empty(),
                "pending merge {:?} has no drained parents",
                pending.merged_id,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor(range_id: RangeId, next_offset: u64, start: &[u8], end: &[u8]) -> RangeCursor {
        RangeCursor::new(range_id, next_offset, start.to_vec(), end.to_vec())
    }

    fn split_transition(l: RangeId, r: RangeId, split_point: &[u8]) -> RangeTransition {
        RangeTransition::Split {
            left_range_id: l,
            right_range_id: r,
            split_point: split_point.to_vec(),
        }
    }

    fn merge_transition(merged: RangeId, merged_from: [RangeId; 2]) -> RangeTransition {
        RangeTransition::Merged {
            merged_range_id: merged,
            merged_from,
        }
    }

    /// Split: parent fully drained → replaced by two children with disjoint
    /// keyspaces around the split point, both at offset 0.
    #[test]
    fn split_replaces_parent_with_two_children() {
        let mut set = RangeCursorSet::new(vec![cursor(RangeId(0), 1001, b"a", b"c")]);
        let added = set.apply_drained(RangeId(0), split_transition(RangeId(1), RangeId(2), b"b"));

        assert_eq!(added.len(), 2);
        assert_eq!(added[0], cursor(RangeId(1), 0, b"a", b"b"));
        assert_eq!(added[1], cursor(RangeId(2), 0, b"b", b"c"));

        assert_eq!(set.len(), 2);
    }

    /// Two-parent merge, first parent draining: M is **deferred** as
    /// pending. The consumer must not start fetching M until the sibling P2
    /// has drained too — otherwise M would advance past records the consumer
    /// owes to keys whose predecessor (P2) hasn't been drained yet.
    #[test]
    fn merge_first_parent_defers_m_when_sibling_tracked() {
        let mut set = RangeCursorSet::new(vec![
            cursor(RangeId(1), 0, b"a", b"b"),
            cursor(RangeId(2), 0, b"b", b"c"),
        ]);
        let added = set.apply_drained(
            RangeId(1),
            merge_transition(RangeId(3), [RangeId(1), RangeId(2)]),
        );

        assert_eq!(added.len(), 0);

        // P1 dropped; P2 still tracked; M not yet a fetchable cursor.
        let ids: Vec<RangeId> = set.cursors().iter().map(|c| c.range_id).collect();
        assert_eq!(ids, vec![RangeId(2)]);
    }

    /// Two-parent merge, second parent draining: M is activated now with
    /// the **union** keyspace [a, c) at offset 0 — both predecessors have
    /// drained, the per-key chains for every key in M's keyspace are
    /// satisfied, the consumer may safely start reading M from its first
    /// record.
    #[test]
    fn merge_second_parent_activates_m_with_union_keyspace() {
        let mut set = RangeCursorSet::new(vec![
            cursor(RangeId(1), 0, b"a", b"b"),
            cursor(RangeId(2), 0, b"b", b"c"),
        ]);
        set.apply_drained(
            RangeId(1),
            merge_transition(RangeId(3), [RangeId(1), RangeId(2)]),
        );

        let added = set.apply_drained(
            RangeId(2),
            merge_transition(RangeId(3), [RangeId(1), RangeId(2)]),
        );

        assert_eq!(added.len(), 1);
        assert_eq!(added[0], cursor(RangeId(3), 0, b"a", b"c"));

        // Only M remains, spanning both pre-merge keyspaces.
        assert_eq!(set.len(), 1);
        assert_eq!(set.cursors()[0].range_id, RangeId(3));
    }

    /// Single-parent consumer reading only `[a, b)`: P1 drains, M is added
    /// with P1's keyspace immediately (no sibling tracked, no need to defer).
    /// P2 never enters the set. The doc's "consumer never learns of, or
    /// waits on, P2" case.
    #[test]
    fn single_parent_consumer_follows_into_m_without_tracking_sibling() {
        let mut set = RangeCursorSet::new(vec![cursor(RangeId(1), 0, b"a", b"b")]);
        let added = set.apply_drained(
            RangeId(1),
            merge_transition(RangeId(3), [RangeId(1), RangeId(2)]),
        );

        assert_eq!(added.len(), 1);
        assert_eq!(added[0], cursor(RangeId(3), 0, b"a", b"b"));

        let cursors = set.cursors();
        assert_eq!(cursors.len(), 1);
        assert_eq!(cursors[0].range_id, RangeId(3));
    }

    #[test]
    #[should_panic(expected = "duplicate range_id")]
    fn duplicate_range_id_panics_invariants() {
        RangeCursorSet::new(vec![
            cursor(RangeId(1), 0, b"a", b"b"),
            cursor(RangeId(1), 0, b"c", b"d"),
        ]);
    }

    #[test]
    #[should_panic(expected = "overlapping keyspaces")]
    fn overlapping_keyspaces_panic_invariants() {
        RangeCursorSet::new(vec![
            cursor(RangeId(1), 0, b"a", b"c"),
            cursor(RangeId(2), 0, b"b", b"d"),
        ]);
    }
}
