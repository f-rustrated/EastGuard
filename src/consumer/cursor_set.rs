//! `CursorSet` — the consumer's set of per-range cursors.
//! Mutated only via `apply_fetch_result`, which takes the range_id the
//! consumer just fetched, the `next_entry_id` from the response, and the
//! in-band `RangeProgressSignal`. Returns a `CursorAction` describing what
//! changed so the caller can drive the next fetch without diffing the set.
//!
//! Lineage discipline (Ordering Guarantees): a successor range is
//! not read until its sole owning predecessor has been drained to the sealed
//! `end_offset`. The set enforces this by holding only the cursors the
//! consumer should currently be fetching — successors only appear after
//! their predecessor drains.

use super::cursor::RangeCursor;
use crate::connections::protocol::{RangeProgressSignal, RangeTransition};
use crate::consumer::{ParkedMerge, ParkedMerges};
use crate::control_plane::metadata::RangeId;
use crate::test_traits::TAssertInvariant;
use std::collections::HashSet;

/// What changed in the cursor set when a fetch response was applied. The
/// caller uses this to schedule the next fetch — `KeepGoing` says "fetch
/// `range_id` again at `next_offset`"; `Transitioned` says "stop fetching
/// `dropped`, start fetching each `added` cursor."
#[derive(Debug, PartialEq, Eq)]
pub enum CursorAction {
    /// Same cursor, advance to `next_offset`.
    KeepGoing { range_id: RangeId, next_offset: u64 },
    /// The fetched range was sealed and drained; cursors transitioned per
    /// the lineage rule.
    Transitioned {
        dropped: Box<[RangeId]>,
        added: Box<[RangeCursor]>,
    },
}

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

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.cursors.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.cursors.is_empty()
    }

    /// Advance the cursor for `range_id` to `next_entry_id` and react to the
    /// in-band progress signal.
    ///
    /// - `Active` → `KeepGoing` (consumer should refetch).
    /// - `Sealed { end_offset, .. }` with `next_entry_id <= end_offset` →
    ///   `KeepGoing` (still draining the sealed range).
    /// - `Sealed { end_offset, transition }` with `next_entry_id > end_offset`
    ///   → drop this cursor and apply the transition, returning
    ///   `Transitioned`.
    pub fn apply_fetch_result(
        &mut self,
        range_id: RangeId,
        next_entry_id: u64,
        signal: RangeProgressSignal,
    ) -> CursorAction {
        let idx = self
            .cursors
            .iter()
            .position(|c| c.range_id == range_id)
            .expect("apply_fetch_result called for range not in cursor set");
        self.cursors[idx].next_offset = next_entry_id;

        let cursor_action = match signal {
            RangeProgressSignal::Active => CursorAction::KeepGoing {
                range_id,
                next_offset: next_entry_id,
            },
            RangeProgressSignal::Sealed {
                end_offset,
                transition,
            } => {
                // `end_offset` is inclusive — the last entry in the sealed
                // range. Draining means we've fetched past it. The fetch
                // response's `next_entry_id` is the offset to fetch next; if
                // that's strictly > end_offset, we're done with this range.
                if next_entry_id <= end_offset {
                    return CursorAction::KeepGoing {
                        range_id,
                        next_offset: next_entry_id,
                    };
                }
                let drained = self.cursors.remove(idx);
                let added = self.apply_transition(drained, transition);
                self.cursors.extend(added.clone());

                CursorAction::Transitioned {
                    dropped: Box::new([range_id]),
                    added,
                }
            }
        };

        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();

        cursor_action
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

    fn sealed_split(
        end_offset: u64,
        l: RangeId,
        r: RangeId,
        split_point: &[u8],
    ) -> RangeProgressSignal {
        RangeProgressSignal::Sealed {
            end_offset,
            transition: RangeTransition::Split {
                left_range_id: l,
                right_range_id: r,
                split_point: split_point.to_vec(),
            },
        }
    }

    fn sealed_merged(
        end_offset: u64,
        merged: RangeId,
        merged_from: [RangeId; 2],
    ) -> RangeProgressSignal {
        RangeProgressSignal::Sealed {
            end_offset,
            transition: RangeTransition::Merged {
                merged_range_id: merged,
                merged_from,
            },
        }
    }

    #[test]
    fn active_signal_returns_keep_going_with_advanced_offset() {
        let mut set = RangeCursorSet::new(vec![cursor(RangeId(0), 100, b"a", b"c")]);
        let action = set.apply_fetch_result(RangeId(0), 101, RangeProgressSignal::Active);
        assert_eq!(
            action,
            CursorAction::KeepGoing {
                range_id: RangeId(0),
                next_offset: 101,
            }
        );
        assert_eq!(set.cursors()[0].next_offset, 101);
    }

    /// Sealed range, but consumer hasn't drained past `end_offset` yet —
    /// must keep fetching until next_entry_id > end_offset (per the doc:
    /// "drain to end offset, then follow the transition").
    #[test]
    fn sealed_before_drained_keeps_going() {
        let mut set = RangeCursorSet::new(vec![cursor(RangeId(0), 100, b"a", b"c")]);
        let action = set.apply_fetch_result(
            RangeId(0),
            150,
            sealed_split(200, RangeId(1), RangeId(2), b"b"),
        );
        assert_eq!(
            action,
            CursorAction::KeepGoing {
                range_id: RangeId(0),
                next_offset: 150,
            }
        );
        // Cursor still in set; no transition.
        assert_eq!(set.len(), 1);
        assert_eq!(set.cursors()[0].range_id, RangeId(0));
    }

    /// Split: parent fully drained → replaced by two children with disjoint
    /// keyspaces around the split point, both at offset 0.
    #[test]
    fn split_replaces_parent_with_two_children() {
        let mut set = RangeCursorSet::new(vec![cursor(RangeId(0), 1001, b"a", b"c")]);
        let action = set.apply_fetch_result(
            RangeId(0),
            1001,
            sealed_split(1000, RangeId(1), RangeId(2), b"b"),
        );
        match action {
            CursorAction::Transitioned { dropped, added } => {
                assert_eq!(dropped[0], RangeId(0));
                assert_eq!(added.len(), 2);
                assert_eq!(added[0], cursor(RangeId(1), 0, b"a", b"b"));
                assert_eq!(added[1], cursor(RangeId(2), 0, b"b", b"c"));
            }
            other => panic!("expected Transitioned, got {other:?}"),
        }
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
        let action = set.apply_fetch_result(
            RangeId(1),
            101,
            sealed_merged(100, RangeId(3), [RangeId(1), RangeId(2)]),
        );
        assert_eq!(
            action,
            CursorAction::Transitioned {
                dropped: Box::new([RangeId(1)]),
                added: Box::new([]),
            }
        );
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
        set.apply_fetch_result(
            RangeId(1),
            101,
            sealed_merged(100, RangeId(3), [RangeId(1), RangeId(2)]),
        );
        let action = set.apply_fetch_result(
            RangeId(2),
            101,
            sealed_merged(100, RangeId(3), [RangeId(1), RangeId(2)]),
        );
        assert_eq!(
            action,
            CursorAction::Transitioned {
                dropped: Box::new([RangeId(2)]),
                added: Box::new([cursor(RangeId(3), 0, b"a", b"c")]),
            }
        );
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
        set.apply_fetch_result(
            RangeId(1),
            101,
            sealed_merged(100, RangeId(3), [RangeId(1), RangeId(2)]),
        );
        let cursors = set.cursors();
        assert_eq!(cursors.len(), 1);
        assert_eq!(cursors[0].range_id, RangeId(3));
        assert_eq!(cursors[0].keyspace_start, b"a".to_vec());
        assert_eq!(cursors[0].keyspace_end, b"b".to_vec());
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
