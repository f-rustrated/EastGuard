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

use crate::client::TopicDetail;
use crate::connections::protocol::RangeDetail;
use crate::connections::protocol::RangeTransition;
use crate::control_plane::metadata::EntryId;
use crate::control_plane::metadata::{RangeId, RangeState};
use crate::test_traits::TAssertInvariant;
use std::collections::HashSet;

struct ParkedMerge {
    /// The merged range's wire ID.
    merged_id: RangeId,

    /// `Vec` instead of `HashMap` — for `n` this small the linear scan is cache-friendly and
    /// avoids HashMap's per-entry hashing + sparse-storage overhead.
    drained: Vec<RangeCursor>,
}

#[derive(Default)]
struct ParkedMerges(Vec<ParkedMerge>);

impl ParkedMerges {
    fn push(&mut self, pending: ParkedMerge) {
        self.0.push(pending);
    }

    fn try_complete_merge(
        &mut self,
        merged_range_id: RangeId,
        drained: &RangeCursor,
    ) -> Option<RangeCursor> {
        let pos = self.0.iter().position(|p| p.merged_id == merged_range_id)?;
        let parked = self.0.swap_remove(pos).drained.pop()?;
        let mut m = parked.into_merged_cursor(merged_range_id);
        m.absorb(drained.clone());
        Some(m)
    }
}

/// What keys the consumer wants to read. Drives which ranges get cursors.
#[derive(Debug, Clone)]
pub enum KeyInterest {
    AllKeys,
    /// Half-open `[start, end)`.
    KeySpan {
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

impl KeyInterest {
    /// Does this interest cover any keys in `r.keyspace_start..r.keyspace_end`?
    pub(crate) fn matches(&self, r: &RangeDetail) -> bool {
        match self {
            KeyInterest::AllKeys => true,
            KeyInterest::KeySpan { start, end } => {
                r.keyspace_start < *end && *start < r.keyspace_end
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StartPolicy {
    Latest,
    Earliest,
}

impl std::str::FromStr for StartPolicy {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "latest" => Ok(StartPolicy::Latest),
            "earliest" => Ok(StartPolicy::Earliest),
            _ => anyhow::bail!("Start policy must be 'earliest' or 'latest'"),
        }
    }
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

    pub(crate) fn build_cursors(
        detail: &TopicDetail,
        interest: KeyInterest,
        policy: StartPolicy,
    ) -> RangeCursorSet {
        let cursors = match policy {
            StartPolicy::Latest => RangeCursor::latest_cursors(detail, &interest),
            StartPolicy::Earliest => RangeCursor::earliest_cursors(detail, &interest),
        };
        RangeCursorSet::new(cursors)
    }

    pub fn contains(&self, range_id: RangeId) -> bool {
        self.cursors.iter().any(|c| c.range_id == range_id)
    }

    pub fn get(&self, range_id: RangeId) -> Option<&RangeCursor> {
        self.cursors.iter().find(|c| c.range_id == range_id)
    }

    pub fn iter(&self) -> std::slice::Iter<'_, RangeCursor> {
        self.cursors.iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, RangeCursor> {
        self.cursors.iter_mut()
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
            *c = cursor;
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

// ! Per-range read cursor — the position the consumer is at within one range,
// ! paired with the keyspace bounds that travel with it through lineage
// ! transitions (so split/merge can carve / extend the cursor's keyspace
// ! without going back to metadata).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeCursor {
    pub range_id: RangeId,
    pub next_entry_id: EntryId,
    pub skip_batch_offsets_below: Option<u64>,
    pub skip_absolute_offsets_below: Option<u64>,
    pub next_absolute_offset: u64,
    pub keyspace_start: Vec<u8>,
    pub keyspace_end: Vec<u8>,
}

impl RangeCursor {
    pub fn new(
        range_id: RangeId,
        next_entry_id: EntryId,
        keyspace_start: Vec<u8>,
        keyspace_end: Vec<u8>,
    ) -> Self {
        Self {
            range_id,
            next_entry_id,
            skip_batch_offsets_below: None,
            skip_absolute_offsets_below: None,
            next_absolute_offset: 0,
            keyspace_start,
            keyspace_end,
        }
    }

    pub fn with_skip_batch_offsets_below(mut self, skip_batch_offsets_below: Option<u64>) -> Self {
        self.skip_batch_offsets_below = skip_batch_offsets_below;
        self
    }

    pub fn with_skip_absolute_offsets_below(
        mut self,
        skip_absolute_offsets_below: Option<u64>,
    ) -> Self {
        self.skip_absolute_offsets_below = skip_absolute_offsets_below;
        self
    }

    pub(crate) fn seek_to_absolute_offset(&mut self, absolute_offset: u64) {
        self.next_entry_id = EntryId::MIN;
        self.skip_batch_offsets_below = None;
        self.skip_absolute_offsets_below = Some(absolute_offset);
        self.next_absolute_offset = 0;
    }

    pub fn with_next_absolute_offset(mut self, next_absolute_offset: u64) -> Self {
        self.next_absolute_offset = next_absolute_offset;
        self
    }
    fn latest_cursors(detail: &TopicDetail, interest: &KeyInterest) -> Vec<Self> {
        detail
            .ranges
            .iter()
            .filter(|r| r.state == RangeState::Active)
            .filter(|r| interest.matches(r))
            .map(|r| {
                // Metadata can choose the active ranges, but only ListOffsets can
                // resolve the live tail. The async consumer bootstrap overwrites
                // this placeholder before starting fetch tasks.
                let start_entry = r.active_segment.as_ref().map_or(0, |s| *s.start_entry_id);
                RangeCursor::new(
                    r.range_id,
                    EntryId(start_entry),
                    r.keyspace_start.clone(),
                    r.keyspace_end.clone(),
                )
            })
            .collect()
    }

    /// A "root" range is one with no predecessor in lineage: not the product
    /// of a merge (`merged_from = None`) and not the child of any split (no
    /// other range's `split_into` mentions it). For a freshly-created topic
    /// that has only split, the original full-keyspace range is the sole
    /// root.
    fn earliest_cursors(detail: &TopicDetail, interest: &KeyInterest) -> Vec<Self> {
        let split_children: HashSet<RangeId> = detail
            .ranges
            .iter()
            .filter_map(|r| r.split_into)
            .flat_map(|(l, r)| [l, r])
            .collect();

        detail
            .ranges
            .iter()
            .filter(|r| r.merged_from.is_none() && !split_children.contains(&r.range_id))
            .filter(|r| interest.matches(r))
            .map(|r| {
                RangeCursor::new(
                    r.range_id,
                    r.first_segment_start_offset().map_or(EntryId(0), |id| id),
                    r.keyspace_start.clone(),
                    r.keyspace_end.clone(),
                )
            })
            .collect()
    }

    fn split(
        self,
        left_range_id: RangeId,
        right_range_id: RangeId,
        split_point: Vec<u8>,
    ) -> [RangeCursor; 2] {
        let left = RangeCursor::new(
            RangeId(*left_range_id),
            EntryId::default(),
            self.keyspace_start.clone(),
            split_point.clone(),
        );
        let right = RangeCursor::new(
            RangeId(*right_range_id),
            EntryId::default(),
            split_point.clone(),
            self.keyspace_end.clone(),
        );
        [left, right]
    }

    /// Consume this drained parent cursor and become the merged cursor it
    /// transitions to. Used when M doesn't yet exist in the cursor set (this is
    /// the first of the merge's parents to drain in the consumer's view).
    fn into_merged_cursor(self, merged_id: RangeId) -> Self {
        Self {
            range_id: merged_id,
            next_entry_id: EntryId::default(),
            ..self
        }
    }

    fn absorb(&mut self, half: RangeCursor) {
        if half.keyspace_start < self.keyspace_start {
            self.keyspace_start = half.keyspace_start.clone();
        }
        if half.keyspace_end > self.keyspace_end {
            self.keyspace_end = half.keyspace_end.clone();
        }
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
    use super::*;
    use crate::connections::protocol::{RangeDetail, SegmentDetail, TopicState};
    use crate::control_plane::metadata::{RangeId, RangeState, SegmentId, TopicId};

    fn cursor(range_id: RangeId, next_entry_id: EntryId, start: &[u8], end: &[u8]) -> RangeCursor {
        RangeCursor::new(range_id, next_entry_id, start.to_vec(), end.to_vec())
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

    fn range(
        range_id: u64,
        state: RangeState,
        start: &[u8],
        end: &[u8],
        split_into: Option<(RangeId, RangeId)>,
        merged_into: Option<RangeId>,
        merged_from: Option<(RangeId, RangeId)>,
    ) -> RangeDetail {
        RangeDetail {
            range_id: RangeId(range_id),
            keyspace_start: start.to_vec(),
            keyspace_end: end.to_vec(),
            state,
            active_segment: state.eq(&RangeState::Active).then(|| SegmentDetail {
                segment_id: SegmentId(0),
                start_entry_id: 0.into(),
                end_entry_id: None,
                replica_set: vec![],
            }),
            sealed_segments: Box::default(),
            split_into,
            merged_into,
            merged_from,
        }
    }

    fn topic(ranges: Vec<RangeDetail>) -> TopicDetail {
        TopicDetail {
            topic_id: TopicId(0),
            name: "t".into(),
            state: TopicState::Active,
            ranges: ranges.into_boxed_slice(),
        }
    }

    #[test]
    fn latest_picks_only_active_ranges() {
        let t = topic(vec![
            range(
                0,
                RangeState::Sealed,
                b"",
                b"\xff",
                Some((RangeId(1), RangeId(2))),
                None,
                None,
            ),
            range(1, RangeState::Active, b"", b"m", None, None, None),
            range(2, RangeState::Active, b"m", b"\xff", None, None, None),
        ]);
        let set = RangeCursorSet::build_cursors(&t, KeyInterest::AllKeys, StartPolicy::Latest);
        let ids: Vec<RangeId> = set.iter().map(|c| c.range_id).collect();
        assert!(ids.contains(&RangeId(1)));
        assert!(ids.contains(&RangeId(2)));
        assert!(!ids.contains(&RangeId(0)));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn latest_filters_by_keyspan_interest() {
        let t = topic(vec![
            range(1, RangeState::Active, b"", b"m", None, None, None),
            range(2, RangeState::Active, b"m", b"\xff", None, None, None),
        ]);
        // Interest [a, c) — only overlaps range 1 ([, m)).
        let set = RangeCursorSet::build_cursors(
            &t,
            KeyInterest::KeySpan {
                start: b"a".to_vec(),
                end: b"c".to_vec(),
            },
            StartPolicy::Latest,
        );
        let ids: Vec<RangeId> = set.iter().map(|c| c.range_id).collect();
        assert_eq!(ids, vec![RangeId(1)]);
    }

    #[test]
    fn earliest_picks_lineage_roots_skipping_split_children() {
        // Topic created with one full-keyspace range, later split into 1 + 2.
        let t = topic(vec![
            range(
                0,
                RangeState::Sealed,
                b"",
                b"\xff",
                Some((RangeId(1), RangeId(2))),
                None,
                None,
            ),
            range(1, RangeState::Active, b"", b"m", None, None, None),
            range(2, RangeState::Active, b"m", b"\xff", None, None, None),
        ]);
        let set = RangeCursorSet::build_cursors(&t, KeyInterest::AllKeys, StartPolicy::Earliest);

        let ids: Vec<RangeId> = set.iter().map(|c| c.range_id).collect();
        // Earliest = original root (range 0), not the split children.
        assert_eq!(ids, vec![RangeId(0)]);
    }

    #[test]
    fn earliest_skips_merge_products() {
        // Two original ranges 1, 2 merged into 3.
        let t = topic(vec![
            range(
                1,
                RangeState::Sealed,
                b"",
                b"m",
                None,
                Some(RangeId(3)),
                None,
            ),
            range(
                2,
                RangeState::Sealed,
                b"m",
                b"\xff",
                None,
                Some(RangeId(3)),
                None,
            ),
            range(
                3,
                RangeState::Active,
                b"",
                b"\xff",
                None,
                None,
                Some((RangeId(1), RangeId(2))),
            ),
        ]);
        let set = RangeCursorSet::build_cursors(&t, KeyInterest::AllKeys, StartPolicy::Earliest);

        let ids: Vec<RangeId> = set.iter().map(|c| c.range_id).collect();
        // 1 and 2 are the roots (no merged_from, not split children); 3 has
        // merged_from set so it's not a root.
        assert!(ids.contains(&RangeId(1)));
        assert!(ids.contains(&RangeId(2)));
        assert!(!ids.contains(&RangeId(3)));
    }

    /// Split: parent fully drained → replaced by two children with disjoint
    /// keyspaces around the split point, both at offset 0.
    #[test]
    fn split_replaces_parent_with_two_children() {
        let mut set = RangeCursorSet::new(vec![cursor(RangeId(0), 1001.into(), b"a", b"c")]);
        let added = set.apply_drained(RangeId(0), split_transition(RangeId(1), RangeId(2), b"b"));

        assert_eq!(added.len(), 2);
        assert_eq!(added[0], cursor(RangeId(1), EntryId::default(), b"a", b"b"));
        assert_eq!(added[1], cursor(RangeId(2), EntryId::default(), b"b", b"c"));

        assert_eq!(set.len(), 2);
    }

    /// Two-parent merge, first parent draining: M is **deferred** as
    /// pending. The consumer must not start fetching M until the sibling P2
    /// has drained too — otherwise M would advance past records the consumer
    /// owes to keys whose predecessor (P2) hasn't been drained yet.
    #[test]
    fn merge_first_parent_defers_m_when_sibling_tracked() {
        let mut set = RangeCursorSet::new(vec![
            cursor(RangeId(1), EntryId::default(), b"a", b"b"),
            cursor(RangeId(2), EntryId::default(), b"b", b"c"),
        ]);
        let added = set.apply_drained(
            RangeId(1),
            merge_transition(RangeId(3), [RangeId(1), RangeId(2)]),
        );

        assert_eq!(added.len(), 0);

        // P1 dropped; P2 still tracked; M not yet a fetchable cursor.
        let ids: Vec<RangeId> = set.iter().map(|c| c.range_id).collect();
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
            cursor(RangeId(1), EntryId::default(), b"a", b"b"),
            cursor(RangeId(2), EntryId::default(), b"b", b"c"),
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
        assert_eq!(added[0], cursor(RangeId(3), EntryId::default(), b"a", b"c"));

        // Only M remains, spanning both pre-merge keyspaces.
        assert_eq!(set.len(), 1);
        assert_eq!(set.get(RangeId(3)).unwrap().range_id, RangeId(3));
    }

    /// Single-parent consumer reading only `[a, b)`: P1 drains, M is added
    /// with P1's keyspace immediately (no sibling tracked, no need to defer).
    /// P2 never enters the set. The doc's "consumer never learns of, or
    /// waits on, P2" case.
    #[test]
    fn single_parent_consumer_follows_into_m_without_tracking_sibling() {
        let mut set = RangeCursorSet::new(vec![cursor(RangeId(1), EntryId::default(), b"a", b"b")]);
        let added = set.apply_drained(
            RangeId(1),
            merge_transition(RangeId(3), [RangeId(1), RangeId(2)]),
        );

        assert_eq!(added.len(), 1);
        assert_eq!(added[0], cursor(RangeId(3), EntryId::default(), b"a", b"b"));

        assert_eq!(set.len(), 1);
        assert_eq!(set.get(RangeId(3)).unwrap().range_id, RangeId(3));
    }

    #[test]
    #[should_panic(expected = "duplicate range_id")]
    fn duplicate_range_id_panics_invariants() {
        RangeCursorSet::new(vec![
            cursor(RangeId(1), EntryId::default(), b"a", b"b"),
            cursor(RangeId(1), EntryId::default(), b"c", b"d"),
        ]);
    }

    #[test]
    #[should_panic(expected = "overlapping keyspaces")]
    fn overlapping_keyspaces_panic_invariants() {
        RangeCursorSet::new(vec![
            cursor(RangeId(1), EntryId::default(), b"a", b"c"),
            cursor(RangeId(2), EntryId::default(), b"b", b"d"),
        ]);
    }
}
