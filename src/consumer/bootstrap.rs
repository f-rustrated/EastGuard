//! Bootstrap helpers — build the initial `CursorSet` from a `TopicDetail`
//! snapshot (obtained via `DescribeTopic`).
//!
//! Two start policies — `Latest` lands cursors on currently-active ranges
//! (skips historical sealed ones); `Earliest` lands on the lineage roots and
//! walks forward via fetch transitions. The interest filter (`KeyInterest`)
//! picks which sub-keyspaces get cursors at all.

use std::collections::HashSet;

use crate::connections::protocol::{RangeDetail, TopicDetail};
use crate::control_plane::metadata::RangeState;

use super::cursor::RangeCursor;
use super::cursor_set::RangeCursorSet;

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
    fn matches(&self, r: &RangeDetail) -> bool {
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

/// Module-local helper that bundles the `(detail, interest)` pair the
/// bootstrap path walks.
struct CursorBootstrap<'a> {
    detail: &'a TopicDetail,
    interest: KeyInterest,
}

impl<'a> CursorBootstrap<'a> {
    fn build(
        detail: &'a TopicDetail,
        interest: KeyInterest,
        policy: StartPolicy,
    ) -> RangeCursorSet {
        let bstp = Self { detail, interest };

        let cursors = match policy {
            StartPolicy::Latest => bstp.latest_cursors(),
            StartPolicy::Earliest => bstp.earliest_cursors(),
        };
        RangeCursorSet::new(cursors)
    }

    fn latest_cursors(&self) -> Vec<RangeCursor> {
        self.detail
            .ranges
            .iter()
            .filter(|r| r.state == RangeState::Active)
            .filter(|r| self.interest.matches(r))
            .map(RangeCursor::from)
            .collect()
    }

    /// A "root" range is one with no predecessor in lineage: not the product
    /// of a merge (`merged_from = None`) and not the child of any split (no
    /// other range's `split_into` mentions it). For a freshly-created topic
    /// that has only split, the original full-keyspace range is the sole
    /// root.
    fn earliest_cursors(&self) -> Vec<RangeCursor> {
        let split_children: HashSet<u64> = self
            .detail
            .ranges
            .iter()
            .filter_map(|r| r.split_into)
            .flat_map(|(l, r)| [l, r])
            .collect();

        self.detail
            .ranges
            .iter()
            .filter(|r| r.merged_from.is_none() && !split_children.contains(&r.range_id))
            .filter(|r| self.interest.matches(r))
            .map(RangeCursor::from)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connections::protocol::{SegmentDetail, TopicState};
    use crate::control_plane::metadata::RangeId;

    fn range(
        range_id: u64,
        state: RangeState,
        start: &[u8],
        end: &[u8],
        split_into: Option<(u64, u64)>,
        merged_into: Option<u64>,
        merged_from: Option<(u64, u64)>,
    ) -> RangeDetail {
        RangeDetail {
            range_id,
            keyspace_start: start.to_vec(),
            keyspace_end: end.to_vec(),
            state,
            active_segment: state.eq(&RangeState::Active).then(|| SegmentDetail {
                segment_id: 0,
                start_offset: 0,
                end_offset: None,
                replica_set: vec![],
            }),
            split_into,
            merged_into,
            merged_from,
        }
    }

    fn topic(ranges: Vec<RangeDetail>) -> TopicDetail {
        TopicDetail {
            name: "t".into(),
            state: TopicState::Active,
            ranges,
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
                Some((1, 2)),
                None,
                None,
            ),
            range(1, RangeState::Active, b"", b"m", None, None, None),
            range(2, RangeState::Active, b"m", b"\xff", None, None, None),
        ]);
        let set = CursorBootstrap::build(&t, KeyInterest::AllKeys, StartPolicy::Latest);
        let ids: Vec<RangeId> = set.cursors().iter().map(|c| c.range_id).collect();
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
        let set = CursorBootstrap::build(
            &t,
            KeyInterest::KeySpan {
                start: b"a".to_vec(),
                end: b"c".to_vec(),
            },
            StartPolicy::Latest,
        );
        let ids: Vec<RangeId> = set.cursors().iter().map(|c| c.range_id).collect();
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
                Some((1, 2)),
                None,
                None,
            ),
            range(1, RangeState::Active, b"", b"m", None, None, None),
            range(2, RangeState::Active, b"m", b"\xff", None, None, None),
        ]);
        let set = CursorBootstrap::build(&t, KeyInterest::AllKeys, StartPolicy::Earliest);

        let ids: Vec<RangeId> = set.cursors().iter().map(|c| c.range_id).collect();
        // Earliest = original root (range 0), not the split children.
        assert_eq!(ids, vec![RangeId(0)]);
    }

    #[test]
    fn earliest_skips_merge_products() {
        // Two original ranges 1, 2 merged into 3.
        let t = topic(vec![
            range(1, RangeState::Sealed, b"", b"m", None, Some(3), None),
            range(2, RangeState::Sealed, b"m", b"\xff", None, Some(3), None),
            range(
                3,
                RangeState::Active,
                b"",
                b"\xff",
                None,
                None,
                Some((1, 2)),
            ),
        ]);
        let set = CursorBootstrap::build(&t, KeyInterest::AllKeys, StartPolicy::Earliest);

        let ids: Vec<RangeId> = set.cursors().iter().map(|c| c.range_id).collect();
        // 1 and 2 are the roots (no merged_from, not split children); 3 has
        // merged_from set so it's not a root.
        assert!(ids.contains(&RangeId(1)));
        assert!(ids.contains(&RangeId(2)));
        assert!(!ids.contains(&RangeId(3)));
    }
}
