//! Per-range read cursor — the position the consumer is at within one range,
//! paired with the keyspace bounds that travel with it through lineage
//! transitions (so split/merge can carve / extend the cursor's keyspace
//! without going back to metadata).

use std::collections::HashSet;

use crate::client::TopicDetail;
use crate::client::consumer::cursor_set::KeyInterest;
use crate::connections::protocol::RangeDetail;
use crate::control_plane::metadata::{RangeId, RangeState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeCursor {
    pub range_id: RangeId,
    pub next_entry_id: u64,
    pub keyspace_start: Vec<u8>,
    pub keyspace_end: Vec<u8>,
}

impl RangeCursor {
    pub fn new(
        range_id: RangeId,
        next_entry_id: u64,
        keyspace_start: Vec<u8>,
        keyspace_end: Vec<u8>,
    ) -> Self {
        Self {
            range_id,
            next_entry_id,
            keyspace_start,
            keyspace_end,
        }
    }
    pub(crate) fn latest_cursors(detail: &TopicDetail, interest: &KeyInterest) -> Vec<Self> {
        detail
            .ranges
            .iter()
            .filter(|r| r.state == RangeState::Active)
            .filter(|r| interest.matches(r))
            .map(|r| {
                let start_offset = r.active_segment.as_ref().map_or(0, |s| s.start_entry_id);
                RangeCursor::new(
                    r.range_id,
                    start_offset,
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
    pub(crate) fn earliest_cursors(detail: &TopicDetail, interest: &KeyInterest) -> Vec<Self> {
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
                    r.first_segment_start_offset().unwrap_or(0),
                    r.keyspace_start.clone(),
                    r.keyspace_end.clone(),
                )
            })
            .collect()
    }

    pub fn split(
        self,
        left_range_id: RangeId,
        right_range_id: RangeId,
        split_point: Vec<u8>,
    ) -> [RangeCursor; 2] {
        let left = RangeCursor::new(
            RangeId(*left_range_id),
            0,
            self.keyspace_start.clone(),
            split_point.clone(),
        );
        let right = RangeCursor::new(
            RangeId(*right_range_id),
            0,
            split_point.clone(),
            self.keyspace_end.clone(),
        );
        [left, right]
    }

    /// Consume this drained parent cursor and become the merged cursor it
    /// transitions to. Used when M doesn't yet exist in the cursor set (this is
    /// the first of the merge's parents to drain in the consumer's view).
    pub fn into_merged_cursor(self, merged_id: RangeId) -> Self {
        Self {
            range_id: merged_id,
            next_entry_id: 0,
            ..self
        }
    }

    pub fn absorb(&mut self, half: RangeCursor) {
        if half.keyspace_start < self.keyspace_start {
            self.keyspace_start = half.keyspace_start.clone();
        }
        if half.keyspace_end > self.keyspace_end {
            self.keyspace_end = half.keyspace_end.clone();
        }
    }
}
