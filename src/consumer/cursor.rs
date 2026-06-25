//! Per-range read cursor — the position the consumer is at within one range,
//! paired with the keyspace bounds that travel with it through lineage
//! transitions (so split/merge can carve / extend the cursor's keyspace
//! without going back to metadata).

use crate::connections::protocol::RangeDetail;
use crate::control_plane::metadata::RangeId;

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

impl From<&RangeDetail> for RangeCursor {
    fn from(r: &RangeDetail) -> Self {
        RangeCursor::new(
            RangeId(r.range_id),
            0,
            r.keyspace_start.clone(),
            r.keyspace_end.clone(),
        )
    }
}
