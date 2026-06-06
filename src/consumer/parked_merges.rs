use crate::{consumer::RangeCursor, control_plane::metadata::RangeId};

pub(super) struct ParkedMerge {
    /// The merged range's wire ID.
    pub(super) merged_id: RangeId,

    /// `Vec` instead of `HashMap` — for `n` this small the linear scan is cache-friendly and
    /// avoids HashMap's per-entry hashing + sparse-storage overhead.
    pub(super) drained: Vec<RangeCursor>,
}
#[derive(Default)]
pub(super) struct ParkedMerges(pub(super) Vec<ParkedMerge>);

impl ParkedMerges {
    pub(super) fn get_idx(&self, target: RangeId) -> Option<usize> {
        self.0.iter().position(|p| p.merged_id == target)
    }

    pub(super) fn push(&mut self, pending: ParkedMerge) {
        self.0.push(pending);
    }

    pub(super) fn try_complete_merge(
        &mut self,
        merged_range_id: RangeId,
        drained: &RangeCursor,
    ) -> Option<RangeCursor> {
        let pos = self.get_idx(merged_range_id)?;
        let parked = self.0.swap_remove(pos).drained.pop()?;
        let mut m = parked.into_merged_cursor(merged_range_id);
        m.absorb(drained.clone());
        Some(m)
    }
}
