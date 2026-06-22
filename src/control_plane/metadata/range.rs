use super::constants::*;
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::{HashMap, VecDeque};

use crate::control_plane::metadata::{
    RangeId, ReplicaSet, SegmentId, SegmentMeta, SegmentMetaState, SplitRange,
    command::{MetadataCommand, RollSegment},
    error::MetadataError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum RangeState {
    Active,
    Sealed,
    Deleting,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct RangeMeta {
    pub range_id: RangeId,
    pub keyspace_start: Vec<u8>,
    pub keyspace_end: Vec<u8>,
    pub state: RangeState,
    pub active_segment: Option<SegmentId>,
    pub segments: HashMap<SegmentId, SegmentMeta>,
    pub next_segment_id: u64,
    pub next_offset: u64,
    pub split_into: Option<[RangeId; 2]>,
    pub merged_into: Option<RangeId>,
    pub merged_from: Option<[RangeId; 2]>,
    pub seal_history: RangeSealHistory,
}

impl RangeMeta {
    pub(crate) fn new(
        range_id: RangeId,
        keyspace_start: Vec<u8>,
        keyspace_end: Vec<u8>,
        replica_set: ReplicaSet,
        created_at: u64,
    ) -> Self {
        let segment_id = SegmentId(0);
        let segment = SegmentMeta::new(segment_id, replica_set, 0, created_at);

        RangeMeta {
            range_id,
            keyspace_start,
            keyspace_end,
            state: RangeState::Active,
            active_segment: Some(segment_id),
            segments: HashMap::from([(segment_id, segment)]),
            next_segment_id: 1,
            next_offset: 0,
            split_into: None,
            merged_into: None,
            merged_from: None,
            seal_history: RangeSealHistory::default(),
        }
    }

    pub(crate) fn split(
        &mut self,
        cmd: SplitRange,
        left_id: RangeId,
        right_id: RangeId,
    ) -> Result<(RangeMeta, RangeMeta), MetadataError> {
        if !self.valid_split_point(&cmd.split_point) {
            return Err(MetadataError::InvalidSplitPoint);
        }
        let _ = self.validate_active()?;

        self.seal(cmd.created_at)?;
        self.split_into = Some([left_id, right_id]);
        let left = RangeMeta::new(
            left_id,
            self.keyspace_start.clone(),
            cmd.split_point.clone(),
            cmd.left_replica_set,
            cmd.created_at,
        )
        .with_split_origin(cmd.created_at);

        let right = RangeMeta::new(
            right_id,
            cmd.split_point,
            self.keyspace_end.clone(),
            cmd.right_replica_set,
            cmd.created_at,
        )
        .with_split_origin(cmd.created_at);

        Ok((left, right))
    }

    fn with_split_origin(mut self, created_at: u64) -> Self {
        self.seal_history = RangeSealHistory {
            seal_timestamps: VecDeque::new(),
            created_by_split_at: Some(created_at),
        };
        self
    }
    pub(crate) fn validate_active(&self) -> Result<SegmentId, MetadataError> {
        if self.state != RangeState::Active {
            return Err(MetadataError::RangeNotActive);
        }
        self.active_segment.ok_or(MetadataError::RangeNotActive)
    }

    pub(crate) fn should_split(&self, sealed_at: u64) -> bool {
        self.seal_history.should_split(sealed_at)
    }

    pub(crate) fn is_active_segment(&self, expected: SegmentId) -> Result<bool, MetadataError> {
        let active_seg_id = self.validate_active()?;
        Ok(active_seg_id == expected)
    }

    pub(crate) fn correct_end_offset(&mut self, segment_id: SegmentId, end_entry_id: Option<u64>) {
        let Some(end_entry_id) = end_entry_id else {
            return;
        };
        let Some(active_seg_id) = self.active_segment else {
            return;
        };
        let Some(seg) = self.segments.get_mut(&segment_id) else {
            return;
        };
        if seg.state != SegmentMetaState::Sealed || seg.end_entry_id.is_some() {
            return;
        }
        seg.end_entry_id = Some(end_entry_id);
        self.next_offset = end_entry_id + 1;
        if let Some(active_seg) = self.segments.get_mut(&active_seg_id) {
            active_seg.start_entry_id = end_entry_id + 1;
        }
    }

    pub(crate) fn valid_split_point(&self, split_point: &Vec<u8>) -> bool {
        split_point > &self.keyspace_start && split_point < &self.keyspace_end
    }

    pub(crate) fn build_split_proposal(
        &self,
        cmd: &RollSegment,
    ) -> Result<MetadataCommand, MetadataError> {
        let mid = self.compute_midpoint();
        if !self.valid_split_point(&mid) {
            return Err(MetadataError::InvalidSplitPoint);
        }
        Ok(MetadataCommand::SplitRange(SplitRange {
            topic_id: cmd.segment_key.topic_id,
            range_id: cmd.segment_key.range_id,
            split_point: mid,
            created_at: cmd.sealed_at,
            left_replica_set: cmd.new_replica_set.clone(),
            right_replica_set: cmd.new_replica_set.clone(),
        }))
    }

    pub(crate) fn roll_segment(&mut self, cmd: RollSegment) -> Result<SegmentId, MetadataError> {
        let segment = self
            .segments
            .get_mut(&cmd.segment_key.segment_id)
            .ok_or(MetadataError::SegmentNotFound)?;
        segment.seal(cmd.end_entry_id, cmd.sealed_at)?;

        let start_offset = cmd.end_entry_id.map_or(0, |id| id + 1);
        let new_segment_id = SegmentId(self.next_segment_id);
        let new_segment = SegmentMeta::new(
            new_segment_id,
            cmd.new_replica_set,
            start_offset,
            cmd.sealed_at,
        );

        self.segments.insert(new_segment_id, new_segment);
        self.active_segment = Some(new_segment_id);
        self.seal_history.record_seal(cmd.sealed_at);
        self.next_segment_id += 1;
        self.next_offset = start_offset;

        Ok(new_segment_id)
    }

    pub(crate) fn merge(
        &mut self,
        other: &mut RangeMeta,
        merged_id: RangeId,
        replica_set: ReplicaSet,
        requested_at: u64,
    ) -> Result<RangeMeta, MetadataError> {
        self.validate_mergeable(other)?;
        self.seal_for_merge(other, merged_id, requested_at)?;
        Ok(self.build_merged_range(other, merged_id, replica_set, requested_at))
    }

    fn validate_mergeable(&self, other: &RangeMeta) -> Result<(), MetadataError> {
        if !self.is_next_to(other) {
            return Err(MetadataError::RangesNotAdjacent);
        }
        self.validate_sealable()?;
        other.validate_sealable()?;
        Ok(())
    }

    fn seal_for_merge(
        &mut self,
        other: &mut RangeMeta,
        merged_id: RangeId,
        requested_at: u64,
    ) -> Result<(), MetadataError> {
        self.seal(requested_at)?;
        other.seal(requested_at)?;
        self.merged_into = Some(merged_id);
        other.merged_into = Some(merged_id);
        Ok(())
    }

    fn build_merged_range(
        &self,
        other: &RangeMeta,
        merged_id: RangeId,
        replica_set: ReplicaSet,
        requested_at: u64,
    ) -> RangeMeta {
        let (merged_start, merged_end) = self.merged_keyspace(other);
        let mut merged = RangeMeta::new(
            merged_id,
            merged_start,
            merged_end,
            replica_set,
            requested_at,
        );
        merged.merged_from = Some(Self::ordered_pair(self.range_id, other.range_id));
        merged
    }

    fn merged_keyspace(&self, other: &RangeMeta) -> (Vec<u8>, Vec<u8>) {
        if self.keyspace_start <= other.keyspace_start {
            (self.keyspace_start.clone(), other.keyspace_end.clone())
        } else {
            (other.keyspace_start.clone(), self.keyspace_end.clone())
        }
    }

    fn ordered_pair(a: RangeId, b: RangeId) -> [RangeId; 2] {
        if a < b { [a, b] } else { [b, a] }
    }

    fn validate_sealable(&self) -> Result<(), MetadataError> {
        if let Some(seg_id) = self.active_segment
            && let Some(seg) = self.segments.get(&seg_id)
            && seg.state != SegmentMetaState::Active
        {
            return Err(MetadataError::SegmentNotActive);
        }
        Ok(())
    }

    pub(crate) fn seal(&mut self, created_at: u64) -> Result<(), MetadataError> {
        if let Some(seg_id) = self.active_segment
            && let Some(seg) = self.segments.get_mut(&seg_id)
        {
            seg.seal(Some(self.next_offset.saturating_sub(1)), created_at)?;
        }
        self.state = RangeState::Sealed;
        self.active_segment = None;
        Ok(())
    }

    pub(crate) fn is_next_to(&self, other: &RangeMeta) -> bool {
        self.keyspace_end == other.keyspace_start || other.keyspace_end == self.keyspace_start
    }

    pub(super) fn delete(&mut self) {
        self.state = RangeState::Deleting;
        self.active_segment = None;
        for segment in self.segments.values_mut() {
            segment.state = SegmentMetaState::Deleting;
        }
    }

    /// Retention: mark the named segments `Deleting`, returning the ids actually
    /// transitioned. Only `Sealed → Deleting` — an `Active` (write head) or an
    /// already-`Deleting`/absent id is skipped, so the call is idempotent and can
    /// never delete the write head. Callers pass an oldest-first prefix (the sweep
    /// does so by construction); that no-hole property is a structural invariant
    /// (`assert_retention_prefix`, d7 #2), not re-checked on the hot path.
    pub(crate) fn delete_segments(&mut self, segment_ids: &[SegmentId]) -> Vec<SegmentId> {
        let mut deleted = Vec::new();
        for sid in segment_ids {
            if let Some(seg) = self.segments.get_mut(sid)
                && seg.state == SegmentMetaState::Sealed
            {
                seg.state = SegmentMetaState::Deleting;
                deleted.push(*sid);
            }
        }
        deleted
    }

    /// Retention: sealed segments expired under `retention_ms` as of `now`
    /// (`sealed_at + retention_ms < now`), as an oldest-first prefix by segment_id.
    /// Take-while semantics — stops at the first non-expired sealed segment (or the
    /// active head) so the result is always a contiguous prefix, never a hole.
    pub(crate) fn expired_sealed_prefix(&self, now: u64, retention_ms: u64) -> Vec<SegmentId> {
        let mut segs: Vec<&SegmentMeta> = self.segments.values().collect();
        segs.sort_by_key(|s| s.segment_id.0);
        let mut out = Vec::new();
        for seg in segs {
            match seg.state {
                // Already gone from the live prefix; keep scanning newer segments.
                SegmentMetaState::Deleting => continue,
                SegmentMetaState::Sealed => match seg.sealed_at {
                    Some(sealed_at) if sealed_at.saturating_add(retention_ms) < now => {
                        out.push(seg.segment_id)
                    }
                    // First non-expired (or unknown sealed_at) → stop: prefix only.
                    _ => break,
                },
                // Reached the write head.
                SegmentMetaState::Active => break,
            }
        }
        out
    }

    /// Compute the midpoint of two byte-slice keyspace bounds.
    pub fn compute_midpoint(&self) -> Vec<u8> {
        let len = self
            .keyspace_start
            .len()
            .max(self.keyspace_end.len())
            .max(1);

        let mut s = vec![0u8; len];
        let mut e = vec![0u8; len];
        s[..self.keyspace_start.len()].copy_from_slice(&self.keyspace_start);
        e[..self.keyspace_end.len()].copy_from_slice(&self.keyspace_end);

        let mut result = vec![0u8; len];
        let mut carry = 0u16;
        for i in 0..len {
            let val = carry * 256 + s[i] as u16 + e[i] as u16;
            result[i] = (val / 2) as u8;
            carry = val % 2;
        }

        result
    }

    pub(crate) fn mergeable_with(&self, r2: &RangeMeta, now: u64) -> bool {
        let r1_recent = self.seal_history.recent_seal_count(now);
        let r2_recent = r2.seal_history.recent_seal_count(now);

        r1_recent == MERGE_SEAL_THRESHOLD && r2_recent == MERGE_SEAL_THRESHOLD
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct RangeSealHistory {
    pub seal_timestamps: VecDeque<u64>,
    pub created_by_split_at: Option<u64>,
}

impl RangeSealHistory {
    pub fn record_seal(&mut self, sealed_at: u64) {
        self.seal_timestamps.push_back(sealed_at);
        let cutoff = sealed_at.saturating_sub(MEASUREMENT_WINDOW_MS);
        while self.seal_timestamps.front().is_some_and(|&t| t <= cutoff) {
            self.seal_timestamps.pop_front();
        }
    }

    pub fn seal_count(&self) -> usize {
        self.seal_timestamps.len()
    }

    pub fn should_split(&self, now: u64) -> bool {
        if self.seal_count() < SPLIT_SEAL_THRESHOLD {
            return false;
        }
        match self.created_by_split_at {
            Some(split_at) => now.saturating_sub(split_at) >= SPLIT_COOLDOWN_MS,
            None => true,
        }
    }

    pub fn recent_seal_count(&self, now: u64) -> usize {
        let cutoff = now.saturating_sub(MEASUREMENT_WINDOW_MS);
        self.seal_timestamps.iter().filter(|&&t| t > cutoff).count()
    }
}

#[cfg(any(test, debug_assertions))]
pub mod props {
    use crate::test_traits::TAssertInvariant;

    use super::*;

    impl TAssertInvariant for RangeMeta {
        fn assert_invariants(&self) {
            self.assert_active_segment_state();
            self.assert_single_active_segment();
            for sid in self.segments.keys() {
                assert!(
                    sid.0 < self.next_segment_id,
                    "segment ID >= next_segment_id"
                );
            }
            self.assert_sealed_segments();
            assert!(
                self.split_into.is_none() || self.merged_into.is_none(),
                "range both split and merged"
            );
            self.assert_seal_history();
            self.assert_offset_chain();
            self.assert_retention_prefix();
        }
    }

    impl RangeMeta {
        /// Invariant: each range has at most one segment in `Active` state.
        /// An Active range has exactly one; Sealed/Deleting ranges have zero.
        /// Two concurrently-active segments would admit conflicting writes at the
        /// same offset on the same range.
        fn assert_single_active_segment(&self) {
            let active_count = self
                .segments
                .values()
                .filter(|s| s.state == SegmentMetaState::Active)
                .count();
            let expected = match self.state {
                RangeState::Active => 1,
                RangeState::Sealed | RangeState::Deleting => 0,
            };
            assert_eq!(
                active_count, expected,
                "range {:?} (state={:?}) has {} active segments, expected {}",
                self.range_id, self.state, active_count, expected,
            );
        }

        /// Invariants: ordered by segment_id, segments form a contiguous
        /// offset chain (`seg[N].end_offset + 1 == seg[N+1].start_offset`) wherever
        /// `end_offset` is known, and `next_offset` covers the active head. The
        /// chain may have one trailing hole — a death-triggered roll seals with
        /// `end_offset = None` and is later patched by `correct_end_offset`.
        fn assert_offset_chain(&self) {
            let mut segs: Vec<&SegmentMeta> = self.segments.values().collect();
            segs.sort_by_key(|s| s.segment_id.0);
            for window in segs.windows(2) {
                let (prev, next) = (&window[0], &window[1]);
                if let Some(end) = prev.end_entry_id {
                    assert_eq!(
                        end + 1,
                        next.start_entry_id,
                        "range {:?} offset gap: seg {:?} end_offset+1={} != seg {:?} start_offset={}",
                        self.range_id,
                        prev.segment_id,
                        end + 1,
                        next.segment_id,
                        next.start_entry_id,
                    );
                }
            }
            if let Some(active_seg_id) = self.active_segment
                && let Some(active_seg) = self.segments.get(&active_seg_id)
            {
                assert!(
                    self.next_offset >= active_seg.start_entry_id,
                    "range {:?} next_offset ({}) < active segment start_offset ({})",
                    self.range_id,
                    self.next_offset,
                    active_seg.start_entry_id,
                );
            }
        }

        fn assert_sealed_segments(&self) {
            for seg in self.segments.values() {
                if seg.state == SegmentMetaState::Sealed {
                    assert!(seg.sealed_at.is_some(), "sealed segment missing sealed_at");
                }
            }
        }
        fn assert_seal_history(&self) {
            let ts = &self.seal_history.seal_timestamps;
            for i in 1..ts.len() {
                assert!(ts[i - 1] <= ts[i], "seal_history timestamps not sorted");
            }
        }

        /// D7 retention invariants: (#1) the active head is never `Deleting`, so a
        /// range under retention can still be written; (#2) `Deleting` segments form
        /// an oldest-first prefix by segment_id — every survivor has a higher
        /// segment_id, so survivors are a contiguous suffix and a forward reader
        /// crosses the retained boundary exactly once (no hole).
        fn assert_retention_prefix(&self) {
            if let Some(active_id) = self.active_segment
                && let Some(seg) = self.segments.get(&active_id)
            {
                assert_ne!(
                    seg.state,
                    SegmentMetaState::Deleting,
                    "range {:?}: active segment is Deleting",
                    self.range_id,
                );
            }
            let max_deleting = self
                .segments
                .values()
                .filter(|s| s.state == SegmentMetaState::Deleting)
                .map(|s| s.segment_id.0)
                .max();
            let min_surviving = self
                .segments
                .values()
                .filter(|s| s.state != SegmentMetaState::Deleting)
                .map(|s| s.segment_id.0)
                .min();
            if let (Some(md), Some(ms)) = (max_deleting, min_surviving) {
                assert!(
                    md < ms,
                    "range {:?}: Deleting segment_id {} >= surviving {} (not an oldest-first prefix)",
                    self.range_id,
                    md,
                    ms,
                );
            }
        }

        fn assert_active_segment_state(&self) {
            match self.state {
                RangeState::Active => {
                    assert!(
                        self.active_segment.is_some(),
                        "active range missing active_segment"
                    );
                    let seg_id = self.active_segment.unwrap();
                    let seg = self
                        .segments
                        .get(&seg_id)
                        .expect("active_segment points to missing segment");
                    assert_eq!(
                        seg.state,
                        SegmentMetaState::Active,
                        "active_segment not in Active state"
                    );
                }
                RangeState::Sealed | RangeState::Deleting => {
                    assert!(
                        self.active_segment.is_none(),
                        "sealed/deleting range has active_segment"
                    );
                }
            }
        }
    }
}
