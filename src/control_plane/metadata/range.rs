use super::constants::*;
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::HashMap;

use crate::{
    client::ServerError,
    control_plane::{
        Replicas,
        metadata::{
            EntryId, RangeId, SegmentId, SegmentMeta, SegmentMetaState, SplitRange,
            command::{RollSegment, SegmentRollIntent},
            error::MetadataError,
        },
    },
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
    pub next_offset: EntryId,
    pub split_into: Option<[RangeId; 2]>,
    pub merged_into: Option<RangeId>,
    pub merged_from: Option<[RangeId; 2]>,
    pub load_state: RangeLoadState,
}

impl RangeMeta {
    pub(crate) fn new(
        range_id: RangeId,
        keyspace_start: Vec<u8>,
        keyspace_end: Vec<u8>,
        replica_set: Replicas,
        created_at: u64,
    ) -> Self {
        let segment_id = SegmentId(0);
        let segment = SegmentMeta::new(segment_id, replica_set, EntryId::MIN, created_at);

        RangeMeta {
            range_id,
            keyspace_start,
            keyspace_end,
            state: RangeState::Active,
            active_segment: Some(segment_id),
            segments: HashMap::from([(segment_id, segment)]),
            next_segment_id: 1,
            next_offset: EntryId::MIN,
            split_into: None,
            merged_into: None,
            merged_from: None,
            load_state: RangeLoadState::Unclassified,
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
        );

        let right = RangeMeta::new(
            right_id,
            cmd.split_point,
            self.keyspace_end.clone(),
            cmd.right_replica_set,
            cmd.created_at,
        );

        Ok((left, right))
    }

    pub(crate) fn active_write_segment(&self) -> Result<&SegmentMeta, ServerError> {
        self.active_segment
            .map(|id| self.segments.get(&id).unwrap())
            .ok_or(ServerError::StaleRange)
    }

    pub(crate) fn validate_active(&self) -> Result<SegmentId, MetadataError> {
        if self.state != RangeState::Active {
            return Err(MetadataError::RangeNotActive);
        }
        self.active_segment.ok_or(MetadataError::RangeNotActive)
    }

    pub(crate) fn should_split(&self) -> bool {
        self.load_state.should_split()
    }

    pub(crate) fn correct_end_offset(
        &mut self,
        segment_id: SegmentId,
        end_entry_id: EntryId,
    ) -> Option<Replicas> {
        let seg = self.segments.get_mut(&segment_id)?;

        if seg.state != SegmentMetaState::Sealed || seg.end_entry_id.is_some() {
            return None;
        }

        seg.end_entry_id = Some(end_entry_id);
        let replica_set = seg.replica_set.clone();

        self.next_offset = end_entry_id + 1;
        if let Some(active_seg_id) = self.active_segment
            && let Some(active_seg) = self.segments.get_mut(&active_seg_id)
        {
            active_seg.start_entry_id = end_entry_id + 1;
        }
        Some(replica_set)
    }

    pub(crate) fn valid_split_point(&self, split_point: &Vec<u8>) -> bool {
        split_point > &self.keyspace_start && split_point < &self.keyspace_end
    }

    pub(crate) fn build_split_proposal(
        &self,
        cmd: &RollSegment,
    ) -> Result<SplitRange, MetadataError> {
        let mid = self.compute_midpoint();
        if !self.valid_split_point(&mid) {
            return Err(MetadataError::InvalidSplitPoint);
        }

        // TODO on split, replica set may be dynamically decided based on loads.
        // Currently, we are just cloning `new_replica_set`
        Ok(SplitRange {
            topic_id: cmd.segment_key.topic_id,
            range_id: cmd.segment_key.range_id,
            split_point: mid,
            created_at: cmd.sealed_at,
            left_replica_set: cmd.new_replica_set.clone(),
            right_replica_set: cmd.new_replica_set.clone(),
        })
    }

    pub(crate) fn roll_segment(&mut self, cmd: RollSegment) -> Result<SegmentId, MetadataError> {
        let segment = self
            .segments
            .get_mut(&cmd.segment_key.segment_id)
            .ok_or(MetadataError::SegmentNotFound)?;
        segment.seal(cmd.end_entry_id, cmd.sealed_at)?;

        let start_offset = cmd.end_entry_id.map_or(EntryId::MIN, |id| id + 1);
        let new_segment_id = SegmentId(self.next_segment_id);
        let new_segment = SegmentMeta::new(
            new_segment_id,
            cmd.new_replica_set,
            start_offset,
            cmd.sealed_at,
        );

        self.segments.insert(new_segment_id, new_segment);
        self.active_segment = Some(new_segment_id);
        self.load_state.apply(&cmd.intent);
        self.next_segment_id += 1;
        self.next_offset = start_offset;

        Ok(new_segment_id)
    }

    pub(crate) fn merge(
        &mut self,
        other: &mut RangeMeta,
        merged_id: RangeId,
        replica_set: Replicas,
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
        replica_set: Replicas,
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
            seg.seal(None, created_at)?;
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

    pub(crate) fn mergeable_with(&self, r2: &RangeMeta) -> bool {
        self.load_state == RangeLoadState::Idle && r2.load_state == RangeLoadState::Idle
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum RangeLoadState {
    Unclassified,
    Idle,
    Pressure { consecutive_rolls: u8 },
}

impl RangeLoadState {
    pub fn apply(&mut self, intent: &SegmentRollIntent) {
        match intent {
            SegmentRollIntent::DataPressure => {
                let consecutive_rolls = match self {
                    Self::Pressure { consecutive_rolls } => *consecutive_rolls + 1,
                    Self::Unclassified | Self::Idle => 1,
                };
                *self = Self::Pressure {
                    consecutive_rolls: consecutive_rolls.min(SPLIT_SEAL_THRESHOLD),
                };
            }
            SegmentRollIntent::IdleMaintenance => *self = Self::Idle,
            SegmentRollIntent::ReplicationFailure | SegmentRollIntent::Recovery => {}
            SegmentRollIntent::BoundaryCorrection => {
                debug_assert!(
                    false,
                    "boundary correction applied as an active segment roll"
                );
            }
        }
    }

    pub fn should_split(&self) -> bool {
        matches!(
            self,
            Self::Pressure{ consecutive_rolls }
                if *consecutive_rolls >= SPLIT_SEAL_THRESHOLD
        )
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
            self.assert_load_state();
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
        fn assert_load_state(&self) {
            if let RangeLoadState::Pressure { consecutive_rolls } = &self.load_state {
                assert!(
                    *consecutive_rolls > 0 && *consecutive_rolls <= SPLIT_SEAL_THRESHOLD,
                    "pressure streak outside its valid range",
                );
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
