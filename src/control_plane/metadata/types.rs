use std::collections::{HashMap, VecDeque};

use bincode::{Decode, Encode};

use crate::control_plane::{
    NodeId,
    metadata::{
        RangeId, SegmentId, SplitRange, TopicId,
        command::{MergeRange, MetadataCommand, RollSegment},
        error::MetadataError,
        strategy::{PartitionStrategy, StoragePolicy},
    },
};
use crate::data_plane::SegmentKey;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum TopicState {
    Active,
    Sealed,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum RangeState {
    Active,
    Sealed,
    Deleting,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum SegmentState {
    Active,
    Sealed,
    Reassigning { from: NodeId, to: NodeId },
    Deleting,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SegmentMeta {
    pub segment_id: SegmentId,
    pub state: SegmentState,
    pub replica_set: Vec<NodeId>,
    pub size_bytes: u64,
    pub start_offset: u64,
    pub end_offset: Option<u64>,
    pub created_at: u64,
    pub sealed_at: Option<u64>,
}

impl SegmentMeta {
    pub(crate) fn new(
        segment_id: SegmentId,
        replica_set: Vec<NodeId>,
        start_offset: u64,
        created_at: u64,
    ) -> Self {
        SegmentMeta {
            segment_id,
            state: SegmentState::Active,
            replica_set,
            size_bytes: 0,
            start_offset,
            end_offset: None,
            created_at,
            sealed_at: None,
        }
    }
    pub(crate) fn seal(
        &mut self,
        end_offset: Option<u64>,
        sealed_at: u64,
    ) -> Result<(), MetadataError> {
        if self.state != SegmentState::Active {
            return Err(MetadataError::SegmentNotActive);
        }

        self.state = SegmentState::Sealed;
        self.end_offset = end_offset;
        self.sealed_at = Some(sealed_at);

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
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
        replica_set: Vec<NodeId>,
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

    pub(crate) fn correct_end_offset(
        &mut self,
        segment_id: SegmentId,
        end_entry_id: Option<u64>,
    ) {
        let Some(end_entry_id) = end_entry_id else {
            return;
        };
        let Some(active_seg_id) = self.active_segment else {
            return;
        };
        let Some(seg) = self.segments.get_mut(&segment_id) else {
            return;
        };
        if seg.state != SegmentState::Sealed || seg.end_offset.is_some() {
            return;
        }
        seg.end_offset = Some(end_entry_id);
        self.next_offset = end_entry_id + 1;
        if let Some(active_seg) = self.segments.get_mut(&active_seg_id) {
            active_seg.start_offset = end_entry_id + 1;
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
            topic_id: cmd.topic_id,
            range_id: cmd.range_id,
            split_point: mid,
            created_at: cmd.sealed_at,
            left_replica_set: cmd.new_replica_set.clone(),
            right_replica_set: cmd.new_replica_set.clone(),
        }))
    }

    pub(crate) fn roll_segment(&mut self, cmd: RollSegment) -> Result<SegmentId, MetadataError> {
        let segment = self
            .segments
            .get_mut(&cmd.segment_id)
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
        replica_set: Vec<NodeId>,
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
        replica_set: Vec<NodeId>,
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
            && seg.state != SegmentState::Active
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

    fn delete(&mut self) {
        self.state = RangeState::Deleting;
        self.active_segment = None;
        for segment in self.segments.values_mut() {
            segment.state = SegmentState::Deleting;
        }
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

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct TopicMeta {
    pub id: TopicId,
    pub name: String,
    pub state: TopicState,
    pub storage_policy: StoragePolicy,
    pub active_ranges: Vec<RangeId>,
    pub ranges: HashMap<RangeId, RangeMeta>,
    pub next_range_id: u64,
}
impl TopicMeta {
    pub(crate) fn new(
        name: String,
        id: TopicId,
        replica_set: Vec<NodeId>,
        created_at: u64,
        storage_policy: StoragePolicy,
    ) -> Self {
        let range_id = RangeId(0);
        let range = RangeMeta::new(
            range_id,
            KEYSPACE_MIN.to_vec(),
            KEYSPACE_MAX.to_vec(),
            replica_set,
            created_at,
        );

        TopicMeta {
            id,
            name,
            state: TopicState::Active,
            storage_policy,
            active_ranges: vec![range_id],
            ranges: HashMap::from([(range_id, range)]),
            next_range_id: 1,
        }
    }

    pub(crate) fn get_ranges_mut<const N: usize>(
        &mut self,
        ranges: [&RangeId; N],
    ) -> Result<[&mut RangeMeta; N], MetadataError> {
        let options = self.ranges.get_disjoint_mut(ranges);

        let vec_ranges: Vec<&mut RangeMeta> = options
            .into_iter()
            .map(|opt| opt.ok_or(MetadataError::RangeNotFound)) // Bubble up the error
            .collect::<Result<Vec<_>, _>>()?;

        // ! SAFETY: We can safely unwrap() here because we know the Vec was built
        // ! from an array of exactly size N, so it will never fail.
        Ok(vec_ranges.try_into().unwrap())
    }

    pub(crate) fn validate_active(&self) -> Result<(), MetadataError> {
        (self.state == TopicState::Active)
            .then_some(())
            .ok_or(MetadataError::TopicNotActive(self.id))
    }
    pub(crate) fn is_merge_eligible(&self) -> bool {
        self.state == TopicState::Active && self.can_split() && self.active_ranges.len() >= 2
    }

    pub(crate) fn can_split(&self) -> bool {
        self.storage_policy.partition_strategy != PartitionStrategy::Fixed
    }

    pub(crate) fn active_segments_for_node(
        &self,
        node_id: &NodeId,
    ) -> Vec<(SegmentKey, Vec<NodeId>)> {
        if self.state != TopicState::Active {
            return Vec::new();
        }
        self.active_ranges
            .iter()
            .filter_map(|range_id| {
                let range = self.ranges.get(range_id)?;
                let seg_id = range.active_segment?;
                let seg = range.segments.get(&seg_id)?;
                seg.replica_set.contains(node_id).then(|| {
                    (
                        SegmentKey::new(self.id, *range_id, seg_id),
                        seg.replica_set.clone(),
                    )
                })
            })
            .collect()
    }

    pub(crate) fn stats(&self) -> TopicStats {
        let range_count = self.ranges.len() as u32;
        let total_bytes: u64 = self
            .ranges
            .values()
            .flat_map(|r| r.segments.values())
            .map(|s| s.size_bytes)
            .sum();
        TopicStats {
            name: self.name.clone(),
            range_count,
            total_bytes,
        }
    }

    pub(crate) fn find_mergeable_pair(&self, now: u64) -> Option<MetadataCommand> {
        if !self.is_merge_eligible() {
            return None;
        }
        self.active_ranges.windows(2).find_map(|pair| {
            self.try_merge_pair(&self.ranges[&pair[0]], &self.ranges[&pair[1]], now)
        })
    }

    fn try_merge_pair(&self, r1: &RangeMeta, r2: &RangeMeta, now: u64) -> Option<MetadataCommand> {
        if r1.state != RangeState::Active || r2.state != RangeState::Active {
            return None;
        }
        if !r1.mergeable_with(r2, now) {
            return None;
        }
        let replica_set = r1
            .active_segment
            .and_then(|sid| r1.segments.get(&sid))
            .map(|s| s.replica_set.clone())
            .unwrap_or_default();
        Some(MetadataCommand::MergeRange(MergeRange {
            topic_id: self.id,
            range_id_1: r1.range_id,
            range_id_2: r2.range_id,
            created_at: now,
            merged_replica_set: replica_set,
        }))
    }

    pub(crate) fn execute_split(
        &mut self,
        cmd: SplitRange,
    ) -> Result<(RangeId, RangeId), MetadataError> {
        let left_id = RangeId(self.next_range_id);
        let right_id = RangeId(self.next_range_id + 1);
        let range = self
            .ranges
            .get_mut(&cmd.range_id)
            .ok_or(MetadataError::RangeNotFound)?;
        let (left, right) = range.split(cmd, left_id, right_id)?;
        let parent_range_id = range.range_id;
        self.next_range_id += 2;
        self.active_ranges.retain(|id| *id != parent_range_id);
        self.ranges.insert(left_id, left);
        self.ranges.insert(right_id, right);
        self.insert_range_sorted(left_id);
        self.insert_range_sorted(right_id);
        Ok((left_id, right_id))
    }

    pub(crate) fn execute_merge(&mut self, cmd: MergeRange) -> Result<RangeId, MetadataError> {
        let merged_id = RangeId(self.next_range_id);
        let [r1, r2] = self.get_ranges_mut([&cmd.range_id_1, &cmd.range_id_2])?;
        let merged = r1.merge(r2, merged_id, cmd.merged_replica_set, cmd.created_at)?;
        self.ranges.insert(merged_id, merged);
        self.active_ranges
            .retain(|id| *id != cmd.range_id_1 && *id != cmd.range_id_2);
        self.insert_range_sorted(merged_id);
        self.next_range_id += 1;
        Ok(merged_id)
    }

    pub(crate) fn insert_range_sorted(&mut self, range_id: RangeId) {
        let start = &self.ranges[&range_id].keyspace_start;
        let pos = self
            .active_ranges
            .iter()
            .position(|id| &self.ranges[id].keyspace_start > start)
            .unwrap_or(self.active_ranges.len());
        self.active_ranges.insert(pos, range_id);
    }

    pub(crate) fn seal_range(
        &mut self,
        range_id: RangeId,
        sealed_at: u64,
    ) -> Result<(), MetadataError> {
        let range = self
            .ranges
            .get_mut(&range_id)
            .ok_or(MetadataError::RangeNotFound)?;

        range.seal(sealed_at)?;
        Ok(())
    }

    pub(crate) fn delete(&mut self) {
        self.state = TopicState::Deleted;
        self.active_ranges.clear();

        for range in self.ranges.values_mut() {
            range.delete();
        }
    }
}

// --- Keyspace Constants ---
// Full partition-key space covered by a topic's ranges. Lexicographic ordering on Vec<u8>.
// [0xFF] is a 1-byte placeholder — production should use &[0xFF; 32] for a 256-bit ring.
pub const KEYSPACE_MIN: &[u8] = &[];
pub const KEYSPACE_MAX: &[u8] = &[0xFF];

// --- Hot Range Detection Constants ---
pub const SPLIT_SEAL_THRESHOLD: usize = 3;
pub const MEASUREMENT_WINDOW_MS: u64 = 300_000; // 5 min sliding window
pub const SPLIT_COOLDOWN_MS: u64 = 300_000; // 5 min cooldown after a split
pub const MERGE_SEAL_THRESHOLD: usize = 0; // both ranges must be fully idle

#[derive(Default, Debug, Clone, PartialEq, Eq, Encode, Decode)]
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

    impl TAssertInvariant for TopicMeta {
        fn assert_invariants(&self) {
            for rid in self.ranges.keys() {
                assert!(rid.0 < self.next_range_id, "range ID >= next_range_id");
            }
            if self.state == TopicState::Active {
                self.assert_keyspace_coverage();
            }
            for range in self.ranges.values() {
                TAssertInvariant::assert_invariants(range);
                self.assert_split_children_cooldown(range);
            }
        }
    }

    impl TopicMeta {
        fn assert_split_children_cooldown(&self, range: &RangeMeta) {
            let Some([left, right]) = range.split_into else {
                return;
            };
            if let Some(left_range) = self.ranges.get(&left) {
                assert!(
                    left_range.seal_history.created_by_split_at.is_some(),
                    "split child missing created_by_split_at"
                );
            }
            if let Some(right_range) = self.ranges.get(&right) {
                assert!(
                    right_range.seal_history.created_by_split_at.is_some(),
                    "split child missing created_by_split_at"
                );
            }
        }

        fn assert_keyspace_coverage(&self) {
            if self.active_ranges.is_empty() {
                return;
            }
            let ranges: Vec<&RangeMeta> = self
                .active_ranges
                .iter()
                .map(|id| &self.ranges[id])
                .collect();
            assert_eq!(
                ranges[0].keyspace_start, KEYSPACE_MIN,
                "first range must start at MIN"
            );
            assert_eq!(
                ranges.last().unwrap().keyspace_end,
                KEYSPACE_MAX,
                "last range must end at MAX"
            );
            for w in ranges.windows(2) {
                assert_eq!(
                    w[0].keyspace_end, w[1].keyspace_start,
                    "keyspace gap between active ranges"
                );
            }
        }
    }

    impl TAssertInvariant for RangeMeta {
        fn assert_invariants(&self) {
            self.assert_active_segment_state();
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
        }
    }

    impl RangeMeta {
        fn assert_sealed_segments(&self) {
            for seg in self.segments.values() {
                if seg.state == SegmentState::Sealed {
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
                        SegmentState::Active,
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

pub struct TopicStats {
    pub name: String,
    pub range_count: u32,
    pub total_bytes: u64,
}
