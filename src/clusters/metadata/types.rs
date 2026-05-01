use std::collections::{HashMap, VecDeque};

use bincode::{Decode, Encode};

use crate::clusters::{
    NodeId,
    metadata::{
        RangeId, SegmentId, SplitRange, TopicId,
        error::MetadataError,
        strategy::{PartitionStrategy, StoragePolicy},
    },
};

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
    pub(crate) fn seal(&mut self, end_offset: u64, sealed_at: u64) -> Result<(), MetadataError> {
        if self.state != SegmentState::Active {
            return Err(MetadataError::SegmentNotActive);
        }

        self.state = SegmentState::Sealed;
        self.end_offset = Some(end_offset);
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

    pub(crate) fn valid_split_point(&self, split_point: &Vec<u8>) -> bool {
        split_point > &self.keyspace_start && split_point < &self.keyspace_end
    }
    pub(crate) fn roll_segment(
        &mut self,
        segment_to_seal: SegmentId,
        replica_set: Vec<NodeId>,
        requested_at: u64,
    ) -> Result<bool, MetadataError> {
        let segment = self
            .segments
            .get_mut(&segment_to_seal)
            .ok_or(MetadataError::SegmentNotFound)?;
        segment.seal(self.next_offset.saturating_sub(1), requested_at)?;

        let new_segment_id = SegmentId(self.next_segment_id);
        let new_segment = SegmentMeta::new(
            new_segment_id,
            replica_set.clone(),
            self.next_offset,
            requested_at,
        );

        self.segments.insert(new_segment_id, new_segment);
        self.active_segment = Some(new_segment_id);
        self.seal_history.record_seal(requested_at);
        self.next_segment_id += 1;

        Ok(self.seal_history.should_split(requested_at))
    }

    pub(crate) fn merge(
        &mut self,
        other: &mut RangeMeta,
        merged_id: RangeId,
        replica_set: Vec<NodeId>,
        requested_at: u64,
    ) -> Result<RangeMeta, MetadataError> {
        if !self.is_next_to(other) {
            return Err(MetadataError::RangesNotAdjacent);
        }

        let (merged_start, merged_end) = if self.keyspace_start <= other.keyspace_start {
            (self.keyspace_start.clone(), other.keyspace_end.clone())
        } else {
            (other.keyspace_start.clone(), self.keyspace_end.clone())
        };

        self.seal(requested_at)?;
        other.seal(requested_at)?;
        self.merged_into = Some(merged_id);
        other.merged_into = Some(merged_id);
        let mut merged = RangeMeta::new(
            merged_id,
            merged_start,
            merged_end,
            replica_set,
            requested_at,
        );

        // To make sure of determinsim
        let merged_from = if self.range_id < other.range_id {
            [self.range_id, other.range_id]
        } else {
            [other.range_id, self.range_id]
        };

        merged.merged_from = Some(merged_from);

        Ok(merged)
    }

    pub(crate) fn seal(&mut self, created_at: u64) -> Result<(), MetadataError> {
        if let Some(seg_id) = self.active_segment
            && let Some(seg) = self.segments.get_mut(&seg_id)
        {
            seg.seal(self.next_offset.saturating_sub(1), created_at)?;
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
    pub(crate) fn can_split(&self) -> bool {
        self.storage_policy.partition_strategy != PartitionStrategy::Fixed
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
