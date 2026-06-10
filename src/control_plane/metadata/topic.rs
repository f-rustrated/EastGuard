use std::collections::HashMap;

use super::constants::*;
use super::*;
use crate::{
    control_plane::{
        NodeId,
        metadata::{
            error::MetadataError,
            strategy::{PartitionStrategy, StoragePolicy},
        },
    },
    data_plane::SegmentKey,
};

use bincode::{Decode, Encode};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum TopicState {
    Active,
    Sealed,
    Deleted,
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
        replica_set: ReplicaSet,
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
    ) -> Box<[(SegmentKey, ReplicaSet)]> {
        if self.state != TopicState::Active {
            return Box::new([]);
        }
        self.active_ranges
            .iter()
            .filter_map(|range_id| {
                let seg = self.get_active_segment(range_id)?;
                seg.replica_set.contains(node_id).then(|| {
                    (
                        SegmentKey::new(self.id, *range_id, seg.segment_id),
                        seg.replica_set.clone(),
                    )
                })
            })
            .collect()
    }

    /// Every active segment with its replica set and current start offset, for
    /// the leader's periodic assignment re-drive. Returns `(key, replica_set,
    /// start_offset)` so a re-driven `SegmentAssignment` can recreate a segment
    /// that missed its one-shot delivery with the correct starting offset.
    pub(crate) fn active_segment_assignments(&self) -> Box<[(SegmentKey, ReplicaSet, u64)]> {
        if self.state != TopicState::Active {
            return Box::new([]);
        }
        self.active_ranges
            .iter()
            .filter_map(|range_id| {
                let seg = self.get_active_segment(range_id)?;
                Some((
                    SegmentKey::new(self.id, *range_id, seg.segment_id),
                    seg.replica_set.clone(),
                    seg.start_offset,
                ))
            })
            .collect()
    }

    /// Active segments whose `replica_set` contains at least one node not in
    /// `live` state.
    pub(crate) fn active_segments_with_dead_members(
        &self,
        live: &std::collections::HashSet<NodeId>,
    ) -> Box<[(SegmentKey, ReplicaSet)]> {
        if self.state != TopicState::Active {
            return Box::new([]);
        }
        self.active_ranges
            .iter()
            .filter_map(|range_id| {
                let seg = self.get_active_segment(range_id)?;
                let has_dead = seg.replica_set.iter().any(|n| !live.contains(n));
                has_dead.then(|| {
                    (
                        SegmentKey::new(self.id, *range_id, seg.segment_id),
                        seg.replica_set.clone(),
                    )
                })
            })
            .collect()
    }

    /// Route a partition key to the active range that owns it. Active ranges
    /// tile `[KEYSPACE_MIN, KEYSPACE_MAX]` contiguously (invariant 1), so the
    /// owner is the active range with the greatest `keyspace_start <= key`.
    pub(crate) fn route_active_range(&self, key: &[u8]) -> Option<&RangeMeta> {
        self.active_ranges.iter().rev().find_map(|rid| {
            let range = self.ranges.get(rid)?;
            (range.keyspace_start.as_slice() <= key).then_some(range)
        })
    }

    pub(crate) fn get_active_segment(&self, range_id: &RangeId) -> Option<&SegmentMeta> {
        let range = self.ranges.get(range_id)?;
        let seg_id = range.active_segment?;
        let seg = range.segments.get(&seg_id)?;
        Some(seg)
    }

    pub(crate) fn route_active_segment_key(&self, key: &[u8]) -> Option<SegmentKey> {
        let range = self.route_active_range(key)?;
        Some(SegmentKey::new(
            self.id,
            range.range_id,
            range.active_segment?,
        ))
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

    pub(crate) fn merged_from(&self, range_id: RangeId) -> Option<[RangeId; 2]> {
        self.ranges.get(&range_id).and_then(|m| m.merged_from)
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

    // Range-seal metadata op. Currently has no caller (the refactor that split
    // `types.rs` dropped it) — kept as the seal API the lifecycle path will use.
    #[allow(dead_code)]
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

pub struct TopicStats {
    pub name: String,
    pub range_count: u32,
    pub total_bytes: u64,
}

#[cfg(any(test, debug_assertions))]
pub mod props {
    use super::*;
    use crate::test_traits::TAssertInvariant;

    impl TAssertInvariant for TopicMeta {
        fn assert_invariants(&self) {
            for rid in self.ranges.keys() {
                assert!(rid.0 < self.next_range_id, "range ID >= next_range_id");
            }
            if self.state == TopicState::Active {
                self.assert_keyspace_coverage();
            }
            self.assert_fixed_strategy_single_range();
            self.assert_delete_cascade();
            for range in self.ranges.values() {
                range.assert_invariants();

                self.assert_split_children_cooldown(range);
            }
        }
    }

    impl TopicMeta {
        /// Invariant: a Fixed-strategy topic rejects splits at apply, so its
        /// range topology can never grow beyond the initial single full-keyspace
        /// range. Sealed topics retain that range; active topics must still have
        /// exactly one active range.
        fn assert_fixed_strategy_single_range(&self) {
            if self.storage_policy.partition_strategy != PartitionStrategy::Fixed {
                return;
            }
            assert_eq!(
                self.ranges.len(),
                1,
                "Fixed-strategy topic {:?} has {} ranges; splits should be rejected",
                self.id,
                self.ranges.len(),
            );
            if self.state == TopicState::Active {
                assert_eq!(
                    self.active_ranges.len(),
                    1,
                    "Fixed-strategy active topic {:?} has {} active ranges",
                    self.id,
                    self.active_ranges.len(),
                );
            }
        }

        /// Invariant: deleting a topic cascades to every range and every
        /// segment in one applied operation. A Deleted topic with any range or
        /// segment not marked Deleting would leak storage past GC.
        fn assert_delete_cascade(&self) {
            if self.state != TopicState::Deleted {
                return;
            }
            assert!(
                self.active_ranges.is_empty(),
                "Deleted topic {:?} still has {} active ranges",
                self.id,
                self.active_ranges.len(),
            );
            for range in self.ranges.values() {
                assert_eq!(
                    range.state,
                    RangeState::Deleting,
                    "Deleted topic {:?} has range {:?} in state {:?}",
                    self.id,
                    range.range_id,
                    range.state,
                );
                for seg in range.segments.values() {
                    assert_eq!(
                        seg.state,
                        SegmentMetaState::Deleting,
                        "Deleted topic {:?} has segment {:?} in state {:?}",
                        self.id,
                        seg.segment_id,
                        seg.state,
                    );
                }
            }
        }

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
            let ranges: Box<[&RangeMeta]> = self
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
}

#[cfg(test)]
mod routing_tests {
    use super::*;

    fn policy() -> StoragePolicy {
        StoragePolicy {
            retention_ms: 1000,
            replication_factor: 1,
            partition_strategy: PartitionStrategy::AutoSplit,
        }
    }

    fn topic() -> TopicMeta {
        TopicMeta::new("t".into(), TopicId(1), vec![NodeId::new("n")], 0, policy())
    }

    #[test]
    fn route_single_range_covers_all_keys() {
        let t = topic();
        assert_eq!(
            t.route_active_range(b"anything").unwrap().range_id,
            RangeId(0)
        );
        assert_eq!(t.route_active_range(b"").unwrap().range_id, RangeId(0));
        assert_eq!(t.route_active_range(&[0xFF]).unwrap().range_id, RangeId(0));
    }

    #[test]
    fn route_after_split_picks_correct_child() {
        let mut t = topic();
        let (left, right) = t
            .execute_split(SplitRange {
                topic_id: TopicId(1),
                range_id: RangeId(0),
                split_point: vec![0x80],
                created_at: 1,
                left_replica_set: vec![NodeId::new("n")],
                right_replica_set: vec![NodeId::new("n")],
            })
            .unwrap();
        // left = [MIN, 0x80), right = [0x80, MAX].
        assert_eq!(t.route_active_range(b"").unwrap().range_id, left);
        assert_eq!(t.route_active_range(&[0x10]).unwrap().range_id, left);
        assert_eq!(t.route_active_range(&[0x80]).unwrap().range_id, right);
        assert_eq!(t.route_active_range(&[0xC0]).unwrap().range_id, right);
    }
}
