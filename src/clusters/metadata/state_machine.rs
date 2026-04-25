use super::types::*;
use crate::clusters::{
    NodeId,
    metadata::{
        RangeId, SegmentId, TopicId,
        error::MetadataError,
        strategy::{PartitionStrategy, StoragePolicy},
    },
};
use std::collections::HashMap;

#[derive(Default)]
pub struct MetadataStateMachine {
    topics: HashMap<TopicId, TopicMeta>,
    topic_name_index: HashMap<String, TopicId>,
    next_topic_id: u64,
}

impl MetadataStateMachine {
    pub(crate) fn get_topic(&self, id: &TopicId) -> Option<&TopicMeta> {
        self.topics.get(id)
    }

    pub fn get_topic_by_name(&self, name: &str) -> Option<&TopicMeta> {
        self.topic_name_index
            .get(name)
            .and_then(|id| self.topics.get(id))
    }

    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }

    pub fn create_topic(
        &mut self,
        name: String,
        storage_policy: StoragePolicy,
        replica_set: Vec<NodeId>,
        created_at: u64, // ? this can be internally generated?
    ) -> Result<TopicId, MetadataError> {
        if self.topic_name_index.contains_key(&name) {
            return Err(MetadataError::TopicNameAlreadyExists(name));
        }
        let topic_id = TopicId(self.next_topic_id);
        let topic = TopicMeta::new(name, topic_id, replica_set, created_at, storage_policy);

        self.topic_name_index.insert(topic.name.clone(), topic_id);
        self.topics.insert(topic.id, topic);
        self.next_topic_id += 1;
        Ok(topic_id)
    }

    pub fn apply_seal_segment(
        &mut self,
        topic_id: TopicId,
        range_id: RangeId,
        segment_id: SegmentId,
        sealed_at: u64,
        new_replica_set: Vec<NodeId>,
    ) -> Result<(), MetadataError> {
        let topic = self
            .topics
            .get_mut(&topic_id)
            .ok_or(MetadataError::TopicNotFound(topic_id))?;

        if topic.state != TopicState::Active {
            return Err(MetadataError::TopicNotActive(topic_id));
        }

        let range = topic
            .ranges
            .get_mut(&range_id)
            .ok_or(MetadataError::RangeNotFound(topic_id, range_id))?;

        if range.state != RangeState::Active {
            return Err(MetadataError::RangeNotActive(topic_id, range_id));
        }

        if range.active_segment != Some(segment_id) {
            return Err(MetadataError::SegmentNotActive(
                topic_id, range_id, segment_id,
            ));
        }

        let segment = range
            .segments
            .get_mut(&segment_id)
            .ok_or(MetadataError::SegmentNotFound(
                topic_id, range_id, segment_id,
            ))?;

        if segment.state != SegmentState::Active {
            return Err(MetadataError::SegmentNotActive(
                topic_id, range_id, segment_id,
            ));
        }

        segment.state = SegmentState::Sealed;
        segment.end_offset = Some(range.next_offset.saturating_sub(1));
        segment.sealed_at = Some(sealed_at);

        let new_segment_id = SegmentId(range.next_segment_id);
        range.next_segment_id += 1;

        let new_segment = SegmentMeta {
            segment_id: new_segment_id,
            state: SegmentState::Active,
            replica_set: new_replica_set,
            size_bytes: 0,
            start_offset: range.next_offset,
            end_offset: None,
            created_at: sealed_at,
            sealed_at: None,
        };

        range.segments.insert(new_segment_id, new_segment);
        range.active_segment = Some(new_segment_id);

        Ok(())
    }

    pub fn apply_split_range(
        &mut self,
        topic_id: TopicId,
        range_id: RangeId,
        split_point: Vec<u8>,
        created_at: u64,
        child1_replica_set: Vec<NodeId>,
        child2_replica_set: Vec<NodeId>,
    ) -> Result<(RangeId, RangeId), MetadataError> {
        let topic = self
            .topics
            .get_mut(&topic_id)
            .ok_or(MetadataError::TopicNotFound(topic_id))?;

        if topic.state != TopicState::Active {
            return Err(MetadataError::TopicNotActive(topic_id));
        }

        if topic.storage_policy.partition_strategy == PartitionStrategy::Fixed {
            return Err(MetadataError::SplitNotAllowed(topic_id));
        }

        let range = topic
            .ranges
            .get_mut(&range_id)
            .ok_or(MetadataError::RangeNotFound(topic_id, range_id))?;

        if range.state != RangeState::Active {
            return Err(MetadataError::RangeNotActive(topic_id, range_id));
        }

        if split_point <= range.keyspace_start || split_point >= range.keyspace_end {
            return Err(MetadataError::InvalidSplitPoint);
        }

        // Seal parent's active segment
        if let Some(seg_id) = range.active_segment
            && let Some(seg) = range.segments.get_mut(&seg_id)
        {
            seg.state = SegmentState::Sealed;
            seg.end_offset = Some(range.next_offset.saturating_sub(1));
            seg.sealed_at = Some(created_at);
        }

        let parent_start = range.keyspace_start.clone();
        let parent_end = range.keyspace_end.clone();
        range.state = RangeState::Sealed;
        range.active_segment = None;

        // Allocate child IDs
        let child1_id = RangeId(topic.next_range_id);
        topic.next_range_id += 1;
        let child2_id = RangeId(topic.next_range_id);
        topic.next_range_id += 1;

        // Record lineage on parent
        topic.ranges.get_mut(&range_id).unwrap().split_into = Some([child1_id, child2_id]);

        // Create child ranges
        let child1 = Self::new_range(
            child1_id,
            parent_start,
            split_point.clone(),
            child1_replica_set,
            created_at,
        );
        let child2 = Self::new_range(
            child2_id,
            split_point,
            parent_end,
            child2_replica_set,
            created_at,
        );

        topic.ranges.insert(child1_id, child1);
        topic.ranges.insert(child2_id, child2);

        // Update active_ranges: remove parent, insert children sorted
        topic.active_ranges.retain(|id| *id != range_id);
        Self::insert_range_sorted(&topic.ranges, &mut topic.active_ranges, child1_id);
        Self::insert_range_sorted(&topic.ranges, &mut topic.active_ranges, child2_id);

        Ok((child1_id, child2_id))
    }

    pub fn apply_merge_range(
        &mut self,
        topic_id: TopicId,
        range_id_1: RangeId,
        range_id_2: RangeId,
        created_at: u64,
        merged_replica_set: Vec<NodeId>,
    ) -> Result<RangeId, MetadataError> {
        let topic = self
            .topics
            .get_mut(&topic_id)
            .ok_or(MetadataError::TopicNotFound(topic_id))?;

        if topic.state != TopicState::Active {
            return Err(MetadataError::TopicNotActive(topic_id));
        }

        // Validate both ranges exist and are Active
        {
            let r1 = topic
                .ranges
                .get(&range_id_1)
                .ok_or(MetadataError::RangeNotFound(topic_id, range_id_1))?;
            let r2 = topic
                .ranges
                .get(&range_id_2)
                .ok_or(MetadataError::RangeNotFound(topic_id, range_id_2))?;

            if r1.state != RangeState::Active {
                return Err(MetadataError::RangeNotActive(topic_id, range_id_1));
            }
            if r2.state != RangeState::Active {
                return Err(MetadataError::RangeNotActive(topic_id, range_id_2));
            }

            // Check adjacency: one's end must equal the other's start
            let adjacent =
                r1.keyspace_end == r2.keyspace_start || r2.keyspace_end == r1.keyspace_start;
            if !adjacent {
                return Err(MetadataError::RangesNotAdjacent(range_id_1, range_id_2));
            }
        }

        // Determine ordering by keyspace
        let (lower_id, upper_id) = {
            let r1 = &topic.ranges[&range_id_1];
            let r2 = &topic.ranges[&range_id_2];
            if r1.keyspace_start <= r2.keyspace_start {
                (range_id_1, range_id_2)
            } else {
                (range_id_2, range_id_1)
            }
        };

        let merged_start = topic.ranges[&lower_id].keyspace_start.clone();
        let merged_end = topic.ranges[&upper_id].keyspace_end.clone();

        // Seal both ranges and their active segments
        Self::seal_range(&mut topic.ranges, lower_id, created_at);
        Self::seal_range(&mut topic.ranges, upper_id, created_at);

        // Allocate merged range
        let merged_id = RangeId(topic.next_range_id);
        topic.next_range_id += 1;

        // Record lineage
        topic.ranges.get_mut(&range_id_1).unwrap().merged_into = Some(merged_id);
        topic.ranges.get_mut(&range_id_2).unwrap().merged_into = Some(merged_id);

        let mut merged = Self::new_range(
            merged_id,
            merged_start,
            merged_end,
            merged_replica_set,
            created_at,
        );
        merged.merged_from = Some([range_id_1, range_id_2]);

        topic.ranges.insert(merged_id, merged);

        // Update active_ranges
        topic
            .active_ranges
            .retain(|id| *id != range_id_1 && *id != range_id_2);
        Self::insert_range_sorted(&topic.ranges, &mut topic.active_ranges, merged_id);

        Ok(merged_id)
    }

    pub fn apply_delete_topic(&mut self, topic_id: TopicId) -> Result<(), MetadataError> {
        let topic = self
            .topics
            .get_mut(&topic_id)
            .ok_or(MetadataError::TopicNotFound(topic_id))?;

        topic.state = TopicState::Deleted;
        topic.active_ranges.clear();

        for range in topic.ranges.values_mut() {
            range.state = RangeState::Deleting;
            range.active_segment = None;
            for segment in range.segments.values_mut() {
                segment.state = SegmentState::Deleting;
            }
        }

        self.topic_name_index.remove(&topic.name);

        Ok(())
    }

    // --- Private helpers ---

    fn new_range(
        range_id: RangeId,
        keyspace_start: Vec<u8>,
        keyspace_end: Vec<u8>,
        replica_set: Vec<NodeId>,
        created_at: u64,
    ) -> RangeMeta {
        let segment_id = SegmentId(0);
        let segment = SegmentMeta {
            segment_id,
            state: SegmentState::Active,
            replica_set,
            size_bytes: 0,
            start_offset: 0,
            end_offset: None,
            created_at,
            sealed_at: None,
        };

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
        }
    }

    fn seal_range(ranges: &mut HashMap<RangeId, RangeMeta>, range_id: RangeId, sealed_at: u64) {
        let range = ranges.get_mut(&range_id).unwrap();
        if let Some(seg_id) = range.active_segment
            && let Some(seg) = range.segments.get_mut(&seg_id)
        {
            seg.state = SegmentState::Sealed;
            seg.end_offset = Some(range.next_offset.saturating_sub(1));
            seg.sealed_at = Some(sealed_at);
        }
        range.state = RangeState::Sealed;
        range.active_segment = None;
    }

    fn insert_range_sorted(
        ranges: &HashMap<RangeId, RangeMeta>,
        active_ranges: &mut Vec<RangeId>,
        range_id: RangeId,
    ) {
        let start = &ranges[&range_id].keyspace_start;
        let pos = active_ranges
            .iter()
            .position(|id| ranges[id].keyspace_start > *start)
            .unwrap_or(active_ranges.len());
        active_ranges.insert(pos, range_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_policy() -> StoragePolicy {
        StoragePolicy {
            retention_ms: 3_600_000,
            replication_factor: 3,
            partition_strategy: PartitionStrategy::AutoSplit,
        }
    }

    fn fixed_policy() -> StoragePolicy {
        StoragePolicy {
            retention_ms: 3_600_000,
            replication_factor: 3,
            partition_strategy: PartitionStrategy::Fixed,
        }
    }

    fn replica_set() -> Vec<NodeId> {
        vec![
            NodeId::new("node-1"),
            NodeId::new("node-2"),
            NodeId::new("node-3"),
        ]
    }

    fn create_topic(sm: &mut MetadataStateMachine, name: &str) -> TopicId {
        sm.create_topic(name.to_string(), default_policy(), replica_set(), 1000)
            .unwrap()
    }

    // --- CreateTopic ---

    #[test]
    fn create_topic_basic() {
        let mut sm = MetadataStateMachine::default();
        let id = create_topic(&mut sm, "blue");

        let topic = sm.get_topic(&id).unwrap();
        assert_eq!(topic.name, "blue");
        assert_eq!(topic.state, TopicState::Active);
        assert_eq!(topic.active_ranges.len(), 1);
        assert_eq!(topic.ranges.len(), 1);

        let range = &topic.ranges[&RangeId(0)];
        assert_eq!(range.state, RangeState::Active);
        assert!(range.active_segment.is_some());
        assert_eq!(range.segments.len(), 1);
    }

    #[test]
    fn create_topic_duplicate_name_rejected() {
        let mut sm = MetadataStateMachine::default();
        create_topic(&mut sm, "blue");

        let result = sm.create_topic("blue".to_string(), default_policy(), replica_set(), 2000);
        assert_eq!(
            result,
            Err(MetadataError::TopicNameAlreadyExists("blue".to_string()))
        );
    }

    #[test]
    fn create_topic_increments_id() {
        let mut sm = MetadataStateMachine::default();
        let id1 = create_topic(&mut sm, "alpha");
        let id2 = create_topic(&mut sm, "beta");

        assert_eq!(id1, TopicId(0));
        assert_eq!(id2, TopicId(1));
        assert_eq!(sm.topic_count(), 2);
    }

    #[test]
    fn create_topic_full_keyspace() {
        let mut sm = MetadataStateMachine::default();
        let id = create_topic(&mut sm, "blue");

        let topic = sm.get_topic(&id).unwrap();
        let range = &topic.ranges[&RangeId(0)];
        assert_eq!(range.keyspace_start, KEYSPACE_MIN);
        assert_eq!(range.keyspace_end, KEYSPACE_MAX);
    }

    #[test]
    fn create_topic_initial_offsets() {
        let mut sm = MetadataStateMachine::default();
        let id = create_topic(&mut sm, "blue");

        let topic = sm.get_topic(&id).unwrap();
        let range = &topic.ranges[&RangeId(0)];
        assert_eq!(range.next_offset, 0);

        let seg = &range.segments[&SegmentId(0)];
        assert_eq!(seg.start_offset, 0);
        assert_eq!(seg.end_offset, None);
    }

    #[test]
    fn create_topic_name_index() {
        let mut sm = MetadataStateMachine::default();
        let id = create_topic(&mut sm, "blue");

        let found = sm.get_topic_by_name("blue").unwrap();
        assert_eq!(found.id, id);
        assert!(sm.get_topic_by_name("red").is_none());
    }

    // --- SealSegment ---

    #[test]
    fn seal_segment_creates_next() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        sm.apply_seal_segment(tid, RangeId(0), SegmentId(0), 2000, replica_set())
            .unwrap();

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.active_segment, Some(SegmentId(1)));
        assert_eq!(range.segments.len(), 2);
    }

    #[test]
    fn seal_segment_sets_end_offset() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        sm.apply_seal_segment(tid, RangeId(0), SegmentId(0), 2000, replica_set())
            .unwrap();

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        let sealed = &range.segments[&SegmentId(0)];
        assert_eq!(sealed.state, SegmentState::Sealed);
        assert_eq!(sealed.end_offset, Some(0));
        assert_eq!(sealed.sealed_at, Some(2000));
    }

    #[test]
    fn seal_segment_increments_segment_id() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        sm.apply_seal_segment(tid, RangeId(0), SegmentId(0), 2000, replica_set())
            .unwrap();
        sm.apply_seal_segment(tid, RangeId(0), SegmentId(1), 3000, replica_set())
            .unwrap();

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.active_segment, Some(SegmentId(2)));
        assert_eq!(range.segments.len(), 3);
        assert_eq!(range.next_segment_id, 3);
    }

    #[test]
    fn seal_segment_bad_topic() {
        let mut sm = MetadataStateMachine::default();
        let result =
            sm.apply_seal_segment(TopicId(99), RangeId(0), SegmentId(0), 2000, replica_set());
        assert_eq!(result, Err(MetadataError::TopicNotFound(TopicId(99))));
    }

    #[test]
    fn seal_segment_bad_range() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let result = sm.apply_seal_segment(tid, RangeId(99), SegmentId(0), 2000, replica_set());
        assert_eq!(result, Err(MetadataError::RangeNotFound(tid, RangeId(99))));
    }

    #[test]
    fn seal_segment_wrong_active_segment() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let result = sm.apply_seal_segment(tid, RangeId(0), SegmentId(99), 2000, replica_set());
        assert_eq!(
            result,
            Err(MetadataError::SegmentNotActive(
                tid,
                RangeId(0),
                SegmentId(99)
            ))
        );
    }

    // --- SplitRange ---

    #[test]
    fn split_range_basic() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = sm
            .apply_split_range(
                tid,
                RangeId(0),
                vec![0x80],
                2000,
                replica_set(),
                replica_set(),
            )
            .unwrap();

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.ranges.len(), 3); // parent + 2 children

        let child1 = &topic.ranges[&c1];
        assert_eq!(child1.keyspace_start, KEYSPACE_MIN);
        assert_eq!(child1.keyspace_end, vec![0x80]);
        assert_eq!(child1.state, RangeState::Active);

        let child2 = &topic.ranges[&c2];
        assert_eq!(child2.keyspace_start, vec![0x80]);
        assert_eq!(child2.keyspace_end, KEYSPACE_MAX);
        assert_eq!(child2.state, RangeState::Active);
    }

    #[test]
    fn split_range_updates_active_ranges() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = sm
            .apply_split_range(
                tid,
                RangeId(0),
                vec![0x80],
                2000,
                replica_set(),
                replica_set(),
            )
            .unwrap();

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.active_ranges, vec![c1, c2]);
        assert!(!topic.active_ranges.contains(&RangeId(0)));
    }

    #[test]
    fn split_range_parent_sealed() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        sm.apply_split_range(
            tid,
            RangeId(0),
            vec![0x80],
            2000,
            replica_set(),
            replica_set(),
        )
        .unwrap();

        let parent = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(parent.state, RangeState::Sealed);
        assert_eq!(parent.active_segment, None);

        // Parent's segment should be sealed
        let seg = &parent.segments[&SegmentId(0)];
        assert_eq!(seg.state, SegmentState::Sealed);
    }

    #[test]
    fn split_range_children_have_segments() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = sm
            .apply_split_range(
                tid,
                RangeId(0),
                vec![0x80],
                2000,
                replica_set(),
                replica_set(),
            )
            .unwrap();

        let topic = sm.get_topic(&tid).unwrap();

        for child_id in [c1, c2] {
            let child = &topic.ranges[&child_id];
            assert_eq!(child.active_segment, Some(SegmentId(0)));
            assert_eq!(child.segments.len(), 1);
            let seg = &child.segments[&SegmentId(0)];
            assert_eq!(seg.state, SegmentState::Active);
            assert_eq!(seg.start_offset, 0);
        }
    }

    #[test]
    fn split_range_lineage() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = sm
            .apply_split_range(
                tid,
                RangeId(0),
                vec![0x80],
                2000,
                replica_set(),
                replica_set(),
            )
            .unwrap();

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.ranges[&RangeId(0)].split_into, Some([c1, c2]));
    }

    #[test]
    fn split_range_fixed_rejected() {
        let mut sm = MetadataStateMachine::default();
        let tid = sm
            .create_topic("ordered".to_string(), fixed_policy(), replica_set(), 1000)
            .unwrap();

        let result = sm.apply_split_range(
            tid,
            RangeId(0),
            vec![0x80],
            2000,
            replica_set(),
            replica_set(),
        );
        assert_eq!(result, Err(MetadataError::SplitNotAllowed(tid)));
    }

    #[test]
    fn split_range_invalid_split_point() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        // Split point at or beyond keyspace_end
        let result = sm.apply_split_range(
            tid,
            RangeId(0),
            vec![0xFF],
            2000,
            replica_set(),
            replica_set(),
        );
        assert_eq!(result, Err(MetadataError::InvalidSplitPoint));

        // Split point at keyspace_start (empty vec)
        let result =
            sm.apply_split_range(tid, RangeId(0), vec![], 2000, replica_set(), replica_set());
        assert_eq!(result, Err(MetadataError::InvalidSplitPoint));
    }

    #[test]
    fn split_range_keyspace_coverage() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = sm
            .apply_split_range(
                tid,
                RangeId(0),
                vec![0x80],
                2000,
                replica_set(),
                replica_set(),
            )
            .unwrap();

        let topic = sm.get_topic(&tid).unwrap();
        let r1 = &topic.ranges[&c1];
        let r2 = &topic.ranges[&c2];

        // Child1.end == Child2.start (no gap)
        assert_eq!(r1.keyspace_end, r2.keyspace_start);
        // Union covers full keyspace
        assert_eq!(r1.keyspace_start, KEYSPACE_MIN);
        assert_eq!(r2.keyspace_end, KEYSPACE_MAX);
    }

    // --- MergeRange ---

    #[test]
    fn merge_range_basic() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = sm
            .apply_split_range(
                tid,
                RangeId(0),
                vec![0x80],
                2000,
                replica_set(),
                replica_set(),
            )
            .unwrap();

        let merged_id = sm
            .apply_merge_range(tid, c1, c2, 3000, replica_set())
            .unwrap();

        let topic = sm.get_topic(&tid).unwrap();
        let merged = &topic.ranges[&merged_id];
        assert_eq!(merged.keyspace_start, KEYSPACE_MIN);
        assert_eq!(merged.keyspace_end, KEYSPACE_MAX);
        assert_eq!(merged.state, RangeState::Active);
    }

    #[test]
    fn merge_range_active_ranges_updated() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = sm
            .apply_split_range(
                tid,
                RangeId(0),
                vec![0x80],
                2000,
                replica_set(),
                replica_set(),
            )
            .unwrap();

        let merged_id = sm
            .apply_merge_range(tid, c1, c2, 3000, replica_set())
            .unwrap();

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.active_ranges, vec![merged_id]);
    }

    #[test]
    fn merge_range_lineage() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = sm
            .apply_split_range(
                tid,
                RangeId(0),
                vec![0x80],
                2000,
                replica_set(),
                replica_set(),
            )
            .unwrap();

        let merged_id = sm
            .apply_merge_range(tid, c1, c2, 3000, replica_set())
            .unwrap();

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.ranges[&c1].merged_into, Some(merged_id));
        assert_eq!(topic.ranges[&c2].merged_into, Some(merged_id));
        assert_eq!(topic.ranges[&merged_id].merged_from, Some([c1, c2]));
    }

    #[test]
    fn merge_range_non_adjacent_rejected() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        // Split into 3 ranges: [min,0x40), [0x40,0x80), [0x80,max)
        let (c1, c2) = sm
            .apply_split_range(
                tid,
                RangeId(0),
                vec![0x80],
                2000,
                replica_set(),
                replica_set(),
            )
            .unwrap();

        let (c1a, _c1b) = sm
            .apply_split_range(tid, c1, vec![0x40], 3000, replica_set(), replica_set())
            .unwrap();

        // c1a=[min,0x40) and c2=[0x80,max) are NOT adjacent
        let result = sm.apply_merge_range(tid, c1a, c2, 4000, replica_set());
        assert_eq!(result, Err(MetadataError::RangesNotAdjacent(c1a, c2)));
    }

    #[test]
    fn merge_range_seals_sources() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = sm
            .apply_split_range(
                tid,
                RangeId(0),
                vec![0x80],
                2000,
                replica_set(),
                replica_set(),
            )
            .unwrap();

        sm.apply_merge_range(tid, c1, c2, 3000, replica_set())
            .unwrap();

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.ranges[&c1].state, RangeState::Sealed);
        assert_eq!(topic.ranges[&c2].state, RangeState::Sealed);
        assert_eq!(topic.ranges[&c1].active_segment, None);
        assert_eq!(topic.ranges[&c2].active_segment, None);
    }

    // --- DeleteTopic ---

    #[test]
    fn delete_topic_cascades() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        sm.apply_delete_topic(tid).unwrap();

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.state, TopicState::Deleted);
        assert!(topic.active_ranges.is_empty());

        for range in topic.ranges.values() {
            assert_eq!(range.state, RangeState::Deleting);
            for seg in range.segments.values() {
                assert_eq!(seg.state, SegmentState::Deleting);
            }
        }
    }

    #[test]
    fn delete_topic_removes_name_index() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        sm.apply_delete_topic(tid).unwrap();

        assert!(sm.get_topic_by_name("blue").is_none());
        // Topic still in map (for GC)
        assert!(sm.get_topic(&tid).is_some());
    }

    #[test]
    fn delete_topic_nonexistent() {
        let mut sm = MetadataStateMachine::default();
        let result = sm.apply_delete_topic(TopicId(99));
        assert_eq!(result, Err(MetadataError::TopicNotFound(TopicId(99))));
    }

    // --- Integration ---

    #[test]
    fn create_split_seal() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, _c2) = sm
            .apply_split_range(
                tid,
                RangeId(0),
                vec![0x80],
                2000,
                replica_set(),
                replica_set(),
            )
            .unwrap();

        // Seal a segment in child range
        sm.apply_seal_segment(tid, c1, SegmentId(0), 3000, replica_set())
            .unwrap();

        let child = &sm.get_topic(&tid).unwrap().ranges[&c1];
        assert_eq!(child.active_segment, Some(SegmentId(1)));
        assert_eq!(child.segments.len(), 2);
    }

    #[test]
    fn split_merge_roundtrip() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = sm
            .apply_split_range(
                tid,
                RangeId(0),
                vec![0x80],
                2000,
                replica_set(),
                replica_set(),
            )
            .unwrap();

        let merged = sm
            .apply_merge_range(tid, c1, c2, 3000, replica_set())
            .unwrap();

        let topic = sm.get_topic(&tid).unwrap();
        let range = &topic.ranges[&merged];
        assert_eq!(range.keyspace_start, KEYSPACE_MIN);
        assert_eq!(range.keyspace_end, KEYSPACE_MAX);
        assert_eq!(topic.active_ranges, vec![merged]);
    }

    #[test]
    fn multiple_topics_independent() {
        let mut sm = MetadataStateMachine::default();
        let t1 = create_topic(&mut sm, "alpha");
        let t2 = create_topic(&mut sm, "beta");

        // Split alpha, leave beta alone
        sm.apply_split_range(
            t1,
            RangeId(0),
            vec![0x80],
            2000,
            replica_set(),
            replica_set(),
        )
        .unwrap();

        let alpha = sm.get_topic(&t1).unwrap();
        assert_eq!(alpha.active_ranges.len(), 2);

        let beta = sm.get_topic(&t2).unwrap();
        assert_eq!(beta.active_ranges.len(), 1);
        assert_eq!(beta.ranges.len(), 1);
    }
}
