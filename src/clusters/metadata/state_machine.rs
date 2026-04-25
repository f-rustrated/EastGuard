use super::command::*;
use super::types::*;
use crate::clusters::metadata::{
    RangeId, SegmentId, TopicId, error::MetadataError, strategy::PartitionStrategy,
};

use MetadataError::*;
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

    pub(crate) fn get_topic_by_name(&self, name: &str) -> Option<&TopicMeta> {
        self.topic_name_index
            .get(name)
            .and_then(|id| self.topics.get(id))
    }

    pub(crate) fn topic_count(&self) -> usize {
        self.topics.len()
    }

    pub(crate) fn apply(&mut self, command: MetadataCommand) -> Result<ApplyResult, MetadataError> {
        use MetadataCommand::*;
        match command {
            CreateTopic(cmd) => self.create_topic(cmd).map(ApplyResult::TopicCreated),
            SealSegment(cmd) => self.seal_segment(cmd).map(|()| ApplyResult::SegmentSealed),
            SplitRange(cmd) => self
                .split_range(cmd)
                .map(|(c1, c2)| ApplyResult::RangeSplit(c1, c2)),
            MergeRange(cmd) => self.merge_range(cmd).map(ApplyResult::RangeMerged),
            DeleteTopic(cmd) => self.delete_topic(cmd).map(|()| ApplyResult::TopicDeleted),
        }
    }

    fn create_topic(&mut self, cmd: CreateTopic) -> Result<TopicId, MetadataError> {
        if self.topic_name_index.contains_key(&cmd.name) {
            return Err(TopicNameAlreadyExists(cmd.name));
        }
        let topic_id = TopicId(self.next_topic_id);
        let topic = TopicMeta::new(
            cmd.name,
            topic_id,
            cmd.replica_set,
            cmd.created_at,
            cmd.storage_policy,
        );
        self.topic_name_index.insert(topic.name.clone(), topic_id);
        self.topics.insert(topic.id, topic);
        self.next_topic_id += 1;
        Ok(topic_id)
    }

    fn seal_segment(&mut self, cmd: SealSegment) -> Result<(), MetadataError> {
        let topic = self
            .topics
            .get_mut(&cmd.topic_id)
            .ok_or(TopicNotFound(cmd.topic_id))?;
        if topic.state != TopicState::Active {
            return Err(TopicNotActive(cmd.topic_id));
        }
        let range = topic.ranges.get_mut(&cmd.range_id).ok_or(RangeNotFound)?;

        if range.state != RangeState::Active {
            return Err(RangeNotActive);
        }
        if range.active_segment != Some(cmd.segment_id) {
            return Err(SegmentNotActive);
        }
        let segment = range
            .segments
            .get_mut(&cmd.segment_id)
            .ok_or(SegmentNotFound)?;

        segment.seal(range.next_offset.saturating_sub(1), cmd.sealed_at)?;

        let new_segment_id = SegmentId(range.next_segment_id);
        range.next_segment_id += 1;
        let new_segment = SegmentMeta {
            segment_id: new_segment_id,
            state: SegmentState::Active,
            replica_set: cmd.new_replica_set,
            size_bytes: 0,
            start_offset: range.next_offset,
            end_offset: None,
            created_at: cmd.sealed_at,
            sealed_at: None,
        };
        range.segments.insert(new_segment_id, new_segment);
        range.active_segment = Some(new_segment_id);
        Ok(())
    }

    fn split_range(&mut self, cmd: SplitRange) -> Result<(RangeId, RangeId), MetadataError> {
        let topic = self
            .topics
            .get_mut(&cmd.topic_id)
            .ok_or(TopicNotFound(cmd.topic_id))?;
        if topic.state != TopicState::Active {
            return Err(TopicNotActive(cmd.topic_id));
        }
        if topic.storage_policy.partition_strategy == PartitionStrategy::Fixed {
            return Err(SplitNotAllowed(cmd.topic_id));
        }
        let range = topic.ranges.get_mut(&cmd.range_id).ok_or(RangeNotFound)?;
        if range.state != RangeState::Active {
            return Err(RangeNotActive);
        }
        if cmd.split_point <= range.keyspace_start || cmd.split_point >= range.keyspace_end {
            return Err(InvalidSplitPoint);
        }

        if let Some(seg_id) = range.active_segment
            && let Some(seg) = range.segments.get_mut(&seg_id)
        {
            seg.state = SegmentState::Sealed;
            seg.end_offset = Some(range.next_offset.saturating_sub(1));
            seg.sealed_at = Some(cmd.created_at);
        }

        let parent_start = range.keyspace_start.clone();
        let parent_end = range.keyspace_end.clone();
        range.state = RangeState::Sealed;
        range.active_segment = None;

        let child1_id = RangeId(topic.next_range_id);
        topic.next_range_id += 1;
        let child2_id = RangeId(topic.next_range_id);
        topic.next_range_id += 1;

        topic.ranges.get_mut(&cmd.range_id).unwrap().split_into = Some([child1_id, child2_id]);

        let child1 = RangeMeta::new(
            child1_id,
            parent_start,
            cmd.split_point.clone(),
            cmd.child1_replica_set,
            cmd.created_at,
        );
        let child2 = RangeMeta::new(
            child2_id,
            cmd.split_point,
            parent_end,
            cmd.child2_replica_set,
            cmd.created_at,
        );
        topic.ranges.insert(child1_id, child1);
        topic.ranges.insert(child2_id, child2);

        topic.active_ranges.retain(|id| *id != cmd.range_id);
        topic.insert_range_sorted(child1_id);
        topic.insert_range_sorted(child2_id);
        Ok((child1_id, child2_id))
    }

    fn merge_range(&mut self, cmd: MergeRange) -> Result<RangeId, MetadataError> {
        let topic = self
            .topics
            .get_mut(&cmd.topic_id)
            .ok_or(TopicNotFound(cmd.topic_id))?;
        if topic.state != TopicState::Active {
            return Err(TopicNotActive(cmd.topic_id));
        }
        {
            let r1 = topic.ranges.get(&cmd.range_id_1).ok_or(RangeNotFound)?;
            let r2 = topic.ranges.get(&cmd.range_id_2).ok_or(RangeNotFound)?;
            if r1.state != RangeState::Active {
                return Err(RangeNotActive);
            }
            if r2.state != RangeState::Active {
                return Err(RangeNotActive);
            }
            let adjacent =
                r1.keyspace_end == r2.keyspace_start || r2.keyspace_end == r1.keyspace_start;
            if !adjacent {
                return Err(RangesNotAdjacent);
            }
        }

        let (lower_id, upper_id) = {
            let r1 = &topic.ranges[&cmd.range_id_1];
            let r2 = &topic.ranges[&cmd.range_id_2];
            if r1.keyspace_start <= r2.keyspace_start {
                (cmd.range_id_1, cmd.range_id_2)
            } else {
                (cmd.range_id_2, cmd.range_id_1)
            }
        };

        let merged_start = topic.ranges[&lower_id].keyspace_start.clone();
        let merged_end = topic.ranges[&upper_id].keyspace_end.clone();

        topic.seal_range(lower_id, cmd.created_at);
        topic.seal_range(upper_id, cmd.created_at);

        let merged_id = RangeId(topic.next_range_id);
        topic.next_range_id += 1;
        topic.ranges.get_mut(&cmd.range_id_1).unwrap().merged_into = Some(merged_id);
        topic.ranges.get_mut(&cmd.range_id_2).unwrap().merged_into = Some(merged_id);

        let mut merged = RangeMeta::new(
            merged_id,
            merged_start,
            merged_end,
            cmd.merged_replica_set,
            cmd.created_at,
        );
        merged.merged_from = Some([cmd.range_id_1, cmd.range_id_2]);
        topic.ranges.insert(merged_id, merged);

        topic
            .active_ranges
            .retain(|id| *id != cmd.range_id_1 && *id != cmd.range_id_2);
        topic.insert_range_sorted(merged_id);

        Ok(merged_id)
    }

    fn delete_topic(&mut self, cmd: DeleteTopic) -> Result<(), MetadataError> {
        let topic = self
            .topics
            .get_mut(&cmd.topic_id)
            .ok_or(TopicNotFound(cmd.topic_id))?;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clusters::{
        NodeId,
        metadata::strategy::{PartitionStrategy, StoragePolicy},
    };

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
        let result = sm.apply(MetadataCommand::CreateTopic(CreateTopic {
            name: name.to_string(),
            storage_policy: default_policy(),
            replica_set: replica_set(),
            created_at: 1000,
        }));
        match result.unwrap() {
            ApplyResult::TopicCreated(id) => id,
            other => panic!("expected TopicCreated, got {:?}", other),
        }
    }

    fn seal_segment(
        sm: &mut MetadataStateMachine,
        topic_id: TopicId,
        range_id: RangeId,
        segment_id: SegmentId,
        sealed_at: u64,
    ) {
        let result = sm.apply(MetadataCommand::SealSegment(SealSegment {
            topic_id,
            range_id,
            segment_id,
            sealed_at,
            new_replica_set: replica_set(),
        }));
        assert_eq!(result.unwrap(), ApplyResult::SegmentSealed);
    }

    fn split_range(
        sm: &mut MetadataStateMachine,
        topic_id: TopicId,
        range_id: RangeId,
        split_point: Vec<u8>,
        created_at: u64,
    ) -> (RangeId, RangeId) {
        let result = sm.apply(MetadataCommand::SplitRange(SplitRange {
            topic_id,
            range_id,
            split_point,
            created_at,
            child1_replica_set: replica_set(),
            child2_replica_set: replica_set(),
        }));
        match result.unwrap() {
            ApplyResult::RangeSplit(c1, c2) => (c1, c2),
            other => panic!("expected RangeSplit, got {:?}", other),
        }
    }

    fn merge_range(
        sm: &mut MetadataStateMachine,
        topic_id: TopicId,
        range_id_1: RangeId,
        range_id_2: RangeId,
        created_at: u64,
    ) -> RangeId {
        let result = sm.apply(MetadataCommand::MergeRange(MergeRange {
            topic_id,
            range_id_1,
            range_id_2,
            created_at,
            merged_replica_set: replica_set(),
        }));
        match result.unwrap() {
            ApplyResult::RangeMerged(id) => id,
            other => panic!("expected RangeMerged, got {:?}", other),
        }
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

        let result = sm.apply(MetadataCommand::CreateTopic(CreateTopic {
            name: "blue".to_string(),
            storage_policy: default_policy(),
            replica_set: replica_set(),
            created_at: 2000,
        }));
        assert_eq!(result, Err(TopicNameAlreadyExists("blue".to_string())));
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

        seal_segment(&mut sm, tid, RangeId(0), SegmentId(0), 2000);

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.active_segment, Some(SegmentId(1)));
        assert_eq!(range.segments.len(), 2);
    }

    #[test]
    fn seal_segment_sets_end_offset() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        seal_segment(&mut sm, tid, RangeId(0), SegmentId(0), 2000);

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

        seal_segment(&mut sm, tid, RangeId(0), SegmentId(0), 2000);
        seal_segment(&mut sm, tid, RangeId(0), SegmentId(1), 3000);

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.active_segment, Some(SegmentId(2)));
        assert_eq!(range.segments.len(), 3);
        assert_eq!(range.next_segment_id, 3);
    }

    #[test]
    fn seal_segment_bad_topic() {
        let mut sm = MetadataStateMachine::default();
        let result = sm.apply(MetadataCommand::SealSegment(SealSegment {
            topic_id: TopicId(99),
            range_id: RangeId(0),
            segment_id: SegmentId(0),
            sealed_at: 2000,
            new_replica_set: replica_set(),
        }));
        assert_eq!(result, Err(TopicNotFound(TopicId(99))));
    }

    #[test]
    fn seal_segment_bad_range() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let result = sm.apply(MetadataCommand::SealSegment(SealSegment {
            topic_id: tid,
            range_id: RangeId(99),
            segment_id: SegmentId(0),
            sealed_at: 2000,
            new_replica_set: replica_set(),
        }));
        assert_eq!(result, Err(RangeNotFound));
    }

    #[test]
    fn seal_segment_wrong_active_segment() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let result = sm.apply(MetadataCommand::SealSegment(SealSegment {
            topic_id: tid,
            range_id: RangeId(0),
            segment_id: SegmentId(99),
            sealed_at: 2000,
            new_replica_set: replica_set(),
        }));
        assert_eq!(result, Err(SegmentNotActive));
    }

    // --- SplitRange ---

    #[test]
    fn split_range_basic() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.ranges.len(), 3);

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

        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.active_ranges, vec![c1, c2]);
        assert!(!topic.active_ranges.contains(&RangeId(0)));
    }

    #[test]
    fn split_range_parent_sealed() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let parent = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(parent.state, RangeState::Sealed);
        assert_eq!(parent.active_segment, None);

        let seg = &parent.segments[&SegmentId(0)];
        assert_eq!(seg.state, SegmentState::Sealed);
    }

    #[test]
    fn split_range_children_have_segments() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

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

        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.ranges[&RangeId(0)].split_into, Some([c1, c2]));
    }

    #[test]
    fn split_range_fixed_rejected() {
        let mut sm = MetadataStateMachine::default();
        let result = sm.apply(MetadataCommand::CreateTopic(CreateTopic {
            name: "ordered".to_string(),
            storage_policy: fixed_policy(),
            replica_set: replica_set(),
            created_at: 1000,
        }));
        let tid = match result.unwrap() {
            ApplyResult::TopicCreated(id) => id,
            other => panic!("expected TopicCreated, got {:?}", other),
        };

        let result = sm.apply(MetadataCommand::SplitRange(SplitRange {
            topic_id: tid,
            range_id: RangeId(0),
            split_point: vec![0x80],
            created_at: 2000,
            child1_replica_set: replica_set(),
            child2_replica_set: replica_set(),
        }));
        assert_eq!(result, Err(SplitNotAllowed(tid)));
    }

    #[test]
    fn split_range_invalid_split_point() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let result = sm.apply(MetadataCommand::SplitRange(SplitRange {
            topic_id: tid,
            range_id: RangeId(0),
            split_point: vec![0xFF],
            created_at: 2000,
            child1_replica_set: replica_set(),
            child2_replica_set: replica_set(),
        }));
        assert_eq!(result, Err(InvalidSplitPoint));

        let result = sm.apply(MetadataCommand::SplitRange(SplitRange {
            topic_id: tid,
            range_id: RangeId(0),
            split_point: vec![],
            created_at: 2000,
            child1_replica_set: replica_set(),
            child2_replica_set: replica_set(),
        }));
        assert_eq!(result, Err(InvalidSplitPoint));
    }

    #[test]
    fn split_range_keyspace_coverage() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let topic = sm.get_topic(&tid).unwrap();
        let r1 = &topic.ranges[&c1];
        let r2 = &topic.ranges[&c2];

        assert_eq!(r1.keyspace_end, r2.keyspace_start);
        assert_eq!(r1.keyspace_start, KEYSPACE_MIN);
        assert_eq!(r2.keyspace_end, KEYSPACE_MAX);
    }

    // --- MergeRange ---

    #[test]
    fn merge_range_basic() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");
        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let merged_id = merge_range(&mut sm, tid, c1, c2, 3000);

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
        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let merged_id = merge_range(&mut sm, tid, c1, c2, 3000);

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.active_ranges, vec![merged_id]);
    }

    #[test]
    fn merge_range_lineage() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");
        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let merged_id = merge_range(&mut sm, tid, c1, c2, 3000);

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.ranges[&c1].merged_into, Some(merged_id));
        assert_eq!(topic.ranges[&c2].merged_into, Some(merged_id));
        assert_eq!(topic.ranges[&merged_id].merged_from, Some([c1, c2]));
    }

    #[test]
    fn merge_range_non_adjacent_rejected() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");
        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);
        let (c1a, _c1b) = split_range(&mut sm, tid, c1, vec![0x40], 3000);

        let result = sm.apply(MetadataCommand::MergeRange(MergeRange {
            topic_id: tid,
            range_id_1: c1a,
            range_id_2: c2,
            created_at: 4000,
            merged_replica_set: replica_set(),
        }));
        assert_eq!(result, Err(RangesNotAdjacent));
    }

    #[test]
    fn merge_range_seals_sources() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");
        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        merge_range(&mut sm, tid, c1, c2, 3000);

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

        sm.apply(MetadataCommand::DeleteTopic(DeleteTopic { topic_id: tid }))
            .unwrap();

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

        sm.apply(MetadataCommand::DeleteTopic(DeleteTopic { topic_id: tid }))
            .unwrap();

        assert!(sm.get_topic_by_name("blue").is_none());
        assert!(sm.get_topic(&tid).is_some());
    }

    #[test]
    fn delete_topic_nonexistent() {
        let mut sm = MetadataStateMachine::default();
        let result = sm.apply(MetadataCommand::DeleteTopic(DeleteTopic {
            topic_id: TopicId(99),
        }));
        assert_eq!(result, Err(TopicNotFound(TopicId(99))));
    }

    // --- Integration ---

    #[test]
    fn create_split_seal() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");
        let (c1, _c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        seal_segment(&mut sm, tid, c1, SegmentId(0), 3000);

        let child = &sm.get_topic(&tid).unwrap().ranges[&c1];
        assert_eq!(child.active_segment, Some(SegmentId(1)));
        assert_eq!(child.segments.len(), 2);
    }

    #[test]
    fn split_merge_roundtrip() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");
        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let merged = merge_range(&mut sm, tid, c1, c2, 3000);

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

        split_range(&mut sm, t1, RangeId(0), vec![0x80], 2000);

        let alpha = sm.get_topic(&t1).unwrap();
        assert_eq!(alpha.active_ranges.len(), 2);

        let beta = sm.get_topic(&t2).unwrap();
        assert_eq!(beta.active_ranges.len(), 1);
        assert_eq!(beta.ranges.len(), 1);
    }
}
