use super::command::*;
use super::types::*;
use crate::clusters::metadata::{RangeId, TopicId, error::MetadataError};

use MetadataError::*;
use std::collections::HashMap;

#[derive(Default)]
pub struct MetadataStateMachine {
    topics: HashMap<TopicId, TopicMeta>,
    topic_name_index: HashMap<String, TopicId>,
    next_topic_id: u64,
    pending_proposals: Vec<MetadataCommand>,
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

    pub(crate) fn take_pending_proposals(&mut self) -> Vec<MetadataCommand> {
        std::mem::take(&mut self.pending_proposals)
    }

    pub(crate) fn apply(&mut self, command: MetadataCommand) -> Result<ApplyResult, MetadataError> {
        use MetadataCommand::*;
        let result = match command {
            CreateTopic(cmd) => self.create_topic(cmd).map(ApplyResult::TopicCreated),
            RollSegment(cmd) => self.roll_segment(cmd).map(|()| ApplyResult::SegmentRolled),
            SplitRange(cmd) => self
                .split_range(cmd)
                .map(|(c1, c2)| ApplyResult::RangeSplit(c1, c2)),
            MergeRange(cmd) => self.merge_range(cmd).map(ApplyResult::RangeMerged),
            DeleteTopic(cmd) => self.delete_topic(cmd).map(|()| ApplyResult::TopicDeleted),
        };
        #[cfg(test)]
        self.assert_invariants();
        result
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

    fn roll_segment(&mut self, cmd: RollSegment) -> Result<(), MetadataError> {
        let topic = self
            .topics
            .get_mut(&cmd.topic_id)
            .ok_or(TopicNotFound(cmd.topic_id))?;
        topic.validate_active()?;
        let can_split = topic.can_split();

        let range = topic.ranges.get_mut(&cmd.range_id).ok_or(RangeNotFound)?;
        let active_seg_id = range.validate_active()?;
        if active_seg_id != cmd.segment_id {
            return Ok(());
        }

        let should_split =
            range.roll_segment(cmd.segment_id, cmd.new_replica_set.clone(), cmd.sealed_at)?;

        if can_split && should_split {
            // The following guards against ranges too narrow to split. When keyspace_start and keyspace_end are 1 unit apart (e.g., [0x40] and [0x41]),
            // integer division truncates: (0x40 + 0x41) / 2 = 0x40, which equals keyspace_start.
            // That fails valid_split_point (requires strictly between), so the split is skipped.
            let mid = range.compute_midpoint();
            if range.valid_split_point(&mid) {
                self.pending_proposals
                    .push(MetadataCommand::SplitRange(SplitRange {
                        topic_id: cmd.topic_id,
                        range_id: cmd.range_id,
                        split_point: mid,
                        created_at: cmd.sealed_at,
                        left_replica_set: cmd.new_replica_set.clone(),
                        right_replica_set: cmd.new_replica_set,
                    }));
            }
        }
        Ok(())
    }

    fn split_range(&mut self, cmd: SplitRange) -> Result<(RangeId, RangeId), MetadataError> {
        let topic = self
            .topics
            .get_mut(&cmd.topic_id)
            .ok_or(TopicNotFound(cmd.topic_id))?;
        topic.validate_active()?;
        if !topic.can_split() {
            return Err(SplitNotAllowed(cmd.topic_id));
        }
        let left_id = RangeId(topic.next_range_id);
        let right_id = RangeId(topic.next_range_id + 1);
        let range = topic.ranges.get_mut(&cmd.range_id).ok_or(RangeNotFound)?;
        let (left, right) = range.split(cmd, left_id, right_id)?;

        topic.next_range_id += 2;
        topic.active_ranges.retain(|id| *id != range.range_id);
        topic.ranges.insert(left_id, left);
        topic.ranges.insert(right_id, right);
        topic.insert_range_sorted(left_id);
        topic.insert_range_sorted(right_id);
        Ok((left_id, right_id))
    }

    fn merge_range(&mut self, cmd: MergeRange) -> Result<RangeId, MetadataError> {
        let topic = self
            .topics
            .get_mut(&cmd.topic_id)
            .ok_or(TopicNotFound(cmd.topic_id))?;

        topic.validate_active()?;
        let merged_id = RangeId(topic.next_range_id);
        let [r1, r2] = topic.get_ranges_mut([&cmd.range_id_1, &cmd.range_id_2])?;
        let merged = r1.merge(r2, merged_id, cmd.merged_replica_set, cmd.created_at)?;
        topic.ranges.insert(merged_id, merged);
        topic
            .active_ranges
            .retain(|id| *id != cmd.range_id_1 && *id != cmd.range_id_2);
        topic.insert_range_sorted(merged_id);
        topic.next_range_id += 1;

        Ok(merged_id)
    }

    // ! SAFETY: When the loop evaluates the [1, 2] pair and decides it is mergeable,
    // ! it generates the event and immediately stops looking at the rest of that topic's ranges
    // !
    // ! However, we still have an asynchronous race condition to worry about.
    // ! Take the following example:
    // !    1. the following method proposes merging (1, 2).
    // !    2. The MergeRange command goes into a queue (or a Raft log) to be processed.
    // !    3. Before the command is executed, range 2 receives a massive burst of traffic and splits into 2A and 2B.
    // !    4. The MergeRange(1, 2) command is finally executed by your merge function.
    // ! By the time the command executes, range 2 might be split, already sealed, or completely deleted
    pub(crate) fn evaluate_merges(&self, now: u64) -> Vec<MetadataCommand> {
        let mut proposals = Vec::new();

        for topic in self.topics.values() {
            if topic.validate_active().is_err() {
                continue;
            }
            if !topic.can_split() {
                continue;
            }
            if topic.active_ranges.len() < 2 {
                continue;
            }

            for pair in topic.active_ranges.windows(2) {
                let (r1, r2) = (&topic.ranges[&pair[0]], &topic.ranges[&pair[1]]);
                if r1.state != RangeState::Active || r2.state != RangeState::Active {
                    continue;
                }

                if r1.mergeable_with(r2, now) {
                    let replica_set = r1
                        .active_segment
                        .and_then(|sid| r1.segments.get(&sid))
                        // ? why taking replicaset from r1 - what about r2?
                        .map(|s| s.replica_set.clone())
                        .unwrap_or_default();

                    proposals.push(MetadataCommand::MergeRange(MergeRange {
                        topic_id: topic.id,
                        range_id_1: r1.range_id,
                        range_id_2: r2.range_id,
                        created_at: now,
                        merged_replica_set: replica_set,
                    }));

                    break;
                }
            }
        }

        proposals
    }

    fn delete_topic(&mut self, cmd: DeleteTopic) -> Result<(), MetadataError> {
        let topic_id = self
            .topic_name_index
            .get(&cmd.name)
            .copied()
            .ok_or(MetadataError::TopicNameNotFound(cmd.name.clone()))?;

        // Safety: topic_name_index and topics are always in sync —
        // see invariant below.
        self.topics.get_mut(&topic_id).unwrap().delete();
        self.topic_name_index.remove(&cmd.name);

        Ok(())
    }

    #[cfg(test)]
    fn assert_invariants(&self) {
        // Invariant 12: topic_name_index and topics always in sync
        assert_eq!(
            self.topic_name_index.len(),
            self.topics
                .values()
                .filter(|t| t.state != TopicState::Deleted)
                .count(),
            "topic_name_index out of sync with non-deleted topics"
        );
        for (name, id) in &self.topic_name_index {
            let topic = self
                .topics
                .get(id)
                .expect("name index points to missing topic");
            assert_eq!(&topic.name, name);
        }

        // Invariant 6: ID monotonicity
        for id in self.topics.keys() {
            assert!(id.0 < self.next_topic_id, "topic ID >= next_topic_id");
        }

        for topic in self.topics.values() {
            // Invariant 6: range ID monotonicity
            for rid in topic.ranges.keys() {
                assert!(rid.0 < topic.next_range_id, "range ID >= next_range_id");
            }

            if topic.state == TopicState::Active {
                // Invariant 1: keyspace coverage — active ranges cover full keyspace
                if !topic.active_ranges.is_empty() {
                    let ranges: Vec<&RangeMeta> = topic
                        .active_ranges
                        .iter()
                        .map(|id| &topic.ranges[id])
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

            for range in topic.ranges.values() {
                match range.state {
                    RangeState::Active => {
                        // Invariant 2: single active segment per active range
                        assert!(
                            range.active_segment.is_some(),
                            "active range missing active_segment"
                        );
                        let seg_id = range.active_segment.unwrap();
                        let seg = range
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
                            range.active_segment.is_none(),
                            "sealed/deleting range has active_segment"
                        );
                    }
                }

                // Invariant 6: segment ID monotonicity
                for sid in range.segments.keys() {
                    assert!(
                        sid.0 < range.next_segment_id,
                        "segment ID >= next_segment_id"
                    );
                }

                // Invariant 3: sealed segments have end_offset set
                for seg in range.segments.values() {
                    if seg.state == SegmentState::Sealed {
                        assert!(
                            seg.end_offset.is_some(),
                            "sealed segment missing end_offset"
                        );
                        assert!(seg.sealed_at.is_some(), "sealed segment missing sealed_at");
                    }
                }

                // Invariant 4: lineage consistency — cannot be both split and merged
                assert!(
                    range.split_into.is_none() || range.merged_into.is_none(),
                    "range both split and merged"
                );

                // Invariant 16: seal_history timestamps are monotonically ordered
                for w in range.seal_history.seal_timestamps.windows(2) {
                    assert!(w[0] <= w[1], "seal_history timestamps not sorted");
                }

                // Invariant 17: split children have created_by_split_at set
                if let Some([left, right]) = range.split_into {
                    if let Some(left_range) = topic.ranges.get(&left) {
                        assert!(
                            left_range.seal_history.created_by_split_at.is_some(),
                            "split child missing created_by_split_at"
                        );
                    }
                    if let Some(right_range) = topic.ranges.get(&right) {
                        assert!(
                            right_range.seal_history.created_by_split_at.is_some(),
                            "split child missing created_by_split_at"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clusters::{
        NodeId,
        metadata::{
            SegmentId,
            strategy::{PartitionStrategy, StoragePolicy},
        },
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

    fn roll_segment(
        sm: &mut MetadataStateMachine,
        topic_id: TopicId,
        range_id: RangeId,
        segment_id: SegmentId,
        sealed_at: u64,
    ) {
        let result = sm.apply(MetadataCommand::RollSegment(RollSegment {
            topic_id,
            range_id,
            segment_id,
            sealed_at,
            new_replica_set: replica_set(),
        }));
        assert_eq!(result.unwrap(), ApplyResult::SegmentRolled);
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
            left_replica_set: replica_set(),
            right_replica_set: replica_set(),
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

    // --- RollSegment ---

    #[test]
    fn roll_segment_creates_next() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 2000);

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.active_segment, Some(SegmentId(1)));
        assert_eq!(range.segments.len(), 2);
    }

    #[test]
    fn roll_segment_increments_segment_id() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 2000);
        roll_segment(&mut sm, tid, RangeId(0), SegmentId(1), 3000);

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.active_segment, Some(SegmentId(2)));
        assert_eq!(range.segments.len(), 3);
        assert_eq!(range.next_segment_id, 3);
    }

    #[test]
    fn roll_segment_bad_topic() {
        let mut sm = MetadataStateMachine::default();
        let result = sm.apply(MetadataCommand::RollSegment(RollSegment {
            topic_id: TopicId(99),
            range_id: RangeId(0),
            segment_id: SegmentId(0),
            sealed_at: 2000,
            new_replica_set: replica_set(),
        }));
        assert_eq!(result, Err(TopicNotFound(TopicId(99))));
    }

    #[test]
    fn roll_segment_bad_range() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let result = sm.apply(MetadataCommand::RollSegment(RollSegment {
            topic_id: tid,
            range_id: RangeId(99),
            segment_id: SegmentId(0),
            sealed_at: 2000,
            new_replica_set: replica_set(),
        }));
        assert_eq!(result, Err(RangeNotFound));
    }

    #[test]
    fn roll_segment_stale_is_noop() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        let result = sm.apply(MetadataCommand::RollSegment(RollSegment {
            topic_id: tid,
            range_id: RangeId(0),
            segment_id: SegmentId(99),
            sealed_at: 2000,
            new_replica_set: replica_set(),
        }));
        assert_eq!(result, Ok(ApplyResult::SegmentRolled));

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.active_segment, Some(SegmentId(0)));
        assert_eq!(range.segments.len(), 1);
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
            left_replica_set: replica_set(),
            right_replica_set: replica_set(),
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
            left_replica_set: replica_set(),
            right_replica_set: replica_set(),
        }));
        assert_eq!(result, Err(InvalidSplitPoint));

        let result = sm.apply(MetadataCommand::SplitRange(SplitRange {
            topic_id: tid,
            range_id: RangeId(0),
            split_point: vec![],
            created_at: 2000,
            left_replica_set: replica_set(),
            right_replica_set: replica_set(),
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

    // --- DeleteTopic ---

    #[test]
    fn delete_topic_cascades() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        sm.apply(MetadataCommand::DeleteTopic(DeleteTopic {
            name: "blue".into(),
        }))
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
        let _tid = create_topic(&mut sm, "blue");

        sm.apply(MetadataCommand::DeleteTopic(DeleteTopic {
            name: "blue".into(),
        }))
        .unwrap();

        assert!(sm.get_topic_by_name("blue").is_none());
    }

    #[test]
    fn delete_topic_nonexistent() {
        let mut sm = MetadataStateMachine::default();
        let result = sm.apply(MetadataCommand::DeleteTopic(DeleteTopic {
            name: "nope".into(),
        }));
        assert_eq!(result, Err(MetadataError::TopicNameNotFound("nope".into())));
    }

    // --- Integration ---

    #[test]
    fn create_split_seal() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");
        let (c1, _c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        roll_segment(&mut sm, tid, c1, SegmentId(0), 3000);

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

    // --- Invariant: Segment immutability after seal ---

    #[test]
    fn roll_already_rolled_segment_is_noop() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 2000);

        let result = sm.apply(MetadataCommand::RollSegment(RollSegment {
            topic_id: tid,
            range_id: RangeId(0),
            segment_id: SegmentId(0),
            sealed_at: 3000,
            new_replica_set: replica_set(),
        }));
        assert_eq!(result, Ok(ApplyResult::SegmentRolled));

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.active_segment, Some(SegmentId(1)));
        assert_eq!(range.segments.len(), 2);
    }

    // --- Hot Range Detection ---

    #[test]
    fn seal_history_records_timestamps() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 1000);
        roll_segment(&mut sm, tid, RangeId(0), SegmentId(1), 2000);
        roll_segment(&mut sm, tid, RangeId(0), SegmentId(2), 3000);

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.seal_history.seal_count(), 3);
    }

    #[test]
    fn seal_history_prunes_old_entries() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 1000);
        roll_segment(&mut sm, tid, RangeId(0), SegmentId(1), 2000);
        // Jump far beyond the window — both old entries pruned
        let far_future = 2000 + MEASUREMENT_WINDOW_MS + 1;
        roll_segment(&mut sm, tid, RangeId(0), SegmentId(2), far_future);

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.seal_history.seal_count(), 1);
    }

    #[test]
    fn should_split_threshold_met() {
        let mut history = RangeSealHistory::default();
        history.record_seal(1000);
        history.record_seal(2000);
        history.record_seal(3000);

        assert!(history.should_split(3000));
    }

    #[test]
    fn should_split_below_threshold() {
        let mut history = RangeSealHistory::default();
        history.record_seal(1000);
        history.record_seal(2000);

        assert!(!history.should_split(2000));
    }

    #[test]
    fn should_split_cooldown_blocks() {
        let mut history = RangeSealHistory {
            seal_timestamps: Vec::new(),
            created_by_split_at: Some(1000),
        };
        history.record_seal(1100);
        history.record_seal(1200);
        history.record_seal(1300);

        // Within cooldown — blocked
        assert!(!history.should_split(1300));

        // After cooldown — allowed
        assert!(history.should_split(1000 + SPLIT_COOLDOWN_MS));
    }

    #[test]
    fn auto_proposal_on_hot_range() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        for i in 0..SPLIT_SEAL_THRESHOLD {
            roll_segment(
                &mut sm,
                tid,
                RangeId(0),
                SegmentId(i as u64),
                1000 * (i as u64 + 1),
            );
        }

        let proposals = sm.take_pending_proposals();
        assert_eq!(proposals.len(), 1);
        assert!(matches!(proposals[0], MetadataCommand::SplitRange(_)));
    }

    #[test]
    fn no_auto_proposal_below_threshold() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        for i in 0..(SPLIT_SEAL_THRESHOLD - 1) {
            roll_segment(
                &mut sm,
                tid,
                RangeId(0),
                SegmentId(i as u64),
                1000 * (i as u64 + 1),
            );
        }

        let proposals = sm.take_pending_proposals();
        assert!(proposals.is_empty());
    }

    #[test]
    fn no_auto_proposal_for_fixed_strategy() {
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

        for i in 0..SPLIT_SEAL_THRESHOLD {
            roll_segment(
                &mut sm,
                tid,
                RangeId(0),
                SegmentId(i as u64),
                2000 * (i as u64 + 1),
            );
        }

        let proposals = sm.take_pending_proposals();
        assert!(proposals.is_empty());
    }

    #[test]
    fn evaluate_merges_cold_adjacent() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");
        split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        // Both children are cold (no seals)
        let proposals = sm.evaluate_merges(2000 + SPLIT_COOLDOWN_MS + 1);
        assert_eq!(proposals.len(), 1);
        assert!(matches!(proposals[0], MetadataCommand::MergeRange(_)));
    }

    #[test]
    fn evaluate_merges_one_hot() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");
        let (c1, _c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);
        sm.take_pending_proposals(); // discard any split proposals

        // Seal one child — makes it hot
        roll_segment(&mut sm, tid, c1, SegmentId(0), 3000);

        let proposals = sm.evaluate_merges(3000);
        assert!(proposals.is_empty());
    }

    #[test]
    fn split_clears_seal_history() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");

        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 2000);
        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 3000);

        let topic = sm.get_topic(&tid).unwrap();
        assert!(topic.ranges[&c1].seal_history.seal_timestamps.is_empty());
        assert!(topic.ranges[&c2].seal_history.seal_timestamps.is_empty());
    }

    #[test]
    fn split_sets_cooldown() {
        let mut sm = MetadataStateMachine::default();
        let tid = create_topic(&mut sm, "blue");
        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(
            topic.ranges[&c1].seal_history.created_by_split_at,
            Some(2000)
        );
        assert_eq!(
            topic.ranges[&c2].seal_history.created_by_split_at,
            Some(2000)
        );
    }
}
