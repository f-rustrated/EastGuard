use crate::control_plane::metadata::SegmentMeta;
use crate::control_plane::metadata::command::*;
use crate::control_plane::metadata::event::*;

use crate::control_plane::NodeId;
use crate::control_plane::Replicas;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::ConsumerGroupAssignment;
use crate::control_plane::metadata::topic::{TopicMeta, TopicState, TopicStats};
use crate::control_plane::metadata::{EntryId, RangeId, SegmentId, TopicId, error::MetadataError};
use crate::data_plane::SegmentKey;
#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;
use MetadataError::*;
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct MetadataStateSnapshot {
    topics: HashMap<TopicId, TopicMeta>,
    next_topic_id: u64,
}

pub struct MetadataState {
    pub(crate) topics: HashMap<TopicId, TopicMeta>,
    pub(crate) last_applied_index: u64,
    topic_name_index: HashMap<String, TopicId>,
    next_topic_id: u64,
    pending_proposals: Vec<MetadataCommand>,
    pending_events: Vec<MetadataEvent>,
}

impl MetadataState {
    pub(crate) fn new(shard_group_id: ShardGroupId) -> Self {
        MetadataState {
            topics: HashMap::new(),
            last_applied_index: 0,
            topic_name_index: HashMap::new(),
            next_topic_id: shard_group_id.0 << 32,
            pending_proposals: Vec::new(),
            pending_events: Vec::new(),
        }
    }

    pub(crate) fn snapshot(&self) -> MetadataStateSnapshot {
        MetadataStateSnapshot {
            topics: self.topics.clone(),
            next_topic_id: self.next_topic_id,
        }
    }

    pub(crate) fn from_snapshot(snapshot: &MetadataStateSnapshot, last_applied_index: u64) -> Self {
        let topics = snapshot.topics.clone();
        let topic_name_index = topics
            .values()
            .map(|topic| (topic.name.clone(), topic.id))
            .collect();
        Self {
            topics,
            last_applied_index,
            topic_name_index,
            next_topic_id: snapshot.next_topic_id,
            pending_proposals: Vec::new(),
            pending_events: Vec::new(),
        }
    }

    pub(crate) fn get_topic(&self, id: &TopicId) -> Option<&TopicMeta> {
        self.topics.get(id)
    }

    pub(crate) fn get_topic_by_name(&self, name: &str) -> Option<&TopicMeta> {
        self.topic_name_index
            .get(name)
            .and_then(|id| self.topics.get(id))
    }

    pub(crate) fn get_segment(&self, key: &SegmentKey) -> Option<&SegmentMeta> {
        let topic = self.get_topic(&key.topic_id)?;
        let range = topic.ranges.get(&key.range_id)?;
        range.segments.get(&key.segment_id)
    }

    pub(crate) fn get_consumer_group_assignment(
        &self,
        topic_name: &str,
        group_id: &str,
        member_id: Uuid,
    ) -> Option<ConsumerGroupAssignment> {
        Some(
            self.get_topic_by_name(topic_name)?
                .consumer_groups
                .get(group_id)?
                .assignment_for(member_id),
        )
    }

    pub(crate) fn topic_names(&self) -> Box<[String]> {
        self.topic_name_index.keys().cloned().collect()
    }

    pub(crate) fn topic_stats(&self) -> Box<[TopicStats]> {
        self.topics.values().map(|t| t.stats()).collect()
    }

    pub(crate) fn take_pending_proposals(&mut self) -> Box<[MetadataCommand]> {
        std::mem::take(&mut self.pending_proposals).into_boxed_slice()
    }

    pub(crate) fn take_pending_events(&mut self) -> Vec<MetadataEvent> {
        std::mem::take(&mut self.pending_events)
    }

    fn raise_event(&mut self, event: impl Into<MetadataEvent>) {
        self.pending_events.push(event.into());
    }

    pub(crate) fn active_segments_for_node(
        &self,
        node_id: &NodeId,
    ) -> Box<[(SegmentKey, Replicas)]> {
        self.topics
            .values()
            .flat_map(|t| t.active_segments_for_node(node_id))
            .collect()
    }

    /// Every active segment across all topics with its replica set and start
    /// offset, for the leader's periodic assignment re-drive.
    pub(crate) fn active_segment_assignments(&self) -> Box<[(SegmentKey, Replicas, EntryId)]> {
        self.topics
            .values()
            .flat_map(|t| t.active_segment_assignments())
            .collect()
    }

    /// Active segments across all topics whose `replica_set` contains at
    /// least one non-live member.
    pub(crate) fn active_segments_with_dead_members(
        &self,
        live: &std::collections::HashSet<NodeId>,
    ) -> Box<[(SegmentKey, Replicas)]> {
        self.topics
            .values()
            .flat_map(|topic| topic.active_segments_with_dead_members(live))
            .collect()
    }

    pub(crate) fn sealed_segments_with_dead_members(
        &self,
        live: &std::collections::HashSet<NodeId>,
    ) -> Box<[(SegmentKey, Replicas)]> {
        self.topics
            .values()
            .flat_map(|topic| topic.sealed_segments_with_dead_members(live))
            .collect()
    }

    pub(crate) fn boundary_unknown_segments(&self) -> Box<[(SegmentKey, Vec<NodeId>)]> {
        self.topics
            .values()
            .flat_map(TopicMeta::boundary_unknown_segments)
            .collect()
    }

    pub(crate) fn under_replicated_sealed_segments(
        &self,
        replication_factor: usize,
    ) -> Box<[(SegmentKey, Replicas)]> {
        self.topics
            .values()
            .flat_map(|topic| topic.under_replicated_sealed_segments(replication_factor))
            .collect()
    }

    pub(crate) fn known_end_sealed_segments(
        &self,
    ) -> Vec<(SegmentKey, EntryId, EntryId, Replicas)> {
        self.topics
            .values()
            .flat_map(TopicMeta::known_end_sealed_segments)
            .collect()
    }

    pub(crate) fn expipred_segments(&self, now: u64) -> Vec<(TopicId, RangeId, Box<[SegmentId]>)> {
        self.topics
            .values()
            .flat_map(|topic| {
                let topic_id = topic.id;
                topic
                    .expired_segments(now)
                    .into_iter()
                    .map(move |(range_id, ids)| (topic_id, range_id, ids))
            })
            .collect()
    }

    pub(crate) fn apply(&mut self, command: MetadataCommand) -> Result<(), MetadataError> {
        use MetadataCommand::*;
        match command {
            CreateTopic(cmd) => self.create_topic(cmd)?,
            RollSegment(cmd) => self.roll_segment(cmd)?,
            SplitRange(cmd) => self.split_range(cmd)?,
            MergeRange(cmd) => self.merge_range(cmd)?,
            DeleteTopic(cmd) => self.delete_topic(cmd)?,
            ReassignSegment(cmd) => self.reassign_segment(cmd)?,
            DeleteSegments(cmd) => self.delete_segments(cmd)?,
            SyncConsumerGroup(cmd) => self.sync_consumer_group(cmd)?,
        }
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
        Ok(())
    }

    fn create_topic(&mut self, cmd: CreateTopic) -> Result<(), MetadataError> {
        if self.topic_name_index.contains_key(&cmd.name) {
            return Err(TopicNameAlreadyExists(cmd.name));
        }
        let topic_id = TopicId(self.next_topic_id);
        let replica_set = cmd.replica_set.clone();
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
        self.raise_event(TopicCreated {
            segment_key: SegmentKey::new(topic_id, RangeId(0), SegmentId(0)),
            replica_set,
        });
        Ok(())
    }

    fn roll_segment(&mut self, cmd: RollSegment) -> Result<(), MetadataError> {
        let topic = self.get_active_topic_mut(cmd.segment_key.topic_id)?;
        let can_split = topic.can_split();
        let range = topic.get_range_mut(&cmd.segment_key.range_id)?;

        let is_active = range.active_segment == Some(cmd.segment_key.segment_id);

        // If Inactive, correction path
        if !is_active {
            let Some(end_entry_id) = cmd.end_entry_id else {
                return Ok(());
            };
            let Some(replica_set) =
                range.correct_end_offset(cmd.segment_key.segment_id, end_entry_id)
            else {
                return Ok(());
            };

            self.raise_event(SegmentBoundaryCorrected {
                segment_key: cmd.segment_key,
                replica_set,
                committed_entry_id: cmd.end_entry_id,
            });
            return Ok(());
        }

        if cmd.intent == SegmentRollIntent::BoundaryCorrection {
            return Ok(());
        }

        // If Active, Roll
        let new_segment_id = range.roll_segment(cmd.clone())?;
        let split_proposal =
            (range.should_split() && can_split).then(|| range.build_split_proposal(&cmd));

        // For segment roll, unless data nodes got changed, consumer
        // TODO For now, it loops over EVERY consumger groups and take epoch snapshot for every topic.
        // TODO To reduce the load, it should specifically target groups that are affected by the possible range split
        let consumer_group_epochs: Box<[ConsumerGroupEpochSnapshot]> = topic
            .consumer_groups
            .keys()
            .filter_map(|group_id| topic.consumer_group_epoch(group_id))
            .collect();
        tracing::debug!("Consumer groups: {:?}", consumer_group_epochs);

        let merge_proposal = (cmd.intent == SegmentRollIntent::IdleMaintenance)
            .then(|| topic.find_mergeable_range_pair(cmd.sealed_at))
            .flatten();

        if let Some(proposal) = split_proposal {
            match proposal {
                Ok(proposal) => self
                    .pending_proposals
                    .push(MetadataCommand::SplitRange(proposal)),
                Err(error) => tracing::debug!(
                    "Split proposal skipped for range {:?}: {:?}",
                    cmd.segment_key.range_id,
                    error
                ),
            }
        }
        if let Some(proposal) = merge_proposal {
            self.pending_proposals.push(proposal);
        }
        self.raise_event(SegmentRolled {
            new_segment_key: cmd.segment_key.with_segment_id(new_segment_id),
            new_replica_set: cmd.new_replica_set,
            end_entry_id: cmd.end_entry_id,
        });

        for epoch in consumer_group_epochs {
            self.raise_event(epoch);
        }
        Ok(())
    }

    /// Re-points a sealed segment's replica set.
    /// `Noop` when the set is unchanged, otherwise `SegmentReassigned`.
    /// The segment must be sealed; an active/deleting/unknown one is rejected.
    fn reassign_segment(&mut self, cmd: ReassignSegment) -> Result<(), MetadataError> {
        let segment = self
            .topics
            .get_mut(&cmd.segment_key.topic_id)
            .ok_or(TopicNotFound(cmd.segment_key.topic_id))?
            .get_mut(cmd.segment_key)?;

        // The dispatch announces the desired replica set; receivers reconcile, so
        // we only carry the sealed bounds (the catch-up target) alongside it.
        let event = segment
            .reassign(cmd.replica_set.clone())?
            .then_some(SegmentReassigned {
                segment_key: cmd.segment_key,
                start_entry_id: segment.start_entry_id,
                sealed_end: segment.end_entry_id,
                new_replica_set: cmd.replica_set,
            });
        if let Some(event) = event {
            self.raise_event(event);
        }
        Ok(())
    }

    /// Retention: mark an oldest-first prefix of a range's sealed segments `Deleting`.
    /// `Noop` when nothing transitions (all named ids already `Deleting`/absent), else
    /// `SegmentsDeleted` carrying the deleted segments grouped by `replica_set` for
    /// batched dispatch. Uses plain `get_mut` (not `validate_active`) so a command
    /// applied after the topic is being deleted is a harmless no-op (already `Deleting`).
    fn delete_segments(&mut self, cmd: DeleteSegments) -> Result<(), MetadataError> {
        let range = self
            .topics
            .get_mut(&cmd.topic_id)
            .ok_or(TopicNotFound(cmd.topic_id))?
            .get_range_mut(&cmd.range_id)?;

        let deleted_ids = range.delete_segments(&cmd.segment_ids);
        if deleted_ids.is_empty() {
            return Ok(());
        }
        // Group the deleted segments by replica_set here.
        let mut groups: Vec<(Replicas, Vec<SegmentKey>)> = Vec::new();
        for sid in &deleted_ids {
            let Some(seg) = range.segments.get(sid) else {
                continue;
            };
            let key = SegmentKey::new(cmd.topic_id, cmd.range_id, *sid);
            match groups.iter_mut().find(|(rs, _)| rs == &seg.replica_set) {
                Some((_, keys)) => keys.push(key),
                None => groups.push((seg.replica_set.clone(), vec![key])),
            }
        }
        self.raise_event(SegmentsDeleted { groups });
        Ok(())
    }

    fn get_active_topic_mut(&mut self, id: TopicId) -> Result<&mut TopicMeta, MetadataError> {
        let topic = self.topics.get_mut(&id).ok_or(TopicNotFound(id))?;
        topic.validate_active()?;
        Ok(topic)
    }

    fn split_range(&mut self, cmd: SplitRange) -> Result<(), MetadataError> {
        let topic = self
            .topics
            .get_mut(&cmd.topic_id)
            .ok_or(TopicNotFound(cmd.topic_id))?;
        topic.validate_active()?;
        if !topic.can_split() {
            return Err(SplitNotAllowed(cmd.topic_id));
        }

        let parent_range = topic
            .ranges
            .get(&cmd.range_id)
            .ok_or(MetadataError::RangeNotFound)?;

        let parent_active_segment = parent_range.active_segment.and_then(|seg_id| {
            let seg = parent_range.segments.get(&seg_id)?;
            Some((
                SegmentKey::new(cmd.topic_id, cmd.range_id, seg_id),
                seg.replica_set.clone(),
            ))
        });

        let (left_id, right_id) = topic.execute_split(cmd.clone())?;

        let consumer_group_epochs = topic.rebalance_consumer_groups();

        self.raise_event(RangeSplit {
            topic_id: cmd.topic_id,
            children: [
                (left_id, SegmentId(0), cmd.left_replica_set),
                (right_id, SegmentId(0), cmd.right_replica_set),
            ],
        });
        if let Some((segment_key, replica_set)) = parent_active_segment {
            self.raise_event(SegmentSealCommitted {
                segment_key,
                replica_set,
            });
        }
        for epoch in consumer_group_epochs {
            self.raise_event(epoch);
        }
        Ok(())
    }

    fn merge_range(&mut self, cmd: MergeRange) -> Result<(), MetadataError> {
        let topic_id = cmd.topic_id;
        let replica_set = cmd.merged_replica_set.clone();
        let topic = self
            .topics
            .get_mut(&topic_id)
            .ok_or(TopicNotFound(topic_id))?;
        topic.validate_active()?;

        let sealed_source_segments = [
            topic.active_segment_key_and_replicas(cmd.range_id_1)?,
            topic.active_segment_key_and_replicas(cmd.range_id_2)?,
        ];

        let merged_id = topic.execute_merge(cmd)?;
        let consumer_group_epochs = topic.rebalance_consumer_groups();

        self.raise_event(RangeMerged {
            segment_key: SegmentKey::new(topic_id, merged_id, SegmentId(0)),
            replica_set,
        });
        for (segment_key, source_replica_set) in sealed_source_segments {
            self.raise_event(SegmentSealCommitted {
                segment_key,
                replica_set: source_replica_set,
            });
        }
        for epoch in consumer_group_epochs {
            self.raise_event(epoch);
        }
        Ok(())
    }

    fn sync_consumer_group(&mut self, cmd: SyncConsumerGroup) -> Result<(), MetadataError> {
        let group_id = cmd.group_id.clone();

        let topic = self
            .topic_name_index
            .get(&cmd.topic_name)
            .and_then(|topic_id| self.topics.get_mut(topic_id))
            .ok_or_else(|| TopicNameNotFound(cmd.topic_name.clone()))?;
        topic.validate_active()?;
        if !topic.sync_consumer_group(cmd) {
            return Ok(());
        }

        let event = topic.consumer_group_epoch(&group_id);
        if let Some(event) = event {
            self.raise_event(event);
        }
        Ok(())
    }

    // ! SAFETY: When the loop evaluates the [1, 2] pair and decides it is mergeable,
    // ! it generates the event and immediately stops looking at the rest of that topic's ranges
    // !
    // ! Caution on race condition. Take the following example:
    // !    1. the following method proposes merging (1, 2).
    // !    2. The MergeRange command goes into a queue (or a Raft log) to be processed.
    // !    3. Before the command is executed, range 2 receives a massive burst of traffic and splits into 2A and 2B.
    // !    4. The MergeRange(1, 2) command is finally executed by your merge function.
    // ! By the time the command executes, range 2 might be split, already sealed, or completely deleted
    // * This is already safe as the 'stale' proposal fails gracefully at apply time, following "stale proposals are safe" invariant
    pub(crate) fn evaluate_merges(&self, now: u64) -> Vec<MetadataCommand> {
        self.topics
            .values()
            .filter_map(|topic| topic.find_mergeable_range_pair(now))
            .collect()
    }

    fn delete_topic(&mut self, cmd: DeleteTopic) -> Result<(), MetadataError> {
        let topic_id = self
            .topic_name_index
            .get(&cmd.name)
            .copied()
            .ok_or(MetadataError::TopicNameNotFound(cmd.name.clone()))?;

        // Safety: topic_name_index and topics are always in sync —
        // see invariant below.
        let topic = self.topics.get_mut(&topic_id).unwrap();
        topic.delete();

        let consumer_group_epochs = topic.rebalance_consumer_groups();

        self.topic_name_index.remove(&cmd.name);

        for epoch in consumer_group_epochs {
            self.raise_event(epoch);
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }
}

#[cfg(any(test, debug_assertions))]
impl crate::test_traits::TAssertInvariant for MetadataState {
    fn assert_invariants(&self) {
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

        for id in self.topics.keys() {
            assert!(id.0 < self.next_topic_id, "topic ID >= next_topic_id");
        }
        for topic in self.topics.values() {
            topic.assert_invariants();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connections::protocol::ConsumerGroupSyncAction;
    use crate::control_plane::membership::ShardGroupId;
    use crate::control_plane::metadata::constants::*;
    use crate::control_plane::metadata::range::*;
    use crate::control_plane::metadata::segment::*;
    use crate::control_plane::{
        NodeId,
        metadata::{
            SegmentId,
            strategy::{PartitionStrategy, StoragePolicy},
        },
    };
    fn default_policy() -> StoragePolicy {
        StoragePolicy {
            retention_ms: Some(3_600_000),
            replication_factor: 3,
            partition_strategy: PartitionStrategy::AutoSplit,
        }
    }

    fn fixed_policy() -> StoragePolicy {
        StoragePolicy {
            retention_ms: Some(3_600_000),
            replication_factor: 3,
            partition_strategy: PartitionStrategy::Fixed,
        }
    }

    fn replica_set() -> Replicas {
        Replicas::new(vec![
            NodeId::new("node-1"),
            NodeId::new("node-2"),
            NodeId::new("node-3"),
        ])
    }

    fn apply_command(
        sm: &mut MetadataState,
        command: MetadataCommand,
    ) -> Result<Vec<MetadataEvent>, MetadataError> {
        sm.apply(command)?;
        Ok(sm.take_pending_events())
    }

    fn create_topic(sm: &mut MetadataState, name: &str) -> TopicId {
        let result = apply_command(
            sm,
            MetadataCommand::CreateTopic(CreateTopic {
                name: name.to_string(),
                storage_policy: default_policy(),
                replica_set: replica_set(),
                created_at: 1000,
            }),
        );
        match result.unwrap().into_iter().next().unwrap() {
            MetadataEvent::TopicCreated(tc) => tc.segment_key.topic_id,
            other => panic!("expected TopicCreated, got {:?}", other),
        }
    }

    #[test]
    fn snapshot_encoding_is_independent_of_hashmap_insertion_order() {
        let first = TopicMeta::new(
            "first".into(),
            TopicId(1),
            replica_set(),
            1000,
            default_policy(),
        );
        let second = TopicMeta::new(
            "second".into(),
            TopicId(2),
            replica_set(),
            1000,
            default_policy(),
        );
        let a = MetadataStateSnapshot {
            topics: HashMap::from([(first.id, first.clone()), (second.id, second.clone())]),
            next_topic_id: 3,
        };
        let b = MetadataStateSnapshot {
            topics: HashMap::from([(second.id, second), (first.id, first)]),
            next_topic_id: 3,
        };

        assert_eq!(borsh::to_vec(&a).unwrap(), borsh::to_vec(&b).unwrap());
    }

    #[test]
    fn consumer_group_generation_is_applied_through_metadata_log_command() {
        let mut sm = MetadataState::new(ShardGroupId(1));
        create_topic(&mut sm, "orders");
        let member = uuid::Uuid::new_v4();
        let command = SyncConsumerGroup {
            req: SyncConsumerGroupRequest {
                topic_name: "orders".into(),
                group_id: "workers".into(),
                member_id: member,
                action: ConsumerGroupSyncAction::Heartbeat,
            },
            observed_at: 100,
            session_timeout_ms: 10_000,
        };

        let events = apply_command(&mut sm, command.clone().into()).unwrap();
        let [MetadataEvent::ConsumerGroupChanged(epoch)] = events.as_slice() else {
            panic!("first member must create a committed generation");
        };
        assert_eq!(*epoch.generation, 1);
        assert_eq!(epoch.ranges.len(), 1);
        let assignment = sm
            .get_consumer_group_assignment("orders", "workers", member)
            .unwrap();
        assert_eq!(*assignment.generation, 1);
        assert_eq!(assignment.ranges.as_ref(), &[RangeId(0)]);

        let mut heartbeat = command;
        heartbeat.observed_at = 200;
        assert!(apply_command(&mut sm, heartbeat.into()).unwrap().is_empty());
        assert_eq!(
            *sm.get_consumer_group_assignment("orders", "workers", member)
                .unwrap()
                .generation,
            1
        );
    }

    fn roll_segment(
        sm: &mut MetadataState,
        topic_id: TopicId,
        range_id: RangeId,
        segment_id: SegmentId,
        sealed_at: u64,
    ) {
        roll_segment_with_intent(
            sm,
            topic_id,
            range_id,
            segment_id,
            sealed_at,
            SegmentRollIntent::DataPressure,
        );
    }

    fn roll_segment_with_intent(
        sm: &mut MetadataState,
        topic_id: TopicId,
        range_id: RangeId,
        segment_id: SegmentId,
        sealed_at: u64,
        intent: SegmentRollIntent,
    ) {
        let result = apply_command(
            sm,
            MetadataCommand::RollSegment(RollSegment {
                segment_key: SegmentKey::new(topic_id, range_id, segment_id),
                sealed_at,
                new_replica_set: replica_set(),
                end_entry_id: None,
                intent,
            }),
        );
        assert!(matches!(
            result.unwrap().into_iter().next().unwrap(),
            MetadataEvent::SegmentRolled(_)
        ));
    }

    // ── D7 retention ───────────────────────────────────────────────────────

    /// Roll the active segment, sealing it at `end_entry_id` / `sealed_at` so the
    /// resulting sealed segment has a known end and seal time (unlike the death-roll
    /// `roll_segment` helper above which seals with `None`).
    fn roll_with_end(
        sm: &mut MetadataState,
        topic_id: TopicId,
        segment_id: SegmentId,
        end_entry_id: u64,
        sealed_at: u64,
    ) {
        let result = apply_command(
            sm,
            MetadataCommand::RollSegment(RollSegment {
                segment_key: SegmentKey::new(topic_id, RangeId(0), segment_id),
                sealed_at,
                new_replica_set: replica_set(),
                end_entry_id: Some(EntryId(end_entry_id)),
                intent: SegmentRollIntent::Recovery,
            }),
        );
        assert!(matches!(
            result.unwrap().into_iter().next().unwrap(),
            MetadataEvent::SegmentRolled(_)
        ));
    }

    fn seg_state(sm: &MetadataState, topic_id: TopicId, segment_id: SegmentId) -> SegmentMetaState {
        sm.get_topic(&topic_id).unwrap().ranges[&RangeId(0)].segments[&segment_id]
            .state
            .clone()
    }

    /// Build a topic with sealed segments 0,1,2 (ends 9/19/29, sealed at 100/200/300)
    /// and an active head 3.
    fn topic_with_three_sealed(sm: &mut MetadataState) -> TopicId {
        let t = create_topic(sm, "t");
        roll_with_end(sm, t, SegmentId(0), 9, 100);
        roll_with_end(sm, t, SegmentId(1), 19, 200);
        roll_with_end(sm, t, SegmentId(2), 29, 300);
        t
    }

    fn delete_segments(
        sm: &mut MetadataState,
        topic_id: TopicId,
        ids: &[u64],
    ) -> Result<Vec<MetadataEvent>, MetadataError> {
        apply_command(
            sm,
            MetadataCommand::DeleteSegments(DeleteSegments {
                topic_id,
                range_id: RangeId(0),
                segment_ids: ids.iter().map(|&i| SegmentId(i)).collect(),
            }),
        )
    }

    #[test]
    fn delete_segments_marks_oldest_prefix_deleting() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let t = topic_with_three_sealed(&mut sm);

        let result = delete_segments(&mut sm, t, &[0, 1])
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let MetadataEvent::SegmentsDeleted(d) = result else {
            panic!("expected SegmentsDeleted, got {result:?}");
        };
        // All three sealed segments share one replica_set → a single group of 2 keys.
        assert_eq!(d.groups.len(), 1);
        assert_eq!(d.groups[0].1.len(), 2);
        assert_eq!(seg_state(&sm, t, SegmentId(0)), SegmentMetaState::Deleting);
        assert_eq!(seg_state(&sm, t, SegmentId(1)), SegmentMetaState::Deleting);
        assert_eq!(seg_state(&sm, t, SegmentId(2)), SegmentMetaState::Sealed);
        assert_eq!(seg_state(&sm, t, SegmentId(3)), SegmentMetaState::Active);
    }

    #[test]
    fn delete_segments_skips_the_active_head() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let t = topic_with_three_sealed(&mut sm);
        // Naming the active head (seg 3) alongside the sealed prefix: only the sealed
        // ones transition; the write head is skipped, never deleted.
        let events = delete_segments(&mut sm, t, &[0, 1, 2, 3]).unwrap();
        let [MetadataEvent::SegmentsDeleted(d)] = events.as_slice() else {
            panic!("expected SegmentsDeleted");
        };
        let total_keys: usize = d.groups.iter().map(|(_, keys)| keys.len()).sum();
        assert_eq!(total_keys, 3);
        assert_eq!(seg_state(&sm, t, SegmentId(3)), SegmentMetaState::Active);
    }

    /// The no-hole property is a structural invariant, not a hot-path check: a
    /// non-prefix deletion (seg 1 while seg 0 survives) trips `assert_retention_prefix`.
    #[test]
    #[should_panic(expected = "not an oldest-first prefix")]
    fn delete_segments_non_prefix_trips_invariant() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let t = topic_with_three_sealed(&mut sm);
        let _ = delete_segments(&mut sm, t, &[1]);
    }

    #[test]
    fn delete_segments_is_idempotent() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let t = topic_with_three_sealed(&mut sm);
        assert!(matches!(
            delete_segments(&mut sm, t, &[0]).unwrap().as_slice(),
            [MetadataEvent::SegmentsDeleted(_)]
        ));
        // Re-applying for an already-Deleting segment is a no-op.
        assert!(delete_segments(&mut sm, t, &[0]).unwrap().is_empty());
    }

    #[test]
    fn expired_prefix_selects_by_age_oldest_first() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let t = topic_with_three_sealed(&mut sm); // sealed_at 100/200/300, retention 3_600_000
        let topic = sm.get_topic(&t).unwrap();

        // now such that segs 0,1 are past the window but seg 2 isn't.
        let now = 200 + 3_600_000 + 1;
        let prefixes = topic.expired_segments(now);
        assert_eq!(prefixes.len(), 1);
        let (range_id, ids) = &prefixes[0];
        assert_eq!(*range_id, RangeId(0));
        assert_eq!(ids.as_ref(), &[SegmentId(0), SegmentId(1)]);
    }

    #[test]
    fn expired_prefix_empty_without_retention() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let t = apply_command(
            &mut sm,
            MetadataCommand::CreateTopic(CreateTopic {
                name: "no-retention".into(),
                storage_policy: StoragePolicy {
                    retention_ms: None,
                    replication_factor: 3,
                    partition_strategy: PartitionStrategy::AutoSplit,
                },
                replica_set: replica_set(),
                created_at: 1000,
            }),
        )
        .map(|r| match r.into_iter().next().unwrap() {
            MetadataEvent::TopicCreated(tc) => tc.segment_key.topic_id,
            other => panic!("{other:?}"),
        })
        .unwrap();
        roll_with_end(&mut sm, t, SegmentId(0), 9, 100);
        // Far past any window, but no policy → nothing expires.
        assert!(
            sm.get_topic(&t)
                .unwrap()
                .expired_segments(u64::MAX)
                .is_empty()
        );
    }

    fn split_range(
        sm: &mut MetadataState,
        topic_id: TopicId,
        range_id: RangeId,
        split_point: Vec<u8>,
        created_at: u64,
    ) -> (RangeId, RangeId) {
        let result = apply_command(
            sm,
            MetadataCommand::SplitRange(SplitRange {
                topic_id,
                range_id,
                split_point,
                created_at,
                left_replica_set: replica_set(),
                right_replica_set: replica_set(),
            }),
        );
        let events = result.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, MetadataEvent::SegmentSealCommitted(_)))
                .count(),
            1
        );
        match events.into_iter().next().unwrap() {
            MetadataEvent::RangeSplit(split) => (split.children[0].0, split.children[1].0),
            other => panic!("expected RangeSplit, got {:?}", other),
        }
    }

    fn merge_range(
        sm: &mut MetadataState,
        topic_id: TopicId,
        range_id_1: RangeId,
        range_id_2: RangeId,
        created_at: u64,
    ) -> RangeId {
        let result = apply_command(
            sm,
            MetadataCommand::MergeRange(MergeRange {
                topic_id,
                range_id_1,
                range_id_2,
                created_at,
                merged_replica_set: replica_set(),
            }),
        );
        let events = result.unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, MetadataEvent::SegmentSealCommitted(_)))
                .count(),
            2
        );
        match events.into_iter().next().unwrap() {
            MetadataEvent::RangeMerged(merged) => merged.segment_key.range_id,
            other => panic!("expected RangeMerged, got {:?}", other),
        }
    }

    /// A surviving subset plus a fresh replacement — what the coordinator picks
    /// when a replica of a sealed segment dies (node-3 → node-4 here).
    fn replacement_set() -> Replicas {
        Replicas::new(vec![
            NodeId::new("node-1"),
            NodeId::new("node-2"),
            NodeId::new("node-4"),
        ])
    }

    // --- ReassignSegment ---

    #[test]
    fn reassign_swaps_a_sealed_segments_replica_set() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let topic_id = create_topic(&mut sm, "blue");
        // Roll so SegmentId(0) becomes Sealed (SegmentId(1) is the new write head).
        roll_segment(&mut sm, topic_id, RangeId(0), SegmentId(0), 2000);
        let sealed = SegmentKey::new(topic_id, RangeId(0), SegmentId(0));

        let result = apply_command(
            &mut sm,
            MetadataCommand::ReassignSegment(ReassignSegment {
                segment_key: sealed,
                replica_set: replacement_set(),
            }),
        )
        .unwrap();

        match result.into_iter().next().unwrap() {
            MetadataEvent::SegmentReassigned(r) => {
                assert_eq!(r.segment_key, sealed);
                assert_eq!(r.new_replica_set, replacement_set());
            }
            other => panic!("expected SegmentReassigned, got {other:?}"),
        }

        let seg = &sm.get_topic(&topic_id).unwrap().ranges[&RangeId(0)].segments[&SegmentId(0)];
        assert_eq!(seg.replica_set, replacement_set());
        assert_eq!(seg.state, SegmentMetaState::Sealed); // stays sealed
    }

    #[test]
    fn reassign_same_set_is_a_noop() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let topic_id = create_topic(&mut sm, "blue");
        roll_segment(&mut sm, topic_id, RangeId(0), SegmentId(0), 2000);
        let sealed = SegmentKey::new(topic_id, RangeId(0), SegmentId(0));

        apply_command(
            &mut sm,
            MetadataCommand::ReassignSegment(ReassignSegment {
                segment_key: sealed,
                replica_set: replacement_set(),
            }),
        )
        .unwrap();

        // Re-applying the identical set (duplicate death detection / re-proposal)
        // changes nothing.
        let again = apply_command(
            &mut sm,
            MetadataCommand::ReassignSegment(ReassignSegment {
                segment_key: sealed,
                replica_set: replacement_set(),
            }),
        )
        .unwrap();
        assert!(again.is_empty());
    }

    #[test]
    fn reassign_rejects_an_active_segment() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let topic_id = create_topic(&mut sm, "blue");
        // SegmentId(0) is the active write head — no roll yet.
        let active = SegmentKey::new(topic_id, RangeId(0), SegmentId(0));

        let result = apply_command(
            &mut sm,
            MetadataCommand::ReassignSegment(ReassignSegment {
                segment_key: active,
                replica_set: replacement_set(),
            }),
        );
        assert!(matches!(result, Err(MetadataError::SegmentNotSealed)));
    }

    #[test]
    fn reassign_rejects_an_unknown_segment() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let topic_id = create_topic(&mut sm, "blue");
        let unknown = SegmentKey::new(topic_id, RangeId(0), SegmentId(99));

        let result = apply_command(
            &mut sm,
            MetadataCommand::ReassignSegment(ReassignSegment {
                segment_key: unknown,
                replica_set: replacement_set(),
            }),
        );
        assert!(matches!(result, Err(MetadataError::SegmentNotFound)));
    }

    // --- CreateTopic ---

    #[test]
    fn create_topic_basic() {
        let mut sm = MetadataState::new(ShardGroupId(0));
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
        let mut sm = MetadataState::new(ShardGroupId(0));
        create_topic(&mut sm, "blue");

        let result = apply_command(
            &mut sm,
            MetadataCommand::CreateTopic(CreateTopic {
                name: "blue".to_string(),
                storage_policy: default_policy(),
                replica_set: replica_set(),
                created_at: 2000,
            }),
        );
        assert_eq!(result, Err(TopicNameAlreadyExists("blue".to_string())));
    }

    #[test]
    fn create_topic_increments_id() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let id1 = create_topic(&mut sm, "alpha");
        let id2 = create_topic(&mut sm, "beta");

        assert_eq!(id1, TopicId(0));
        assert_eq!(id2, TopicId(1));
        assert_eq!(sm.topic_count(), 2);
    }

    #[test]
    fn create_topic_initial_offsets() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let id = create_topic(&mut sm, "blue");

        let topic = sm.get_topic(&id).unwrap();
        let range = &topic.ranges[&RangeId(0)];
        assert_eq!(range.next_offset, EntryId(0));

        let seg = &range.segments[&SegmentId(0)];
        assert_eq!(seg.start_entry_id, EntryId(0));
        assert_eq!(seg.end_entry_id, None);
    }

    #[test]
    fn create_topic_name_index() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let id = create_topic(&mut sm, "blue");

        let found = sm.get_topic_by_name("blue").unwrap();
        assert_eq!(found.id, id);
        assert!(sm.get_topic_by_name("red").is_none());
    }

    // --- RollSegment ---

    #[test]
    fn roll_segment_creates_next() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 2000);

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.active_segment, Some(SegmentId(1)));
        assert_eq!(range.segments.len(), 2);
    }

    #[test]
    fn roll_segment_increments_segment_id() {
        let mut sm = MetadataState::new(ShardGroupId(0));
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
        let mut sm = MetadataState::new(ShardGroupId(0));
        let result = apply_command(
            &mut sm,
            MetadataCommand::RollSegment(RollSegment {
                segment_key: SegmentKey::new(TopicId(99), RangeId(0), SegmentId(0)),
                sealed_at: 2000,
                new_replica_set: replica_set(),
                end_entry_id: None,
                intent: SegmentRollIntent::Recovery,
            }),
        );
        assert_eq!(result, Err(TopicNotFound(TopicId(99))));
    }

    #[test]
    fn roll_segment_bad_range() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        let result = apply_command(
            &mut sm,
            MetadataCommand::RollSegment(RollSegment {
                segment_key: SegmentKey::new(tid, RangeId(99), SegmentId(0)),
                sealed_at: 2000,
                new_replica_set: replica_set(),
                end_entry_id: None,
                intent: SegmentRollIntent::Recovery,
            }),
        );
        assert_eq!(result, Err(RangeNotFound));
    }

    #[test]
    fn roll_segment_stale_is_rejected() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        let result = apply_command(
            &mut sm,
            MetadataCommand::RollSegment(RollSegment {
                segment_key: SegmentKey::new(tid, RangeId(0), SegmentId(99)),
                sealed_at: 2000,
                new_replica_set: replica_set(),
                end_entry_id: None,
                intent: SegmentRollIntent::Recovery,
            }),
        );
        assert_eq!(result, Ok(vec![]));

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.active_segment, Some(SegmentId(0)));
        assert_eq!(range.segments.len(), 1);
    }

    // --- SplitRange ---

    #[test]
    fn split_range_basic() {
        let mut sm = MetadataState::new(ShardGroupId(0));
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
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.active_ranges, vec![c1, c2]);
        assert!(!topic.active_ranges.contains(&RangeId(0)));
    }

    #[test]
    fn split_range_lineage() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.ranges[&RangeId(0)].split_into, Some([c1, c2]));
    }

    #[test]
    fn split_range_fixed_rejected() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let result = apply_command(
            &mut sm,
            MetadataCommand::CreateTopic(CreateTopic {
                name: "ordered".to_string(),
                storage_policy: fixed_policy(),
                replica_set: replica_set(),
                created_at: 1000,
            }),
        );
        let tid = match result.unwrap().into_iter().next().unwrap() {
            MetadataEvent::TopicCreated(tc) => tc.segment_key.topic_id,
            other => panic!("expected TopicCreated, got {:?}", other),
        };

        let split_result = apply_command(
            &mut sm,
            MetadataCommand::SplitRange(SplitRange {
                topic_id: tid,
                range_id: RangeId(0),
                split_point: vec![0x80],
                created_at: 2000,
                left_replica_set: replica_set(),
                right_replica_set: replica_set(),
            }),
        );
        assert_eq!(split_result, Err(SplitNotAllowed(tid)));
    }

    #[test]
    fn split_range_invalid_split_point() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        let upper_bound = apply_command(
            &mut sm,
            MetadataCommand::SplitRange(SplitRange {
                topic_id: tid,
                range_id: RangeId(0),
                split_point: vec![0xFF],
                created_at: 2000,
                left_replica_set: replica_set(),
                right_replica_set: replica_set(),
            }),
        );
        assert_eq!(upper_bound, Err(InvalidSplitPoint));

        let lower_bound = apply_command(
            &mut sm,
            MetadataCommand::SplitRange(SplitRange {
                topic_id: tid,
                range_id: RangeId(0),
                split_point: vec![],
                created_at: 2000,
                left_replica_set: replica_set(),
                right_replica_set: replica_set(),
            }),
        );
        assert_eq!(lower_bound, Err(InvalidSplitPoint));
    }

    #[test]
    fn split_range_keyspace_coverage() {
        let mut sm = MetadataState::new(ShardGroupId(0));
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
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");
        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let merged_id = merge_range(&mut sm, tid, c1, c2, 3000);

        let topic = sm.get_topic(&tid).unwrap();
        let merged = &topic.ranges[&merged_id];
        assert_eq!(merged.keyspace_start, KEYSPACE_MIN);
        assert_eq!(merged.keyspace_end, KEYSPACE_MAX);
        assert_eq!(merged.state, RangeState::Active);
        assert_eq!(merged.load_state, RangeLoadState::Unclassified);
    }

    #[test]
    fn merge_range_active_ranges_updated() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");
        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        let merged_id = merge_range(&mut sm, tid, c1, c2, 3000);

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.active_ranges, vec![merged_id]);
    }

    #[test]
    fn merge_range_lineage() {
        let mut sm = MetadataState::new(ShardGroupId(0));
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
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");
        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);
        let (c1a, _c1b) = split_range(&mut sm, tid, c1, vec![0x40], 3000);

        let result = apply_command(
            &mut sm,
            MetadataCommand::MergeRange(MergeRange {
                topic_id: tid,
                range_id_1: c1a,
                range_id_2: c2,
                created_at: 4000,
                merged_replica_set: replica_set(),
            }),
        );
        assert_eq!(result, Err(RangesNotAdjacent));
    }

    // --- DeleteTopic ---

    #[test]
    fn delete_topic_cascades() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        apply_command(
            &mut sm,
            MetadataCommand::DeleteTopic(DeleteTopic {
                name: "blue".into(),
            }),
        )
        .unwrap();

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.state, TopicState::Deleted);
        assert!(topic.active_ranges.is_empty());

        for range in topic.ranges.values() {
            assert_eq!(range.state, RangeState::Deleting);
            for seg in range.segments.values() {
                assert_eq!(seg.state, SegmentMetaState::Deleting);
            }
        }
    }

    #[test]
    fn delete_topic_removes_name_index() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let _tid = create_topic(&mut sm, "blue");

        apply_command(
            &mut sm,
            MetadataCommand::DeleteTopic(DeleteTopic {
                name: "blue".into(),
            }),
        )
        .unwrap();

        assert!(sm.get_topic_by_name("blue").is_none());
    }

    #[test]
    fn delete_topic_nonexistent() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let result = apply_command(
            &mut sm,
            MetadataCommand::DeleteTopic(DeleteTopic {
                name: "nope".into(),
            }),
        );
        assert_eq!(result, Err(MetadataError::TopicNameNotFound("nope".into())));
    }

    // --- Integration ---

    #[test]
    fn create_split_seal() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");
        let (c1, _c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        roll_segment(&mut sm, tid, c1, SegmentId(0), 3000);

        let child = &sm.get_topic(&tid).unwrap().ranges[&c1];
        assert_eq!(child.active_segment, Some(SegmentId(1)));
        assert_eq!(child.segments.len(), 2);
    }

    #[test]
    fn split_merge_roundtrip() {
        let mut sm = MetadataState::new(ShardGroupId(0));
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
        let mut sm = MetadataState::new(ShardGroupId(0));
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
    fn roll_already_rolled_segment_is_stale() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 2000);

        let result = apply_command(
            &mut sm,
            MetadataCommand::RollSegment(RollSegment {
                segment_key: SegmentKey::new(tid, RangeId(0), SegmentId(0)),
                sealed_at: 3000,
                new_replica_set: replica_set(),
                end_entry_id: None,
                intent: SegmentRollIntent::DataPressure,
            }),
        );
        assert_eq!(result, Ok(vec![]));

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.active_segment, Some(SegmentId(1)));
        assert_eq!(range.segments.len(), 2);
    }

    #[test]
    fn boundary_correction_cannot_roll_active_segment() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        let result = apply_command(
            &mut sm,
            MetadataCommand::RollSegment(RollSegment {
                segment_key: SegmentKey::new(tid, RangeId(0), SegmentId(0)),
                sealed_at: 2000,
                new_replica_set: replica_set(),
                end_entry_id: Some(EntryId(10)),
                intent: SegmentRollIntent::BoundaryCorrection,
            }),
        );

        assert_eq!(result, Ok(vec![]));
        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.active_segment, Some(SegmentId(0)));
        assert_eq!(range.load_state, RangeLoadState::Unclassified);
    }

    // --- Hot Range Detection ---

    #[test]
    fn pressure_rolls_advance_consecutive_streak() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 1000);
        roll_segment(&mut sm, tid, RangeId(0), SegmentId(1), 2000);

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(
            range.load_state,
            RangeLoadState::Pressure {
                consecutive_rolls: 2
            }
        );
    }

    #[test]
    fn idle_roll_resets_pressure() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 1000);
        roll_segment_with_intent(
            &mut sm,
            tid,
            RangeId(0),
            SegmentId(1),
            2000,
            SegmentRollIntent::IdleMaintenance,
        );

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(range.load_state, RangeLoadState::Idle);
    }

    #[test]
    fn neutral_rolls_do_not_change_load_state() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 2000);
        roll_segment_with_intent(
            &mut sm,
            tid,
            RangeId(0),
            SegmentId(1),
            1000,
            SegmentRollIntent::Recovery,
        );
        roll_segment_with_intent(
            &mut sm,
            tid,
            RangeId(0),
            SegmentId(2),
            500,
            SegmentRollIntent::ReplicationFailure,
        );

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(
            range.load_state,
            RangeLoadState::Pressure {
                consecutive_rolls: 1
            }
        );
    }

    #[test]
    fn auto_proposal_on_hot_range() {
        let mut sm = MetadataState::new(ShardGroupId(0));
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
        let mut sm = MetadataState::new(ShardGroupId(0));
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
        let mut sm = MetadataState::new(ShardGroupId(0));
        let result = apply_command(
            &mut sm,
            MetadataCommand::CreateTopic(CreateTopic {
                name: "ordered".to_string(),
                storage_policy: fixed_policy(),
                replica_set: replica_set(),
                created_at: 1000,
            }),
        );
        let tid = match result.unwrap().into_iter().next().unwrap() {
            MetadataEvent::TopicCreated(tc) => tc.segment_key.topic_id,
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
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");
        let (left, right) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);
        roll_segment_with_intent(
            &mut sm,
            tid,
            left,
            SegmentId(0),
            3000,
            SegmentRollIntent::IdleMaintenance,
        );
        roll_segment_with_intent(
            &mut sm,
            tid,
            right,
            SegmentId(0),
            3000,
            SegmentRollIntent::IdleMaintenance,
        );

        let proposals = sm.evaluate_merges(3000);
        assert_eq!(proposals.len(), 1);
        assert!(matches!(proposals[0], MetadataCommand::MergeRange(_)));
        assert!(matches!(
            sm.take_pending_proposals().last(),
            Some(MetadataCommand::MergeRange(_))
        ));
    }

    #[test]
    fn split_children_do_not_merge_without_idle_signals() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");
        split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);

        assert!(sm.evaluate_merges(3000).is_empty());
    }

    #[test]
    fn one_idle_child_does_not_merge() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");
        let (left, _right) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);
        roll_segment_with_intent(
            &mut sm,
            tid,
            left,
            SegmentId(0),
            3000,
            SegmentRollIntent::IdleMaintenance,
        );

        assert!(sm.evaluate_merges(3000).is_empty());
    }

    #[test]
    fn evaluate_merges_one_hot() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");
        let (c1, _c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 2000);
        sm.take_pending_proposals(); // discard any split proposals

        // Seal one child — makes it hot
        roll_segment(&mut sm, tid, c1, SegmentId(0), 3000);

        let proposals = sm.evaluate_merges(3000);
        assert!(proposals.is_empty());
    }

    #[test]
    fn split_children_start_unclassified() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 2000);
        let (c1, c2) = split_range(&mut sm, tid, RangeId(0), vec![0x80], 3000);

        let topic = sm.get_topic(&tid).unwrap();
        assert_eq!(topic.ranges[&c1].load_state, RangeLoadState::Unclassified);
        assert_eq!(topic.ranges[&c2].load_state, RangeLoadState::Unclassified);
    }

    // --- D3: active_segments_for_node ---

    #[test]
    fn active_segments_for_node_returns_matching() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        let segments = sm.active_segments_for_node(&NodeId::new("node-1"));
        assert_eq!(segments.len(), 1);
        let (key, rs) = &segments[0];
        assert_eq!(key.topic_id, tid);
        assert_eq!(key.range_id, RangeId(0));
        assert_eq!(key.segment_id, SegmentId(0));
        assert!(rs.contains(&NodeId::new("node-1")));
    }

    #[test]
    fn active_segments_for_node_excludes_non_member() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        create_topic(&mut sm, "blue");

        let segments = sm.active_segments_for_node(&NodeId::new("node-99"));
        assert!(segments.is_empty());
    }

    #[test]
    fn active_segments_for_node_excludes_sealed() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");
        roll_segment(&mut sm, tid, RangeId(0), SegmentId(0), 2000);

        let segments = sm.active_segments_for_node(&NodeId::new("node-1"));
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].0.segment_id, SegmentId(1));
    }

    // --- D3: end_entry_id in RollSegment ---

    #[test]
    fn roll_segment_uses_end_entry_id() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        let result = apply_command(
            &mut sm,
            MetadataCommand::RollSegment(RollSegment {
                segment_key: SegmentKey::new(tid, RangeId(0), SegmentId(0)),
                sealed_at: 2000,
                new_replica_set: replica_set(),
                end_entry_id: Some(EntryId(42000)),
                intent: SegmentRollIntent::DataPressure,
            }),
        );

        let result = result.unwrap();
        assert!(matches!(
            result.as_slice(),
            [MetadataEvent::SegmentRolled(SegmentRolled {
                end_entry_id: Some(EntryId(42000)),
                ..
            })]
        ));

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        let sealed = &range.segments[&SegmentId(0)];
        assert_eq!(sealed.end_entry_id, Some(EntryId(42000)));
        let new_seg = &range.segments[&SegmentId(1)];
        assert_eq!(new_seg.start_entry_id, EntryId(42001));
    }

    // --- D3: end-offset correction ---

    #[test]
    fn end_offset_correction_updates_placeholder() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        // Death-triggered roll with end_entry_id=0 (placeholder)
        let _ = apply_command(
            &mut sm,
            MetadataCommand::RollSegment(RollSegment {
                segment_key: SegmentKey::new(tid, RangeId(0), SegmentId(0)),
                sealed_at: 2000,
                new_replica_set: replica_set(),
                end_entry_id: None,
                intent: SegmentRollIntent::Recovery,
            }),
        );

        assert_eq!(
            sm.get_topic(&tid).unwrap().ranges[&RangeId(0)].segments[&SegmentId(0)].end_entry_id,
            None
        );

        // Segment leader's RollSegment arrives with correct end_entry_id
        let result = apply_command(
            &mut sm,
            MetadataCommand::RollSegment(RollSegment {
                segment_key: SegmentKey::new(tid, RangeId(0), SegmentId(0)),
                sealed_at: 2500,
                new_replica_set: replica_set(),
                end_entry_id: Some(EntryId(42000)),
                intent: SegmentRollIntent::Recovery,
            }),
        );
        assert!(result.is_ok());

        let range = &sm.get_topic(&tid).unwrap().ranges[&RangeId(0)];
        assert_eq!(
            range.segments[&SegmentId(0)].end_entry_id,
            Some(EntryId(42000))
        );
        assert_eq!(range.segments[&SegmentId(1)].start_entry_id, EntryId(42001));
    }

    #[test]
    fn end_offset_correction_rejected_when_already_set() {
        let mut sm = MetadataState::new(ShardGroupId(0));
        let tid = create_topic(&mut sm, "blue");

        // Normal roll with actual end_entry_id
        let _ = apply_command(
            &mut sm,
            MetadataCommand::RollSegment(RollSegment {
                segment_key: SegmentKey::new(tid, RangeId(0), SegmentId(0)),
                sealed_at: 2000,
                new_replica_set: replica_set(),
                end_entry_id: Some(EntryId(1000)),
                intent: SegmentRollIntent::DataPressure,
            }),
        );

        // Duplicate roll is rejected (end_offset already set)
        let result = apply_command(
            &mut sm,
            MetadataCommand::RollSegment(RollSegment {
                segment_key: SegmentKey::new(tid, RangeId(0), SegmentId(0)),
                sealed_at: 2500,
                new_replica_set: replica_set(),
                end_entry_id: Some(EntryId(42000)),
                intent: SegmentRollIntent::DataPressure,
            }),
        );
        assert_eq!(result, Ok(vec![]));
    }
}
