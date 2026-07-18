use crate::control_plane::Replicas;
use crate::control_plane::consensus::multi_raft::RollRequestContext;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::control_plane::metadata::{EntryId, RangeId, SegmentId, TopicId};
use crate::data_plane::SegmentKey;
use crate::data_plane::consumer_offset_management::ledger::{ConsumerOffsetKey, EpochSeal};
use crate::data_plane::messages::command::{
    AssignSegmentCatchUp, DataPlanePeerMessage, DeleteSegments, PlaceSegment, SegmentMetaSealed,
    SegmentRollCommitted,
};
use crate::data_plane::transport::command::DataTransportCommand;
use crate::impl_from_variant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicCreated {
    pub segment_key: SegmentKey,
    pub replica_set: Replicas,
}
impl TopicCreated {
    pub fn into_command(self, shard_group_id: ShardGroupId) -> DataTransportCommand {
        DataTransportCommand::send_to_targets(
            vec![self.replica_set[0].clone()],
            PlaceSegment {
                segment_key: self.segment_key,
                shard_group_id,
                replica_set: self.replica_set,
                start_entry_id: EntryId::MIN,
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRolled {
    pub new_segment_key: SegmentKey,
    pub new_replica_set: Replicas,
    pub end_entry_id: Option<EntryId>,
    pub consumer_group_epochs: Box<[ConsumerGroupEpochSnapshot]>,
}

impl SegmentRolled {
    pub fn into_command(
        self,
        ctx: Option<RollRequestContext>,
        shard_group_id: ShardGroupId,
    ) -> Vec<DataTransportCommand> {
        let start = self.end_entry_id.map_or(EntryId::MIN, |id| id + 1);
        let mut v = vec![DataTransportCommand::send_to_targets(
            vec![self.new_replica_set[0].clone()],
            PlaceSegment {
                segment_key: self.new_segment_key,
                shard_group_id,
                replica_set: self.new_replica_set.clone(),
                start_entry_id: start,
            },
        )];

        if let Some(ctx) = ctx {
            v.push(DataTransportCommand::send_to_targets(
                vec![ctx.requester],
                SegmentRollCommitted {
                    old_segment_key: ctx.segment_key,
                    new_segment_id: self.new_segment_key.segment_id,
                    new_replica_set: self.new_replica_set,
                },
            ));
        }
        for epoch in self.consumer_group_epochs {
            v.extend(epoch.into_commands());
        }
        v
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReassigned {
    pub segment_key: SegmentKey,
    pub start_entry_id: EntryId,
    /// Committed end = the catch-up target. `None` only for a segment whose end
    /// was never established; those aren't selected for reassignment, so the
    /// dispatch just emits nothing.
    pub sealed_end: Option<EntryId>,
    pub new_replica_set: Replicas,
}

impl SegmentReassigned {
    pub fn into_catch_up_commands(self, shard_group_id: ShardGroupId) -> Vec<DataTransportCommand> {
        let Some(sealed_end) = self.sealed_end else {
            return vec![];
        };
        self.new_replica_set
            .iter()
            .map(|member| {
                DataTransportCommand::send_to_targets(
                    vec![member.clone()],
                    AssignSegmentCatchUp {
                        segment_key: self.segment_key,
                        shard_group_id,
                        start_entry_id: self.start_entry_id,
                        sealed_end_entry_id: sealed_end,
                        replica_set: self.new_replica_set.clone(),
                    },
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeSplit {
    pub topic_id: TopicId,
    pub children: [(RangeId, SegmentId, Replicas); 2],
    pub parent_active_segment: Option<(SegmentKey, Replicas)>,
    pub consumer_group_epochs: Box<[ConsumerGroupEpochSnapshot]>,
}

impl RangeSplit {
    pub fn into_command(self, shard_group_id: ShardGroupId) -> Vec<DataTransportCommand> {
        let mut cmds: Vec<DataTransportCommand> = self
            .children
            .into_iter()
            .map(|(range_id, segment_id, replica_set)| {
                let target = replica_set[0].clone();
                DataTransportCommand::send_to_targets(
                    vec![target],
                    PlaceSegment {
                        segment_key: SegmentKey::new(self.topic_id, range_id, segment_id),
                        shard_group_id,
                        replica_set,
                        start_entry_id: EntryId::MIN,
                    },
                )
            })
            .collect();

        // sealing parent segment
        if let Some((parent_key, parent_replica_set)) = self.parent_active_segment
            && !parent_replica_set.is_empty()
        {
            cmds.push(DataTransportCommand::send_to_targets(
                parent_replica_set.0,
                SegmentMetaSealed {
                    segment_key: parent_key,
                    committed_entry_id: None,
                },
            ));
        }

        for epoch in self.consumer_group_epochs {
            cmds.extend(epoch.into_commands());
        }

        cmds
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeMerged {
    pub segment_key: SegmentKey,
    pub replica_set: Replicas,
    pub consumer_group_epochs: Box<[ConsumerGroupEpochSnapshot]>,
}
impl RangeMerged {
    pub fn into_commands(self, shard_group_id: ShardGroupId) -> Vec<DataTransportCommand> {
        let target = self.replica_set[0].clone();
        let mut commands = vec![DataTransportCommand::send_to_targets(
            vec![target],
            PlaceSegment {
                segment_key: self.segment_key,
                shard_group_id,
                replica_set: self.replica_set,
                start_entry_id: EntryId::MIN,
            },
        )];
        for epoch in self.consumer_group_epochs {
            commands.extend(epoch.into_commands());
        }
        commands
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupEpochSnapshot {
    pub topic_id: TopicId,
    pub group_id: String,
    pub generation: GenerationId,
    pub ranges: Box<[(RangeId, Replicas)]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicDeleted {
    pub consumer_group_epochs: Box<[ConsumerGroupEpochSnapshot]>,
}

impl TopicDeleted {
    pub fn into_commands(self) -> Vec<DataTransportCommand> {
        self.consumer_group_epochs
            .into_vec()
            .into_iter()
            .flat_map(ConsumerGroupEpochSnapshot::into_commands)
            .collect()
    }
}

impl ConsumerGroupEpochSnapshot {
    pub fn into_commands(self) -> Vec<DataTransportCommand> {
        self.ranges
            .into_iter()
            .filter(|(_, replicas)| !replicas.is_empty())
            .map(|(range_id, replicas)| {
                DataTransportCommand::send_to_targets(
                    replicas.0,
                    DataPlanePeerMessage::ConsumerGroupEpochSealed(EpochSeal {
                        key: ConsumerOffsetKey {
                            topic_id: self.topic_id,
                            range_id,
                            group_id: self.group_id.clone(),
                        },
                        generation: self.generation,
                    }),
                )
            })
            .collect()
    }
}

/// Retention deletion (D7). The deleted segments are pre-grouped by `replica_set`
/// (in `delete_segments`, which already reads each segment's set) — one group becomes
/// one batched `DeleteSegments` to that set's nodes. Segments in a range can sit on
/// different replica sets, so there may be several groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentsDeleted {
    pub groups: Vec<(Replicas, Vec<SegmentKey>)>,
}

impl SegmentsDeleted {
    pub fn into_commands(self) -> Vec<DataTransportCommand> {
        self.groups
            .into_iter()
            .map(|(replica_set, keys)| {
                DataTransportCommand::send_to_targets(
                    replica_set.0,
                    DeleteSegments {
                        segment_keys: keys.into_boxed_slice(),
                    },
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentBoundaryCorrected {
    pub segment_key: SegmentKey,
    pub replica_set: Replicas,
    pub committed_entry_id: Option<EntryId>,
}

impl SegmentBoundaryCorrected {
    pub fn into_commands(self) -> Vec<DataTransportCommand> {
        if self.replica_set.is_empty() {
            vec![]
        } else {
            vec![DataTransportCommand::send_to_targets(
                self.replica_set.0,
                SegmentMetaSealed {
                    segment_key: self.segment_key,
                    committed_entry_id: self.committed_entry_id,
                },
            )]
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyResult {
    TopicCreated(TopicCreated),
    SegmentRolled(SegmentRolled),
    RangeSplit(RangeSplit),
    RangeMerged(RangeMerged),
    SegmentReassigned(SegmentReassigned),
    SegmentsDeleted(SegmentsDeleted),
    SegmentBoundaryCorrected(SegmentBoundaryCorrected),
    ConsumerGroupChanged(ConsumerGroupEpochSnapshot),
    TopicDeleted(TopicDeleted),
    Noop,
}

impl_from_variant!(
    ApplyResult,
    TopicCreated,
    SegmentRolled,
    RangeSplit,
    RangeMerged,
    SegmentReassigned,
    SegmentsDeleted,
    SegmentBoundaryCorrected,
    ConsumerGroupChanged(ConsumerGroupEpochSnapshot),
    TopicDeleted
);
