use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::SealContext;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
use crate::data_plane::SegmentKey;
use crate::data_plane::messages::command::{SealResponse, SegmentAssignment};
use crate::data_plane::transport::command::DataTransportCommand;
use crate::impl_from_variant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicCreated {
    pub segment_key: SegmentKey,
    pub replica_set: Vec<NodeId>,
}
impl TopicCreated {
    pub fn into_command(self, shard_group_id: ShardGroupId) -> DataTransportCommand {
        DataTransportCommand::send_to_targets(
            vec![self.replica_set[0].clone()],
            SegmentAssignment {
                segment_key: self.segment_key,
                shard_group_id,
                replica_set: self.replica_set,
                start_entry_id: 0,
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRolled {
    pub new_segment_key: SegmentKey,
    pub new_replica_set: Vec<NodeId>,
    pub end_entry_id: Option<u64>,
}

impl SegmentRolled {
    pub fn into_command(
        self,
        ctx: Option<SealContext>,
        shard_group_id: ShardGroupId,
    ) -> Vec<DataTransportCommand> {
        let start = self.end_entry_id.map_or(0, |id| id + 1);
        let mut v = vec![DataTransportCommand::send_to_targets(
            vec![self.new_replica_set[0].clone()],
            SegmentAssignment {
                segment_key: self.new_segment_key,
                shard_group_id,
                replica_set: self.new_replica_set.clone(),
                start_entry_id: start,
            },
        )];

        if let Some(ctx) = ctx {
            v.push(DataTransportCommand::send_to_targets(
                vec![ctx.requester],
                SealResponse {
                    old_segment_key: ctx.old_segment_key,
                    new_segment_id: self.new_segment_key.segment_id,
                    new_replica_set: self.new_replica_set,
                },
            ));
        }
        v
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeSplit {
    pub topic_id: TopicId,
    pub children: [(RangeId, SegmentId, Vec<NodeId>); 2],
}

impl RangeSplit {
    pub fn into_command(self, shard_group_id: ShardGroupId) -> Vec<DataTransportCommand> {
        self.children
            .into_iter()
            .map(|(range_id, segment_id, replica_set)| {
                let target = replica_set[0].clone();
                DataTransportCommand::send_to_targets(
                    vec![target],
                    SegmentAssignment {
                        segment_key: SegmentKey::new(self.topic_id, range_id, segment_id),
                        shard_group_id,
                        replica_set,
                        start_entry_id: 0,
                    },
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeMerged {
    pub segment_key: SegmentKey,
    pub replica_set: Vec<NodeId>,
}
impl RangeMerged {
    pub fn into_command(self, shard_group_id: ShardGroupId) -> DataTransportCommand {
        let target = self.replica_set[0].clone();

        DataTransportCommand::send_to_targets(
            vec![target],
            SegmentAssignment {
                segment_key: self.segment_key,
                shard_group_id,
                replica_set: self.replica_set,
                start_entry_id: 0,
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyResult {
    TopicCreated(TopicCreated),
    SegmentRolled(SegmentRolled),
    RangeSplit(RangeSplit),
    RangeMerged(RangeMerged),
    TopicDeleted,
    Noop,
}

impl_from_variant!(
    ApplyResult,
    TopicCreated,
    SegmentRolled,
    RangeSplit,
    RangeMerged
);
