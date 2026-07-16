use std::collections::HashSet;

use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::{NodeId, Replicas};
use crate::data_plane::SegmentKey;
use crate::data_plane::consumer_offset_management::ledger::{
    ConsumerOffsetUpdate, EpochSeal, OffsetRecord,
};
use crate::data_plane::messages::command::{
    CommitConsumerOffset, ConsumerOffsetCommitAck, ConsumerOffsetSnapshotInstalled,
};
use crate::impl_from_variant;

use borsh::{BorshDeserialize, BorshSerialize};
use tokio::sync::oneshot;

pub(crate) struct PendingOffsetMutation {
    pub(crate) record: OffsetRecord,
    pub(crate) completion: OffsetMutationCompletion,
}
impl PendingOffsetMutation {
    pub(crate) fn new(
        record: impl Into<OffsetRecord>,
        completion: impl Into<OffsetMutationCompletion>,
    ) -> Self {
        Self {
            record: record.into(),
            completion: completion.into(),
        }
    }
}

impl From<ReplicateConsumerOffset> for PendingOffsetMutation {
    fn from(cmd: ReplicateConsumerOffset) -> Self {
        Self::new(OffsetRecord::OffsetCommit(cmd.update.clone()), cmd)
    }
}
impl From<EpochSeal> for PendingOffsetMutation {
    fn from(cmd: EpochSeal) -> Self {
        PendingOffsetMutation::new(
            OffsetRecord::EpochSeal(cmd.clone()),
            OffsetMutationCompletion::EpochSeal,
        )
    }
}

pub(crate) struct LeaderOffsetCommitApplied {
    pub(crate) replica_set: Replicas,
    pub(crate) required_followers: HashSet<NodeId>,
    pub(crate) reply: oneshot::Sender<ConsumerOffsetCommitAck>,
}

pub(crate) enum OffsetMutationCompletion {
    EpochSeal,
    LeaderCommit(LeaderOffsetCommitApplied),
    ReplicaCommit(ReplicateConsumerOffset),
    Bootstrap(ConsumerOffsetSnapshotInstalled),
}

impl_from_variant!(
    OffsetMutationCompletion,
    LeaderCommit(LeaderOffsetCommitApplied),
    ReplicaCommit(ReplicateConsumerOffset),
    Bootstrap(ConsumerOffsetSnapshotInstalled)
);

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ReplicateConsumerOffset {
    pub seq: u64,
    pub replica_set: Replicas,
    pub update: ConsumerOffsetUpdate,
}

pub(crate) enum FutureOffsetCommit {
    Client(CommitConsumerOffset),
    Replica(ReplicateConsumerOffset),
}

pub(crate) struct OffsetPlacement {
    pub(crate) segment_key: SegmentKey,
    pub(crate) shard_group_id: ShardGroupId,
    pub(crate) leader: NodeId,
    pub(crate) replicas: Replicas,
    pub(crate) ready_replicas: HashSet<NodeId>,
    pub(crate) bootstrap_acked: HashSet<NodeId>,
    pub(crate) placement_ack_sent: bool,
}

impl OffsetPlacement {
    pub(crate) fn compare(
        &self,
        other_key: &SegmentKey,
        other: &Replicas,
    ) -> Option<FollowerPlacementObservation> {
        if self.segment_key == *other_key {
            if &self.replicas != other {
                tracing::warn!(
                    ?other_key,
                    current_replicas = ?self.replicas,
                    observed_replicas = ?other,
                    "ignored conflicting replica set for an existing segment"
                );
            }

            return Some(if &self.replicas == other {
                // No need to place again - idempotency
                FollowerPlacementObservation::Unchanged
            } else {
                FollowerPlacementObservation::Conflict
            });
        }
        if self.segment_key.segment_id >= other_key.segment_id {
            return Some(FollowerPlacementObservation::Stale);
        }
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FollowerPlacementObservation {
    Accepted,
    Unchanged,
    Stale,
    Conflict,
}
