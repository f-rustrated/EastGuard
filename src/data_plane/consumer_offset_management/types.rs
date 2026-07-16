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
    pub segment_key: SegmentKey,
    pub replica_set: Replicas,
    pub update: ConsumerOffsetUpdate,
}

pub(crate) enum FutureOffsetCommit {
    Client(CommitConsumerOffset),
    Replica(ReplicateConsumerOffset),
}

pub(crate) struct OffsetPlacement {
    pub(crate) segment_key: SegmentKey,
    pub(crate) shard_group_id: ShardGroupId, // needed for delayed coordinator routing
    pub(crate) leader: NodeId,
    pub(crate) replicas: Replicas,
    pub(crate) ready_replicas: HashSet<NodeId>,
    pub(crate) bootstrap_acked: HashSet<NodeId>,
    pub(crate) placement_ack_sent: bool,
}

impl OffsetPlacement {
    // Ensure the requested segment is the current placement &&
    // The configured leader is offset-ready.
    pub(crate) fn can_bootstrap_replicas(&self, segment_key: &SegmentKey) -> bool {
        self.segment_key == *segment_key && self.is_ready(&self.leader)
    }

    pub(crate) fn is_ready(&self, n: &NodeId) -> bool {
        self.ready_replicas.contains(n)
    }

    pub(crate) fn is_fully_ready(&self) -> bool {
        self.ready_replicas.len() == self.replicas.len()
    }

    pub(crate) fn compare(&self, other_key: &SegmentKey, other: &Replicas) -> PlacementObservation {
        if self.segment_key == *other_key {
            if &self.replicas != other {
                tracing::warn!(
                    ?other_key,
                    current_replicas = ?self.replicas,
                    observed_replicas = ?other,
                    "ignored conflicting replica set for an existing segment"
                );
            }

            return if &self.replicas == other {
                // No need to place again - idempotency
                PlacementObservation::Unchanged
            } else {
                PlacementObservation::Conflict
            };
        }
        if self.segment_key.segment_id >= other_key.segment_id {
            return PlacementObservation::Stale;
        }
        PlacementObservation::Accepted
    }

    pub(crate) fn unready_followers(&self) -> Box<[NodeId]> {
        self.replicas
            .followers()
            .filter(|node| !self.ready_replicas.contains(*node))
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlacementObservation {
    Accepted,
    Unchanged,
    Stale,
    Conflict,
}
