use crate::control_plane::{NodeId, Replicas};
use crate::data_plane::consumer_offset_management::ledger::{ConsumerOffsetUpdate, OffsetRecord};
use crate::data_plane::messages::command::{CommitConsumerOffset, ConsumerOffsetCommitAck};
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

pub(crate) struct LeaderOffsetCommitApplied {
    pub(crate) replica_set: Replicas,
    pub(crate) reply: oneshot::Sender<ConsumerOffsetCommitAck>,
}

pub(crate) enum OffsetMutationCompletion {
    EpochSeal,
    LeaderCommit(LeaderOffsetCommitApplied),
    ReplicaCommit(ReplicaOffsetCommit),
}

impl_from_variant!(
    OffsetMutationCompletion,
    LeaderCommit(LeaderOffsetCommitApplied),
    ReplicaCommit(ReplicaOffsetCommit)
);

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ReplicaOffsetCommit {
    pub seq: u64,
    pub replica_set: Replicas,
    pub update: ConsumerOffsetUpdate,
}

pub(crate) enum FutureOffsetCommit {
    Client(CommitConsumerOffset),
    Replica(ReplicaOffsetCommit),
}

pub(crate) struct OffsetPlacement {
    pub(crate) leader: NodeId,
    pub(crate) replicas: Replicas,
}
