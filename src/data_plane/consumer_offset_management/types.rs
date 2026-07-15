use crate::control_plane::NodeId;
use crate::data_plane::consumer_offset_management::ledger::OffsetRecord;
use crate::data_plane::messages::command::{
    CommitConsumerOffset, ConsumerOffsetCommitAck, ReplicaOffsetCommit,
};
use crate::impl_from_variant;

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
    pub(crate) replica_set: Vec<NodeId>,
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

pub(crate) enum FutureOffsetCommit {
    Client(CommitConsumerOffset),
    Replica(ReplicaOffsetCommit),
}

pub(crate) struct OffsetPlacement {
    pub(crate) replica_set: Vec<NodeId>,
}
