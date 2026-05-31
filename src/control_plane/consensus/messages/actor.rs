use tokio::sync::oneshot;

use crate::control_plane::NodeId;
use crate::control_plane::membership::{NodeDead, ShardGroupId};
use crate::control_plane::metadata::types::TopicStats;

use super::command::{
    ConsensusCommand, CoordinatorSealRequest, EnsureGroup, GroupStatus, HandleNodeJoin,
    PacketReceived, ProposeError, RaftPropose, RemoveGroup, TopicDetailQueryResult,
};
use super::timer::RaftTimeoutCallback;
use crate::impl_from_variant_via;

/// Commands received by the MultiRaftActor from external sources (tokio-dependent).
///
/// Split from [`MultiRaftCommand`] because query variants carry `oneshot::Sender` reply
/// channels — tokio types that cannot live inside the pure sync state machine.
#[allow(dead_code)]
pub enum MultiRaftActorCommand {
    /// Fire-and-forget: no reply channel needed.
    ConsensusCommand(ConsensusCommand),
    /// Query the current leader of a shard group.
    GetLeader {
        group_id: ShardGroupId,
        reply: oneshot::Sender<Option<NodeId>>,
    },
    /// Propose a command to a shard group's Raft log. Leader-only.
    Propose {
        propose: RaftPropose,
        reply: oneshot::Sender<Result<(), ProposeError>>,
    },
    /// Query all topic names from all shard groups on this node.
    GetTopics { reply: oneshot::Sender<Vec<String>> },
    /// Query per-topic stats from all shard groups on this node.
    GetTopicStats {
        reply: oneshot::Sender<Vec<TopicStats>>,
    },
    /// Data plane SealRequest forwarded to coordinator for Raft proposal.
    Coordinator(CoordinatorSealRequest),
    /// Used by `ClientHandler` to decide whether to handle a write locally
    /// (this node is leader) or return `NotLeader` so the client can redirect.
    GetGroupStatus {
        shard_group_id: ShardGroupId,
        reply: oneshot::Sender<GroupStatus>,
    },
    /// Serves two callers: `DescribeTopic` reads (any replica) and the
    /// `AlreadyExists` pre-check before proposing `CreateTopic`.
    GetTopicDetail {
        shard_group_id: ShardGroupId,
        topic_name: String,
        reply: oneshot::Sender<TopicDetailQueryResult>,
    },
    /// Returns true when every shard group on this node has a known Raft leader.
    /// Used by test helpers to detect cluster readiness without a write probe.
    #[cfg(test)]
    IsReady {
        reply: oneshot::Sender<bool>,
    },
}

impl From<ConsensusCommand> for MultiRaftActorCommand {
    fn from(cmd: ConsensusCommand) -> Self {
        MultiRaftActorCommand::ConsensusCommand(cmd)
    }
}

impl From<RaftTimeoutCallback> for MultiRaftActorCommand {
    fn from(cb: RaftTimeoutCallback) -> Self {
        MultiRaftActorCommand::ConsensusCommand(ConsensusCommand::Timeout(cb))
    }
}

impl_from_variant_via!(
    MultiRaftActorCommand,
    ConsensusCommand,
    PacketReceived,
    EnsureGroup,
    RemoveGroup,
    NodeDead,
    HandleNodeJoin,
);

/// Pairs a reply channel with its computed value so the actor can send all replies
/// after draining outbound packets and timer commands — the flush protocol requires
/// side effects to leave the state machine before any caller is unblocked.
pub(crate) enum DeferredReply {
    GetLeader(oneshot::Sender<Option<NodeId>>, Option<NodeId>),
    Propose(
        oneshot::Sender<Result<(), ProposeError>>,
        Result<(), ProposeError>,
    ),
    GetTopics(oneshot::Sender<Vec<String>>, Vec<String>),
    GetTopicStats(oneshot::Sender<Vec<TopicStats>>, Vec<TopicStats>),
    GetGroupStatus(oneshot::Sender<GroupStatus>, GroupStatus),
    GetTopicDetail(oneshot::Sender<TopicDetailQueryResult>, TopicDetailQueryResult),
    #[cfg(test)]
    IsReady(oneshot::Sender<bool>, bool),
}