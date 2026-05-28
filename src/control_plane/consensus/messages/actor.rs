use tokio::sync::oneshot;

use crate::control_plane::NodeId;
use crate::control_plane::membership::{NodeDead, ShardGroupId};
use crate::control_plane::metadata::types::TopicStats;

use super::command::{
    ConsensusCommand, CoordinatorCommand, EnsureGroup, HandleNodeJoin, PacketReceived,
    ProposeError, RaftPropose, RemoveGroup,
};
use super::timer::RaftTimeoutCallback;
use crate::impl_from_variant_via;

/// Commands received by the MultiRaftActor from external sources (tokio-dependent).
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
    Coordinator(CoordinatorCommand),
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

pub(crate) enum DeferredReply {
    GetLeader(oneshot::Sender<Option<NodeId>>, Option<NodeId>),
    Propose(
        oneshot::Sender<Result<(), ProposeError>>,
        Result<(), ProposeError>,
    ),
    GetTopics(oneshot::Sender<Vec<String>>, Vec<String>),
    GetTopicStats(oneshot::Sender<Vec<TopicStats>>, Vec<TopicStats>),
}
