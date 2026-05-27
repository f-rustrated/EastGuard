use tokio::sync::oneshot;

use crate::control_plane::NodeId;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::types::TopicStats;

use super::command::{
    CoordinatorCommand, EnsureGroup, HandleNodeDeath, HandleNodeJoin, MultiRaftCommand,
    PacketReceived, ProposeError, RaftPropose, RemoveGroup,
};
use super::timer::RaftTimeoutCallback;
use crate::impl_from_variant_via;

/// Commands received by the MultiRaftActor from external sources (tokio-dependent).
#[allow(dead_code)]
pub enum MultiRaftActorCommand {
    /// Fire-and-forget: no reply channel needed.
    Command(MultiRaftCommand),
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

impl From<MultiRaftCommand> for MultiRaftActorCommand {
    fn from(cmd: MultiRaftCommand) -> Self {
        MultiRaftActorCommand::Command(cmd)
    }
}

impl From<RaftTimeoutCallback> for MultiRaftActorCommand {
    fn from(cb: RaftTimeoutCallback) -> Self {
        MultiRaftActorCommand::Command(MultiRaftCommand::Timeout(cb))
    }
}

impl_from_variant_via!(
    MultiRaftActorCommand,
    MultiRaftCommand,
    PacketReceived,
    EnsureGroup,
    RemoveGroup,
    HandleNodeDeath,
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
