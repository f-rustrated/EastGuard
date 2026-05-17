use tokio::sync::oneshot;

use crate::clusters::NodeId;
use crate::clusters::swims::ShardGroupId;

use super::command::{MultiRaftCommand, ProposeError, RaftCommand};
use super::timer::RaftTimeoutCallback;

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
        shard_group_id: ShardGroupId,
        command: RaftCommand,
        reply: oneshot::Sender<Result<(), ProposeError>>,
    },
    /// Query all topic names from all shard groups on this node.
    GetTopics {
        reply: oneshot::Sender<Vec<String>>,
    },
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

pub(crate) enum DeferredReply {
    GetLeader(oneshot::Sender<Option<NodeId>>, Option<NodeId>),
    Propose(oneshot::Sender<Result<(), ProposeError>>, Result<(), ProposeError>),
    GetTopics(oneshot::Sender<Vec<String>>, Vec<String>),
}
