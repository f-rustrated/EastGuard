use tokio::sync::oneshot;

use crate::clusters::NodeId;
use crate::clusters::swims::ShardGroupId;

use super::command::{MultiRaftCommand, MultiRaftReply, ProposeError, RaftCommand};
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

pub type OnReply = Box<dyn FnOnce(MultiRaftReply) + Send>;

impl MultiRaftActorCommand {
    pub(crate) fn split(self) -> (MultiRaftCommand, Option<OnReply>) {
        match self {
            Self::Command(cmd) => (cmd, None),
            Self::GetLeader { group_id, reply } => (
                MultiRaftCommand::GetLeader { group_id },
                Some(Box::new(move |r| {
                    if let MultiRaftReply::GetLeader(v) = r {
                        let _ = reply.send(v);
                    }
                })),
            ),
            Self::Propose {
                shard_group_id,
                command,
                reply,
            } => (
                MultiRaftCommand::Propose {
                    shard_group_id,
                    command,
                },
                Some(Box::new(move |r| {
                    if let MultiRaftReply::Propose(v) = r {
                        let _ = reply.send(v);
                    }
                })),
            ),
        }
    }
}
