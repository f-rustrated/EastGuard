#![allow(dead_code)]

use bincode::{Decode, Encode};
use tokio::sync::oneshot;

use crate::clusters::NodeId;
use crate::clusters::metadata::command::MetadataCommand;
use crate::clusters::raft::messages::rpc::{OutboundRaftPacket, RaftRpc};
use crate::clusters::raft::messages::timer::RaftTimeoutCallback;
use crate::clusters::swims::{ShardGroup, ShardGroupId};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum RaftCommand {
    Noop,
    Metadata(MetadataCommand),
}

#[derive(Debug, PartialEq, Eq, Decode, Encode)]
pub enum ProposeError {
    NotLeader(Option<NodeId>),
    ShardNotFound,
}

pub(crate) enum MultiRaftCommand {
    PacketReceived {
        shard_group_id: ShardGroupId,
        from: NodeId,
        rpc: RaftRpc,
    },
    Timeout(RaftTimeoutCallback),
    EnsureGroup {
        group: ShardGroup,
    },
    RemoveGroup {
        group_id: ShardGroupId,
    },
    GetLeader {
        group_id: ShardGroupId,
    },
    Propose {
        shard_group_id: ShardGroupId,
        command: RaftCommand,
    },
    HandleNodeDeath {
        dead_node_id: NodeId,
    },
    HandleNodeJoin {
        new_node_id: NodeId,
        affected_groups: Vec<ShardGroup>,
    },
}

pub(crate) enum MultiRaftReply {
    None,
    GetLeader(Option<NodeId>),
    Propose(Result<(), ProposeError>),
}

/// Commands received by the MultiRaftActor from external sources.
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

pub(crate) type OnReply = Box<dyn FnOnce(MultiRaftReply) + Send>;

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

/// Commands sent from MultiRaftActor to RaftTransportActor.
#[derive(Debug)]
pub enum RaftTransportCommand {
    Send(Vec<OutboundRaftPacket>),
    DisconnectPeer(NodeId),
}
