use crate::control_plane::membership::topology::ShardLeaderEntry;
use crate::control_plane::membership::{ShardGroup, ShardGroupId};
use crate::control_plane::{NodeAddress, NodeId, SwimNode};

use super::command::{SwimCommand, SwimTimeOutCallback};

/// Actor-level command envelope. Lives only in east-guard (tokio-dependent).
#[derive(Debug)]
pub(crate) enum SwimActorCommand {
    Command(SwimCommand),
    Query(QueryCommand),
}

#[derive(Debug)]
pub enum QueryCommand {
    GetMembers {
        reply: tokio::sync::oneshot::Sender<Vec<SwimNode>>,
    },
    ResolveAddress {
        node_id: NodeId,
        reply: tokio::sync::oneshot::Sender<Option<NodeAddress>>,
    },
    ResolveShardGroup {
        key: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<Option<ShardGroup>>,
    },
    GetShardInfo {
        key: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<Option<(ShardGroup, Option<ShardLeaderEntry>)>>,
    },
    ResolveShardLeader {
        shard_group_id: ShardGroupId,
        reply: tokio::sync::oneshot::Sender<Option<ShardLeaderEntry>>,
    },
}

impl From<SwimCommand> for SwimActorCommand {
    fn from(cmd: SwimCommand) -> Self {
        SwimActorCommand::Command(cmd)
    }
}

impl From<SwimTimeOutCallback> for SwimActorCommand {
    fn from(cb: SwimTimeOutCallback) -> Self {
        SwimActorCommand::Command(SwimCommand::Timeout(cb))
    }
}

impl From<QueryCommand> for SwimActorCommand {
    fn from(q: QueryCommand) -> Self {
        SwimActorCommand::Query(q)
    }
}
