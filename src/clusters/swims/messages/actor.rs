use crate::clusters::swims::topology::ShardLeaderEntry;
use crate::clusters::swims::{ShardGroup, ShardGroupId};
use crate::clusters::{NodeAddress, NodeId, SwimNode};

use super::command::{SwimCommand, SwimTimeOutCallback};

/// Actor-level command envelope. Lives only in east-guard (tokio-dependent).
#[derive(Debug)]
pub(crate) enum SwimActorCommand {
    Protocol(SwimCommand),
    Query(SwimQueryCommand),
}

#[derive(Debug)]
pub enum SwimQueryCommand {
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
    ResolveShardLeader {
        shard_group_id: ShardGroupId,
        reply: tokio::sync::oneshot::Sender<Option<ShardLeaderEntry>>,
    },
    GetShardInfo {
        key: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<Option<(ShardGroup, Option<ShardLeaderEntry>)>>,
    },
}

impl From<SwimCommand> for SwimActorCommand {
    fn from(cmd: SwimCommand) -> Self {
        SwimActorCommand::Protocol(cmd)
    }
}

impl From<SwimTimeOutCallback> for SwimActorCommand {
    fn from(cb: SwimTimeOutCallback) -> Self {
        SwimActorCommand::Protocol(SwimCommand::Timeout(cb))
    }
}

impl From<SwimQueryCommand> for SwimActorCommand {
    fn from(q: SwimQueryCommand) -> Self {
        SwimActorCommand::Query(q)
    }
}
