use bincode::{Decode, Encode};

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

#[allow(dead_code)]
pub enum MultiRaftCommand {
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

#[allow(dead_code)]
pub(crate) enum MultiRaftReply {
    None,
    GetLeader(Option<NodeId>),
    Propose(Result<(), ProposeError>),
}

/// Commands sent from MultiRaftActor to RaftTransportActor.
#[derive(Debug)]
pub enum RaftTransportCommand {
    Send(Vec<OutboundRaftPacket>),
    DisconnectPeer(NodeId),
}
