use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::rpc::{OutboundRaftPacket, RaftRpc};
use crate::control_plane::consensus::messages::timer::RaftTimeoutCallback;
use crate::control_plane::membership::{ShardGroup, ShardGroupId};
use crate::control_plane::metadata::command::MetadataCommand;
use crate::data_plane::messages::command::SealRequest;
use crate::impl_from_variant;

pub struct InboundRaftRpc {
    pub shard_group_id: ShardGroupId,
    pub from: NodeId,
    pub rpc: RaftRpc,
}

pub struct EnsureGroup {
    pub group: ShardGroup,
}

pub struct RemoveGroup {
    pub group_id: ShardGroupId,
}

pub struct MetadataProposal {
    pub shard_group_id: ShardGroupId,
    pub command: MetadataCommand,
}

pub struct HandleNodeJoin {
    pub new_node_id: NodeId,
}

pub enum RaftProtocolMessage {
    InboundRaftRpc(InboundRaftRpc),
    Timeout(RaftTimeoutCallback),
    EnsureGroup(EnsureGroup),
    RemoveGroup(RemoveGroup),
    HandleNodeDeath(NodeId),
    HandleNodeJoin(HandleNodeJoin),
}

impl_from_variant!(
    RaftProtocolMessage,
    InboundRaftRpc(InboundRaftRpc),
    Timeout(RaftTimeoutCallback),
    EnsureGroup,
    RemoveGroup,
    HandleNodeDeath(NodeId),
    HandleNodeJoin
);

/// Commands sent from MultiRaftActor to RaftTransportActor.
#[derive(Debug)]
pub enum RaftTransportCommand {
    Send(Box<[OutboundRaftPacket]>),
    DisconnectPeer(NodeId),
}

#[derive(Debug)]
pub struct CoordinatorSealRequest {
    pub request: SealRequest,
}
