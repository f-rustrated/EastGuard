use bincode::{Decode, Encode};

use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::rpc::{OutboundRaftPacket, RaftRpc};
use crate::control_plane::consensus::messages::timer::RaftTimeoutCallback;
use crate::control_plane::membership::{ShardGroup, ShardGroupId};
use crate::control_plane::metadata::command::MetadataCommand;
use crate::data_plane::SegmentKey;
use crate::impl_from_variant;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum RaftCommand {
    Noop,
    Metadata(MetadataCommand),
}

#[derive(Debug, PartialEq, Eq, Decode, Encode)]
pub enum ProposeError {
    NotLeader(Option<NodeId>),
    ShardNotFound,
    ShardGroupRemoved,
}

pub struct PacketReceived {
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

pub struct RaftPropose {
    pub shard_group_id: ShardGroupId,
    pub command: RaftCommand,
}

pub struct HandleNodeDeath {
    pub dead_node_id: NodeId,
}

pub struct HandleNodeJoin {
    pub new_node_id: NodeId,
    pub affected_groups: Vec<ShardGroup>,
}

#[allow(dead_code)]
pub enum MultiRaftCommand {
    PacketReceived(PacketReceived),
    Timeout(RaftTimeoutCallback),
    EnsureGroup(EnsureGroup),
    RemoveGroup(RemoveGroup),
    Propose(RaftPropose),
    HandleNodeDeath(HandleNodeDeath),
    HandleNodeJoin(HandleNodeJoin),
}

impl_from_variant!(
    MultiRaftCommand,
    PacketReceived,
    Timeout(RaftTimeoutCallback),
    EnsureGroup,
    RemoveGroup,
    Propose(RaftPropose),
    HandleNodeDeath,
    HandleNodeJoin
);

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

#[allow(dead_code)]
#[derive(Debug)]
pub struct CoordinatorSealRequest {
    pub requester: NodeId,
    pub segment_key: SegmentKey,
    pub failed_nodes: Vec<NodeId>,
    pub end_entry_id: u64,
}

#[derive(Debug)]
pub enum CoordinatorCommand {
    SealRequest(CoordinatorSealRequest),
}
