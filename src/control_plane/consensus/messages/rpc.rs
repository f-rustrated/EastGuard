use bincode::{Decode, Encode};

use crate::control_plane::NodeId;
use crate::control_plane::consensus::raft::log::LogEntry;
use crate::control_plane::membership::ShardGroupId;
use crate::impl_from_variant;

#[derive(Debug, Clone, Encode, Decode)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub node_id: NodeId,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Box<[LogEntry]>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub node_id: NodeId,
    pub success: bool,
    pub last_log_index: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum RaftRpc {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
}

impl_from_variant!(
    RaftRpc,
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse
);

#[derive(Debug)]
pub struct OutboundRaftPacket {
    pub shard_group_id: ShardGroupId,
    pub target: NodeId,
    pub rpc: RaftRpc,
}

impl OutboundRaftPacket {
    pub(crate) fn new(
        shard_group_id: ShardGroupId,
        target: NodeId,
        rpc: impl Into<RaftRpc>,
    ) -> Self {
        Self {
            shard_group_id,
            target,
            rpc: rpc.into(),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct WireRaftMessage {
    pub shard_group_id: ShardGroupId,
    pub sender: NodeId,
    pub rpc: RaftRpc,
}
