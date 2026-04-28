use bincode::{Decode, Encode};

use crate::clusters::NodeAddress;
use crate::clusters::NodeId;
use crate::clusters::metadata::command::{CreateTopic, DeleteTopic};
use crate::clusters::metadata::strategy::StoragePolicy;
use crate::clusters::raft::messages::{ProposeError, RaftCommand};

#[derive(Decode, Encode)]
pub enum ConnectionRequests {
    Connection(ConnectionRequest),
    Query(QueryCommand),
    Propose(ProposeRequest),
}

#[derive(Decode, Encode)]
pub struct ConnectionRequest {}

#[allow(unused)]
#[derive(Decode, Encode)]
pub struct SessionRequest {}

#[derive(Decode, Encode)]
pub enum QueryCommand {
    GetMembers,
    GetShardInfo { key: Vec<u8> },
}

#[derive(Debug, Decode, Encode)]
pub struct ShardInfoResponse {
    pub shard_group_id: u64,
    pub leader_node_id: Option<String>,
    pub leader_addr: Option<NodeAddress>,
}

#[derive(Decode, Encode, Clone)]
pub struct ProposeRequest {
    pub resource_key: Vec<u8>,
    pub command: ClientCommand,
    pub forwarded: bool,
}

#[derive(Clone, Decode, Encode)]
pub enum ClientCommand {
    CreateTopic {
        name: String,
        storage_policy: StoragePolicy,
    },
    DeleteTopic {
        name: String,
    },
}

impl ClientCommand {
    pub fn into_raft_command(self, replica_set: Vec<NodeId>) -> RaftCommand {
        let now_ms: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_millis() as u64;

        let meta = match self {
            Self::CreateTopic {
                name,
                storage_policy,
            } => CreateTopic {
                name,
                storage_policy,
                replica_set,
                created_at: now_ms,
            }
            .into(),
            Self::DeleteTopic { name } => DeleteTopic { name }.into(),
        };
        RaftCommand::Metadata(meta)
    }
}

#[derive(Debug, Decode, Encode)]
pub enum ProposeResponse {
    Success,
    Error(ProposeError),
}
