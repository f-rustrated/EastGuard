use bincode::{Decode, Encode};

use crate::clusters::NodeId;
use crate::clusters::metadata::command::{
    CreateTopic, DeleteTopic, MergeRange, SealSegment, SplitRange,
};
use crate::clusters::metadata::strategy::StoragePolicy;
use crate::clusters::metadata::{RangeId, SegmentId, TopicId};
use crate::clusters::raft::messages::RaftCommand;

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
}

#[derive(Decode, Encode)]
pub struct ProposeRequest {
    pub resource_key: Vec<u8>,
    pub command: ClientCommand,
}

#[derive(Decode, Encode)]
pub enum ClientCommand {
    CreateTopic {
        name: String,
        storage_policy: StoragePolicy,
    },
    SealSegment {
        topic_id: TopicId,
        range_id: RangeId,
        segment_id: SegmentId,
    },
    SplitRange {
        topic_id: TopicId,
        range_id: RangeId,
        split_point: Vec<u8>,
    },
    MergeRange {
        topic_id: TopicId,
        range_id_1: RangeId,
        range_id_2: RangeId,
    },
    DeleteTopic {
        topic_id: TopicId,
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
            Self::SealSegment {
                topic_id,
                range_id,
                segment_id,
            } => SealSegment {
                topic_id,
                range_id,
                segment_id,
                sealed_at: now_ms,
                new_replica_set: replica_set,
            }
            .into(),
            Self::SplitRange {
                topic_id,
                range_id,
                split_point,
            } => SplitRange {
                topic_id,
                range_id,
                split_point,
                created_at: now_ms,
                left_replica_set: replica_set.clone(),
                right_replica_set: replica_set,
            }
            .into(),
            Self::MergeRange {
                topic_id,
                range_id_1,
                range_id_2,
            } => MergeRange {
                topic_id,
                range_id_1,
                range_id_2,
                created_at: now_ms,
                merged_replica_set: replica_set,
            }
            .into(),
            Self::DeleteTopic { topic_id } => DeleteTopic { topic_id }.into(),
        };
        RaftCommand::Metadata(meta)
    }
}

#[derive(Decode, Encode)]
pub enum ProposeResponse {
    Success,
    NotLeader,
    ShardNotFound,
    Error(String),
}
