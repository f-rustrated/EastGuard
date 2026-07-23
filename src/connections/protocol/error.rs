use borsh::{BorshDeserialize, BorshSerialize};

use crate::connections::protocol::data_plane::ConsumerOffsetGenerationMismatch;
use crate::control_plane::NodeAddressInfo;
use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::data_plane::ProduceError;

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, thiserror::Error)]
pub enum ServerError {
    #[error("shard not local")]
    ShardNotLocal { hint_node: Option<NodeAddressInfo> },

    #[error("not metadata raft leader")]
    NotRaftLeader {
        leader_addr: Option<NodeAddressInfo>,
    },

    #[error("not write leader")]
    NotWriteLeader {
        leader_addr: Option<NodeAddressInfo>,
    },

    #[error("topic metadata redirect")]
    TopicMetadataRedirect { owner: NodeAddressInfo },

    #[error("topic not found")]
    TopicNotFound,

    #[error("topic already exists")]
    AlreadyExists,

    #[error("stale range")]
    StaleRange,

    #[error("produce rejected: {0}")]
    ProduceRejected(ProduceError),

    #[error("entry id out of range")]
    EntryIdOutOfRange,

    #[error("keyspace bound narrowed")]
    KeyspaceBoundNarrowed,

    #[error("segment not local")]
    SegmentNotLocal,

    #[error("stale consumer group epoch: {0:?}")]
    StaleConsumerGroupEpoch(GenerationId),

    #[error("consumer offset generation mismatch")]
    ConsumerOffsetGenerationMismatch(ConsumerOffsetGenerationMismatch),

    #[error("invalid split point")]
    InvalidSplitPoint,

    #[error("internal server error: {0}")]
    Internal(String),
}

impl ServerError {
    pub fn is_routing_error(&self) -> bool {
        matches!(
            self,
            ServerError::ShardNotLocal { .. }
                | ServerError::NotRaftLeader { .. }
                | ServerError::NotWriteLeader { .. }
                | ServerError::TopicMetadataRedirect { .. }
                | ServerError::SegmentNotLocal
                | ServerError::StaleRange
        )
    }
}
