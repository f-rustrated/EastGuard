use bincode::{Decode, Encode};

use crate::{
    clusters::{
        NodeId,
        metadata::{RangeId, SegmentId, TopicId, strategy::StoragePolicy},
    },
    impl_from_variant,
};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct CreateTopic {
    pub name: String,
    pub storage_policy: StoragePolicy,
    pub replica_set: Vec<NodeId>,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SealSegment {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub segment_id: SegmentId,
    pub sealed_at: u64,
    pub new_replica_set: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SplitRange {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub split_point: Vec<u8>,
    pub created_at: u64,
    pub left_replica_set: Vec<NodeId>,
    pub right_replica_set: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct MergeRange {
    pub topic_id: TopicId,
    pub range_id_1: RangeId,
    pub range_id_2: RangeId,
    pub created_at: u64,
    pub merged_replica_set: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DeleteTopic {
    pub topic_id: TopicId,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum MetadataCommand {
    CreateTopic(CreateTopic),
    SealSegment(SealSegment),
    SplitRange(SplitRange),
    MergeRange(MergeRange),
    DeleteTopic(DeleteTopic),
}

impl_from_variant!(
    MetadataCommand,
    CreateTopic,
    SealSegment,
    SplitRange,
    MergeRange,
    DeleteTopic
);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyResult {
    TopicCreated(TopicId),
    SegmentSealed,
    RangeSplit(RangeId, RangeId),
    RangeMerged(RangeId),
    TopicDeleted,
}
