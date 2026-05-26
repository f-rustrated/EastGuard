use crate::clusters::NodeId;
use crate::clusters::metadata::{RangeId, SegmentId, TopicId};
use crate::impl_from_variant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicCreated {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub segment_id: SegmentId,
    pub replica_set: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRolled {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub new_segment_id: SegmentId,
    pub new_replica_set: Vec<NodeId>,
    pub end_entry_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeSplit {
    pub topic_id: TopicId,
    pub children: [(RangeId, SegmentId, Vec<NodeId>); 2],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeMerged {
    pub topic_id: TopicId,
    pub merged_range_id: RangeId,
    pub segment_id: SegmentId,
    pub replica_set: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyResult {
    TopicCreated(TopicCreated),
    SegmentRolled(SegmentRolled),
    RangeSplit(RangeSplit),
    RangeMerged(RangeMerged),
    TopicDeleted,
    Noop,
}

impl_from_variant!(
    ApplyResult,
    TopicCreated,
    SegmentRolled,
    RangeSplit,
    RangeMerged
);
