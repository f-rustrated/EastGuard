use crate::control_plane::NodeId;
use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
use crate::data_plane::SegmentKey;
use crate::impl_from_variant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicCreated {
    pub segment_key: SegmentKey,
    pub replica_set: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRolled {
    pub new_segment_key: SegmentKey,
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
    pub segment_key: SegmentKey,
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
