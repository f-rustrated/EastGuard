use crate::clusters::metadata::{RangeId, SegmentId, TopicId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataError {
    TopicNotFound(TopicId),
    TopicNameAlreadyExists(String),
    TopicNotActive(TopicId),
    RangeNotFound(TopicId, RangeId),
    RangeNotActive(TopicId, RangeId),
    SegmentNotFound(TopicId, RangeId, SegmentId),
    SegmentNotActive(TopicId, RangeId, SegmentId),
    SplitNotAllowed(TopicId),
    RangesNotAdjacent(RangeId, RangeId),
    InvalidSplitPoint,
}
