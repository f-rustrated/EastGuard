use crate::clusters::metadata::TopicId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataError {
    TopicNotFound(TopicId),
    TopicNameNotFound(String),
    TopicNameAlreadyExists(String),
    TopicNotActive(TopicId),
    RangeNotFound,
    RangeNotActive,
    SegmentNotFound,
    SegmentNotActive,
    SplitNotAllowed(TopicId),
    RangesNotAdjacent,
    InvalidSplitPoint,
}
