use crate::control_plane::metadata::TopicId;

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
    SegmentNotSealed,
    SplitNotAllowed(TopicId),
    RangesNotAdjacent,
    InvalidSplitPoint,
}
