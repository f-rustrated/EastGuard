use crate::control_plane::metadata::TopicId;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MetadataError {
    #[error("topic not found: {0:?}")]
    TopicNotFound(TopicId),
    #[error("topic name not found: {0}")]
    TopicNameNotFound(String),
    #[error("topic name already exists: {0}")]
    TopicNameAlreadyExists(String),
    #[error("topic not active: {0:?}")]
    TopicNotActive(TopicId),
    #[error("range not found")]
    RangeNotFound,
    #[error("range not active")]
    RangeNotActive,
    #[error("segment not found")]
    SegmentNotFound,
    #[error("segment not active")]
    SegmentNotActive,
    #[error("segment not sealed")]
    SegmentNotSealed,
    #[error("split not allowed for topic: {0:?}")]
    SplitNotAllowed(TopicId),
    #[error("ranges not adjacent")]
    RangesNotAdjacent,
    #[error("invalid split point")]
    InvalidSplitPoint,
}
