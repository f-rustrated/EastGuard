use crate::clusters::NodeId;
use bincode::{Decode, Encode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct TopicId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct RangeId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct SegmentId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum TopicState {
    Active,
    Sealed,
    Deleted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum RangeState {
    Active,
    Sealed,
    Deleting,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum SegmentState {
    Active,
    Sealed,
    Reassigning { from: NodeId, to: NodeId },
    Deleting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum PartitionStrategy {
    AutoSplit,
    Fixed,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct StoragePolicy {
    /// How long sealed segments are retained before deletion, in milliseconds.
    pub retention_ms: u64,
    /// Number of replicas maintained for each segment.
    pub replication_factor: u8,
    /// How the keyspace is partitioned: size-triggered auto-split or fixed boundaries.
    pub partition_strategy: PartitionStrategy,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct TopicMeta {
    /// Unique identifier for this topic.
    pub topic_id: TopicId,
    /// Human-readable name; unique among non-deleted topics.
    pub name: String,
    /// Current lifecycle state.
    pub state: TopicState,
    /// Retention and replication settings applied to all segments under this topic.
    pub storage_policy: StoragePolicy,
    /// Ordered list of range IDs that together cover the topic's full keyspace.
    pub ranges: Vec<RangeId>,
    /// Unix timestamp (ms) when this topic was created.
    pub created_at: u64,
    /// Unix timestamp (ms) when this topic transitioned to `TopicState.Sealed`, if applicable.
    pub sealed_at: Option<u64>,
    /// Unix timestamp (ms) when this topic transitioned to `TopicState.Deleted`, if applicable.
    pub deleted_at: Option<u64>
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct RangeMeta {
    /// Unique identifier for this range.
    pub range_id: RangeId,
    /// The topic this range belongs to.
    pub topic_id: TopicId,
    /// Inclusive start of the keyspace interval covered by this range.
    pub keyspace_start: Vec<u8>,
    /// Exclusive end of the keyspace interval covered by this range.
    pub keyspace_end: Vec<u8>,
    /// Current lifecycle state.
    pub state: RangeState,
    /// All segments in this range in creation order.
    pub segments: Vec<SegmentId>,
    /// The segment currently accepting writes; `None` when the range is sealed or deleting.
    pub active_segment: Option<SegmentId>,
    /// If this range was split, the two child range IDs that replaced it.
    pub split_into: Option<[RangeId; 2]>,
    /// If this range was merged away, the ID of the range that absorbed it.
    pub merged_into: Option<RangeId>,
    /// If this range was produced by a merge, the two source range IDs that were consumed.
    pub merged_from: Option<[RangeId; 2]>,
    /// Unix timestamp (ms) when this range was created.
    pub created_at: u64,
    /// Unix timestamp (ms) when this range transitioned to `Sealed`, if applicable.
    pub sealed_at: Option<u64>,
}

impl RangeMeta {
    pub fn active_segment_id(&self) -> Option<SegmentId> {
        matches!(self.state, RangeState::Active)
            .then(|| self.segments.last().copied())
            .flatten()
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SegmentMeta {
    /// Unique identifier for this segment.
    pub segment_id: SegmentId,
    /// The range this segment belongs to.
    pub range_id: RangeId,
    /// The topic this segment belongs to (denormalized for fast lookup).
    pub topic_id: TopicId,
    /// Current lifecycle state.
    pub state: SegmentState,
    /// Nodes holding a replica of this segment.
    pub replica_set: Vec<NodeId>,
    /// Approximate size of this segment in bytes.
    pub size_bytes: u64,
    /// Unix timestamp (ms) when this segment was created.
    pub created_at: u64,
    /// Unix timestamp (ms) when this segment was sealed; used with `retention_ms` to schedule GC.
    pub sealed_at: Option<u64>,
}
