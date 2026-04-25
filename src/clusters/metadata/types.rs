use std::collections::HashMap;

use bincode::{Decode, Encode};

use crate::clusters::{
    NodeId,
    metadata::{RangeId, SegmentId, TopicId, strategy::StoragePolicy},
};

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

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SegmentMeta {
    pub segment_id: SegmentId,
    pub state: SegmentState,
    pub replica_set: Vec<NodeId>,
    pub size_bytes: u64,
    pub start_offset: u64,
    pub end_offset: Option<u64>,
    pub created_at: u64,
    pub sealed_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RangeMeta {
    pub range_id: RangeId,
    pub keyspace_start: Vec<u8>,
    pub keyspace_end: Vec<u8>,
    pub state: RangeState,
    pub active_segment: Option<SegmentId>,
    pub segments: HashMap<SegmentId, SegmentMeta>,
    pub next_segment_id: u64,
    pub next_offset: u64,
    pub split_into: Option<[RangeId; 2]>,
    pub merged_into: Option<RangeId>,
    pub merged_from: Option<[RangeId; 2]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct TopicMeta {
    pub id: TopicId,
    pub name: String,
    pub state: TopicState,
    pub storage_policy: StoragePolicy,
    pub active_ranges: Vec<RangeId>,
    pub ranges: HashMap<RangeId, RangeMeta>,
    pub next_range_id: u64,
}
impl TopicMeta {
    pub(crate) fn new(
        name: String,
        id: TopicId,
        replica_set: Vec<NodeId>,
        created_at: u64,
        storage_policy: StoragePolicy,
    ) -> Self {
        let range_id = RangeId(0);
        let segment_id = SegmentId(0);

        let segment = SegmentMeta {
            segment_id,
            state: SegmentState::Active,
            replica_set,
            size_bytes: 0,
            start_offset: 0,
            end_offset: None,
            created_at,
            sealed_at: None,
        };

        let range = RangeMeta {
            range_id,
            keyspace_start: KEYSPACE_MIN.to_vec(),
            keyspace_end: KEYSPACE_MAX.to_vec(),
            state: RangeState::Active,
            active_segment: Some(segment_id),
            segments: HashMap::from([(segment_id, segment)]),
            next_segment_id: 1,
            next_offset: 0,
            split_into: None,
            merged_into: None,
            merged_from: None,
        };

        TopicMeta {
            id,
            name,
            state: TopicState::Active,
            storage_policy,
            active_ranges: vec![range_id],
            ranges: HashMap::from([(range_id, range)]),
            next_range_id: 1,
        }
    }
}

// --- Keyspace Constants ---

pub const KEYSPACE_MIN: &[u8] = &[];
pub const KEYSPACE_MAX: &[u8] = &[0xFF];
