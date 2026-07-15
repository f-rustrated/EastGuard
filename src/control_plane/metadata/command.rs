use std::ops::Deref;

use borsh::{BorshDeserialize, BorshSerialize};
use uuid::Uuid;

use crate::{
    connections::protocol::ConsumerGroupSyncAction,
    control_plane::{
        NodeId,
        metadata::{EntryId, RangeId, SegmentId, TopicId, strategy::StoragePolicy},
    },
    data_plane::SegmentKey,
    impl_from_variant,
};

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct CreateTopic {
    pub name: String,
    pub storage_policy: StoragePolicy,
    pub replica_set: Vec<NodeId>,
    pub created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct RollSegment {
    pub segment_key: SegmentKey,
    pub sealed_at: u64,
    pub new_replica_set: Vec<NodeId>,
    /// None for SWIM-death-triggered seals — the coordinator doesn't know
    /// the actual committed offset. Corrected later via `correct_end_offset`
    /// or D5 sealed segment repair.
    pub end_entry_id: Option<EntryId>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct SplitRange {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub split_point: Vec<u8>,
    pub created_at: u64,
    pub left_replica_set: Vec<NodeId>,
    pub right_replica_set: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct MergeRange {
    pub topic_id: TopicId,
    pub range_id_1: RangeId,
    pub range_id_2: RangeId,
    pub created_at: u64,
    pub merged_replica_set: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct DeleteTopic {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ReassignSegment {
    pub segment_key: SegmentKey,
    pub replica_set: Vec<NodeId>,
}

/// Retention: mark an oldest-first **prefix** of one range's sealed segments
/// `Deleting`. Plural by nature — a retention sweep expires a run of old segments,
/// not one. See `docs/data-plane/d7_retention_gc.md`.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct DeleteSegments {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    /// Oldest-first prefix of the range's sealed segments to delete.
    pub segment_ids: Box<[SegmentId]>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct SyncConsumerGroup {
    pub req: SyncConsumerGroupRequest,
    // TODO consider using logical clock
    pub observed_at: u64,
    pub session_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct SyncConsumerGroupRequest {
    pub topic_name: String,
    pub group_id: String,
    pub member_id: Uuid,
    pub action: ConsumerGroupSyncAction,
}

impl Deref for SyncConsumerGroup {
    type Target = SyncConsumerGroupRequest;

    fn deref(&self) -> &Self::Target {
        &self.req
    }
}

impl SyncConsumerGroup {
    pub(crate) fn new(req: SyncConsumerGroupRequest) -> Self {
        const SESSION_TIMEOUT_MS: u64 = 10_000;
        let observed_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        SyncConsumerGroup {
            req,
            observed_at,
            session_timeout_ms: SESSION_TIMEOUT_MS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum MetadataCommand {
    CreateTopic(CreateTopic),
    RollSegment(RollSegment),
    SplitRange(SplitRange),
    MergeRange(MergeRange),
    DeleteTopic(DeleteTopic),
    ReassignSegment(ReassignSegment),
    DeleteSegments(DeleteSegments),
    SyncConsumerGroup(SyncConsumerGroup),
}

impl_from_variant!(
    MetadataCommand,
    CreateTopic,
    RollSegment,
    SplitRange,
    MergeRange,
    DeleteTopic,
    ReassignSegment,
    DeleteSegments,
    SyncConsumerGroup
);
