use std::ops::Deref;

use borsh::{BorshDeserialize, BorshSerialize};
use uuid::Uuid;

use crate::{
    connections::protocol::ConsumerGroupMemberAction,
    control_plane::{
        Replicas,
        metadata::{AclResource, EntryId, RangeId, SegmentId, TopicId, strategy::StoragePolicy},
    },
    data_plane::SegmentKey,
    impl_from_variant,
};

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct CreateTopic {
    pub name: String,
    pub storage_policy: StoragePolicy,
    pub replica_set: Replicas,
    pub created_at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum SegmentRollIntent {
    DataPressure,
    IdleMaintenance,
    ReplicationFailure,
    Recovery,
    BoundaryCorrection,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct RollSegment {
    pub segment_key: SegmentKey,
    pub sealed_at: u64,
    pub new_replica_set: Replicas,
    /// None for SWIM-death-triggered seals — the coordinator doesn't know
    /// the actual committed offset. Corrected later via `correct_end_offset`
    /// or D5 sealed segment repair.
    pub end_entry_id: Option<EntryId>,
    pub intent: SegmentRollIntent,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct SplitRange {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub split_point: Vec<u8>,
    pub created_at: u64,
    pub left_replica_set: Replicas,
    pub right_replica_set: Replicas,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct MergeRange {
    pub topic_id: TopicId,
    pub range_id_1: RangeId,
    pub range_id_2: RangeId,
    pub created_at: u64,
    pub merged_replica_set: Replicas,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct DeleteTopic {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ReassignSegment {
    pub segment_key: SegmentKey,
    pub replica_set: Replicas,
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
pub struct UpdateConsumerGroupMember {
    pub req: UpdateConsumerGroupMemberRequest,
    // TODO consider using logical clock
    pub observed_at: u64,
    pub session_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct OpenProducerSession {
    pub topic_name: String,
    pub producer_id: Uuid,
    pub session_nonce: Uuid,
    pub observed_at: u64,
    pub session_timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ExpireProducerSessions {
    pub topic_id: TopicId,
    pub observed_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct GrantAcl {
    pub resource: AclResource,
    pub principal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct RevokeAcl {
    pub resource: AclResource,
    pub principal: String,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct UpdateConsumerGroupMemberRequest {
    pub topic_name: String,
    pub group_id: String,
    pub member_id: Uuid,
    pub action: ConsumerGroupMemberAction,
}

impl Deref for UpdateConsumerGroupMember {
    type Target = UpdateConsumerGroupMemberRequest;

    fn deref(&self) -> &Self::Target {
        &self.req
    }
}

impl UpdateConsumerGroupMember {
    pub(crate) fn new(req: UpdateConsumerGroupMemberRequest) -> Self {
        const SESSION_TIMEOUT_MS: u64 = 10_000;
        let observed_at = crate::now_ms();
        UpdateConsumerGroupMember {
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
    UpdateConsumerGroupMember(UpdateConsumerGroupMember),
    OpenProducerSession(OpenProducerSession),
    ExpireProducerSessions(ExpireProducerSessions),
    GrantAcl(GrantAcl),
    RevokeAcl(RevokeAcl),
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
    UpdateConsumerGroupMember,
    OpenProducerSession,
    ExpireProducerSessions,
    GrantAcl,
    RevokeAcl
);
