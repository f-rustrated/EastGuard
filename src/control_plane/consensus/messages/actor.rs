use tokio::sync::oneshot;
use uuid::Uuid;

use crate::connections::protocol::ServerError;
use crate::control_plane::NodeId;
use crate::control_plane::consensus::raft::errors::ProposalError;
use crate::control_plane::consensus::raft::states::security::{AclRecord, AdmissionRecord};
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::{AclResource, ConsumerGroupAssignment, TopicMeta, TopicStats};
use crate::data_plane::messages::command::{
    DurableSegmentEndReported, SegmentCaughtUp, SegmentPlaced,
};
use crate::security::CertificatePrincipal;

use super::command::{
    EnsureGroup, InboundRaftRpc, MetadataProposal, ProposeSegmentRoll, RaftProtocolMessage,
    RemoveGroup,
};
use super::timer::RaftTimeoutCallback;
use crate::impl_from_variant;
use crate::impl_from_variant_via;

pub enum MultiRaftActorCommand {
    /// Fire-and-forget: internal Raft protocol messages, timeouts, and SWIM topology updates.
    ProtocolMessage(RaftProtocolMessage),
    /// Query the current leader of a shard group.
    GetLeader {
        group_id: ShardGroupId,
        reply: oneshot::Sender<Option<NodeId>>,
    },

    GetPeers {
        group_id: ShardGroupId,
        reply: oneshot::Sender<Box<[NodeId]>>,
    },
    /// Propose a command to a shard group's Raft log. Leader-only.
    ClientProposal {
        propose: MetadataProposal,
        reply: oneshot::Sender<Result<(), ProposalError>>,
    },
    /// Query all topic names from all shard groups on this node.
    GetTopics {
        reply: oneshot::Sender<Box<[String]>>,
    },
    /// Query per-topic stats from all shard groups on this node.
    GetTopicStats {
        reply: oneshot::Sender<Box<[TopicStats]>>,
    },
    /// Query full metadata for a single topic by name. Returns `None` when the
    /// topic's metadata is not hosted on this node (i.e. this node is not in
    /// the topic's owning shard group). Callers above use that signal to issue
    /// a redirect rather than to declare the topic missing — only the metadata
    /// owner can authoritatively report absence.
    GetTopicMetadata {
        topic_name: String,
        reply: oneshot::Sender<Option<TopicMeta>>,
    },
    GetAclSnapshot(GetAclSnapshot),
    GetAdmission(GetAdmission),
    GetConsumerGroupAssignment(GetConsumerGroupAssignment),
    /// Data-plane request forwarded to the metadata coordinator for proposal.
    ProposeSegmentRoll(ProposeSegmentRoll),
    /// Data-leader confirmation that it received a `PlaceSegment`. Marks the
    /// segment confirmed so the leader's heartbeat sweep stops re-driving it.
    AssignmentAck(SegmentPlaced),
    /// A survivor's reply to a leader-crash `RequestDurableSegmentEnd` — its durable extent
    /// for the segment, gathered to recover the committed seal end.
    DurableSegmentEndReported(DurableSegmentEndReported),
    /// A replica's confirmation that it holds a reassigned sealed segment through
    /// `sealed_end`. Clears the member from the coordinator's catch-up re-drive so
    /// the heartbeat sweep stops re-announcing the assignment.
    SegmentCaughtUp(SegmentCaughtUp),
}

pub struct GetConsumerGroupAssignment {
    pub(crate) topic_name: String,
    pub(crate) group_id: String,
    pub(crate) member_id: Uuid,
    pub(crate) reply: oneshot::Sender<Option<ConsumerGroupAssignment>>,
}

/// Returns committed ACL state after a quorum-backed read barrier on the
/// shard leader.
///
/// The caller has already routed the resource to this shard. A missing ACL is
/// returned as an empty record so it can be cached as a bounded denial.
pub struct GetAclSnapshot {
    pub(crate) shard_group_id: ShardGroupId,
    pub(crate) resource: AclResource,
    pub(crate) reply: oneshot::Sender<Result<AclRecord, ServerError>>,
}

/// Returns one admission record after a quorum-backed read barrier on the
/// metadata shard leader.
pub struct GetAdmission {
    pub(crate) shard_group_id: ShardGroupId,
    pub(crate) node_certificate_principal: CertificatePrincipal,
    pub(crate) reply: oneshot::Sender<Result<Option<AdmissionRecord>, ServerError>>,
}

impl From<RaftProtocolMessage> for MultiRaftActorCommand {
    fn from(cmd: RaftProtocolMessage) -> Self {
        MultiRaftActorCommand::ProtocolMessage(cmd)
    }
}

impl From<RaftTimeoutCallback> for MultiRaftActorCommand {
    fn from(cb: RaftTimeoutCallback) -> Self {
        MultiRaftActorCommand::ProtocolMessage(RaftProtocolMessage::Timeout(cb))
    }
}

impl_from_variant_via!(
    MultiRaftActorCommand,
    RaftProtocolMessage,
    InboundRaftRpc,
    EnsureGroup,
    RemoveGroup,
);

impl_from_variant!(
    MultiRaftActorCommand,
    GetAclSnapshot,
    GetAdmission,
    GetConsumerGroupAssignment,
);

/// A synchronous actor result held until the end-of-batch reply flush.
pub(crate) struct DeferredResponse<T> {
    pub(crate) reply: oneshot::Sender<T>,
    pub(crate) value: T,
}

impl<T> DeferredResponse<T> {
    pub(crate) fn send(self) {
        let _ = self.reply.send(self.value);
    }
}

pub(crate) enum DeferredReply {
    GetLeader(DeferredResponse<Option<NodeId>>),
    GetPeers(DeferredResponse<Box<[NodeId]>>),
    Propose(DeferredResponse<Result<(), ProposalError>>),
    GetTopics(DeferredResponse<Box<[String]>>),
    GetTopicStats(DeferredResponse<Box<[TopicStats]>>),
    GetTopicMetadata(DeferredResponse<Option<TopicMeta>>),
    GetAclSnapshot(DeferredResponse<Result<AclRecord, ServerError>>),
    GetAdmission(DeferredResponse<Result<Option<AdmissionRecord>, ServerError>>),
    GetConsumerGroupAssignment(DeferredResponse<Option<ConsumerGroupAssignment>>),
}
