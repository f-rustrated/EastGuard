use tokio::sync::oneshot;

use crate::control_plane::NodeId;
use crate::control_plane::consensus::raft::errors::ProposalError;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::{TopicMeta, TopicStats};
use crate::data_plane::messages::command::{CatchUpAck, SealBoundaryReport, SegmentAssignmentAck};

use super::command::{
    CoordinatorSealRequest, EnsureGroup, HandleNodeJoin, InboundRaftRpc, MetadataProposal,
    RaftProtocolMessage, RemoveGroup,
};
use super::timer::RaftTimeoutCallback;
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
    /// Data plane SealRequest forwarded to coordinator for Raft proposal.
    Coordinator(CoordinatorSealRequest),
    /// Data-leader confirmation that it received a `SegmentAssignment`. Marks the
    /// segment confirmed so the leader's heartbeat sweep stops re-driving it.
    AssignmentAck(SegmentAssignmentAck),
    /// A survivor's reply to a leader-crash `SealBoundaryQuery` — its durable extent
    /// for the segment, gathered to recover the committed seal end.
    SealBoundaryReport(SealBoundaryReport),
    /// A replica's confirmation that it holds a reassigned sealed segment through
    /// `sealed_end`. Clears the member from the coordinator's catch-up re-drive so
    /// the heartbeat sweep stops re-announcing the assignment.
    CatchUpAck(CatchUpAck),
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
    HandleNodeJoin,
);

pub(crate) enum DeferredReply {
    GetLeader(oneshot::Sender<Option<NodeId>>, Option<NodeId>),
    GetPeers(oneshot::Sender<Box<[NodeId]>>, Box<[NodeId]>),
    Propose(
        oneshot::Sender<Result<(), ProposalError>>,
        Result<(), ProposalError>,
    ),
    GetTopics(oneshot::Sender<Box<[String]>>, Box<[String]>),
    GetTopicStats(oneshot::Sender<Box<[TopicStats]>>, Box<[TopicStats]>),
    GetTopicMetadata(oneshot::Sender<Option<TopicMeta>>, Option<TopicMeta>),
}
