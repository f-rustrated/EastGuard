use tokio::sync::oneshot;

use crate::control_plane::NodeId;
use crate::control_plane::membership::{NodeDead, ShardGroupId};
use crate::control_plane::metadata::{TopicMeta, TopicStats};
use crate::data_plane::messages::command::SegmentAssignmentAck;

use super::command::{
    ClientProposalError, CoordinatorSealRequest, EnsureGroup, HandleNodeJoin, InboundRaftRpc,
    MetadataProposal, RaftProtocolMessage, RemoveGroup,
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
        reply: oneshot::Sender<Vec<NodeId>>,
    },
    /// Propose a command to a shard group's Raft log. Leader-only.
    ClientProposal {
        propose: MetadataProposal,
        reply: oneshot::Sender<Result<(), ClientProposalError>>,
    },
    /// Query all topic names from all shard groups on this node.
    GetTopics { reply: oneshot::Sender<Vec<String>> },
    /// Query per-topic stats from all shard groups on this node.
    GetTopicStats {
        reply: oneshot::Sender<Vec<TopicStats>>,
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
    NodeDead,
    HandleNodeJoin,
);

pub(crate) enum DeferredReply {
    GetLeader(oneshot::Sender<Option<NodeId>>, Option<NodeId>),
    GetPeers(oneshot::Sender<Vec<NodeId>>, Vec<NodeId>),
    Propose(
        oneshot::Sender<Result<(), ClientProposalError>>,
        Result<(), ClientProposalError>,
    ),
    GetTopics(oneshot::Sender<Vec<String>>, Vec<String>),
    GetTopicStats(oneshot::Sender<Vec<TopicStats>>, Vec<TopicStats>),
    GetTopicMetadata(oneshot::Sender<Option<TopicMeta>>, Option<TopicMeta>),
}
