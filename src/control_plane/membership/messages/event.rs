use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::{HandleNodeJoin, MultiRaftActorCommand};
use crate::impl_from_variant;

#[derive(Debug)]
pub struct NodeDead {
    pub dead_node_id: NodeId,
}

#[derive(Debug)]
pub struct NodeAlive {
    pub node_id: NodeId,
}

#[derive(Debug)]
pub enum MembershipEvent {
    NodeAlive(NodeAlive),
    NodeDead(NodeDead),
}

impl_from_variant!(MembershipEvent, NodeAlive, NodeDead);

impl MembershipEvent {
    pub(crate) fn node_id(&self) -> &NodeId {
        match self {
            MembershipEvent::NodeAlive(evt) => &evt.node_id,
            MembershipEvent::NodeDead(evt) => &evt.dead_node_id,
        }
    }
}

impl NodeDead {
    pub fn new(dead_node_id: NodeId) -> Self {
        Self { dead_node_id }
    }
}

impl MembershipEvent {
    /// Turn a membership event into the consensus-side notification command.
    /// The command intentionally carries no topology snapshot — handlers load
    /// fresh from the shared topology reader at processing time, so they see
    /// the latest cluster shape even if topology drifted between this event's
    /// emission and the handler running.
    pub(crate) fn into_raft_command(self, local_node_id: &NodeId) -> Option<MultiRaftActorCommand> {
        if self.node_id() == local_node_id {
            return None;
        }
        match self {
            MembershipEvent::NodeDead(evt) => Some(evt.into()),
            MembershipEvent::NodeAlive(evt) => Some(
                HandleNodeJoin {
                    new_node_id: evt.node_id,
                }
                .into(),
            ),
        }
    }
}
