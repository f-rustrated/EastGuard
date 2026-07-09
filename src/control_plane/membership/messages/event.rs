use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::{
    HandleNodeJoin, MultiRaftActorCommand, RaftProtocolMessage,
};

#[derive(Debug)]
pub enum MembershipEvent {
    NodeAlive(NodeId),
    NodeDead(NodeId),
}

impl MembershipEvent {
    fn node_id(&self) -> &NodeId {
        match self {
            MembershipEvent::NodeAlive(node_id) => node_id,
            MembershipEvent::NodeDead(node_id) => node_id,
        }
    }

    pub(crate) fn is_self(&self, other: &NodeId) -> bool {
        self.node_id() == other
    }

    /// Turn a membership event into the consensus-side notification command.
    /// The command intentionally carries no topology snapshot — handlers load
    /// fresh from the shared topology reader at processing time, so they see
    /// the latest cluster shape even if topology drifted between this event's
    /// emission and the handler running.
    pub(crate) fn into_raft_command(self, local_node_id: &NodeId) -> Option<MultiRaftActorCommand> {
        if self.is_self(local_node_id) {
            return None;
        }
        match self {
            MembershipEvent::NodeDead(node_id) => {
                Some(RaftProtocolMessage::HandleNodeDeath(node_id).into())
            }
            MembershipEvent::NodeAlive(node_id) => Some(
                HandleNodeJoin {
                    new_node_id: node_id,
                }
                .into(),
            ),
        }
    }
}
