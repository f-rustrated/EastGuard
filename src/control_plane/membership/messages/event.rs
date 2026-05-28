use std::net::SocketAddr;

use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::{HandleNodeJoin, MultiRaftActorCommand};
use crate::control_plane::membership::topology::Topology;
use crate::impl_from_variant;

#[derive(Debug)]
pub struct NodeDead {
    pub dead_node_id: NodeId,
    pub live_nodes: Vec<NodeId>,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct NodeAlive {
    pub node_id: NodeId,
    pub addr: SocketAddr,
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
    pub fn new(dead_node_id: NodeId, live_nodes: Vec<NodeId>) -> Self {
        Self {
            dead_node_id,
            live_nodes,
        }
    }
}

impl MembershipEvent {
    pub(crate) fn into_raft_command(
        self,
        local_node_id: &NodeId,
        topology: &Topology,
    ) -> Option<MultiRaftActorCommand> {
        if self.node_id() == local_node_id {
            return None;
        }
        match self {
            MembershipEvent::NodeDead(evt) => Some(evt.into()),
            MembershipEvent::NodeAlive(evt) => {
                let affected_groups: Vec<_> = topology
                    .shard_groups_for_node(&evt.node_id)
                    .into_iter()
                    .cloned()
                    .collect();
                if affected_groups.is_empty() {
                    return None;
                }
                Some(
                    HandleNodeJoin {
                        new_node_id: evt.node_id,
                        affected_groups,
                    }
                    .into(),
                )
            }
        }
    }
}
