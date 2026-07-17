#![allow(unused)]
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::SwimNodeState::Alive;
use crate::impl_new_struct_wrapper;

/// A node identifier paired with its currently-known client address.
/// Addresses come from SWIM membership; consumers cache them.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct NodeAddressInfo {
    pub node_id: NodeId,
    pub addr: NodeAddress,
}

impl NodeAddressInfo {
    pub fn new(node: NodeId, addr: NodeAddress) -> Self {
        Self {
            node_id: node,
            addr,
        }
    }
    pub(crate) fn client_addr(&self) -> SocketAddr {
        self.addr.client_addr
    }

    pub(crate) fn cluster_addr(&self) -> SocketAddr {
        self.addr.cluster_addr
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct NodeAddress {
    pub cluster_addr: SocketAddr,
    pub client_addr: SocketAddr,
    data_addr: SocketAddr,
}

impl NodeAddress {
    #[cfg(test)]
    pub fn test(cluster_addr: SocketAddr, client_addr: SocketAddr) -> Self {
        Self::new(
            cluster_addr,
            client_addr,
            SocketAddr::new(cluster_addr.ip(), cluster_addr.port() + 1),
        )
    }

    pub fn new(cluster_addr: SocketAddr, client_addr: SocketAddr, data_addr: SocketAddr) -> Self {
        Self {
            cluster_addr,
            client_addr,
            data_addr,
        }
    }

    pub(crate) fn cluster_addr(&self) -> SocketAddr {
        self.cluster_addr
    }

    pub(crate) fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    pub(crate) fn data_addr(&self) -> SocketAddr {
        self.data_addr
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct SwimNode {
    pub node_id: NodeId,
    pub addr: NodeAddress,
    pub state: SwimNodeState,
    pub incarnation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, BorshSerialize, BorshDeserialize)]
pub enum SwimNodeState {
    Alive,
    Suspect,
    Dead,
}

impl SwimNodeState {
    pub fn not_alive(self) -> bool {
        self != Alive
    }
}

impl SwimNode {
    #[inline]
    pub(crate) fn encoded_size(&self) -> usize {
        borsh::to_vec(self).map(|v| v.len()).unwrap_or(0)
    }

    pub(crate) fn resolve_state(&mut self, state: SwimNodeState, incarnation: u64) -> bool {
        let old_state = self.state;
        let old_inc = self.incarnation;
        if incarnation > self.incarnation {
            self.incarnation = incarnation;
            self.state = state;
        } else if incarnation == self.incarnation {
            self.state = self.state.max(state);
        }
        self.state != old_state || self.incarnation != old_inc
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, BorshSerialize, BorshDeserialize)]
pub struct NodeId(Arc<str>);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into().into())
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::ops::Deref for NodeId {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for NodeId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub struct Replicas(pub Box<[NodeId]>);
crate::impl_new_struct_wrapper!(Replicas, Box<[NodeId]>);

impl Replicas {
    pub fn new(nodes: Vec<NodeId>) -> Self {
        assert!(!nodes.is_empty(), "Replica set cannot be empty");
        Self(nodes.into_boxed_slice())
    }

    pub fn leader(&self) -> Option<&NodeId> {
        self.first()
    }

    pub fn followers(&self) -> impl Iterator<Item = &NodeId> {
        self.iter().skip(1)
    }

    pub fn extend(&mut self, nodes: &[NodeId]) {
        let new_nodes = [&*self.0, nodes].concat();
        self.0 = new_nodes.into_boxed_slice();
    }

    pub fn change_leader(&mut self, pos: usize) {
        // Take until pos and rotate to right so
        // [A,B,C] -> [C,A,B]
        self.0[0..=pos].rotate_right(1);
    }

    pub fn except_for(&self, node: &NodeId) -> Vec<NodeId> {
        self.0.iter().filter(|n| *n != node).cloned().collect()
    }
}
