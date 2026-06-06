#![allow(unused)]
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bincode::{Decode, Encode};

use crate::control_plane::BINCODE_CONFIG;
use crate::control_plane::SwimNodeState::Alive;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct NodeAddress {
    pub cluster_addr: SocketAddr,
    client_addr: SocketAddr,
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

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SwimNode {
    pub node_id: NodeId,
    pub addr: NodeAddress,
    pub state: SwimNodeState,
    pub incarnation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
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
        bincode::encode_to_vec(self, BINCODE_CONFIG)
            .map(|v| v.len())
            .unwrap_or(0)
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Encode, Decode)]
pub struct NodeId(Arc<str>);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into().into())
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self(s.into())
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
