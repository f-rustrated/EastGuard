use std::net::SocketAddr;

use bincode::{Decode, Encode};

use crate::clusters::BINCODE_CONFIG;
use crate::clusters::SwimNodeState::Alive;

// Used to decide what to say. You must include Dead/Suspect nodes in your messages so that other nodes learn about these failures.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SwimNode {
    pub node_id: NodeId,
    pub addr: SocketAddr,
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
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Encode, Decode)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
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
