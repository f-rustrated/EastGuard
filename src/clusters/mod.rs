// ==========================================
// MESSAGES & DATA STRUCTURES
// ==========================================

use bincode::{Decode, Encode};
use std::net::SocketAddr;
mod gossip_buffer;
mod livenode_tracker;

pub(crate) mod swim;
pub(crate) mod topology;
pub(crate) mod transport;

#[cfg(test)]
pub mod tests;
mod types;
mod swim_state_machine;

pub(crate) use types::messages::*;
pub(crate) use types::node::*;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
