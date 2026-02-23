use std::net::SocketAddr;
mod gossip_buffer;
mod livenode_tracker;

pub(crate) mod topology;
pub(crate) mod transport;

pub(crate) mod actors;
mod swim_protocol;
pub(crate) mod ticker;

mod types;

pub(crate) use types::messages::*;
pub(crate) use types::node::*;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
