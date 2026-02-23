use std::net::SocketAddr;
mod gossip_buffer;
mod livenode_tracker;

pub(crate) mod swim;
pub(crate) mod topology;
pub(crate) mod transport;

mod swim_protocol;
pub(crate) mod swim_ticker;
#[cfg(test)]
pub mod tests;
mod types;

pub(crate) use types::messages::*;
pub(crate) use types::node::*;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
