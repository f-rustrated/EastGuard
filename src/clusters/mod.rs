use std::net::SocketAddr;

pub(crate) mod actors;

pub(crate) mod ticker;

pub(crate) mod swims;
mod types;

pub(crate) use types::messages::*;
pub(crate) use types::node::*;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[cfg(test)]
pub(crate) mod tests;
