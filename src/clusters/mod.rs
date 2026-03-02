use std::net::SocketAddr;

pub mod swims;
pub mod transport;
mod types;

pub use types::node::*;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[cfg(test)]
pub(crate) mod tests;
