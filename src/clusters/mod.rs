use std::net::SocketAddr;

pub(crate) mod swims;
pub(crate) mod tickers;
pub(crate) mod transport;
mod types;

pub(crate) use types::node::*;
pub(crate) use types::swim_messages::*;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

#[cfg(test)]
pub(crate) mod tests;
