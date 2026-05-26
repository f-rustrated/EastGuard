pub(crate) mod consensus;
pub(crate) mod membership;
pub(crate) mod metadata;
mod types;

pub(crate) use types::node::*;

pub(crate) const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
