#![allow(dead_code)]

use crate::control_plane::metadata::command::MetadataCommand;

#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Option<MetadataCommand>,
}
