// --- Policy ---

use bincode::{Decode, Encode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum PartitionStrategy {
    AutoSplit,
    Fixed,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct StoragePolicy {
    pub retention_ms: u64,
    pub replication_factor: u64,
    pub partition_strategy: PartitionStrategy,
}
