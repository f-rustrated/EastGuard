// --- Policy ---

use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum PartitionStrategy {
    AutoSplit,
    Fixed,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct StoragePolicy {
    pub retention_ms: Option<u64>,
    pub replication_factor: u64,
    pub partition_strategy: PartitionStrategy,
}
