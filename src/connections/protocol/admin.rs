//! Admin wire types — operator and integration-test affordances. Not part of
//! the consumer/producer hot path: describe cluster, force-split a range,
//! introspect the shard ring. Used by the CLI and by `it/` test helpers.

use std::net::SocketAddr;

use borsh::{BorshDeserialize, BorshSerialize};

#[derive(Clone, BorshSerialize, BorshDeserialize)]
pub enum AdminRequest {
    DescribeCluster,
    ListHostedTopicsWithStats,
    // Internal/debug queries — also used by integration test helpers.
    GetShardInfo { key: Vec<u8> },
    GetShardLeader { shard_group_id: u64 },
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub enum AdminResponse {
    // DescribeCluster
    ClusterInfo { nodes: Box<[NodeInfo]> },
    // ListHostedTopicsWithStats
    TopicStats { topics: Box<[TopicStats]> },
    // SplitRange
    RangeSplit,
    InvalidSplitPoint,
    // GetShardInfo
    ShardInfo { detail: Option<ShardDetail> },
    // GetShardLeader
    ShardLeader { leader: Option<String> },
    // All admin operations
    InternalError(String),
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct ShardDetail {
    pub shard_group_id: u64,
    pub leader_node_id: Option<String>,
    pub leader_addr: Option<SocketAddr>,
    pub member_node_ids: Box<[String]>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub addr: SocketAddr,
    pub state: NodeState,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum NodeState {
    Alive,
    Suspect,
    Dead,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct TopicStats {
    pub name: String,
    pub range_count: u32,
    pub total_bytes: u64,
}
