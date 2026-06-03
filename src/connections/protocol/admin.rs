//! Admin wire types — operator and integration-test affordances. Not part of
//! the consumer/producer hot path: describe cluster, force-split a range,
//! introspect the shard ring. Used by the CLI and by `it/` test helpers.

use std::net::SocketAddr;

use bincode::{Decode, Encode};

#[derive(Clone, Encode, Decode)]
pub enum AdminRequest {
    DescribeCluster,
    ListHostedTopicsWithStats,
    SplitRange {
        topic_name: String,
        range_id: u64,
        split_point: Vec<u8>,
    },
    // Internal/debug queries — also used by integration test helpers.
    GetShardInfo {
        key: Vec<u8>,
    },
    GetShardLeader {
        shard_group_id: u64,
    },
}

#[derive(Debug, Encode, Decode)]
pub enum AdminResponse {
    // DescribeCluster
    ClusterInfo { nodes: Vec<NodeInfo> },
    // ListHostedTopicsWithStats
    TopicStats { topics: Vec<TopicStats> },
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

#[derive(Debug, Encode, Decode)]
pub struct ShardDetail {
    pub shard_group_id: u64,
    pub leader_node_id: Option<String>,
    pub leader_addr: Option<SocketAddr>,
    pub member_node_ids: Vec<String>,
}

#[derive(Debug, Encode, Decode)]
pub struct NodeInfo {
    pub node_id: String,
    pub addr: SocketAddr,
    pub state: NodeState,
}

#[derive(Debug, Encode, Decode)]
pub enum NodeState {
    Alive,
    Suspect,
    Dead,
}

#[derive(Debug, Encode, Decode)]
pub struct TopicStats {
    pub name: String,
    pub range_count: u32,
    pub total_bytes: u64,
}
