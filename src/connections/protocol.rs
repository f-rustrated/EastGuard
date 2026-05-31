#![allow(dead_code)]

use std::net::SocketAddr;

use bincode::{Decode, Encode};

use crate::control_plane::metadata::strategy::StoragePolicy;

// ── Top-level dispatch ─────────────────────────────────────────────────────

#[derive(Clone, Encode, Decode)]
pub enum ClientRequest {
    ControlPlane(ControlPlaneRequest),
    DataPlane(DataPlaneRequest),
    Admin(AdminRequest),
}

#[derive(Debug, Encode, Decode)]
pub enum ClientResponse {
    ControlPlane(ControlPlaneResponse),
    DataPlane(DataPlaneResponse),
    Admin(AdminResponse),
}

// ── Control Plane ──────────────────────────────────────────────────────────
//
// Routing is the client's responsibility. The server returns NotLeader or
// ShardNotLocal when it cannot handle a request locally; the client retries.

#[derive(Clone, Encode, Decode)]
pub enum ControlPlaneRequest {
    CreateTopic {
        name: String,
        storage_policy: StoragePolicy,
    },
    DeleteTopic {
        name: String,
    },
    ListHostedTopics,
    DescribeTopic {
        name: String,
    },
}

#[derive(Debug, Encode, Decode)]
pub enum ControlPlaneResponse {
    // CreateTopic
    TopicCreated,
    AlreadyExists,
    // DeleteTopic
    TopicDeleted,
    TopicNotFound,
    // ListHostedTopics
    TopicList { topics: Vec<TopicSummary> },
    // DescribeTopic
    TopicDetail(TopicDetail),
    // Redirect errors — client reconnects and retries on the indicated node
    /// This node is not the Raft leader (writes) or any shard group host (reads).
    /// `leader_addr` is `None` when an election is in progress and no leader is yet known.
    NotLeader { leader_addr: Option<SocketAddr> },
    /// This node does not host the shard group for the requested topic.
    /// `hint_node` is a live shard group member the client can retry on.
    ShardNotLocal { hint_node: SocketAddr },
    // All control plane operations
    InternalError(String),
}

#[derive(Debug, Encode, Decode)]
pub struct TopicSummary {
    pub name: String,
    pub range_count: u32,
    pub state: TopicState,
}

#[derive(Debug, Encode, Decode)]
pub enum TopicState {
    Active,
    Deleting,
}

#[derive(Debug, Encode, Decode)]
pub struct TopicDetail {
    pub name: String,
    pub ranges: Vec<RangeDetail>,
}

#[derive(Debug, Encode, Decode)]
pub struct RangeDetail {
    pub range_id: u64,
    pub keyspace_start: Vec<u8>,
    pub keyspace_end: Vec<u8>,
    pub active_segment_id: Option<u64>,
    pub state: RangeState,
}

#[derive(Debug, Encode, Decode)]
pub enum RangeState {
    Active,
    Sealed,
    Deleting,
}

// ── Data Plane ─────────────────────────────────────────────────────────────
//
// The client is responsible for routing to the correct node using its local
// routing cache. When this node is not the right destination, a redirect error
// is returned so the client can reconnect and retry.

#[derive(Clone, Encode, Decode)]
pub enum DataPlaneRequest {
    Produce {
        topic_name: String,
        // Used by the server to locate the target range; never stored.
        routing_key: Vec<u8>,
        // Pre-serialized and optionally compressed blob produced by the client.
        // The broker stamps an entry_id and stores/replicates this opaque payload as-is.
        data: Vec<u8>,
        record_count: u32,
    },
    Fetch {
        topic_name: String,
        range_id: u64,
        entry_id: u64,
        // Position within the entry to start from; 0 means the first record.
        record_index: u32,
        max_bytes: u32,
    },
    ListOffsets {
        topic_name: String,
        range_id: u64,
    },
}

#[derive(Debug, Encode, Decode)]
pub enum DataPlaneResponse {
    // Produce
    Produced {
        entry_id: u64,
    },
    RangeSplitting {
        left_range_id: u64,
        right_range_id: u64,
        split_point: Vec<u8>,
    },
    // Fetch
    Fetched {
        entries: Vec<Entry>,
        next_entry_id: u64,
        range_status: RangeStatus,
    },
    EntryIdOutOfRange,
    // ListOffsets
    Offsets {
        start_entry_id: u64,
        committed_entry_id: u64,
    },
    // Redirect errors — client reconnects and retries on the indicated node
    NotLeader {
        leader_addr: Option<SocketAddr>,
    },
    ShardNotLocal {
        hint_node: SocketAddr,
    },
    TopicNotFound,
    InternalError(String),
}

/// A single entry as served to the consumer.
/// The broker never parses `data` — it is stored and replicated opaque.
/// Consumers decompress and parse records from `data` using `record_count`.
#[derive(Debug, Encode, Decode)]
pub struct Entry {
    pub entry_id: u64,
    pub data: Vec<u8>,
    pub record_count: u32,
}

#[derive(Debug, Encode, Decode)]
pub enum RangeStatus {
    Active,
    Sealed {
        end_offset: u64,
        transition: RangeTransition,
    },
}

#[derive(Debug, Encode, Decode)]
pub enum RangeTransition {
    Split {
        left_range_id: u64,
        right_range_id: u64,
        split_point: Vec<u8>,
    },
    Merged {
        merged_range_id: u64,
    },
}

// ── Admin ──────────────────────────────────────────────────────────────────

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
    #[cfg(test)]
    IsClusterReady,
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
    #[cfg(test)]
    ClusterReady(bool),
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
