//! Control-plane wire types — topic lifecycle and metadata lookups.
//!
//! The server never forwards or proxies; it redirects and the client retries on
//! the indicated node. Two redirect shapes, by what's wrong:
//! - `TopicMetadataRedirect` — this node doesn't host the topic's metadata shard
//!   group (wrong host). Returned by `DescribeTopic` and by the writes when not a
//!   member; points at a member.
//! - `NotRaftLeader` — a member but not the metadata Raft leader (wrong role).
//!   Returned by `CreateTopic` / `DeleteTopic`; points at the leader (or `None`
//!   until it's known).
//!
//! Both are retriable: addresses come from SWIM, which converges eventually, so a
//! stale or absent target costs a retry, never correctness. See
//! d4_consumer_range_tracking.md §Bootstrap and d6_produce_consume_api.md.

use std::collections::HashMap;
use std::net::SocketAddr;

use bincode::{Decode, Encode};

use crate::control_plane::NodeId;
use crate::control_plane::metadata::strategy::StoragePolicy;

use crate::control_plane::metadata::{
    RangeMeta, RangeState, SegmentMeta, TopicMeta, TopicState as MetaTopicState,
};

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
    TopicList {
        topics: Box<[TopicSummary]>,
    },
    // DescribeTopic — when this node owns the topic's metadata
    TopicDetail(TopicDetail),
    // DescribeTopic — when this node does not own the topic's metadata.
    TopicMetadataRedirect {
        owner: NodeAddressInfo,
    },
    // Write landed on a member that isn't the metadata Raft leader — client retries
    // there. `None` when the leader isn't yet known.
    NotRaftLeader {
        leader_addr: Option<NodeAddressInfo>,
    },
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

/// Full metadata snapshot for a topic, returned by `DescribeTopic`. Includes every
/// range — active, sealed, and deleting — so a consumer can walk lineage from any
/// historical ancestor forward through splits and merges.
#[derive(Debug, Encode, Decode)]
pub struct TopicDetail {
    /// Resolved id, so a consumer can fetch by id directly from a holding replica
    /// (no name re-resolution on the data node). See `FetchByIdRequest`.
    pub topic_id: u64,
    pub name: String,
    pub state: TopicState,
    pub ranges: Box<[RangeDetail]>,
}

/// Per-range metadata. Lineage fields (`split_into`, `merged_into`, `merged_from`)
/// let the consumer reconstruct the DAG; only the predecessor that owned a given
/// key needs to be drained before its successor. See d4_consumer_range_tracking.md.
#[derive(Debug, Encode, Decode)]
pub struct RangeDetail {
    pub range_id: u64,
    pub keyspace_start: Vec<u8>,
    pub keyspace_end: Vec<u8>,
    pub state: RangeState,
    /// `None` once the range is sealed or deleting.
    pub active_segment: Option<SegmentDetail>,
    /// Set when this range was split: the two child range IDs.
    pub split_into: Option<(u64, u64)>,
    /// Set when this range was merged into a successor.
    pub merged_into: Option<u64>,
    /// Set when this range was created by merging two predecessors.
    pub merged_from: Option<(u64, u64)>,
}

/// Per-segment metadata exposed to consumers. The replica set carries resolved
/// client addresses so the consumer can pick a node to send fetches to without a
/// separate address-resolution round trip.
#[derive(Debug, Encode, Decode)]
pub struct SegmentDetail {
    pub segment_id: u64,
    pub start_offset: u64,
    /// `None` while the segment is still active (the write head).
    pub end_offset: Option<u64>,
    pub replica_set: Vec<NodeAddressInfo>,
}

/// A node identifier paired with its currently-known client address.
/// Addresses come from SWIM membership; consumers cache them.
#[derive(Debug, Clone, Encode, Decode)]
pub struct NodeAddressInfo {
    pub node_id: String,
    pub client_addr: SocketAddr,
}

// Wire constructors that know how to build the consumer-facing snapshot from
// the metadata layer's in-memory state. Live here (not on `TopicMeta`) so the
// dependency stays one-directional: wire knows metadata, metadata knows
// nothing of the wire. The `addresses` snapshot is fetched once at the
// boundary (SWIM round-trip) and threaded through synchronously so the
// describe path has no per-replica awaits.
impl TopicDetail {
    pub(crate) fn from_meta(meta: TopicMeta, addresses: &HashMap<NodeId, SocketAddr>) -> Self {
        let ranges = meta
            .ranges
            .into_values()
            .map(|range| RangeDetail::from_meta(range, addresses))
            .collect();
        TopicDetail {
            topic_id: meta.id.0,
            name: meta.name,
            state: match meta.state {
                MetaTopicState::Active => TopicState::Active,
                // Wire-level coarsening: clients only see "tearing down"; the
                // internal Sealed → Deleted progression is GC bookkeeping.
                MetaTopicState::Sealed | MetaTopicState::Deleted => TopicState::Deleting,
            },
            ranges,
        }
    }
}

impl RangeDetail {
    pub(crate) fn from_meta(range: RangeMeta, addresses: &HashMap<NodeId, SocketAddr>) -> Self {
        let active_segment = range
            .active_segment
            .and_then(|id| range.segments.get(&id))
            .map(|seg| SegmentDetail::from_meta(seg, addresses));
        RangeDetail {
            range_id: range.range_id.0,
            keyspace_start: range.keyspace_start,
            keyspace_end: range.keyspace_end,
            state: range.state,
            active_segment,
            split_into: range.split_into.map(|[l, r]| (l.0, r.0)),
            merged_into: range.merged_into.map(|m| m.0),
            merged_from: range.merged_from.map(|[a, b]| (a.0, b.0)),
        }
    }
}

impl SegmentDetail {
    pub(crate) fn from_meta(seg: &SegmentMeta, addresses: &HashMap<NodeId, SocketAddr>) -> Self {
        let replica_set = seg
            .replica_set
            .iter()
            .filter_map(|node| {
                addresses.get(node).map(|addr| NodeAddressInfo {
                    node_id: node.to_string(),
                    client_addr: *addr,
                })
            })
            .collect();
        SegmentDetail {
            segment_id: seg.segment_id.0,
            start_offset: seg.start_entry_id,
            end_offset: seg.end_entry_id,
            replica_set,
        }
    }
}
