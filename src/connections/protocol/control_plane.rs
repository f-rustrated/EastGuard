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

use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::NodeId;
use crate::control_plane::metadata::strategy::StoragePolicy;

use crate::control_plane::metadata::{
    RangeId, RangeMeta, RangeState, SegmentMeta, SegmentMetaState, TopicMeta,
    TopicState as MetaTopicState,
};

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
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

#[derive(Debug, BorshSerialize, BorshDeserialize)]
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

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct TopicSummary {
    pub name: String,
    pub range_count: u32,
    pub state: TopicState,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum TopicState {
    Active,
    Deleting,
}

/// Full metadata snapshot for a topic, returned by `DescribeTopic`. Includes every
/// range — active, sealed, and deleting — so a consumer can walk lineage from any
/// historical ancestor forward through splits and merges.
#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct TopicDetail {
    /// Resolved id, so a consumer can fetch by id directly from a holding replica
    /// (no name re-resolution on the data node). See `FetchByIdRequest`.
    pub topic_id: u64,
    pub name: String,
    pub state: TopicState,
    pub ranges: Box<[RangeDetail]>,
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

/// Per-range metadata. Lineage fields (`split_into`, `merged_into`, `merged_from`)
/// let the consumer reconstruct the DAG; only the predecessor that owned a given
/// key needs to be drained before its successor. See d4_consumer_range_tracking.md.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct RangeDetail {
    pub range_id: RangeId,
    pub keyspace_start: Vec<u8>,
    pub keyspace_end: Vec<u8>,
    pub state: RangeState,
    /// `None` once the range is sealed or deleting.
    pub active_segment: Option<SegmentDetail>,
    /// All sealed segments in this range, sorted by start_offset.
    pub sealed_segments: Box<[SegmentDetail]>,
    /// Set when this range was split: the two child range IDs.
    pub split_into: Option<(RangeId, RangeId)>,
    pub merged_into: Option<RangeId>,
    pub merged_from: Option<(RangeId, RangeId)>,
}

impl RangeDetail {
    pub(crate) fn end_entry_id(&self) -> u64 {
        self.sealed_segments
            .last()
            .and_then(|s| s.end_entry_id)
            .unwrap_or(0)
    }
    pub(crate) fn from_meta(range: RangeMeta, addresses: &HashMap<NodeId, SocketAddr>) -> Self {
        let active_segment = range
            .active_segment
            .and_then(|id| range.segments.get(&id))
            .map(|seg| SegmentDetail::from_meta(seg, addresses));
        let mut sealed_segments: Vec<SegmentDetail> = range
            .segments
            .values()
            .filter(|seg| seg.state == SegmentMetaState::Sealed)
            .map(|seg| SegmentDetail::from_meta(seg, addresses))
            .collect();
        sealed_segments.sort_by_key(|s| s.start_entry_id);
        RangeDetail {
            range_id: range.range_id,
            keyspace_start: range.keyspace_start,
            keyspace_end: range.keyspace_end,
            state: range.state,
            active_segment,
            sealed_segments: sealed_segments.into_boxed_slice(),
            split_into: range.split_into.map(|[l, r]| (l, r)),
            merged_into: range.merged_into,
            merged_from: range.merged_from.map(|[a, b]| (a, b)),
        }
    }

    /// Find the segment in this range that covers the specified `entry_id`.
    pub fn find_segment_for_offset(&self, entry_id: u64) -> Option<&SegmentDetail> {
        // Check if it is in the active segment
        if let Some(seg) = &self.active_segment
            && entry_id >= seg.start_entry_id
        {
            return Some(seg);
        }
        // Check sealed segments
        self.sealed_segments.iter().find(|seg| {
            if entry_id >= seg.start_entry_id {
                if let Some(end) = seg.end_entry_id {
                    entry_id <= end
                } else {
                    true
                }
            } else {
                false
            }
        })
    }

    /// Get the start offset of the very first segment in the range (either sealed or active).
    pub fn first_segment_start_offset(&self) -> Option<u64> {
        if let Some(seg) = self.sealed_segments.first() {
            Some(seg.start_entry_id)
        } else {
            self.active_segment.as_ref().map(|seg| seg.start_entry_id)
        }
    }
}

/// Per-segment metadata exposed to consumers. The replica set carries resolved
/// client addresses so the consumer can pick a node to send fetches to without a
/// separate address-resolution round trip.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SegmentDetail {
    pub segment_id: u64,
    pub start_entry_id: u64,
    /// `None` while the segment is still active (the write head).
    pub end_entry_id: Option<u64>,
    pub replica_set: Vec<NodeAddressInfo>,
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
            start_entry_id: seg.start_entry_id,
            end_entry_id: seg.end_entry_id,
            replica_set,
        }
    }

    /// Pick a replica from the segment's replica set.
    /// load-balances randomly across all replicas
    pub fn pick_replica(&self) -> Option<SocketAddr> {
        if self.replica_set.is_empty() {
            return None;
        }

        use rand::RngExt;
        let idx = rand::rng().random_range(0..self.replica_set.len());
        Some(self.replica_set[idx].client_addr)
    }
}

/// A node identifier paired with its currently-known client address.
/// Addresses come from SWIM membership; consumers cache them.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct NodeAddressInfo {
    pub node_id: String,
    pub client_addr: SocketAddr,
}
