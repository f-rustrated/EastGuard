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

use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::control_plane::metadata::strategy::StoragePolicy;
use crate::control_plane::{NodeAddress, NodeAddressInfo, NodeId};
use crate::impl_from_variant;
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::{HashMap, HashSet};

use crate::control_plane::metadata::{
    EntryId, RangeId, RangeMeta, RangeState, SegmentId, SegmentMeta, SegmentMetaState,
    SyncConsumerGroupRequest, TopicId, TopicMeta, TopicState,
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
    SyncConsumerGroup(SyncConsumerGroupRequest),
    OpenProducerSession(OpenProducerSessionRequest),
}

impl_from_variant!(
    ControlPlaneRequest,
    SyncConsumerGroup(SyncConsumerGroupRequest),
    OpenProducerSession(OpenProducerSessionRequest)
);

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct OpenProducerSessionRequest {
    pub topic_name: String,
    pub producer_id: uuid::Uuid,
    pub session_nonce: uuid::Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum ConsumerGroupSyncAction {
    Heartbeat,
    Leave,
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
    ConsumerGroupAssignment(ConsumerGroupAssignmentResponse),
    ConsumerGroupLeft,
    ProducerSessionOpened(ProducerSessionOpened),
    // All control plane operations
    InternalError(String),
}

#[derive(Debug, Clone, Copy, BorshSerialize, BorshDeserialize)]
pub struct ProducerSessionOpened {
    pub incarnation: u32,
    pub expires_at: u64,
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct ConsumerGroupAssignmentResponse {
    pub generation: GenerationId,
    pub ranges: Box<[RangeId]>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct TopicSummary {
    pub name: String,
    pub range_count: u32,
    pub state: TopicState,
}

/// Full metadata snapshot for a topic, returned by `DescribeTopic`. Includes every
/// range — active, sealed, and deleting — so a consumer can walk lineage from any
/// historical ancestor forward through splits and merges.
#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub struct TopicDetail {
    /// Resolved id, so a consumer can fetch by id directly from a holding replica
    /// (no name re-resolution on the data node). See `FetchByIdRequest`.
    pub topic_id: TopicId,
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
    pub(crate) fn from_meta(meta: TopicMeta, addresses: &HashMap<NodeId, NodeAddress>) -> Self {
        let ranges = meta
            .ranges
            .into_values()
            .map(|range| RangeDetail::from_meta(range, addresses))
            .collect();
        TopicDetail {
            topic_id: meta.id,
            name: meta.name,
            state: meta.state,
            ranges,
        }
    }

    pub(crate) fn active_ranges(&self) -> Vec<RangeId> {
        self.ranges
            .iter()
            .filter(|r| r.state == RangeState::Active)
            .map(|r| r.range_id)
            .collect()
    }

    pub fn rebalance(
        &self,
        assigned: HashSet<RangeId>,
        need_start: impl Fn(&RangeId) -> bool,
    ) -> RebalancePlan {
        let effective = self.effective_group_ranges(assigned);
        let mut to_start: Vec<_> = effective
            .iter()
            .filter(|range| need_start(range))
            .copied()
            .collect();
        to_start.sort_by_key(|range| self.lineage_depth(*range));

        RebalancePlan {
            effective,
            to_start,
        }
    }

    /// Expand the Raft-backed active-range assignment with sealed predecessors that
    /// still need one SDK member to drain them. A split delegates its parent to the
    /// first child; a merge delegates both parents to the merged child. Repeating
    /// the expansion handles multi-level lineage while preserving a single owner.
    pub fn effective_group_ranges(&self, mut effective: HashSet<RangeId>) -> HashSet<RangeId> {
        loop {
            let mut changed = false;
            for range in &*self.ranges {
                let delegated = range
                    .split_into
                    .is_some_and(|(first, _)| effective.contains(&first))
                    || range
                        .merged_into
                        .is_some_and(|merged| effective.contains(&merged));
                if delegated {
                    changed |= effective.insert(range.range_id);
                }
            }
            if !changed {
                return effective;
            }
        }
    }

    pub fn lineage_depth(&self, range_id: RangeId) -> usize {
        self.ranges
            .iter()
            .filter(|candidate| {
                candidate
                    .split_into
                    .is_some_and(|children| children.0 == range_id || children.1 == range_id)
                    || candidate.merged_into == Some(range_id)
            })
            .map(|parent| self.lineage_depth(parent.range_id).saturating_add(1))
            .max()
            .unwrap_or_default()
    }

    /// Ranges whose durable checkpoints are needed to start `ranges` safely:
    /// each requested range plus its complete predecessor lineage.
    pub(crate) fn checkpoint_lookup_ranges(&self, ranges: &[RangeId]) -> Box<[RangeId]> {
        let mut required = ranges.iter().copied().collect::<HashSet<_>>();
        for range_id in ranges {
            self.collect_checkpoint_ancestors(*range_id, &mut required);
        }
        required.into_iter().collect()
    }

    fn collect_checkpoint_ancestors(&self, range_id: RangeId, required: &mut HashSet<RangeId>) {
        for parent in self.ranges.iter().filter(|candidate| {
            // Either split or merge
            candidate
                .split_into
                .is_some_and(|children| children.0 == range_id || children.1 == range_id)
                || candidate.merged_into == Some(range_id)
        }) {
            if required.insert(parent.range_id) {
                self.collect_checkpoint_ancestors(parent.range_id, required);
            }
        }
    }
}

pub struct RebalancePlan {
    pub effective: HashSet<RangeId>,
    pub to_start: Vec<RangeId>,
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
    pub(crate) fn end_entry_id(&self) -> EntryId {
        self.sealed_segments
            .last()
            .and_then(|s| s.end_entry_id)
            .unwrap_or(EntryId::MIN)
    }
    fn from_meta(range: RangeMeta, addresses: &HashMap<NodeId, NodeAddress>) -> Self {
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
    pub fn find_segment_for_offset(&self, entry_id: EntryId) -> Option<&SegmentDetail> {
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
    pub fn first_segment_start_offset(&self) -> Option<EntryId> {
        if let Some(seg) = self.sealed_segments.first() {
            Some(seg.start_entry_id)
        } else {
            self.active_segment.as_ref().map(|seg| seg.start_entry_id)
        }
    }

    /// End-exclusive bound for a full-range scan. The active tracker alone can
    /// be empty immediately after a roll while sealed segments still contain
    /// records, so both sources contribute to the high-water mark.
    pub(crate) fn scan_end_exclusive(&self, active_next: EntryId) -> Option<EntryId> {
        let first = self.first_segment_start_offset()?;
        let sealed_next = self
            .sealed_segments
            .last()
            .and_then(|segment| segment.end_entry_id)
            .map(|end| end.saturating_add(1))
            .unwrap_or(EntryId::MIN);
        let end = active_next.max(sealed_next);
        (end > first).then_some(end)
    }
}

/// Per-segment metadata exposed to consumers. The replica set carries resolved
/// client addresses so the consumer can pick a node to send fetches to without a
/// separate address-resolution round trip.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SegmentDetail {
    pub segment_id: SegmentId,
    pub start_entry_id: EntryId,
    /// `None` while the segment is still active (the write head).
    pub end_entry_id: Option<EntryId>,
    pub replica_set: Vec<NodeAddressInfo>,
}

impl SegmentDetail {
    fn from_meta(seg: &SegmentMeta, addresses: &HashMap<NodeId, NodeAddress>) -> Self {
        let replica_set = seg
            .replica_set
            .iter()
            .filter_map(|node| {
                addresses.get(node).map(|addr| NodeAddressInfo {
                    node_id: node.clone(),
                    addr: *addr,
                })
            })
            .collect();
        SegmentDetail {
            segment_id: seg.segment_id,
            start_entry_id: seg.start_entry_id,
            end_entry_id: seg.end_entry_id,
            replica_set,
        }
    }

    /// Pick a replica from the segment's replica set.
    /// load-balances randomly across all replicas
    pub fn pick_replica(&self) -> Option<NodeAddressInfo> {
        if self.replica_set.is_empty() {
            return None;
        }

        use rand::RngExt;
        let idx = rand::rng().random_range(0..self.replica_set.len());
        Some(self.replica_set[idx].clone())
    }

    /// Active tails must be read from the write leader because followers may
    /// legitimately lag and report an empty tail. Sealed segments are immutable
    /// and can be read from any replica.
    pub fn pick_read_replica(&self) -> Option<NodeAddressInfo> {
        if self.end_entry_id.is_none() {
            self.replica_set.first().cloned()
        } else {
            self.pick_replica()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_scan_includes_sealed_tail_when_active_is_empty() {
        let range = RangeDetail {
            range_id: RangeId(0),
            keyspace_start: vec![],
            keyspace_end: vec![u8::MAX],
            state: RangeState::Active,
            active_segment: Some(SegmentDetail {
                segment_id: SegmentId(1),
                start_entry_id: EntryId(1),
                end_entry_id: None,
                replica_set: vec![],
            }),
            sealed_segments: vec![SegmentDetail {
                segment_id: SegmentId(0),
                start_entry_id: EntryId(0),
                end_entry_id: Some(EntryId(0)),
                replica_set: vec![],
            }]
            .into_boxed_slice(),
            split_into: None,
            merged_into: None,
            merged_from: None,
        };

        assert_eq!(range.scan_end_exclusive(EntryId(1)), Some(EntryId(1)));
    }
}
