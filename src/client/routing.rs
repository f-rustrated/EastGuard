//! Routing cache — per-topic snapshot derived from `DescribeTopic`, so the hot path
//! routes without a metadata round-trip. A hint, never the truth: the server's
//! per-request check corrects a stale entry with a redirect (`redirect.rs`), so the
//! cache only ever costs an extra hop, never a wrong target.
use crate::connections::protocol::{RangeDetail, TopicDetail};
use crate::control_plane::NodeAddressInfo;
use crate::control_plane::metadata::{RangeId, RangeState, TopicId};
use arc_swap::ArcSwap;
use std::collections::HashMap;
#[cfg(any(test, debug_assertions))]
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;

/// Per-topic routing cache. Built once from a `DescribeTopic` response and
/// replaced wholesale on refresh.
pub(crate) struct TopicRouting {
    pub topic_id: TopicId,
    range_placements: Box<[RangePlacement]>,
    active_ranges: Box<[ActiveRangeRoute]>,
}

/// Replica placement for active and historical ranges used by readers.
struct RangePlacement {
    range_id: RangeId,
    replica_addrs: Box<[NodeAddressInfo]>,
}
impl RangePlacement {
    fn from(r: &RangeDetail) -> Self {
        let placement = r
            .active_segment
            .as_ref()
            .or_else(|| r.sealed_segments.last());
        RangePlacement {
            range_id: r.range_id,
            replica_addrs: placement
                .map(|segment| segment.replica_set.iter().cloned().collect())
                .unwrap_or_default(),
        }
    }
}

/// Active keyspace owner. Leader resolution is optional and independent of ownership.
struct ActiveRangeRoute {
    range_id: RangeId,
    keyspace_start: Vec<u8>,
    write_leader: Option<NodeAddressInfo>,
}

impl ActiveRangeRoute {
    fn from(t: &TopicDetail) -> Vec<Self> {
        t.ranges
            .iter()
            .filter(|range| range.state == RangeState::Active)
            .map(|range| ActiveRangeRoute {
                range_id: range.range_id,
                keyspace_start: range.keyspace_start.clone(),
                write_leader: range
                    .active_segment
                    .as_ref()
                    .and_then(|segment| segment.replica_set.first().cloned()),
            })
            .collect()
    }
}

impl TopicRouting {
    fn from_detail(detail: &TopicDetail) -> Self {
        let range_placements = detail.ranges.iter().map(RangePlacement::from).collect();
        let mut active_ranges = ActiveRangeRoute::from(detail);
        active_ranges.sort_unstable_by(|a, b| a.keyspace_start.cmp(&b.keyspace_start));

        let routing = Self {
            topic_id: detail.topic_id,
            range_placements,
            active_ranges: active_ranges.into_boxed_slice(),
        };

        #[cfg(any(test, debug_assertions))]
        routing.assert_invariants();

        routing
    }

    fn active_range(&self, routing_key: &[u8]) -> Option<&ActiveRangeRoute> {
        let insertion = self
            .active_ranges
            .partition_point(|range| range.keyspace_start.as_slice() <= routing_key);
        self.active_ranges.get(insertion.checked_sub(1)?)
    }

    /// The active range owning `routing_key` — mirrors the server's
    /// `route_active_range`: the active range with the greatest `keyspace_start ≤ key`.
    /// Returns its write leader when that address is currently resolved.
    pub(crate) fn write_leader(&self, routing_key: &[u8]) -> Option<NodeAddressInfo> {
        self.active_range(routing_key)?.write_leader.clone()
    }

    pub(crate) fn range_id(&self, routing_key: &[u8]) -> Option<RangeId> {
        Some(self.active_range(routing_key)?.range_id)
    }

    pub(crate) fn replicas(&self, range_id: RangeId) -> Option<&[NodeAddressInfo]> {
        self.range_placements
            .iter()
            .find(|range| range.range_id == range_id)
            .map(|range| range.replica_addrs.as_ref())
    }

    pub(crate) fn write_leader_for_range(&self, range_id: RangeId) -> Option<NodeAddressInfo> {
        self.range_placements
            .iter()
            .find(|range| range.range_id == range_id)
            .and_then(|range| range.replica_addrs.first().cloned())
    }

    pub(crate) fn active_range_ids(&self) -> impl Iterator<Item = RangeId> + '_ {
        self.active_ranges.iter().map(|range| range.range_id)
    }
}

/// Thread-safe, lock-free per-topic cache. Entries are replaced on refresh and
/// dropped on `invalidate` (when a redirect reveals staleness).
pub(crate) struct RoutingCache {
    topics: ArcSwap<HashMap<String, Arc<TopicRouting>>>,
}

impl RoutingCache {
    pub(crate) fn new() -> Self {
        Self {
            topics: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    pub(crate) fn get(&self, topic: &str) -> Option<Arc<TopicRouting>> {
        self.topics.load().get(topic).cloned()
    }

    pub(crate) fn range_id(&self, topic: &str, routing_key: &[u8]) -> Option<RangeId> {
        self.get(topic)?.range_id(routing_key)
    }

    pub(crate) fn insert(&self, detail: &TopicDetail) {
        let routing = Arc::new(TopicRouting::from_detail(detail));
        self.topics.rcu(|current_topics| {
            let mut next_topics = (**current_topics).clone();
            next_topics.insert(detail.name.clone(), routing.clone());
            Arc::new(next_topics)
        });
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    pub(crate) fn invalidate(&self, topic: &str) {
        self.topics.rcu(|current_topics| {
            // Optimistic check: if it's already gone, don't allocate a new map
            if !current_topics.contains_key(topic) {
                return current_topics.clone();
            }
            let mut next_topics = (**current_topics).clone();
            next_topics.remove(topic);
            Arc::new(next_topics)
        });
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    /// Cached write leader for `routing_key` in `topic`, if the topic is cached and
    /// the key falls in an active range.
    pub(crate) fn write_leader(&self, topic: &str, routing_key: &[u8]) -> Option<NodeAddressInfo> {
        self.get(topic)?.write_leader(routing_key)
    }

    /// Cached *active-segment* replica addresses for a range (C3 tailing fetch).
    /// Historical reads need the sealed-segment extension — see `RangePlacement`.
    pub(crate) fn replicas(
        &self,
        topic: &str,
        range_id: RangeId,
    ) -> Option<Box<[NodeAddressInfo]>> {
        Some(self.get(topic)?.replicas(range_id)?.into())
    }
}

#[cfg(test)]
impl RoutingCache {
    /// Test seam: rewrite every active range's cached write leader for `topic` to
    /// `wrong`, simulating a stale cache (the real leader has moved). A produce then
    /// self-corrects via the server's `NotWriteLeader` redirect.
    pub(crate) fn poison_write_leaders(&self, topic: &str, wrong: NodeAddressInfo) {
        self.topics.rcu(|current_topics| {
            let Some(routing) = current_topics.get(topic) else {
                return current_topics.clone();
            };

            let range_placements = routing
                .range_placements
                .iter()
                .map(|range| RangePlacement {
                    range_id: range.range_id,
                    replica_addrs: if range.replica_addrs.is_empty() {
                        Box::new([])
                    } else {
                        std::iter::once(wrong.clone())
                            .chain(range.replica_addrs.iter().skip(1).cloned())
                            .collect()
                    },
                })
                .collect();

            let poisoned = Arc::new(TopicRouting {
                topic_id: routing.topic_id,
                range_placements,
                active_ranges: routing
                    .active_ranges
                    .iter()
                    .map(|range| ActiveRangeRoute {
                        range_id: range.range_id,
                        keyspace_start: range.keyspace_start.clone(),
                        write_leader: range.write_leader.as_ref().map(|_| wrong.clone()),
                    })
                    .collect(),
            });

            let mut next_topics = (**current_topics).clone();
            next_topics.insert(topic.to_string(), poisoned);
            Arc::new(next_topics)
        });
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }
}

#[cfg(any(test, debug_assertions))]
impl TAssertInvariant for TopicRouting {
    fn assert_invariants(&self) {
        // No two RangePlacement entries in range_placements share the same range_id.
        let mut seen_placements = HashSet::new();
        for placement in self.range_placements.iter() {
            assert!(
                seen_placements.insert(placement.range_id),
                "duplicate range_id {:?} in range_placements for topic {:?}",
                placement.range_id,
                self.topic_id
            );
        }

        //  Unique Active Range IDs:
        let mut seen_active_ranges = HashSet::new();
        for range in self.active_ranges.iter() {
            assert!(
                seen_active_ranges.insert(range.range_id),
                "duplicate active range_id {:?} in active_ranges for topic {:?}",
                range.range_id,
                self.topic_id
            );
        }

        // active_ranges must be strictly sorted by keyspace_start so binary search lookup
        // (partition_point) behaves deterministically and correctly.
        for window in self.active_ranges.windows(2) {
            assert!(
                window[0].keyspace_start < window[1].keyspace_start,
                "active_ranges in topic {:?} are not strictly sorted by keyspace_start: {:?} >= {:?}",
                self.topic_id,
                window[0].keyspace_start,
                window[1].keyspace_start
            );
        }

        // Placement Coverage (Subset Property):
        // Every range_id in active_ranges must exist in range_placements.
        for active in self.active_ranges.iter() {
            assert!(
                seen_placements.contains(&active.range_id),
                "active range_id {:?} in topic {:?} missing from range_placements",
                active.range_id,
                self.topic_id
            );
        }

        // Write Leader Alignment with Replica Set:
        // If an active range specifies a write_leader, the corresponding RangePlacement must
        // have replica_addrs non-empty and its first element equal to write_leader.
        for active in self.active_ranges.iter() {
            if let Some(leader) = &active.write_leader {
                let placement = self
                    .range_placements
                    .iter()
                    .find(|p| p.range_id == active.range_id)
                    .unwrap_or_else(|| {
                        panic!(
                            "missing placement for active range_id {:?}",
                            active.range_id
                        )
                    });
                assert!(
                    !placement.replica_addrs.is_empty(),
                    "active range {:?} has write_leader {:?} but empty replica_addrs in placement",
                    active.range_id,
                    leader
                );
                assert_eq!(
                    placement.replica_addrs.first(),
                    Some(leader),
                    "active range {:?} write_leader {:?} mismatch with first replica address {:?}",
                    active.range_id,
                    leader,
                    placement.replica_addrs.first()
                );
            }
        }
    }
}

#[cfg(any(test, debug_assertions))]
impl TAssertInvariant for RoutingCache {
    fn assert_invariants(&self) {
        let topics = self.topics.load();
        for routing in topics.values() {
            routing.assert_invariants();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connections::protocol::SegmentDetail;
    use crate::control_plane::metadata::TopicState;
    use crate::control_plane::{NodeAddress, NodeId};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn leader(port: u16) -> NodeAddressInfo {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
        NodeAddressInfo::new(
            NodeId::new(format!("node-{port}")),
            NodeAddress::test(addr, addr),
        )
    }

    fn make_segment_detail(segment_id: u64, replicas: Vec<NodeAddressInfo>) -> SegmentDetail {
        SegmentDetail {
            segment_id: crate::control_plane::metadata::SegmentId(segment_id),
            start_entry_id: crate::control_plane::metadata::EntryId(0),
            end_entry_id: None,
            replica_set: replicas,
        }
    }

    fn make_range_detail(
        range_id: u64,
        start_key: &[u8],
        state: RangeState,
        leader_addr: Option<NodeAddressInfo>,
    ) -> RangeDetail {
        let active_segment = leader_addr.map(|addr| make_segment_detail(range_id * 10, vec![addr]));
        RangeDetail {
            range_id: RangeId(range_id),
            keyspace_start: start_key.to_vec(),
            keyspace_end: vec![],
            state,
            active_segment,
            sealed_segments: Box::new([]),
            split_into: None,
            merged_into: None,
            merged_from: None,
        }
    }

    fn make_topic_detail(name: &str, ranges: Vec<RangeDetail>) -> TopicDetail {
        TopicDetail {
            topic_id: TopicId(1),
            name: name.to_string(),
            state: TopicState::Active,
            ranges: ranges.into_boxed_slice(),
        }
    }

    #[test]
    fn test_topic_routing_from_detail_and_lookups() {
        let node1 = leader(8001);
        let node2 = leader(8002);

        // Pass active ranges out of order to ensure from_detail sorts them
        let r2 = make_range_detail(2, b"m", RangeState::Active, Some(node2.clone()));
        let r1 = make_range_detail(1, b"", RangeState::Active, Some(node1.clone()));
        let r_sealed = make_range_detail(3, b"z", RangeState::Sealed, None);

        let detail = make_topic_detail("orders", vec![r2, r1, r_sealed]);
        let routing = TopicRouting::from_detail(&detail);

        assert_eq!(
            routing.active_range_ids().collect::<Vec<_>>(),
            vec![RangeId(1), RangeId(2)]
        );
        assert_eq!(routing.write_leader(b"a"), Some(node1.clone()));
        assert_eq!(routing.write_leader(b"m"), Some(node2.clone()));
        assert_eq!(routing.range_id(b"abc"), Some(RangeId(1)));
        assert_eq!(routing.range_id(b"xyz"), Some(RangeId(2)));

        let replicas_r1 = routing.replicas(RangeId(1)).unwrap();
        assert_eq!(replicas_r1.len(), 1);
        assert_eq!(replicas_r1[0], node1);

        assert_eq!(routing.write_leader_for_range(RangeId(1)), Some(node1));
        assert_eq!(routing.write_leader_for_range(RangeId(2)), Some(node2));
        assert_eq!(routing.write_leader_for_range(RangeId(3)), None);
    }

    #[test]
    fn test_routing_cache_lifecycle() {
        let cache = RoutingCache::new();
        assert!(cache.get("test-topic").is_none());

        let node1 = leader(8001);
        let r1 = make_range_detail(1, b"", RangeState::Active, Some(node1.clone()));
        let detail = make_topic_detail("test-topic", vec![r1]);

        cache.insert(&detail);
        assert!(cache.get("test-topic").is_some());
        assert_eq!(cache.range_id("test-topic", b"key1"), Some(RangeId(1)));
        assert_eq!(
            cache.write_leader("test-topic", b"key1"),
            Some(node1.clone())
        );
        assert_eq!(
            cache.replicas("test-topic", RangeId(1)).unwrap().as_ref(),
            &[node1]
        );

        cache.invalidate("test-topic");
        assert!(cache.get("test-topic").is_none());
        assert_eq!(cache.write_leader("test-topic", b"key1"), None);
    }

    #[test]
    fn test_routing_cache_poison_write_leaders() {
        let cache = RoutingCache::new();
        let orig_node = leader(8001);
        let r1 = make_range_detail(1, b"", RangeState::Active, Some(orig_node));
        let detail = make_topic_detail("test-topic", vec![r1]);

        cache.insert(&detail);

        let wrong_node = leader(9999);
        cache.poison_write_leaders("test-topic", wrong_node.clone());

        assert_eq!(cache.write_leader("test-topic", b"key1"), Some(wrong_node));
    }

    #[test]
    fn test_keyspace_routing_boundaries() {
        let n1 = leader(8001);
        let n2 = leader(8002);
        let n3 = leader(8003);

        let r1 = make_range_detail(1, b"", RangeState::Active, Some(n1.clone()));
        let r2 = make_range_detail(2, b"f", RangeState::Active, Some(n2.clone()));
        let r3 = make_range_detail(3, b"p", RangeState::Active, Some(n3.clone()));

        let detail = make_topic_detail("boundary-topic", vec![r1, r2, r3]);
        let routing = TopicRouting::from_detail(&detail);

        // Boundary tests for partition_point routing
        assert_eq!(routing.range_id(b""), Some(RangeId(1)));
        assert_eq!(routing.range_id(b"a"), Some(RangeId(1)));
        assert_eq!(routing.range_id(b"e"), Some(RangeId(1)));
        assert_eq!(routing.range_id(b"f"), Some(RangeId(2)));
        assert_eq!(routing.range_id(b"g"), Some(RangeId(2)));
        assert_eq!(routing.range_id(b"o"), Some(RangeId(2)));
        assert_eq!(routing.range_id(b"p"), Some(RangeId(3)));
        assert_eq!(routing.range_id(b"z"), Some(RangeId(3)));
    }

    #[test]
    fn test_invariants_pass_on_valid_routing() {
        let node = leader(8001);
        let r1 = make_range_detail(1, b"", RangeState::Active, Some(node));
        let detail = make_topic_detail("valid-topic", vec![r1]);

        let cache = RoutingCache::new();
        cache.insert(&detail);
        cache.assert_invariants();
    }

    #[test]
    fn test_invariants_fail_on_unsorted_active_ranges() {
        let invalid_routing = TopicRouting {
            topic_id: TopicId(1),
            range_placements: Box::new([
                RangePlacement {
                    range_id: RangeId(1),
                    replica_addrs: Box::new([]),
                },
                RangePlacement {
                    range_id: RangeId(2),
                    replica_addrs: Box::new([]),
                },
            ]),
            active_ranges: Box::new([
                ActiveRangeRoute {
                    range_id: RangeId(2),
                    keyspace_start: b"z".to_vec(),
                    write_leader: None,
                },
                ActiveRangeRoute {
                    range_id: RangeId(1),
                    keyspace_start: b"a".to_vec(),
                    write_leader: None,
                },
            ]),
        };

        assert!(std::panic::catch_unwind(|| invalid_routing.assert_invariants()).is_err());
    }

    #[test]
    fn test_invariants_fail_on_missing_placement() {
        let invalid_routing = TopicRouting {
            topic_id: TopicId(1),
            range_placements: Box::new([]),
            active_ranges: Box::new([ActiveRangeRoute {
                range_id: RangeId(1),
                keyspace_start: b"a".to_vec(),
                write_leader: None,
            }]),
        };

        assert!(std::panic::catch_unwind(|| invalid_routing.assert_invariants()).is_err());
    }

    #[test]
    fn test_invariants_fail_on_duplicate_placement() {
        let invalid_routing = TopicRouting {
            topic_id: TopicId(1),
            range_placements: Box::new([
                RangePlacement {
                    range_id: RangeId(1),
                    replica_addrs: Box::new([]),
                },
                RangePlacement {
                    range_id: RangeId(1),
                    replica_addrs: Box::new([]),
                },
            ]),
            active_ranges: Box::new([]),
        };

        assert!(std::panic::catch_unwind(|| invalid_routing.assert_invariants()).is_err());
    }

    #[test]
    fn test_invariants_fail_on_write_leader_mismatch() {
        let invalid_routing = TopicRouting {
            topic_id: TopicId(1),
            range_placements: Box::new([RangePlacement {
                range_id: RangeId(1),
                replica_addrs: Box::new([leader(8001)]),
            }]),
            active_ranges: Box::new([ActiveRangeRoute {
                range_id: RangeId(1),
                keyspace_start: b"a".to_vec(),
                write_leader: Some(leader(9999)),
            }]),
        };

        assert!(std::panic::catch_unwind(|| invalid_routing.assert_invariants()).is_err());
    }
}
