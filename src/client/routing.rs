//! Routing cache — per-topic snapshot derived from `DescribeTopic`, so the hot path
//! routes without a metadata round-trip. A hint, never the truth: the server's
//! per-request check corrects a stale entry with a redirect (`redirect.rs`), so the
//! cache only ever costs an extra hop, never a wrong target.
use crate::connections::protocol::{NodeAddressInfo, RangeDetail, TopicDetail};
use crate::control_plane::metadata::{RangeId, TopicId};
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

/// Per-topic routing snapshot. Built once from a `DescribeTopic` response and
/// replaced wholesale on refresh.
pub(crate) struct TopicRouting {
    pub topic_id: TopicId,
    ranges: Box<[RangeRoute]>,
}

/// One active range's placement. Sealed/deleting ranges have no write leader.
struct RangeRoute {
    range_id: RangeId,
    keyspace_start: Vec<u8>,
    /// `replica_set[0]` of the active segment, the produce target. `None` when the
    /// range has no active segment (sealed/deleting) or its replica set is empty.
    write_leader: Option<NodeAddressInfo>,
    replicas: Box<[NodeAddressInfo]>,
}

// Active segment only — correct for produce (the write head) and a tailing consumer.
// Historical/cold reads live in *sealed* segments whose placement can differ, so they
// need per-segment replicas; that's blocked on the DescribeTopic sealed-segment
// extension (`RangeDetail` exposes only `active_segment` today) and is a C3 concern.
// See c1_routing_and_connections.md / d4.
impl From<&RangeDetail> for RangeRoute {
    fn from(range: &RangeDetail) -> Self {
        let placement = range
            .active_segment
            .as_ref()
            .or_else(|| range.sealed_segments.last());
        let replicas: Box<[NodeAddressInfo]> = placement
            .map(|seg| seg.replica_set.iter().cloned().collect())
            .unwrap_or_default();
        RangeRoute {
            range_id: range.range_id,
            keyspace_start: range.keyspace_start.clone(),
            write_leader: range
                .active_segment
                .as_ref()
                .and_then(|_| replicas.first().cloned()),
            replicas,
        }
    }
}

impl TopicRouting {
    /// The active range owning `routing_key` — mirrors the server's
    /// `route_active_range`: the active range with the greatest `keyspace_start ≤ key`.
    /// Returns its write leader.
    pub(crate) fn write_leader(&self, routing_key: &[u8]) -> Option<NodeAddressInfo> {
        self.ranges
            .iter()
            .rev()
            .find(|r| r.write_leader.is_some() && r.keyspace_start.as_slice() <= routing_key)
            .and_then(|r| r.write_leader.clone())
    }

    pub(crate) fn range_id(&self, routing_key: &[u8]) -> Option<RangeId> {
        self.ranges
            .iter()
            .rev()
            .find(|r| r.write_leader.is_some() && r.keyspace_start.as_slice() <= routing_key)
            .map(|r| r.range_id)
    }

    pub(crate) fn replicas(&self, range_id: RangeId) -> Option<&[NodeAddressInfo]> {
        self.ranges
            .iter()
            .find(|r| r.range_id == range_id)
            .map(|r| r.replicas.as_ref())
    }

    pub(crate) fn write_leader_for_range(&self, range_id: RangeId) -> Option<NodeAddressInfo> {
        self.ranges
            .iter()
            .find(|range| range.range_id == range_id)
            .and_then(|range| range.replicas.first().cloned())
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
        let mut ranges: Vec<RangeRoute> = detail.ranges.iter().map(RangeRoute::from).collect();
        // Sort ranges by keyspace_start so that reverse-scan and binary search work
        ranges.sort_unstable_by(|a, b| a.keyspace_start.cmp(&b.keyspace_start));

        let topic_routing = TopicRouting {
            topic_id: detail.topic_id,
            ranges: ranges.into_boxed_slice(),
        };

        let routing = Arc::new(topic_routing);
        self.topics.rcu(|current_topics| {
            let mut next_topics = (**current_topics).clone();
            next_topics.insert(detail.name.clone(), routing.clone());
            Arc::new(next_topics)
        });
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
    }

    /// Cached write leader for `routing_key` in `topic`, if the topic is cached and
    /// the key falls in an active range.
    pub(crate) fn write_leader(&self, topic: &str, routing_key: &[u8]) -> Option<NodeAddressInfo> {
        self.get(topic)?.write_leader(routing_key)
    }

    /// Cached *active-segment* replica addresses for a range (C3 tailing fetch).
    /// Historical reads need the sealed-segment extension — see `RangeRoute`.
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

            let ranges = routing
                .ranges
                .iter()
                .map(|r| RangeRoute {
                    range_id: r.range_id,
                    keyspace_start: r.keyspace_start.clone(),
                    write_leader: r.write_leader.as_ref().map(|_| wrong.clone()),
                    replicas: r.replicas.clone(),
                })
                .collect();

            let poisoned = Arc::new(TopicRouting {
                topic_id: routing.topic_id,
                ranges,
            });

            let mut next_topics = (**current_topics).clone();
            next_topics.insert(topic.to_string(), poisoned);
            Arc::new(next_topics)
        });
    }
}
