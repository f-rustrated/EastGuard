//! Routing cache — per-topic snapshot derived from `DescribeTopic`, so the hot path
//! routes without a metadata round-trip. A hint, never the truth: the server's
//! per-request check corrects a stale entry with a redirect (`redirect.rs`), so the
//! cache only ever costs an extra hop, never a wrong target.
use crate::connections::protocol::{RangeDetail, TopicDetail};
use crate::control_plane::NodeAddressInfo;
use crate::control_plane::metadata::{RangeId, RangeState, TopicId};
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

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

        Self {
            topic_id: detail.topic_id,
            range_placements,
            active_ranges: active_ranges.into_boxed_slice(),
        }
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::{NodeAddress, NodeId};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn leader(port: u16) -> NodeAddressInfo {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
        NodeAddressInfo::new(
            NodeId::new(format!("node-{port}")),
            NodeAddress::test(addr, addr),
        )
    }

    fn active_range(
        range_id: u64,
        keyspace_start: &[u8],
        leader_resolved: bool,
    ) -> ActiveRangeRoute {
        ActiveRangeRoute {
            range_id: RangeId(range_id),
            keyspace_start: keyspace_start.to_vec(),
            write_leader: leader_resolved.then(|| leader(range_id as u16 + 1)),
        }
    }
}
