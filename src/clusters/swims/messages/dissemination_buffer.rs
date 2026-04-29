use bincode::{Decode, Encode};

use crate::clusters::swims::topology::ShardGroupId;
use crate::clusters::{BINCODE_CONFIG, NodeAddress, NodeId, SwimNode};

// UDP does not handle splitting large messages up.
// Preventing IP fragmentation is therefore necessary unless we have dedicated fragmentation handling logics.
// MTU 1500 - IP header 20 - UDP header 8 = 1472, minus ~70 bytes header overhead.
pub(in crate::clusters::swims) const MAX_GOSSIP_BYTES: usize = 1400;

// a standard Vec is often faster than all of the alternatives like priority queue or hash map with entries being 64.
const MAX_ENTRIES: usize = 64;
const LAMBDA: u32 = 3;

pub(in crate::clusters::swims) type SwimBuffer = DisseminationBuffer<SwimNode>;
pub(in crate::clusters::swims) type ShardLeaderGossipBuffer = DisseminationBuffer<ShardLeaderInfo>;

pub(in crate::clusters::swims) trait Disseminable: Clone {
    type Key: PartialEq;
    fn key(&self) -> Self::Key;
    fn should_replace(&self, other: &Self) -> bool;
    fn size(&self) -> usize;
}

impl Disseminable for SwimNode {
    type Key = crate::clusters::NodeId;
    fn key(&self) -> Self::Key {
        self.node_id.clone()
    }
    fn should_replace(&self, _other: &Self) -> bool {
        true
    }
    fn size(&self) -> usize {
        self.encoded_size()
    }
}

pub(in crate::clusters::swims) struct DisseminationBuffer<T: Disseminable> {
    entries: Vec<DisseminationEntry<T>>,
}

impl<T: Disseminable> Default for DisseminationBuffer<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

#[derive(PartialEq, Eq)]
struct DisseminationEntry<T> {
    item: T,
    remaining: u32,
}

impl<T: Disseminable> DisseminationBuffer<T> {
    pub(in crate::clusters::swims) fn enqueue(&mut self, item: T, cluster_size: usize) {
        if item.size() > MAX_GOSSIP_BYTES {
            tracing::error!(
                size = item.size(),
                limit = MAX_GOSSIP_BYTES,
                "Poison pill detected: Single entry exceeds the packet size limit. Dropping update."
            );
            return;
        }

        let remaining = dissemination_count(cluster_size);

        if let Some(pos) = self.entries.iter().position(|e| e.item.key() == item.key()) {
            let existing = &self.entries[pos];
            if !item.should_replace(&existing.item) {
                return;
            }
            self.entries.remove(pos);
        }

        if self.entries.len() >= MAX_ENTRIES {
            tracing::warn!(
                max_entries = MAX_ENTRIES,
                "Gossip buffer full! Evicting the oldest entry prematurely."
            );
            self.entries.pop();
        }

        let insert_pos = self
            .entries
            .binary_search_by(|e| e.remaining.cmp(&remaining).reverse())
            .unwrap_or_else(|pos| pos);

        self.entries
            .insert(insert_pos, DisseminationEntry { item, remaining });
    }

    pub(in crate::clusters::swims) fn collect(&mut self, budget: usize) -> Vec<T> {
        let mut result = Vec::new();
        let mut total_bytes = 0usize;

        for entry in self.entries.iter_mut() {
            let size = entry.item.size();
            if total_bytes + size > budget {
                break;
            }
            total_bytes += size;
            result.push(entry.item.clone());
            entry.remaining = entry.remaining.saturating_sub(1);
        }

        self.entries.retain(|e| e.remaining > 0);
        self.entries.sort_by_key(|b| std::cmp::Reverse(b.remaining));

        result
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ShardLeaderInfo {
    pub shard_group_id: ShardGroupId,
    pub leader_node_id: NodeId,
    pub leader_addr: NodeAddress,
    pub(crate) term: u64,
}

impl ShardLeaderInfo {
    #[inline]
    pub(crate) fn encoded_size(&self) -> usize {
        bincode::encode_to_vec(self, BINCODE_CONFIG)
            .map(|v| v.len())
            .unwrap_or(0)
    }
}

impl Disseminable for ShardLeaderInfo {
    type Key = ShardGroupId;
    fn key(&self) -> ShardGroupId {
        self.shard_group_id
    }
    fn should_replace(&self, other: &Self) -> bool {
        self.term > other.term
    }
    fn size(&self) -> usize {
        self.encoded_size()
    }
}

/// computes LAMBDA * ceil(log₂(N)) (LAMBDA=3, minimum 3) for O(log N) convergence
#[inline]
pub(in crate::clusters::swims) fn dissemination_count(cluster_size: usize) -> u32 {
    if cluster_size <= 1 {
        return LAMBDA;
    }
    let n = cluster_size as f64;
    let count = LAMBDA * (n.log2().ceil() as u32);
    count.max(LAMBDA)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clusters::{NodeAddress, NodeId, SwimNodeState};
    use std::net::SocketAddr;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    fn member(port: u16, state: SwimNodeState, incarnation: u64) -> SwimNode {
        SwimNode {
            node_id: NodeId::new(port.to_string()),
            addr: NodeAddress {
                cluster_addr: addr(port),
                client_addr: addr(port),
            },
            state,
            incarnation,
        }
    }

    #[test]
    fn enqueue_and_collect_returns_members() {
        let mut buf = SwimBuffer::default();
        buf.enqueue(member(1, SwimNodeState::Alive, 0), 10);
        buf.enqueue(member(2, SwimNodeState::Dead, 5), 10);

        let result = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(result.len(), 2);

        let addrs: Vec<_> = result.iter().map(|m| m.addr.cluster_addr).collect();
        assert!(addrs.contains(&addr(1)));
        assert!(addrs.contains(&addr(2)));
    }

    #[test]
    fn collect_decrements_remaining_and_eventually_drains() {
        let mut buf = SwimBuffer::default();
        buf.enqueue(member(1, SwimNodeState::Alive, 0), 2);

        let r1 = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(r1.len(), 1);

        let r2 = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(r2.len(), 1);

        let r3 = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(r3.len(), 1);

        let r4 = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(r4.len(), 0);
    }

    #[test]
    fn duplicate_addr_resets_remaining() {
        let mut buf = SwimBuffer::default();
        buf.enqueue(member(1, SwimNodeState::Alive, 0), 2);

        buf.collect(MAX_GOSSIP_BYTES);
        buf.collect(MAX_GOSSIP_BYTES);

        buf.enqueue(member(1, SwimNodeState::Dead, 1), 2);

        let r1 = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0].state, SwimNodeState::Dead);

        let r2 = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(r2.len(), 1);

        let r3 = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(r3.len(), 1);

        let r4 = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(r4.len(), 0);
    }

    #[test]
    fn priority_order_preserved_across_multiple_collects() {
        let mut buf = SwimBuffer::default();

        buf.enqueue(member(1, SwimNodeState::Alive, 0), 4);

        buf.collect(MAX_GOSSIP_BYTES);
        buf.collect(MAX_GOSSIP_BYTES);

        buf.enqueue(member(2, SwimNodeState::Dead, 1), 4);

        let first = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(
            first[0].addr.cluster_addr,
            addr(2),
            "member(2) should lead (higher remaining)"
        );

        let second = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(
            second[0].addr.cluster_addr,
            addr(2),
            "member(2) should still lead after sort() in previous collect()"
        );
    }

    #[test]
    fn newest_entries_prioritized_over_oldest() {
        let mut buf = SwimBuffer::default();

        buf.enqueue(member(1, SwimNodeState::Alive, 0), 4);

        buf.collect(MAX_GOSSIP_BYTES);

        buf.enqueue(member(2, SwimNodeState::Dead, 1), 4);

        let result = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].addr.cluster_addr, addr(2));
        assert_eq!(result[1].addr.cluster_addr, addr(1));
    }

    #[test]
    fn collect_respects_byte_budget() {
        let mut gossip_buf = SwimBuffer::default();

        let sample_member = member(1, SwimNodeState::Alive, u64::MAX);
        let size = sample_member.encoded_size();

        let budget = size * 5;
        let total = 10;

        for i in 0..total {
            gossip_buf.enqueue(member(i as u16 + 1, SwimNodeState::Alive, u64::MAX), 100);
        }

        let result = gossip_buf.collect(budget);
        assert!(!result.is_empty(), "Should return at least one member");
        assert!(
            result.len() < total,
            "Should have truncated some entries, got {} out of {}",
            result.len(),
            total
        );
        assert!(
            result.len() <= 5,
            "Should fit at most 5 entries in budget of {} bytes",
            budget
        );
    }

    #[test]
    fn evicts_lowest_remaining_when_full() {
        let mut buf = SwimBuffer::default();

        for i in 0..MAX_ENTRIES {
            buf.enqueue(member(i as u16 + 1, SwimNodeState::Alive, 0), 10);
        }

        buf.collect(MAX_GOSSIP_BYTES);

        let new = member(999, SwimNodeState::Dead, 5);
        buf.enqueue(new, 10);

        let result = buf.collect(MAX_GOSSIP_BYTES);
        assert!(result.iter().any(|m| m.addr.cluster_addr == addr(999)));
    }

    #[test]
    fn dissemination_count_scales_with_cluster_size() {
        assert_eq!(dissemination_count(1), 3);
        assert_eq!(dissemination_count(2), 3);
        assert_eq!(dissemination_count(4), 6);
        assert_eq!(dissemination_count(8), 9);
        assert_eq!(dissemination_count(100), 21);
        assert_eq!(dissemination_count(1000), 30);
    }

    // shard leader info tests
    fn leader_info(group: u64, leader: &str, port: u16, term: u64) -> ShardLeaderInfo {
        ShardLeaderInfo {
            shard_group_id: ShardGroupId(group),
            leader_node_id: NodeId::new(leader),
            leader_addr: NodeAddress {
                cluster_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
                client_addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            },
            term,
        }
    }

    #[test]
    fn basic_enqueue_and_collect() {
        let mut buf = ShardLeaderGossipBuffer::default();
        buf.enqueue(leader_info(1, "node-a", 8000, 1), 10);
        buf.enqueue(leader_info(2, "node-b", 8001, 1), 10);

        let result = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn collect_decrements_and_drains() {
        let mut buf = ShardLeaderGossipBuffer::default();
        buf.enqueue(leader_info(1, "node-a", 8000, 1), 2);

        let r1 = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(r1.len(), 1);
        let r2 = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(r2.len(), 1);
        let r3 = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(r3.len(), 1);
        let r4 = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(r4.len(), 0);
    }

    #[test]
    fn higher_term_replaces_existing() {
        let mut buf = ShardLeaderGossipBuffer::default();
        buf.enqueue(leader_info(1, "node-a", 8000, 1), 10);
        buf.enqueue(leader_info(1, "node-b", 8001, 3), 10);

        let result = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].leader_node_id, NodeId::new("node-b"));
        assert_eq!(result[0].term, 3);
    }

    #[test]
    fn lower_term_rejected() {
        let mut buf = ShardLeaderGossipBuffer::default();
        buf.enqueue(leader_info(1, "node-a", 8000, 5), 10);
        buf.enqueue(leader_info(1, "node-b", 8001, 2), 10);

        let result = buf.collect(MAX_GOSSIP_BYTES);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].leader_node_id, NodeId::new("node-a"));
        assert_eq!(result[0].term, 5);
    }

    #[test]
    fn byte_budget_respected() {
        let mut buf = ShardLeaderGossipBuffer::default();
        let sample = leader_info(1, "node-a", 8000, 1);
        let size = sample.encoded_size();
        let budget = size * 2;

        for i in 0..5 {
            buf.enqueue(
                leader_info(i, &format!("node-{}", i), 8000 + i as u16, 1),
                100,
            );
        }

        let result = buf.collect(budget);
        assert!(result.len() <= 2);
        assert!(!result.is_empty());
    }

    #[test]
    fn eviction_on_full_buffer() {
        let mut buf = ShardLeaderGossipBuffer::default();
        for i in 0..64 {
            buf.enqueue(leader_info(i, &format!("node-{}", i), 8000, 1), 10);
        }

        buf.collect(MAX_GOSSIP_BYTES);

        buf.enqueue(leader_info(999, "node-new", 9000, 1), 10);

        let result = buf.collect(MAX_GOSSIP_BYTES);
        assert!(result.iter().any(|i| i.shard_group_id == ShardGroupId(999)));
    }
}
