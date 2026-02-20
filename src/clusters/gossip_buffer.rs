use std::collections::BTreeMap;

use super::Member;

// UDP does not handle splitting large messages up.
// Preventing IP fragmentation is therefore necessary unless we have dedicated frangmentation handling logics.
const MAX_GOSSIP_BYTES: usize = 900;

// a standard Vec is often faster than all of the alternatives like priority queue or hash map with entries being 64.
const MAX_ENTRIES: usize = 64;
const LAMBDA: u32 = 3;

#[derive(Default)]
pub(super) struct GossipBuffer {
    entries: Vec<GossipEntry>,
}

#[derive(PartialEq, Eq)]
struct GossipEntry {
    member: Member,
    remaining: u32,
}

impl PartialOrd for GossipEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.remaining.partial_cmp(&other.remaining)
    }
}
impl Ord for GossipEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.remaining.cmp(&other.remaining)
    }
}

impl GossipBuffer {
    pub(super) fn enqueue(&mut self, member: Member, cluster_size: usize) {
        if member.encoded_size() > MAX_GOSSIP_BYTES {
            // TODO Log an error or return a Result::Err here.
            eprintln!("Single member should never exceed the packet size limit");

            return;
        }

        let remaining = dissemination_count(cluster_size);

        // 1. Remove existing entry if it's already in the buffer
        if let Some(pos) = self
            .entries
            .iter()
            .position(|e| e.member.addr == member.addr)
        {
            self.entries.remove(pos);
        }

        if self.entries.len() >= MAX_ENTRIES {
            // Because the Vec is always sorted descending by `remaining`,
            // the least-used entry is ALWAYS at the very end.
            self.entries.pop();
        }

        // Find exact insertion point
        let insert_pos = self
            .entries
            .binary_search_by(|e| e.remaining.cmp(&remaining).reverse()) // reverse is to search in desc order
            .unwrap_or_else(|pos| pos);

        self.entries
            .insert(insert_pos, GossipEntry { member, remaining });
    }

    pub(super) fn collect(&mut self) -> Vec<Member> {
        let mut result = Vec::new();
        let mut total_bytes = 0usize;
        let mut included_count = 0;

        for entry in &self.entries {
            let size = entry.member.encoded_size();
            if total_bytes + size > MAX_GOSSIP_BYTES {
                // If a single Member's encoded size is strictly greater than MAX_GOSSIP_BYTES
                // (e.g., a 1000-byte member with a 900-byte limit),
                // it acts as a poison pill and will quietly bring the entire gossip protocol to a halt.
                // that's why we need to deny in enqueue.
                break;
            }
            total_bytes += size;
            result.push(entry.member.clone());
            included_count += 1;
        }

        for entry in self.entries.iter_mut().take(included_count) {
            entry.remaining = entry.remaining.saturating_sub(1);
        }

        self.entries.retain(|e| e.remaining > 0);
        self.entries.sort();

        result
    }
}

/// computes LAMBDA * ceil(log₂(N)) (LAMBDA=3, minimum 3) for O(log N) convergence
#[inline]
pub(super) fn dissemination_count(cluster_size: usize) -> u32 {
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
    use crate::clusters::NodeState;
    use std::net::SocketAddr;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    fn member(port: u16, state: NodeState, incarnation: u64) -> Member {
        Member {
            addr: addr(port),
            state,
            incarnation,
        }
    }

    #[test]
    fn enqueue_and_collect_returns_members() {
        let mut buf = GossipBuffer::default();
        buf.enqueue(member(1, NodeState::Alive, 0), 10);
        buf.enqueue(member(2, NodeState::Dead, 5), 10);

        let result = buf.collect();
        assert_eq!(result.len(), 2);

        let addrs: Vec<_> = result.iter().map(|m| m.addr).collect();
        assert!(addrs.contains(&addr(1)));
        assert!(addrs.contains(&addr(2)));
    }

    #[test]
    fn collect_decrements_remaining_and_eventually_drains() {
        let mut buf = GossipBuffer::default();
        // cluster_size=2 → dissemination_count = 3 * ceil(log2(2)) = 3
        buf.enqueue(member(1, NodeState::Alive, 0), 2);

        let r1 = buf.collect();
        assert_eq!(r1.len(), 1);

        let r2 = buf.collect();
        assert_eq!(r2.len(), 1);

        let r3 = buf.collect();
        assert_eq!(r3.len(), 1);

        // After 3 collects, remaining hit 0 and entry was removed
        let r4 = buf.collect();
        assert_eq!(r4.len(), 0);
    }

    #[test]
    fn duplicate_addr_resets_remaining() {
        let mut buf = GossipBuffer::default();
        buf.enqueue(member(1, NodeState::Alive, 0), 2);

        // Collect twice to decrement remaining to 1
        buf.collect();
        buf.collect();

        // Re-enqueue same addr with new state
        buf.enqueue(member(1, NodeState::Dead, 1), 2);

        // Should have fresh remaining (3), so 3 more collects before drain
        let r1 = buf.collect();
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0].state, NodeState::Dead);

        let r2 = buf.collect();
        assert_eq!(r2.len(), 1);

        let r3 = buf.collect();
        assert_eq!(r3.len(), 1);

        let r4 = buf.collect();
        assert_eq!(r4.len(), 0);
    }

    #[test]
    fn newest_entries_prioritized_over_oldest() {
        let mut buf = GossipBuffer::default();

        // Enqueue first entry
        buf.enqueue(member(1, NodeState::Alive, 0), 4);

        // Collect once to decrement first entry's remaining
        buf.collect();

        // Enqueue second entry (has higher remaining because it's fresh)
        buf.enqueue(member(2, NodeState::Dead, 1), 4);

        let result = buf.collect();
        assert_eq!(result.len(), 2);
        // Entry 2 should come first (higher remaining)
        assert_eq!(result[0].addr, addr(2));
        assert_eq!(result[1].addr, addr(1));
    }

    #[test]
    fn collect_respects_byte_budget() {
        let mut gossip_buf = GossipBuffer::default();

        let sample_member = member(1, NodeState::Alive, u64::MAX);
        let size = sample_member.encoded_size();

        // division by size to find out exactly how many whole Member structs can fit into the budget.
        // (For example, if a member is 20 bytes, 900 / 20 = 45 members).
        // +1 is upper bound.
        let max_fitting = MAX_GOSSIP_BYTES / size + 1;

        // Enqueue more than can fit (but within MAX_ENTRIES)
        let total = (max_fitting + 5).min(MAX_ENTRIES);
        assert!(
            total > max_fitting,
            "Need entries to exceed byte budget for this test (member size: {}, max_fitting: {}, MAX_ENTRIES: {})",
            size,
            max_fitting,
            MAX_ENTRIES
        );

        for i in 0..total {
            gossip_buf.enqueue(member(i as u16 + 1, NodeState::Alive, u64::MAX), 100);
        }

        let result = gossip_buf.collect();
        assert!(!result.is_empty(), "Should return at least one member");
        assert!(
            result.len() < total,
            "Should have truncated some entries, got {} out of {}",
            result.len(),
            total
        );
    }

    #[test]
    fn evicts_lowest_remaining_when_full() {
        let mut buf = GossipBuffer::default();

        // Fill to MAX_ENTRIES
        for i in 0..MAX_ENTRIES {
            buf.enqueue(member(i as u16 + 1, NodeState::Alive, 0), 10);
        }

        // Collect once to decrement all entries' remaining
        buf.collect();

        // Add a new entry — should evict one with lowest remaining
        let new = member(999, NodeState::Dead, 5);
        buf.enqueue(new, 10);

        let result = buf.collect();
        // New entry should be present (it has the highest remaining)
        assert!(result.iter().any(|m| m.addr == addr(999)));
    }

    #[test]
    fn dissemination_count_scales_with_cluster_size() {
        assert_eq!(dissemination_count(1), 3);
        // ceil(log2(2)) = 1, 3*1 = 3
        assert_eq!(dissemination_count(2), 3);
        // ceil(log2(4)) = 2, 3*2 = 6
        assert_eq!(dissemination_count(4), 6);
        // ceil(log2(8)) = 3, 3*3 = 9
        assert_eq!(dissemination_count(8), 9);
        // ceil(log2(100)) = 7, 3*7 = 21
        assert_eq!(dissemination_count(100), 21);
        // ceil(log2(1000)) = 10, 3*10 = 30
        assert_eq!(dissemination_count(1000), 30);
    }
}
