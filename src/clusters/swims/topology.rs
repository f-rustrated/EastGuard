use bincode::{Decode, Encode};
use murmur3::murmur3_32;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io::Cursor;

use crate::clusters::swims::messages::dissemination_buffer::ShardLeaderInfo;
use crate::clusters::{NodeAddress, NodeId, SwimNodeState};

/// Deterministic identifier for a shard group, derived from the hash of the first
/// virtual node on the consistent hash ring for a given key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct ShardGroupId(pub u64);

impl ShardGroupId {
    /// Derives a ShardGroupId from a vnode token's hash.
    /// Only valid when the hash comes from an actual vnode position on the ring.
    fn from_vnode_hash(hash: u32) -> Self {
        Self(hash as u64)
    }
}

/// A shard group: the set of physical nodes responsible for a key range on the ring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardGroup {
    pub id: ShardGroupId,
    pub members: Vec<NodeId>,
}

/// Position on the consistent hash ring. Sorted by `(hash, pnode_id, replica_index)`.
/// Collisions don't cause ownership conflicts — just means two vnodes share a position.
/// The tiebreaker ordering determines who appears first in the walk.
#[derive(Eq, PartialEq, Ord, PartialOrd, Debug, Clone)]
pub struct VirtualNodeToken {
    hash: u32,
    pnode_id: NodeId,
    replica_index: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ShardLeaderEntry {
    pub leader_node_id: NodeId,
    pub leader_addr: NodeAddress,
    pub term: u64,
}

#[derive(Clone, Debug)]
pub struct TopologyConfig {
    pub vnodes_per_pnode: u64,
    pub replication_factor: usize,
}

#[derive(Clone, Debug)]
pub struct Topology {
    config: TopologyConfig,
    /// Consistent hash ring: virtual node positions keyed by (hash, pnode_id, replica_index).
    vnodes: BTreeSet<VirtualNodeToken>,

    /// Reverse index: for each physical node, the shard group IDs it participates in.
    /// Rebuilt after every ring mutation. Makes `shard_groups_for_node()` O(1).
    node_group_ids: HashMap<NodeId, Vec<ShardGroupId>>,

    /// Canonical shard group definitions, keyed by ShardGroupId (= nearest vnode's token hash).
    /// Each group maps a ring position to the RF physical nodes responsible for it.
    groups: HashMap<ShardGroupId, ShardGroup>,
    /// Shard leader map: tracks current leader for each shard group.
    /// NOT auto-cleared on node death — Raft re-election gossips new leader with higher term.
    shard_leaders: HashMap<ShardGroupId, ShardLeaderEntry>,
}

impl Topology {
    pub fn new(nodes: impl IntoIterator<Item = NodeId>, config: TopologyConfig) -> Self {
        let mut topology = Self {
            config,
            vnodes: BTreeSet::new(),
            groups: HashMap::new(),
            node_group_ids: HashMap::new(),
            shard_leaders: HashMap::new(),
        };
        for pnode_id in nodes {
            topology.add_pnode(pnode_id);
        }
        topology.rebuild_reverse_index();
        topology
    }

    pub(crate) fn update(&mut self, node_id: NodeId, state: &SwimNodeState) {
        match state {
            SwimNodeState::Alive => {
                self.insert_node(node_id);
            }
            SwimNodeState::Dead => {
                self.remove_node(&node_id);
            }
            SwimNodeState::Suspect => {}
        }
    }

    fn add_pnode(&mut self, pnode_id: NodeId) {
        if self.node_group_ids.contains_key(&pnode_id) {
            return;
        }
        for replica_index in 0..self.config.vnodes_per_pnode {
            self.vnodes
                .insert(generate_vnode_token(&pnode_id, replica_index));
        }
    }

    fn remove_pnode(&mut self, pnode_id: &NodeId) -> bool {
        if !self.node_group_ids.contains_key(pnode_id) {
            return false;
        }
        for replica_index in 0..self.config.vnodes_per_pnode {
            self.vnodes
                .remove(&generate_vnode_token(pnode_id, replica_index));
        }
        true
    }

    fn walk_clockwise_from(&self, hash: u32) -> impl Iterator<Item = &VirtualNodeToken> {
        let start = VirtualNodeToken {
            hash,
            pnode_id: NodeId::new(""),
            replica_index: 0,
        };
        self.vnodes
            .range(&start..)
            .chain(self.vnodes.range(..&start))
    }

    fn token_owners_at(&self, hash: u32, n: usize) -> Vec<&NodeId> {
        if self.vnodes.is_empty() || n == 0 {
            return Vec::new();
        }
        let mut result: Vec<&NodeId> = Vec::with_capacity(n);
        for token in self.walk_clockwise_from(hash) {
            if result.iter().any(|o| **o == token.pnode_id) {
                continue;
            }
            result.push(&token.pnode_id);
            if result.len() == n {
                break;
            }
        }
        result
    }

    fn insert_node(&mut self, pnode_id: NodeId) {
        self.add_pnode(pnode_id);
        self.rebuild_reverse_index();
    }

    fn remove_node(&mut self, pnode_id: &NodeId) -> bool {
        let removed = self.remove_pnode(pnode_id);
        if removed {
            self.rebuild_reverse_index();
        }
        removed
    }

    /// Rebuild groups and reverse index from the ring. O(total_vnodes × RF).
    /// Called after every ring mutation.
    fn rebuild_reverse_index(&mut self) {
        self.groups.clear();
        self.node_group_ids.clear();
        let mut seen = HashSet::new();

        for token in &self.vnodes {
            let id = ShardGroupId::from_vnode_hash(token.hash);
            if !seen.insert(id) {
                continue;
            }
            let owners = self.token_owners_at(token.hash, self.config.replication_factor);
            let members: Vec<NodeId> = owners.into_iter().cloned().collect();
            for member in &members {
                self.node_group_ids
                    .entry(member.clone())
                    .or_default()
                    .push(id);
            }
            self.groups.insert(id, ShardGroup { id, members });
        }
    }

    #[allow(dead_code)]
    pub fn token_owners_for(&self, key: &[u8], n: usize) -> Vec<&NodeId> {
        let hash = hash_stable(key);
        self.token_owners_at(hash, n)
    }

    /// Returns the shard group responsible for `key`.
    ///
    /// Walks the consistent hash ring clockwise from the key's hash position to
    /// find the nearest vnode. That vnode's token hash becomes the `ShardGroupId`.
    /// Looks up the canonical `ShardGroup` from the `groups` map.
    ///
    /// Returns `None` if the ring is empty.
    #[allow(dead_code)]
    pub fn shard_group_for(&self, key: &[u8]) -> Option<&ShardGroup> {
        let hash = hash_stable(key);
        let nearest_token = self.walk_clockwise_from(hash).next()?;
        let id = ShardGroupId::from_vnode_hash(nearest_token.hash);
        self.groups.get(&id)
    }

    /// Returns all unique shard groups that `node_id` participates in.
    /// O(G) where G = number of groups for this node (lookups into `groups` map).
    pub fn shard_groups_for_node(&self, node_id: &NodeId) -> Vec<&ShardGroup> {
        self.node_group_ids
            .get(node_id)
            .map(|ids| ids.iter().filter_map(|id| self.groups.get(id)).collect())
            .unwrap_or_default()
    }

    #[expect(dead_code)]
    pub fn num_nodes(&self) -> usize {
        self.node_group_ids.len()
    }

    pub fn update_shard_leader(&mut self, info: &ShardLeaderInfo) -> bool {
        if let Some(existing) = self.shard_leaders.get(&info.shard_group_id)
            && info.term <= existing.term
        {
            return false;
        }
        self.shard_leaders.insert(
            info.shard_group_id,
            ShardLeaderEntry {
                leader_node_id: info.leader_node_id.clone(),
                leader_addr: info.leader_addr,
                term: info.term,
            },
        );
        true
    }

    #[allow(dead_code)]
    pub fn shard_leader(&self, shard_group_id: ShardGroupId) -> Option<&ShardLeaderEntry> {
        self.shard_leaders.get(&shard_group_id)
    }

    #[allow(dead_code)]
    pub fn all_shard_leaders(&self) -> &HashMap<ShardGroupId, ShardLeaderEntry> {
        &self.shard_leaders
    }
}

fn generate_vnode_token(node_id: &NodeId, replica_index: u64) -> VirtualNodeToken {
    let mut buf = Vec::with_capacity(node_id.len() + 8);
    buf.extend_from_slice(node_id.as_bytes());
    buf.extend_from_slice(&replica_index.to_be_bytes());
    VirtualNodeToken {
        hash: hash_stable(&buf),
        pnode_id: node_id.clone(),
        replica_index,
    }
}

/// We shouldn't use DefaultHasher because its algorithm and seed are intentionally unstable across
/// processes, runs, and Rust versions, making the same input produce different hashes in a
/// distributed system.
#[inline]
fn hash_stable(key: &[u8]) -> u32 {
    let mut cursor = Cursor::new(key);
    murmur3_32(&mut cursor, 0).expect("Murmur3 hashing failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn topology_from(nodes: &[&str], config: TopologyConfig) -> Topology {
        let ids = nodes.iter().map(|id| NodeId::new(*id));
        Topology::new(ids, config)
    }

    #[test]
    fn new_builds_correct_node_and_vnode_counts() {
        let topology = topology_from(
            &["node-0", "node-1", "node-2"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        assert_eq!(topology.node_group_ids.len(), 3);
        assert_eq!(topology.vnodes.len(), 12);

        assert!(topology.node_group_ids.contains_key("node-1"));
        assert!(topology.node_group_ids.contains_key("node-2"));
        assert!(!topology.node_group_ids.contains_key("node-3"));
    }

    #[test]
    fn insert_node_adds_node_and_vnodes_to_ring() {
        let mut topology = topology_from(
            &["node-0", "node-1", "node-2"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        topology.insert_node(NodeId::new("node-3"));

        assert_eq!(topology.node_group_ids.len(), 4);
        assert_eq!(topology.vnodes.len(), 16);
        assert!(topology.node_group_ids.contains_key("node-3"));
    }

    #[test]
    fn insert_node_is_idempotent_for_duplicate_id() {
        let mut topology = topology_from(
            &["node-0", "node-1", "node-2"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        topology.insert_node(NodeId::new("node-2"));

        assert_eq!(topology.node_group_ids.len(), 3);
        assert_eq!(topology.vnodes.len(), 12);
    }

    #[test]
    fn remove_node_cleans_up_node_and_its_vnodes() {
        let mut topology = topology_from(
            &["node-0", "node-1", "node-2"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        let removed = topology.remove_node(&NodeId::new("node-0"));
        assert!(removed);
        assert_eq!(topology.node_group_ids.len(), 2);
        assert_eq!(topology.vnodes.len(), 8);
        assert!(!topology.node_group_ids.contains_key("node-0"));
    }

    #[test]
    fn token_owners_for_returns_distinct_physical_nodes_with_distinct_policy() {
        let nodes: Vec<String> = (0..3).map(|idx| format!("node-{}", idx)).collect();
        let node_strs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
        let topology = topology_from(
            &node_strs,
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        let single_owner = topology.token_owners_for(b"hello", 1);
        assert_eq!(single_owner.len(), 1);

        let multiple_owner = topology.token_owners_for(b"hello", 3);
        assert_eq!(multiple_owner.len(), 3);
        let physical_node_ids: HashSet<&NodeId> = multiple_owner.into_iter().collect();
        assert_eq!(physical_node_ids.len(), 3);
    }

    #[test]
    fn token_owners_for_limited_to_available_node_count_with_distinct_policy() {
        let topology = topology_from(
            &["node-0", "node-1", "node-2"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );
        let owners = topology.token_owners_for(b"any-key", 5);
        assert_eq!(owners.len(), 3);
    }

    #[test]
    fn removing_primary_owner_promotes_next_node() {
        let topology = topology_from(
            &["node-0", "node-1", "node-2"],
            TopologyConfig {
                vnodes_per_pnode: 150,
                replication_factor: 3,
            },
        );

        let key = b"test-key";
        let owners = topology.token_owners_for(key, 2);
        assert_eq!(owners.len(), 2);
        let primary_id = owners[0].clone();
        let successor_id = owners[1].clone();

        let mut topology = topology;
        topology.remove_node(&primary_id);

        let new_owners = topology.token_owners_for(key, 1);
        assert_eq!(new_owners.len(), 1);
        assert_eq!(*new_owners[0], successor_id);
    }

    // --- ShardGroup tests ---

    #[test]
    fn shard_group_returns_replication_factor_members() {
        let topology = topology_from(
            &["node-0", "node-1", "node-2"],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 3,
            },
        );

        let group = topology.shard_group_for(b"topic-blue").unwrap();
        assert_eq!(group.members.len(), 3);

        let unique: HashSet<_> = group.members.iter().collect();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn shard_group_deterministic_across_calls() {
        let topology = topology_from(
            &["node-0", "node-1", "node-2"],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 3,
            },
        );

        let group1 = topology.shard_group_for(b"topic-blue").unwrap();
        let group2 = topology.shard_group_for(b"topic-blue").unwrap();
        assert_eq!(group1, group2);
    }

    #[test]
    fn shard_group_id_differs_for_different_keys() {
        let topology = topology_from(
            &["node-0", "node-1", "node-2"],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 2,
            },
        );

        let group_a = topology.shard_group_for(b"topic-alpha").unwrap();
        let group_b = topology.shard_group_for(b"topic-beta").unwrap();
        assert_ne!(group_a.id, group_b.id);
        assert!(topology.groups.contains_key(&group_a.id));
        assert!(topology.groups.contains_key(&group_b.id));
    }

    #[test]
    fn shard_group_limited_to_available_nodes() {
        let topology = topology_from(
            &["node-0", "node-1"],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 5,
            },
        );

        let group = topology.shard_group_for(b"any-key").unwrap();
        assert_eq!(group.members.len(), 2);
    }

    #[test]
    fn shard_group_recalculates_on_node_removal() {
        let mut topology = topology_from(
            &["node-0", "node-1", "node-2"],
            TopologyConfig {
                vnodes_per_pnode: 150,
                replication_factor: 3,
            },
        );

        let key = b"test-key";
        let before = topology.shard_group_for(key).unwrap().clone();
        assert_eq!(before.members.len(), 3);

        let removed = before.members[0].clone();
        topology.remove_node(&removed);

        let after = topology.shard_group_for(key).unwrap();
        assert_eq!(after.members.len(), 2);
        assert!(!after.members.contains(&removed));
    }

    #[test]
    fn shard_group_empty_ring_returns_none() {
        let topology = topology_from(
            &[],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 3,
            },
        );

        assert!(topology.shard_group_for(b"any-key").is_none());
    }

    // --- shard_groups_for_node tests ---

    #[test]
    fn shard_groups_for_node_includes_all_participating_groups() {
        let topology = topology_from(
            &["node-0", "node-1", "node-2"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        let groups = topology.shard_groups_for_node(&NodeId::new("node-0"));
        assert!(!groups.is_empty());
        for group in &groups {
            assert!(group.members.contains(&NodeId::new("node-0")));
            assert_eq!(group.members.len(), 3);
        }
    }

    #[test]
    fn shard_groups_for_node_excludes_non_member() {
        let topology = topology_from(
            &["node-0", "node-1", "node-2", "node-3"],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 2,
            },
        );

        let groups_0 = topology.shard_groups_for_node(&NodeId::new("node-0"));
        let groups_3 = topology.shard_groups_for_node(&NodeId::new("node-3"));

        let total_vnodes = 4 * 64; // 256
        assert!(groups_0.len() < total_vnodes);
        assert!(groups_3.len() < total_vnodes);
        assert!(!groups_0.is_empty());
        assert!(!groups_3.is_empty());
    }

    #[test]
    fn shard_groups_for_node_empty_after_removal() {
        let mut topology = topology_from(
            &["node-0", "node-1", "node-2"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        topology.remove_node(&NodeId::new("node-0"));
        let groups = topology.shard_groups_for_node(&NodeId::new("node-0"));
        assert!(groups.is_empty());
    }

    // --- shard leader map tests ---

    #[test]
    fn update_shard_leader_inserts_new_entry() {
        let mut topology = topology_from(
            &["node-0"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 1,
            },
        );

        let info = ShardLeaderInfo {
            shard_group_id: ShardGroupId(42),
            leader_node_id: NodeId::new("node-0"),
            leader_addr: NodeAddress {
                cluster_addr: "127.0.0.1:8080".parse().unwrap(),
                client_addr: "127.0.0.1:7080".parse().unwrap(),
            },
            term: 1,
        };
        assert!(topology.update_shard_leader(&info));

        let entry = topology.shard_leader(ShardGroupId(42)).unwrap();
        assert_eq!(entry.leader_node_id, NodeId::new("node-0"));
        assert_eq!(
            entry.leader_addr.cluster_addr,
            "127.0.0.1:8080".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            entry.leader_addr.client_addr,
            "127.0.0.1:7080".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(entry.term, 1);
    }

    #[test]
    fn update_shard_leader_higher_term_replaces() {
        let mut topology = topology_from(
            &["node-0"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 1,
            },
        );

        let info1 = ShardLeaderInfo {
            shard_group_id: ShardGroupId(42),
            leader_node_id: NodeId::new("node-0"),
            leader_addr: NodeAddress {
                cluster_addr: "127.0.0.1:8080".parse().unwrap(),
                client_addr: "127.0.0.1:8080".parse().unwrap(),
            },
            term: 1,
        };
        topology.update_shard_leader(&info1);

        let info2 = ShardLeaderInfo {
            shard_group_id: ShardGroupId(42),
            leader_node_id: NodeId::new("node-1"),
            leader_addr: NodeAddress {
                cluster_addr: "127.0.0.1:8081".parse().unwrap(),
                client_addr: "127.0.0.1:8081".parse().unwrap(),
            },
            term: 3,
        };
        assert!(topology.update_shard_leader(&info2));

        let entry = topology.shard_leader(ShardGroupId(42)).unwrap();
        assert_eq!(entry.leader_node_id, NodeId::new("node-1"));
        assert_eq!(entry.term, 3);
    }

    #[test]
    fn update_shard_leader_stale_term_rejected() {
        let mut topology = topology_from(
            &["node-0"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 1,
            },
        );

        let info1 = ShardLeaderInfo {
            shard_group_id: ShardGroupId(42),
            leader_node_id: NodeId::new("node-0"),
            leader_addr: NodeAddress {
                cluster_addr: "127.0.0.1:8080".parse().unwrap(),
                client_addr: "127.0.0.1:8080".parse().unwrap(),
            },
            term: 5,
        };
        topology.update_shard_leader(&info1);

        let stale = ShardLeaderInfo {
            shard_group_id: ShardGroupId(42),
            leader_node_id: NodeId::new("node-1"),
            leader_addr: NodeAddress {
                cluster_addr: "127.0.0.1:8081".parse().unwrap(),
                client_addr: "127.0.0.1:8081".parse().unwrap(),
            },
            term: 2,
        };
        assert!(!topology.update_shard_leader(&stale));

        let equal = ShardLeaderInfo {
            shard_group_id: ShardGroupId(42),
            leader_node_id: NodeId::new("node-1"),
            leader_addr: NodeAddress {
                cluster_addr: "127.0.0.1:8081".parse().unwrap(),
                client_addr: "127.0.0.1:8081".parse().unwrap(),
            },
            term: 5,
        };
        assert!(!topology.update_shard_leader(&equal));

        let entry = topology.shard_leader(ShardGroupId(42)).unwrap();
        assert_eq!(entry.leader_node_id, NodeId::new("node-0"));
        assert_eq!(entry.term, 5);
    }

    #[test]
    fn shard_leader_survives_node_death() {
        let mut topology = topology_from(
            &["node-0", "node-1"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 2,
            },
        );

        let info = ShardLeaderInfo {
            shard_group_id: ShardGroupId(42),
            leader_node_id: NodeId::new("node-0"),
            leader_addr: NodeAddress {
                cluster_addr: "127.0.0.1:8080".parse().unwrap(),
                client_addr: "127.0.0.1:8080".parse().unwrap(),
            },
            term: 1,
        };
        topology.update_shard_leader(&info);

        topology.update(NodeId::new("node-0"), &SwimNodeState::Dead);

        let entry = topology.shard_leader(ShardGroupId(42));
        assert!(
            entry.is_some(),
            "shard leader entry must survive node death"
        );
        assert_eq!(entry.unwrap().leader_node_id, NodeId::new("node-0"));
    }

    #[test]
    fn shard_leader_returns_none_for_unknown_group() {
        let topology = topology_from(
            &["node-0"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 1,
            },
        );
        assert!(topology.shard_leader(ShardGroupId(999)).is_none());
    }

    #[test]
    fn all_shard_leaders_returns_full_map() {
        let mut topology = topology_from(
            &["node-0"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 1,
            },
        );

        let info1 = ShardLeaderInfo {
            shard_group_id: ShardGroupId(10),
            leader_node_id: NodeId::new("node-0"),
            leader_addr: NodeAddress {
                cluster_addr: "127.0.0.1:8080".parse().unwrap(),
                client_addr: "127.0.0.1:8080".parse().unwrap(),
            },
            term: 1,
        };
        let info2 = ShardLeaderInfo {
            shard_group_id: ShardGroupId(20),
            leader_node_id: NodeId::new("node-0"),
            leader_addr: NodeAddress {
                cluster_addr: "127.0.0.1:8080".parse().unwrap(),
                client_addr: "127.0.0.1:8080".parse().unwrap(),
            },
            term: 2,
        };
        topology.update_shard_leader(&info1);
        topology.update_shard_leader(&info2);

        let leaders = topology.all_shard_leaders();
        assert_eq!(leaders.len(), 2);
        assert!(leaders.contains_key(&ShardGroupId(10)));
        assert!(leaders.contains_key(&ShardGroupId(20)));
    }
}
