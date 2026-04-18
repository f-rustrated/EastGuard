use murmur3::murmur3_32;
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::net::SocketAddr;

use bincode::{Decode, Encode};

use crate::clusters::{NodeId, SwimNodeState};

/// Deterministic identifier for a shard group, derived from the hash of the first
/// virtual node on the consistent hash ring for a given key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct ShardGroupId(pub u64);

impl ShardGroupId {
    fn new(key: &[u8]) -> Self {
        Self(hash_stable(key) as u64)
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

#[derive(Clone, Debug)]
#[expect(dead_code)]
pub struct VirtualNode {
    replica_index: u64,
    pub pnode_id: NodeId,
    pnode_metadata: PhysicalNodeMetadata,
}

#[derive(Clone, Debug)]
pub struct Topology {
    config: TopologyConfig,
    /// Consistent hash ring: maps virtual node positions to token owners.
    ring: TokenRing,
    /// Reverse index: for each physical node, the set of shard groups it participates in.
    /// Rebuilt after every ring mutation. Makes `shard_groups_for_node()` O(1).
    node_groups: HashMap<NodeId, HashMap<ShardGroupId, ShardGroup>>,
}

#[derive(Default, Clone, Debug)]
struct TokenRing {
    vnodes: BTreeMap<VirtualNodeToken, VirtualNode>,
    pnodes: HashMap<NodeId, PhysicalNodeMetadata>,
}

impl TokenRing {
    fn add_pnode(&mut self, pnode_id: NodeId, metadata: PhysicalNodeMetadata, vnode_cnt: u64) {
        if self.pnodes.contains_key(&pnode_id) {
            return;
        }
        for replica_index in 0..vnode_cnt {
            self.vnodes.insert(
                generate_vnode_token(&pnode_id, replica_index),
                generate_vnode(&pnode_id, &metadata, replica_index),
            );
        }
        self.pnodes.insert(pnode_id, metadata);
    }

    fn remove_pnode(&mut self, pnode_id: &NodeId, vnode_cnt: u64) -> Option<PhysicalNodeMetadata> {
        let metadata = self.pnodes.remove(pnode_id)?;
        for replica_index in 0..vnode_cnt {
            self.vnodes
                .remove(&generate_vnode_token(pnode_id, replica_index));
        }
        Some(metadata)
    }

    fn token_owners_for(&self, key: &[u8], n: usize) -> Vec<&VirtualNode> {
        self.token_owners_at(hash_stable(key), n)
    }

    fn walk_clockwise_from(
        &self,
        hash: u32,
    ) -> impl Iterator<Item = (&VirtualNodeToken, &VirtualNode)> {
        let start = VirtualNodeToken {
            hash,
            pnode_id: NodeId::new(""),
            replica_index: 0,
        };
        self.vnodes
            .range(&start..)
            .chain(self.vnodes.range(..&start))
    }

    fn token_owners_at(&self, hash: u32, n: usize) -> Vec<&VirtualNode> {
        if self.vnodes.is_empty() || n == 0 {
            return Vec::new();
        }
        let mut result: Vec<&VirtualNode> = Vec::with_capacity(n);
        for (_, owner) in self.walk_clockwise_from(hash) {
            if result.iter().any(|o| o.pnode_id == owner.pnode_id) {
                continue;
            }
            result.push(owner);
            if result.len() == n {
                break;
            }
        }
        result
    }
}

#[derive(Clone, Debug)]
pub struct TopologyConfig {
    pub vnodes_per_pnode: u64,
    pub replication_factor: usize,
}

impl Topology {
    pub fn new(nodes: HashMap<NodeId, PhysicalNodeMetadata>, config: TopologyConfig) -> Self {
        let mut token_ring = TokenRing::default();
        for (pnode_id, metadata) in nodes {
            token_ring.add_pnode(pnode_id, metadata, config.vnodes_per_pnode);
        }

        let mut topology = Self {
            config,
            ring: token_ring,
            node_groups: HashMap::new(),
        };
        topology.rebuild_reverse_index();
        topology
    }

    pub(crate) fn update(&mut self, node_id: NodeId, addr: SocketAddr, state: &SwimNodeState) {
        match state {
            SwimNodeState::Alive => {
                self.insert_node(node_id, PhysicalNodeMetadata { address: addr });
            }
            SwimNodeState::Dead => {
                self.remove_node(&node_id);
            }
            SwimNodeState::Suspect => {}
        }
    }

    fn insert_node(&mut self, pnode_id: NodeId, metadata: PhysicalNodeMetadata) {
        // ? what if metadata needs to be changed ?
        self.ring
            .add_pnode(pnode_id, metadata, self.config.vnodes_per_pnode);
        self.rebuild_reverse_index();
    }

    fn remove_node(&mut self, pnode_id: &NodeId) -> Option<PhysicalNodeMetadata> {
        let result = self
            .ring
            .remove_pnode(pnode_id, self.config.vnodes_per_pnode);
        if result.is_some() {
            self.rebuild_reverse_index();
        }
        result
    }

    /// Rebuild the reverse index from the ring. O(total_vnodes × RF).
    /// Called after every ring mutation.
    fn rebuild_reverse_index(&mut self) {
        self.node_groups.clear();
        let mut seen = std::collections::HashSet::new();

        for token in self.ring.vnodes.keys() {
            let id = ShardGroupId(token.hash as u64);
            if !seen.insert(id) {
                continue;
            }
            let owners = self
                .ring
                .token_owners_at(token.hash, self.config.replication_factor);
            let members: Vec<NodeId> = owners.iter().map(|vn| vn.pnode_id.clone()).collect();
            let group = ShardGroup {
                id,
                members: members.clone(),
            };
            for member in &members {
                self.node_groups
                    .entry(member.clone())
                    .or_default()
                    .insert(id, group.clone());
            }
        }
    }

    #[allow(dead_code)]
    pub fn token_owners_for(&self, key: &[u8], n: usize) -> Vec<&VirtualNode> {
        self.ring.token_owners_for(key, n)
    }

    /// Returns the shard group responsible for `key`.
    ///
    /// The group ID is derived deterministically from the key's hash so every node
    /// in the cluster computes the same ID for the same key. Members are the
    /// `replication_factor` distinct physical nodes clockwise from the key's
    /// position on the ring.
    #[allow(dead_code)]
    pub fn shard_group_for(&self, key: &[u8]) -> ShardGroup {
        let owners = self
            .ring
            .token_owners_for(key, self.config.replication_factor);
        ShardGroup {
            members: owners.into_iter().map(|vn| vn.pnode_id.clone()).collect(),
            id: ShardGroupId::new(key),
        }
    }

    /// Returns all unique shard groups that `node_id` participates in.
    /// O(1) lookup from the precomputed reverse index.
    pub fn shard_groups_for_node(&self, node_id: &NodeId) -> Vec<ShardGroup> {
        self.node_groups
            .get(node_id)
            .map(|groups| groups.values().cloned().collect())
            .unwrap_or_default()
    }

    #[expect(dead_code)]
    pub fn num_nodes(&self) -> usize {
        self.ring.pnodes.len()
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

fn generate_vnode(
    node_id: &NodeId,
    metadata: &PhysicalNodeMetadata,
    replica_index: u64,
) -> VirtualNode {
    VirtualNode {
        replica_index,
        pnode_id: node_id.clone(),
        pnode_metadata: metadata.clone(),
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

/// Identity is managed separately via `NodeId`.
#[derive(Clone, Debug)]
#[expect(dead_code)]
pub struct PhysicalNodeMetadata {
    pub address: SocketAddr,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Builds a `Topology` from explicit `(node_id, socket_addr)` pairs.
    fn topology_from(nodes: &[(&str, &str)], config: TopologyConfig) -> Topology {
        let map = nodes
            .iter()
            .map(|(id, addr)| {
                let id = NodeId::new(*id);
                let address: SocketAddr = addr.parse().expect("invalid socket address");
                (id, PhysicalNodeMetadata { address })
            })
            .collect();
        Topology::new(map, config)
    }

    #[test]
    fn new_builds_correct_node_and_vnode_counts() {
        let topology = topology_from(
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        assert_eq!(topology.ring.pnodes.len(), 3);
        assert_eq!(topology.ring.vnodes.len(), 12);

        assert!(topology.ring.pnodes.contains_key("node-1"));
        assert!(topology.ring.pnodes.contains_key("node-2"));
        assert!(!topology.ring.pnodes.contains_key("node-3"));
    }

    #[test]
    fn insert_node_adds_node_and_vnodes_to_ring() {
        let mut topology = topology_from(
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        topology.insert_node(
            NodeId::new("node-3"),
            PhysicalNodeMetadata {
                address: "127.0.0.1:8083".parse().unwrap(),
            },
        );

        assert_eq!(topology.ring.pnodes.len(), 4);
        assert_eq!(topology.ring.vnodes.len(), 16);
        assert!(topology.ring.pnodes.contains_key("node-3"));
    }

    #[test]
    fn insert_node_is_idempotent_for_duplicate_id() {
        let mut topology = topology_from(
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        topology.insert_node(
            NodeId::new("node-2"),
            PhysicalNodeMetadata {
                address: "127.0.0.1:8082".parse().unwrap(),
            },
        );

        assert_eq!(topology.ring.pnodes.len(), 3);
        assert_eq!(topology.ring.vnodes.len(), 12);
    }

    #[test]
    fn remove_node_cleans_up_node_and_its_vnodes() {
        let mut topology = topology_from(
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        let removed = topology.remove_node(&NodeId::new("node-0"));
        assert!(removed.is_some());
        assert_eq!(topology.ring.pnodes.len(), 2);
        assert_eq!(topology.ring.vnodes.len(), 8);
        assert!(!topology.ring.pnodes.contains_key("node-0"));
    }

    #[test]
    fn token_owners_for_returns_distinct_physical_nodes_with_distinct_policy() {
        let node_names: Vec<String> = (0..3).map(|idx| format!("node-{}", idx)).collect();
        let node_addrs: Vec<String> = (0..3).map(|idx| format!("127.0.{}.1:8080", idx)).collect();
        let nodes: Vec<(&str, &str)> = node_names
            .iter()
            .zip(node_addrs.iter())
            .map(|(name, addr)| (name.as_str(), addr.as_str()))
            .collect();
        let topology = topology_from(
            nodes.as_slice(),
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        let single_owner: Vec<&VirtualNode> = topology.token_owners_for("hello".as_bytes(), 1);
        assert_eq!(single_owner.len(), 1);

        let multiple_owner = topology.token_owners_for("hello".as_bytes(), 3);
        assert_eq!(multiple_owner.len(), 3);
        let physical_node_ids: HashSet<NodeId> = multiple_owner
            .iter()
            .map(|node| node.pnode_id.clone())
            .collect();
        assert_eq!(physical_node_ids.len(), 3);
    }

    #[test]
    fn token_owners_for_limited_to_available_node_count_with_distinct_policy() {
        let topology = topology_from(
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );
        // Asking for more replicas than physical nodes exist should return at most 3.
        let owners = topology.token_owners_for(b"any-key", 5);
        assert_eq!(owners.len(), 3);
    }

    #[test]
    fn removing_primary_owner_promotes_next_node() {
        let topology = topology_from(
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 150,
                replication_factor: 3,
            },
        );

        let key = b"test-key";
        let owners = topology.token_owners_for(key, 2);
        assert_eq!(owners.len(), 2);
        let primary_id = owners[0].pnode_id.clone();
        let successor_id = owners[1].pnode_id.clone();

        let mut topology = topology;
        topology.remove_node(&primary_id);

        let new_owners = topology.token_owners_for(key, 1);
        assert_eq!(new_owners.len(), 1);
        assert_eq!(new_owners[0].pnode_id, successor_id);
    }

    // --- ShardGroup tests ---

    #[test]
    fn shard_group_returns_replication_factor_members() {
        let topology = topology_from(
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 3,
            },
        );

        let group = topology.shard_group_for(b"topic-blue");
        assert_eq!(group.members.len(), 3);

        // All members should be distinct
        let unique: HashSet<_> = group.members.iter().collect();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn shard_group_deterministic_across_calls() {
        let topology = topology_from(
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 3,
            },
        );

        let group1 = topology.shard_group_for(b"topic-blue");
        let group2 = topology.shard_group_for(b"topic-blue");
        assert_eq!(group1, group2);
    }

    #[test]
    fn shard_group_id_differs_for_different_keys() {
        let topology = topology_from(
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 2,
            },
        );

        let group_a = topology.shard_group_for(b"topic-alpha");
        let group_b = topology.shard_group_for(b"topic-beta");
        // Murmur3 with seed 0 is deterministic — pin exact IDs
        assert_eq!(group_a.id, ShardGroupId(449050983));
        assert_eq!(group_b.id, ShardGroupId(3972680256));
    }

    #[test]
    fn shard_group_limited_to_available_nodes() {
        let topology = topology_from(
            &[("node-0", "127.0.0.1:8080"), ("node-1", "127.0.0.1:8081")],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 5,
            },
        );

        // Only 2 physical nodes, so group can have at most 2 members
        let group = topology.shard_group_for(b"any-key");
        assert_eq!(group.members.len(), 2);
    }

    #[test]
    fn shard_group_recalculates_on_node_removal() {
        let mut topology = topology_from(
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 150,
                replication_factor: 3,
            },
        );

        let key = b"test-key";
        let before = topology.shard_group_for(key);
        assert_eq!(before.members.len(), 3);

        let removed = before.members[0].clone();
        topology.remove_node(&removed);

        let after = topology.shard_group_for(key);
        assert_eq!(after.members.len(), 2);
        assert!(!after.members.contains(&removed));
    }

    #[test]
    fn shard_group_empty_ring_returns_empty_members() {
        let topology = topology_from(
            &[],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 3,
            },
        );

        let group = topology.shard_group_for(b"any-key");
        assert!(group.members.is_empty());
    }

    // --- shard_groups_for_node tests ---

    #[test]
    fn shard_groups_for_node_includes_all_participating_groups() {
        let topology = topology_from(
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        // With 3 nodes and replication_factor=3, every node is in every group.
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
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
                ("node-3", "127.0.0.1:8083"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 2,
            },
        );

        // With replication_factor=2 and 4 nodes, not every node is in every group.
        let groups_0 = topology.shard_groups_for_node(&NodeId::new("node-0"));
        let groups_3 = topology.shard_groups_for_node(&NodeId::new("node-3"));

        // Each node should have some groups but not all
        let total_vnodes = 4 * 64; // 256
        assert!(groups_0.len() < total_vnodes);
        assert!(groups_3.len() < total_vnodes);
        assert!(!groups_0.is_empty());
        assert!(!groups_3.is_empty());
    }

    #[test]
    fn shard_groups_for_node_empty_after_removal() {
        let mut topology = topology_from(
            &[
                ("node-0", "127.0.0.1:8080"),
                ("node-1", "127.0.0.1:8081"),
                ("node-2", "127.0.0.1:8082"),
            ],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 3,
            },
        );

        topology.remove_node(&NodeId::new("node-0"));
        let groups = topology.shard_groups_for_node(&NodeId::new("node-0"));
        assert!(groups.is_empty());
    }
}
