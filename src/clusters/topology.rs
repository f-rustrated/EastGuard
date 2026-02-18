use bincode::{Decode, Encode};
use murmur3::murmur3_32;
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::net::SocketAddr;

use crate::clusters::NodeState;

/// node_id and replica_index for tie breaker
#[derive(Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct VirtualNodeToken {
    hash: u32,
    pnode_id: PhysicalNodeId,
    replica_index: u64,
}

#[derive(Debug)]
pub struct VirtualNode {
    replica_index: u64,
    pnode_id: PhysicalNodeId,
    pnode_metadata: PhysicalNodeMetadata,
}

pub struct Topology {
    nodes: HashMap<PhysicalNodeId, PhysicalNodeMetadata>,
    config: TopologyConfig,
    /// Consistent hash ring: maps virtual node positions to token owners.
    token_ring: TokenRing,
}

#[derive(Default)]
struct TokenRing(BTreeMap<VirtualNodeToken, VirtualNode>);

impl TokenRing {
    fn add_pnode(
        &mut self,
        pnode_id: &PhysicalNodeId,
        metadata: &PhysicalNodeMetadata,
        vnode_cnt: u64,
    ) {
        for replica_index in 0..vnode_cnt {
            self.0.insert(
                pnode_id.generate_vnode_token(replica_index),
                pnode_id.generate_vnode(&metadata, replica_index),
            );
        }
    }

    fn remove_pnode(&mut self, pnode_id: &PhysicalNodeId, vnode_cnt: u64) {
        for replica_index in 0..vnode_cnt {
            self.0.remove(&pnode_id.generate_vnode_token(replica_index));
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn walk_clockwise(&self, key: &[u8]) -> impl Iterator<Item = &VirtualNode> {
        let hash = hash_stable(key);
        let start = VirtualNodeToken {
            hash,
            pnode_id: PhysicalNodeId::new(""),
            replica_index: 0,
        };

        self.0
            .range(&start..)
            .chain(self.0.range(..&start))
            .map(|(_, owner)| owner)
    }
}

pub struct TopologyConfig {
    pub replicas_per_node: u64,
}

pub enum ReplicaPolicy {
    /// Allow multiple vnodes from the same physical node.
    Any,
    /// Skip vnodes whose physical node is already in the result.
    DistinctNodes,
}

impl Topology {
    pub fn new(
        nodes: HashMap<PhysicalNodeId, PhysicalNodeMetadata>,
        config: TopologyConfig,
    ) -> Self {
        let mut token_ring = TokenRing::default();
        for (pnode_id, metadata) in &nodes {
            token_ring.add_pnode(pnode_id, metadata, config.replicas_per_node);
        }

        Self {
            nodes,
            config,
            token_ring,
        }
    }

    pub(crate) fn update(&mut self, addr: SocketAddr, state: NodeState) {
        let id = PhysicalNodeId::new(addr.to_string());
        match state {
            NodeState::Alive => {
                self.insert_node(id, PhysicalNodeMetadata { address: addr });
            }

            NodeState::Dead => {
                self.remove_node(&id);
            }
            NodeState::Suspect => {}
        }
    }

    fn insert_node(&mut self, pnode_id: PhysicalNodeId, metadata: PhysicalNodeMetadata) {
        // ? what if metadata needs to be changed ?
        if self.nodes.contains_key(&pnode_id) {
            return;
        }

        self.token_ring
            .add_pnode(&pnode_id, &metadata, self.config.replicas_per_node);

        self.nodes.insert(pnode_id, metadata);
    }

    fn remove_node(&mut self, pnode_id: &PhysicalNodeId) -> Option<PhysicalNodeMetadata> {
        let metadata = self.nodes.remove(pnode_id)?;
        self.token_ring
            .remove_pnode(&pnode_id, self.config.replicas_per_node);
        Some(metadata)
    }

    pub fn token_owners_for(
        &self,
        key: &[u8],
        n: usize,
        policy: ReplicaPolicy,
    ) -> Vec<&VirtualNode> {
        if self.token_ring.is_empty() || n == 0 {
            return Vec::new();
        }
        let mut result: Vec<&VirtualNode> = Vec::with_capacity(n);
        for owner in self.token_ring.walk_clockwise(key) {
            if let ReplicaPolicy::DistinctNodes = policy {
                if result.iter().any(|o| o.pnode_id == owner.pnode_id) {
                    continue;
                }
            }
            result.push(owner);
            if result.len() == n {
                break;
            }
        }
        result
    }

    #[cfg(test)]
    pub fn contains_node(&self, id: &PhysicalNodeId) -> bool {
        self.nodes.contains_key(id)
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

#[derive(Hash, Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Encode, Decode)]
pub struct PhysicalNodeId(String);

impl PhysicalNodeId {
    pub(crate) fn new(id: impl Into<String>) -> PhysicalNodeId {
        Self(id.into())
    }

    fn generate_vnode_token(&self, replica_index: u64) -> VirtualNodeToken {
        let mut buf = Vec::with_capacity(self.0.len() + 8);
        buf.extend_from_slice(self.0.as_bytes());
        buf.extend_from_slice(&replica_index.to_be_bytes());
        VirtualNodeToken {
            hash: hash_stable(&buf),
            pnode_id: self.clone(),
            replica_index,
        }
    }

    fn generate_vnode(&self, metadata: &PhysicalNodeMetadata, replica_index: u64) -> VirtualNode {
        VirtualNode {
            replica_index,
            pnode_id: self.clone(),
            pnode_metadata: metadata.clone(),
        }
    }
}

/// Identity is managed separately via `PhysicalNodeId`.
#[derive(Clone, Debug)]
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
                let id: PhysicalNodeId = PhysicalNodeId::new(*id);
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
                replicas_per_node: 4,
            },
        );

        assert_eq!(topology.nodes.len(), 3);
        assert_eq!(topology.token_ring.0.len(), 12);

        assert!(topology.nodes.contains_key(&PhysicalNodeId::new("node-1")));
        assert!(topology.nodes.contains_key(&PhysicalNodeId::new("node-2")));
        assert!(!topology.nodes.contains_key(&PhysicalNodeId::new("node-3")));
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
                replicas_per_node: 4,
            },
        );

        topology.insert_node(
            PhysicalNodeId::new("node-3"),
            PhysicalNodeMetadata {
                address: "127.0.0.1:8083".parse().unwrap(),
            },
        );

        assert_eq!(topology.nodes.len(), 4);
        assert_eq!(topology.token_ring.0.len(), 16);

        assert!(topology.nodes.contains_key(&PhysicalNodeId::new("node-3")));
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
                replicas_per_node: 4,
            },
        );

        topology.insert_node(
            PhysicalNodeId::new("node-2"),
            PhysicalNodeMetadata {
                address: "127.0.0.1:8082".parse().unwrap(),
            },
        );

        assert_eq!(topology.nodes.len(), 3);
        assert_eq!(topology.token_ring.0.len(), 12);
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
                replicas_per_node: 4,
            },
        );

        let removed = topology.remove_node(&PhysicalNodeId::new("node-0"));
        assert!(removed.is_some());
        assert_eq!(topology.nodes.len(), 2);
        assert_eq!(topology.token_ring.0.len(), 8);

        assert!(!topology.nodes.contains_key(&PhysicalNodeId::new("node-0")));
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
                replicas_per_node: 4,
            },
        );

        let single_owner: Vec<&VirtualNode> =
            topology.token_owners_for("hello".as_bytes(), 1, ReplicaPolicy::DistinctNodes);
        assert_eq!(single_owner.len(), 1);

        let multiple_owner =
            topology.token_owners_for("hello".as_bytes(), 3, ReplicaPolicy::DistinctNodes);
        assert_eq!(multiple_owner.len(), 3);
        let physical_node_ids: HashSet<PhysicalNodeId> = multiple_owner
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
                replicas_per_node: 4,
            },
        );
        // Asking for more replicas than physical nodes exist should return at most 3.
        let owners = topology.token_owners_for(b"any-key", 5, ReplicaPolicy::DistinctNodes);
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
                replicas_per_node: 150,
            },
        );

        let key = b"test-key";
        let owners = topology.token_owners_for(key, 2, ReplicaPolicy::DistinctNodes);
        assert_eq!(owners.len(), 2);
        let primary_id = owners[0].pnode_id.clone();
        let successor_id = owners[1].pnode_id.clone();

        let mut topology = topology;
        topology.remove_node(&primary_id);

        let new_owners = topology.token_owners_for(key, 1, ReplicaPolicy::DistinctNodes);
        assert_eq!(new_owners.len(), 1);
        assert_eq!(new_owners[0].pnode_id, successor_id);
    }
}
