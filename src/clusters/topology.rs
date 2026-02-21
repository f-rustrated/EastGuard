use murmur3::murmur3_32;
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::net::SocketAddr;

use crate::clusters::{NodeId, NodeState};

/// node_id and replica_index for tie breaker
#[derive(Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct VirtualNodeToken {
    hash: u32,
    pnode_id: NodeId,
    replica_index: u64,
}

#[derive(Debug)]
pub struct VirtualNode {
    replica_index: u64,
    pub pnode_id: NodeId,
    pnode_metadata: PhysicalNodeMetadata,
}

pub struct Topology {
    config: TopologyConfig,
    /// Consistent hash ring: maps virtual node positions to token owners.
    ring: TokenRing,
}

#[derive(Default)]
struct TokenRing {
    vnodes: BTreeMap<VirtualNodeToken, VirtualNode>,
    pnodes: HashMap<NodeId, PhysicalNodeMetadata>,
}

impl TokenRing {
    fn add_pnode(
        &mut self,
        pnode_id: NodeId,
        metadata: PhysicalNodeMetadata,
        vnode_cnt: u64,
    ) {
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

    fn remove_pnode(
        &mut self,
        pnode_id: &NodeId,
        vnode_cnt: u64,
    ) -> Option<PhysicalNodeMetadata> {
        let metadata = self.pnodes.remove(pnode_id)?;
        for replica_index in 0..vnode_cnt {
            self.vnodes
                .remove(&generate_vnode_token(pnode_id, replica_index));
        }
        Some(metadata)
    }

    fn walk_clockwise(&self, key: &[u8]) -> impl Iterator<Item = &VirtualNode> {
        let hash = hash_stable(key);
        let start = VirtualNodeToken {
            hash,
            pnode_id: NodeId::new(""),
            replica_index: 0,
        };

        self.vnodes
            .range(&start..)
            .chain(self.vnodes.range(..&start))
            .map(|(_, owner)| owner)
    }

    fn token_owners_for(&self, key: &[u8], n: usize) -> Vec<&VirtualNode> {
        if self.vnodes.is_empty() || n == 0 {
            return Vec::new();
        }
        let mut result: Vec<&VirtualNode> = Vec::with_capacity(n);
        for owner in self.walk_clockwise(key) {
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

pub struct TopologyConfig {
    pub vnodes_per_pnode: u64,
}

impl Topology {
    pub fn new(
        nodes: HashMap<NodeId, PhysicalNodeMetadata>,
        config: TopologyConfig,
    ) -> Self {
        let mut token_ring = TokenRing::default();
        for (pnode_id, metadata) in nodes {
            token_ring.add_pnode(pnode_id, metadata, config.vnodes_per_pnode);
        }

        Self {
            config,
            ring: token_ring,
        }
    }

    pub(crate) fn update(&mut self, node_id: NodeId, addr: SocketAddr, state: NodeState) {
        match state {
            NodeState::Alive => {
                self.insert_node(node_id, PhysicalNodeMetadata { address: addr });
            }
            NodeState::Dead => {
                self.remove_node(&node_id);
            }
            NodeState::Suspect => {}
        }
    }

    fn insert_node(&mut self, pnode_id: NodeId, metadata: PhysicalNodeMetadata) {
        // ? what if metadata needs to be changed ?
        self.ring
            .add_pnode(pnode_id, metadata, self.config.vnodes_per_pnode);
    }

    fn remove_node(&mut self, pnode_id: &NodeId) -> Option<PhysicalNodeMetadata> {
        self.ring
            .remove_pnode(pnode_id, self.config.vnodes_per_pnode)
    }

    pub fn token_owners_for(&self, key: &[u8], n: usize) -> Vec<&VirtualNode> {
        self.ring.token_owners_for(key, n)
    }

    #[cfg(test)]
    pub fn contains_node(&self, id: &NodeId) -> bool {
        self.ring.pnodes.contains_key(id)
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

fn generate_vnode(node_id: &NodeId, metadata: &PhysicalNodeMetadata, replica_index: u64) -> VirtualNode {
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
}
