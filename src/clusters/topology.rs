use super::PhysicalNodeId;
use murmur3::murmur3_32;
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::net::SocketAddr;

/// Identity is managed separately via `PhysicalNodeId`.
#[derive(Clone, Debug)]
pub struct PhysicalNodeMetadata {
    pub address: SocketAddr,
}

impl PhysicalNodeMetadata {
    pub fn new(address: SocketAddr) -> Self {
        Self { address }
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct VirtualNodeToken {
    hash: u32,
    node_id: PhysicalNodeId,
    replica_index: u64,
}

#[derive(Debug)]
pub struct VirtualNodeMetadata {
    replica_index: u64,
    physical_node_id: PhysicalNodeId,
}

#[derive(Debug)]
pub struct TokenOwner {
    physical_node_id: PhysicalNodeId,
    physical_node_metadata: PhysicalNodeMetadata,
    virtual_node_metadata: VirtualNodeMetadata,
}

pub struct Topology {
    nodes: HashMap<PhysicalNodeId, PhysicalNodeMetadata>,
    config: TopologyConfig,
    /// Consistent hash ring: maps virtual node positions to token owners.
    token_ring: BTreeMap<VirtualNodeToken, TokenOwner>,
}

pub struct TopologyConfig {
    pub replicas_per_node: u32,
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
        let mut token_ring = BTreeMap::new();
        for (id, metadata) in &nodes {
            for i in 0..config.replicas_per_node {
                let replica_index = i as u64;
                token_ring.insert(
                    ring_key(id, replica_index),
                    token_owner(id, metadata, replica_index),
                );
            }
        }
        Self {
            nodes,
            config,
            token_ring,
        }
    }

    pub fn insert_node(&mut self, id: PhysicalNodeId, metadata: PhysicalNodeMetadata) {
        if self.nodes.contains_key(&id) {
            return;
        }
        for i in 0..self.config.replicas_per_node {
            let replica_index = i as u64;
            self.token_ring.insert(
                ring_key(&id, replica_index),
                token_owner(&id, &metadata, replica_index),
            );
        }
        self.nodes.insert(id, metadata);
    }

    pub fn contains_node(&self, id: &PhysicalNodeId) -> bool {
        self.nodes.contains_key(id)
    }
    
    pub fn remove_node(&mut self, id: &PhysicalNodeId) -> Option<PhysicalNodeMetadata> {
        let metadata = self.nodes.remove(id)?;
        for i in 0..self.config.replicas_per_node {
            self.token_ring.remove(&ring_key(id, i as u64));
        }
        Some(metadata)
    }

    pub fn token_owners_for(
        &self,
        key: &[u8],
        n: usize,
        policy: ReplicaPolicy,
    ) -> Vec<&TokenOwner> {
        if self.token_ring.is_empty() || n == 0 {
            return Vec::new();
        }
        let mut result: Vec<&TokenOwner> = Vec::with_capacity(n);
        for owner in self.ring_walk(key) {
            if let ReplicaPolicy::DistinctNodes = policy {
                if result
                    .iter()
                    .any(|o| o.physical_node_id == owner.physical_node_id)
                {
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

    fn ring_walk(&self, key: &[u8]) -> impl Iterator<Item = &TokenOwner> {
        let hash = hash_stable(key);
        let start = VirtualNodeToken { hash, node_id: "".into(), replica_index: 0 };
        self.token_ring
            .range(&start..)
            .chain(self.token_ring.range(..&start))
            .map(|(_, owner)| owner)
    }
}


fn ring_key(id: &PhysicalNodeId, replica_index: u64) -> VirtualNodeToken {
    let mut buf = Vec::with_capacity(id.0.len() + 8);
    buf.extend_from_slice(id.0.as_bytes());
    buf.extend_from_slice(&replica_index.to_be_bytes());
    VirtualNodeToken {
        hash: hash_stable(&buf),
        node_id: id.clone(),
        replica_index: replica_index,
    }
}

fn token_owner(
    id: &PhysicalNodeId,
    metadata: &PhysicalNodeMetadata,
    replica_index: u64,
) -> TokenOwner {
    TokenOwner {
        physical_node_id: id.clone(),
        physical_node_metadata: metadata.clone(),
        virtual_node_metadata: VirtualNodeMetadata {
            replica_index,
            physical_node_id: id.clone(),
        },
    }
}

/// We shouldn't use DefaultHasher because its algorithm and seed are intentionally unstable across
/// processes, runs, and Rust versions, making the same input produce different hashes in a
/// distributed system.
fn hash_stable(key: &[u8]) -> u32 {
    let mut cursor = Cursor::new(key);
    murmur3_32(&mut cursor, 0).expect("Murmur3 hashing failed")
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
                let id: PhysicalNodeId = (*id).into();
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
        assert_eq!(topology.token_ring.len(), 12);

        assert!(topology.nodes.contains_key(&"node-1".into()));
        assert!(topology.nodes.contains_key(&"node-2".into()));
        assert!(!topology.nodes.contains_key(&"node-3".into()));
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
            "node-3".into(),
            PhysicalNodeMetadata {
                address: "127.0.0.1:8083".parse().unwrap(),
            },
        );

        assert_eq!(topology.nodes.len(), 4);
        assert_eq!(topology.token_ring.len(), 16);

        assert!(topology.nodes.contains_key(&"node-3".into()));
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
            "node-2".into(),
            PhysicalNodeMetadata {
                address: "127.0.0.1:8082".parse().unwrap(),
            },
        );

        assert_eq!(topology.nodes.len(), 3);
        assert_eq!(topology.token_ring.len(), 12);
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

        let removed = topology.remove_node(&"node-0".into());
        assert!(removed.is_some());
        assert_eq!(topology.nodes.len(), 2);
        assert_eq!(topology.token_ring.len(), 8);

        assert!(!topology.nodes.contains_key(&"node-0".into()));
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
        let mut topology = topology_from(
            nodes.as_slice(),
            TopologyConfig {
                replicas_per_node: 4,
            },
        );

        let single_owner =
            topology.token_owners_for("hello".as_bytes(), 1, ReplicaPolicy::DistinctNodes);
        assert_eq!(single_owner.len(), 1);

        let multiple_owner =
            topology.token_owners_for("hello".as_bytes(), 3, ReplicaPolicy::DistinctNodes);
        assert_eq!(multiple_owner.len(), 3);
        let physical_node_ids: HashSet<PhysicalNodeId> = multiple_owner
            .iter()
            .map(|node| node.physical_node_id.clone())
            .collect();
        assert_eq!(physical_node_ids.len(), 3);
    }

    #[test]
    fn token_owners_for_limited_to_available_node_count_with_distinct_policy() {
        let topology = topology_from(
            &[("node-0", "127.0.0.1:8080"), ("node-1", "127.0.0.1:8081"), ("node-2", "127.0.0.1:8082")],
            TopologyConfig { replicas_per_node: 4 },
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
            TopologyConfig { replicas_per_node: 150 },
        );

        let key = b"test-key";
        let owners = topology.token_owners_for(key, 2, ReplicaPolicy::DistinctNodes);
        assert_eq!(owners.len(), 2);
        let primary_id = owners[0].physical_node_id.clone();
        let successor_id = owners[1].physical_node_id.clone();

        let mut topology = topology;
        topology.remove_node(&primary_id);

        let new_owners = topology.token_owners_for(key, 1, ReplicaPolicy::DistinctNodes);
        assert_eq!(new_owners.len(), 1);
        assert_eq!(new_owners[0].physical_node_id, successor_id);
    }
}
