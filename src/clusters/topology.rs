use murmur3::murmur3_32;
use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;
use std::net::SocketAddr;

#[derive(Hash, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct PhysicalNodeId(String);

impl From<&str> for PhysicalNodeId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for PhysicalNodeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Mutable metadata associated with a physical node.
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

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct VNodeToken {
    hash: u32,
    node_id: PhysicalNodeId,
}

pub struct Topology {
    nodes: HashMap<PhysicalNodeId, PhysicalNodeMetadata>,
    config: TopologyConfig,
    /// Consistent hash ring: maps virtual node positions to physical node IDs.
    token_ring: BTreeMap<VNodeToken, PhysicalNodeId>,
}

pub struct TopologyConfig {
    replicas_per_node: u32,
}

impl Topology {
    pub fn new(
        nodes: HashMap<PhysicalNodeId, PhysicalNodeMetadata>,
        config: TopologyConfig,
    ) -> Self {
        let mut token_ring = BTreeMap::new();
        for id in nodes.keys() {
            for i in 0..config.replicas_per_node {
                token_ring.insert(ring_key(id, i as u64), id.clone());
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
            self.token_ring.insert(ring_key(&id, i as u64), id.clone());
        }
        self.nodes.insert(id, metadata);
    }

    pub fn remove_node(&mut self, id: &PhysicalNodeId) {
        if self.nodes.remove(id).is_none() {
            return;
        }
        for i in 0..self.config.replicas_per_node {
            self.token_ring.remove(&ring_key(id, i as u64));
        }
    }

    /// Returns up to `n` distinct nodes by walking the ring from the key's position.
    pub fn token_owners_for(
        &self,
        key: &[u8],
        n: usize,
    ) -> Vec<(&PhysicalNodeId, &PhysicalNodeMetadata)> {
        if self.token_ring.is_empty() || n == 0 {
            return Vec::new();
        }
        let mut result = Vec::with_capacity(n);
        for (_, id) in self.ring_walk(key) {
            if result.iter().any(|(rid, _)| rid == &id) {
                continue;
            }
            if let Some(metadata) = self.nodes.get(id) {
                result.push((id, metadata));
                if result.len() == n {
                    break;
                }
            }
        }
        result
    }

    /// Locates the node responsible for the given key.
    pub fn token_owner_for(&self, key: &[u8]) -> Option<(&PhysicalNodeId, &PhysicalNodeMetadata)> {
        let id = self.ring_walk(key).next().map(|(_, id)| id)?;
        self.nodes.get_key_value(id)
    }

    fn ring_walk(&self, key: &[u8]) -> impl Iterator<Item = (&VNodeToken, &PhysicalNodeId)> {
        let hash = hash_stable(key);
        self.token_ring
            .range(
                VNodeToken {
                    hash,
                    node_id: "".into(),
                }..,
            )
            .chain(self.token_ring.iter())
    }
}

fn ring_key(id: &PhysicalNodeId, replica_index: u64) -> VNodeToken {
    let mut buf = Vec::with_capacity(id.0.len() + 8);
    buf.extend_from_slice(id.0.as_bytes());
    buf.extend_from_slice(&replica_index.to_be_bytes());
    VNodeToken {
        hash: hash_stable(&buf),
        node_id: id.clone(),
    }
}

/// We shouldn’t use DefaultHasher because its algorithm and seed are intentionally unstable across
/// processes, runs, and Rust versions, making the same input produce different hashes in a
/// distributed system.
fn hash_stable(key: &[u8]) -> u32 {
    let mut cursor = Cursor::new(key);
    murmur3_32(&mut cursor, 0).expect("Murmur3 hashing failed")
}
