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
pub struct VirtualNodeToken {
    hash: u32,
    node_id: PhysicalNodeId,
}

pub struct VirtualNodeMetadata {
    replica_index: u64,
    physical_node_id: PhysicalNodeId,
}

pub struct TokenOwner {
    physical_node_id: PhysicalNodeId,
    physical_node: PhysicalNodeMetadata,
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
                token_ring.insert(ring_key(id, replica_index), token_owner(id, metadata, replica_index));
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
            self.token_ring
                .insert(ring_key(&id, replica_index), token_owner(&id, &metadata, replica_index));
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
                if result.iter().any(|o| o.physical_node_id == owner.physical_node_id) {
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
        self.token_ring
            .range(
                VirtualNodeToken {
                    hash,
                    node_id: "".into(),
                }..,
            )
            .chain(self.token_ring.iter())
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
    }
}

fn token_owner(id: &PhysicalNodeId, metadata: &PhysicalNodeMetadata, replica_index: u64) -> TokenOwner {
    TokenOwner {
        physical_node_id: id.clone(),
        physical_node: metadata.clone(),
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
