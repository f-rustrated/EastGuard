use crate::control_plane::membership::messages::dissemination_buffer::ShardLeaderInfo;
use crate::control_plane::{NodeAddressInfo, NodeId, Replicas, SwimNodeState};
use crate::impl_new_struct_wrapper;
#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;
use arc_swap::ArcSwap;
use borsh::{BorshDeserialize, BorshSerialize};
use murmur3::murmur3_32;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;

/// Deterministic identifier for a shard group, derived from the hash of the first
/// virtual node on the consistent hash ring for a given key.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, BorshSerialize, BorshDeserialize,
)]
pub struct ShardGroupId(pub u64);

impl ShardGroupId {
    /// Derives a ShardGroupId from a vnode token's hash.
    /// Only valid when the hash comes from an actual vnode position on the ring.
    fn from_vnode_hash(hash: u32) -> Self {
        Self(hash as u64)
    }
}

impl_new_struct_wrapper!(ShardGroupId, u64);

/// A shard group: the set of physical nodes responsible for a key range on the ring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardGroup {
    pub id: ShardGroupId,
    pub replicas: Replicas,
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
pub struct ShardLeaderEntry {
    pub leader: NodeAddressInfo,
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
    /// Set inside mutators (ring update, shard-leader accepted). The owning
    /// `SwimActor` consumes this flag at the end of each event-loop iteration
    /// via `take_dirty()` to decide whether to publish a fresh snapshot into
    /// the shared `ArcSwap<Topology>`. Stays untouched on no-op mutations
    /// (e.g., stale-term shard-leader updates) so we don't pay clone cost for
    /// publishes that would carry no new information.
    dirty: bool,
}

/// Read-only handle on the shared topology snapshot.
///
/// `SwimActor` is the sole writer; every other actor that needs the current
/// topology holds a clone of this handle. `.load()` returns an `Arc<Topology>`
/// snapshot atomically — lock-free, no contention with the writer or other
/// readers, and internally consistent (no torn reads, no mid-update views).
///
/// Cloning the handle is just an `Arc` bump (shares the underlying `ArcSwap`).
#[derive(Clone)]
pub(crate) struct TopologyReader(Arc<ArcSwap<Topology>>);

impl TopologyReader {
    pub(crate) fn live_nodes(&self) -> Vec<NodeId> {
        self.0.load().live_nodes()
    }

    pub(crate) fn shard_groups_for_node(&self, node_id: &NodeId) -> Box<[ShardGroup]> {
        self.0
            .load()
            .shard_groups_for_node(node_id)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Ring membership of `group_id` in the current snapshot, regardless of
    /// whether the local node is among the members. `None` when the group is
    /// not in the snapshot at all.
    pub(crate) fn group_ring_members(&self, group_id: ShardGroupId) -> Option<Replicas> {
        self.0.load().group(group_id).map(|g| g.replicas.clone())
    }

    /// The cluster's configured replication factor — the target replica-set size a
    /// segment converges back toward after a death shrank it (capacity-return re-fill).
    pub(crate) fn replication_factor(&self) -> usize {
        self.0.load().replication_factor()
    }

    /// The current shard-leader entry for `group_id` from the lock-free snapshot, or
    /// `None` if the gossiped map has no entry yet (the caller falls back to the ring).
    pub(crate) fn shard_leader(&self, group_id: ShardGroupId) -> Option<ShardLeaderEntry> {
        self.0.load().shard_leader(group_id).cloned()
    }

    pub(crate) fn ring_replacements_for(
        &self,
        group_id: ShardGroupId,
        excluded: &HashSet<NodeId>,
        count: usize,
    ) -> Box<[NodeId]> {
        let replacements = self
            .0
            .load()
            .ring_replacements_for(group_id, excluded, count);

        if replacements.len() != count {
            // ! log replacement shortfall.
            tracing::warn!(
                "Reconciliation on {:?}: needed {} replacement(s), pool yielded none — \
             group remains under-replicated until cluster grows",
                group_id,
                count
            );
        }
        replacements
    }
}

/// Construct a (publisher, reader) pair sharing one underlying `ArcSwap`.
///
/// The publisher half stays with `SwimActor` (single writer); the reader can
/// be cloned freely to any number of consumers. Both see the same atomic slot;
/// what differs is the API surface — readers can only `load()`.
pub(crate) fn topology_channel(initial: Topology) -> (Arc<ArcSwap<Topology>>, TopologyReader) {
    let arc = Arc::new(ArcSwap::from_pointee(initial));
    let reader = TopologyReader(arc.clone());
    (arc, reader)
}

impl Topology {
    pub fn new(nodes: impl IntoIterator<Item = NodeId>, config: TopologyConfig) -> Self {
        let mut topology = Self {
            config,
            vnodes: BTreeSet::new(),
            groups: HashMap::new(),
            node_group_ids: HashMap::new(),
            shard_leaders: HashMap::new(),
            dirty: false,
        };
        for pnode_id in nodes {
            topology.add_pnode(pnode_id);
        }
        topology.rebuild_reverse_index();
        // Initial construction is not a "publish-worthy mutation" — the
        // `topology_channel` factory seeds the ArcSwap with this snapshot
        // directly, so we leave `dirty` cleared.
        topology.dirty = false;
        topology
    }

    /// Consume the dirty flag. Returns true if the topology has been mutated
    /// since the last `take_dirty()` call (or since construction). The owning
    /// SwimActor calls this at the end of each event-loop iteration to decide
    /// whether to clone and publish a fresh snapshot.
    pub(crate) fn take_dirty(&mut self) -> bool {
        std::mem::replace(&mut self.dirty, false)
    }

    pub(crate) fn vnodes_per_pnode(&self) -> usize {
        self.config.vnodes_per_pnode as usize
    }

    pub(crate) fn replication_factor(&self) -> usize {
        self.config.replication_factor
    }

    pub(crate) fn update(&mut self, node_id: NodeId, state: &SwimNodeState) {
        match state {
            SwimNodeState::Alive => {
                self.insert_node(node_id);
            }
            SwimNodeState::Dead => {
                self.remove_node(&node_id);
            }
            //TODO clarify implications of this behaviour while removing it from the Livetracker on suspect
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

    fn token_owners_at(&self, hash: u32, n: usize) -> Vec<NodeId> {
        if self.vnodes.is_empty() || n == 0 {
            return Vec::new();
        }
        self.collect_distinct_owners_excluding(hash, n, &HashSet::new())
    }

    fn collect_distinct_owners_excluding(
        &self,
        hash: u32,
        n: usize,
        excluded: &HashSet<NodeId>,
    ) -> Vec<NodeId> {
        let mut result: Vec<NodeId> = Vec::with_capacity(n);
        for token in self.walk_clockwise_from(hash) {
            if excluded.contains(&token.pnode_id) {
                continue;
            }
            if result.contains(&token.pnode_id) {
                continue;
            }
            result.push(token.pnode_id.clone());
            if result.len() == n {
                break;
            }
        }
        result
    }

    fn insert_node(&mut self, pnode_id: NodeId) {
        let already_present = self.node_group_ids.contains_key(&pnode_id);
        self.add_pnode(pnode_id);
        if !already_present {
            self.rebuild_reverse_index();
            self.dirty = true;
        }
    }

    fn remove_node(&mut self, pnode_id: &NodeId) -> bool {
        let removed = self.remove_pnode(pnode_id);
        if removed {
            self.rebuild_reverse_index();
            self.dirty = true;
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
            let replicas =
                Replicas::new(self.token_owners_at(token.hash, self.config.replication_factor));

            for member in &replicas.0 {
                self.node_group_ids
                    .entry(member.clone())
                    .or_default()
                    .push(id);
            }
            self.groups.insert(id, ShardGroup { id, replicas });
        }
    }

    #[cfg(test)]
    fn token_owners_for(&self, key: &[u8], n: usize) -> Vec<NodeId> {
        let hash = hash_stable(key);
        self.token_owners_at(hash, n)
    }

    /// Returns all unique shard groups that `node_id` participates in,
    /// borrowed from this snapshot. `TopologyReader::shard_groups_for_node`
    /// is the owned-clone wrapper that other actors call; this method is the
    /// data-layer primitive (also used directly in tests).
    pub(crate) fn shard_groups_for_node(&self, node_id: &NodeId) -> Vec<&ShardGroup> {
        self.node_group_ids
            .get(node_id)
            .map(|ids| ids.iter().filter_map(|id| self.groups.get(id)).collect())
            .unwrap_or_default()
    }

    /// Snapshot lookup of a shard group by id. Backs
    /// `TopologyReader::group_ring_members`; see the reader method for why
    /// this must not go through a member node.
    pub(crate) fn group(&self, group_id: ShardGroupId) -> Option<&ShardGroup> {
        self.groups.get(&group_id)
    }

    /// Returns the shard group responsible for `key`.
    pub(crate) fn shard_group_for(&self, key: &[u8]) -> Option<&ShardGroup> {
        let hash = hash_stable(key);
        let nearest_token = self.walk_clockwise_from(hash).next()?;
        let id = ShardGroupId::from_vnode_hash(nearest_token.hash);
        self.groups.get(&id)
    }

    /// Walks the ring clockwise from this group's anchor and returns up to `count`
    /// physical nodes that are NOT in `excluded`. Used by reconciliation to pick
    /// replacements when a node leaves a Raft peer set or a segment's replica set.
    ///
    /// Deterministic given the current ring state and the exclusion set, which is
    /// what lets both metadata and data-plane callers reach the same choice for
    /// the same death without coordinating: ask the ring.
    ///
    /// Exclusion is the caller's responsibility — typically the current set plus
    /// the node being replaced. The ring already excludes dead nodes (SWIM removes
    /// them); suspects remain on the ring, so a caller that wants to skip them
    /// must put them in `excluded` explicitly.
    ///
    /// Returns fewer than `count` (possibly zero) when the ring lacks that many
    /// eligible nodes — the caller decides what to do with a short return
    /// (degrade-and-proceed, with severity-appropriate logging).
    pub(crate) fn ring_replacements_for(
        &self,
        group_id: ShardGroupId,
        excluded: &HashSet<NodeId>,
        count: usize,
    ) -> Box<[NodeId]> {
        if count == 0 || self.vnodes.is_empty() {
            return Box::new([]);
        }
        let anchor = group_id.0 as u32;
        self.collect_distinct_owners_excluding(anchor, count, excluded)
            .into_iter()
            .collect()
    }

    pub(crate) fn live_nodes(&self) -> Vec<NodeId> {
        self.node_group_ids.keys().cloned().collect()
    }

    pub(crate) fn update_shard_leader(&mut self, info: &ShardLeaderInfo) -> bool {
        if let Some(existing) = self.shard_leaders.get(&info.shard_group_id)
            && info.term <= existing.term
        {
            return false;
        }
        self.shard_leaders.insert(
            info.shard_group_id,
            ShardLeaderEntry {
                leader: NodeAddressInfo::new(info.leader_node_id.clone(), info.leader_addr),
                term: info.term,
            },
        );
        self.dirty = true;
        true
    }

    pub fn shard_leader(&self, shard_group_id: ShardGroupId) -> Option<&ShardLeaderEntry> {
        self.shard_leaders.get(&shard_group_id)
    }

    #[cfg(test)]
    fn all_shard_leaders(&self) -> &HashMap<ShardGroupId, ShardLeaderEntry> {
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

#[cfg(any(test, debug_assertions))]
pub mod props {
    use super::*;
    impl TAssertInvariant for Topology {
        fn assert_invariants(&self) {
            self.assert_reverse_index_consistent();
            self.assert_groups_consistent();
            self.assert_vnode_counts();
            self.assert_shard_leaders_valid();
        }
    }
    impl Topology {
        fn assert_reverse_index_consistent(&self) {
            for (node_id, group_ids) in &self.node_group_ids {
                for gid in group_ids {
                    let group = self
                        .groups
                        .get(gid)
                        .expect("node_group_ids references missing group");
                    assert!(
                        group.replicas.contains(node_id),
                        "node {:?} in reverse index for group {:?} but not in group members",
                        node_id,
                        gid
                    );
                }
            }
        }

        fn assert_groups_consistent(&self) {
            use std::collections::HashSet;
            for (gid, group) in &self.groups {
                for member in &group.replicas.0 {
                    let ids = self
                        .node_group_ids
                        .get(member)
                        .expect("group member missing from node_group_ids");
                    assert!(
                        ids.contains(gid),
                        "group {:?} member {:?} missing group in reverse index",
                        gid,
                        member
                    );
                }
                let unique: HashSet<&NodeId> = group.replicas.iter().collect();
                assert_eq!(
                    unique.len(),
                    group.replicas.len(),
                    "group {:?} has duplicate members",
                    gid
                );
            }
        }

        /// Invariants : every stored leader entry carries a Raft term >= 1
        /// (Raft's first elected term). A term of 0 is the "no leader yet" sentinel
        /// and must never appear as a stored entry. The map is never auto-cleared
        /// on node death, so the presence check also guards invariant 7
        /// (entries persist until overwritten by a higher term).
        fn assert_shard_leaders_valid(&self) {
            for (gid, entry) in &self.shard_leaders {
                assert!(
                    entry.term >= 1,
                    "shard leader for group {:?} has invalid term 0",
                    gid,
                );
            }
        }

        fn assert_vnode_counts(&self) {
            for node_id in self.node_group_ids.keys() {
                let count = self
                    .vnodes
                    .iter()
                    .filter(|t| t.pnode_id == *node_id)
                    .count();
                assert_eq!(
                    count, self.config.vnodes_per_pnode as usize,
                    "node {:?} has {count} vnodes, expected {}",
                    node_id, self.config.vnodes_per_pnode
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::control_plane::NodeAddress;

    use super::*;
    use std::net::SocketAddr;

    fn topology_from(nodes: &[&str], config: TopologyConfig) -> Topology {
        let ids = nodes.iter().map(|id| NodeId::new(*id));
        Topology::new(ids, config)
    }

    #[test]
    fn new_builds_correct_node_and_vnode_counts() {
        let mut topology = topology_from(
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

        // Construction is not a publish-worthy mutation — the topology_channel
        // factory seeds the ArcSwap with this snapshot directly.
        assert!(!topology.take_dirty());
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
        assert!(
            topology.take_dirty(),
            "inserting a new node must mark dirty"
        );
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
        assert!(
            !topology.take_dirty(),
            "duplicate insert is a no-op and must not mark dirty"
        );
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
        assert!(topology.take_dirty(), "actual removal must mark dirty");

        let removed_again = topology.remove_node(&NodeId::new("node-0"));
        assert!(!removed_again);
        assert!(
            !topology.take_dirty(),
            "removing an absent node must not mark dirty"
        );
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
        let physical_node_ids: HashSet<NodeId> = multiple_owner.into_iter().collect();
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
        assert_eq!(new_owners[0], successor_id);
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
        assert_eq!(group.replicas.len(), 3);

        let unique: HashSet<_> = group.replicas.iter().collect();
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
        assert_eq!(group.replicas.len(), 2);
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
        assert_eq!(before.replicas.len(), 3);

        let removed = before.replicas[0].clone();
        topology.remove_node(&removed);

        let after = topology.shard_group_for(key).unwrap();
        assert_eq!(after.replicas.len(), 2);
        assert!(!after.replicas.contains(&removed));
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
            assert!(group.replicas.contains(&NodeId::new("node-0")));
            assert_eq!(group.replicas.len(), 3);
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

    // --- TopologyReader / topology_channel tests ---

    #[test]
    fn reader_loads_initial_snapshot() {
        let topology = topology_from(
            &["a", "b"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 2,
            },
        );
        let (_pub_handle, reader) = topology_channel(topology);

        assert_eq!(reader.live_nodes().len(), 2);
    }

    #[test]
    fn reader_sees_published_update() {
        let topology = topology_from(
            &["a"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 1,
            },
        );
        let (pub_handle, reader) = topology_channel(topology);
        assert_eq!(reader.live_nodes().len(), 1);

        // Simulate what SwimActor does at the end of an iteration: build a fresh
        // Topology that reflects new state and store it.
        let next = topology_from(
            &["a", "b", "c"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 1,
            },
        );
        pub_handle.store(Arc::new(next));

        assert_eq!(reader.live_nodes().len(), 3);
    }

    #[test]
    fn reader_clone_shares_underlying_swap() {
        let topology = topology_from(
            &["a"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 1,
            },
        );
        let (pub_handle, reader1) = topology_channel(topology);
        let reader2 = reader1.clone();

        pub_handle.store(Arc::new(topology_from(
            &["a", "b"],
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 1,
            },
        )));

        // Both readers see the same new snapshot.
        assert_eq!(reader1.live_nodes().len(), 2);
        assert_eq!(reader2.live_nodes().len(), 2);
    }

    // --- ring_replacements_for tests ---

    fn five_node_topology() -> Topology {
        topology_from(
            &["node-0", "node-1", "node-2", "node-3", "node-4"],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 3,
            },
        )
    }

    #[test]
    fn ring_replacements_returns_node_not_in_excluded() {
        let topology = five_node_topology();
        let group = topology.shard_group_for(b"topic-blue").unwrap().clone();
        let current: HashSet<NodeId> = group.replicas.iter().cloned().collect();

        let picks = topology.ring_replacements_for(group.id, &current, 1);

        assert_eq!(picks.len(), 1);
        assert!(
            !current.contains(&picks[0]),
            "pick {:?} must not be among current members {:?}",
            picks[0],
            current
        );
    }

    #[test]
    fn ring_replacements_skips_every_excluded_node() {
        let topology = five_node_topology();
        let group = topology.shard_group_for(b"topic-blue").unwrap().clone();
        // Exclude four of five nodes: every member plus one extra outsider.
        let mut excluded: HashSet<NodeId> = group.replicas.iter().cloned().collect();
        let outsider = topology
            .live_nodes()
            .into_iter()
            .find(|n| !excluded.contains(n))
            .expect("must have at least one outsider with 5 nodes and RF=3");
        excluded.insert(outsider.clone());

        let picks = topology.ring_replacements_for(group.id, &excluded, 5);

        assert_eq!(
            picks.len(),
            1,
            "exactly one eligible node should remain (5 total − 4 excluded)"
        );
        for p in &picks {
            assert!(
                !excluded.contains(p),
                "pick {:?} must not appear in excluded set",
                p
            );
        }
    }

    #[test]
    fn ring_replacements_returns_fewer_than_count_when_pool_depleted() {
        let topology = five_node_topology();
        let group = topology.shard_group_for(b"topic-blue").unwrap().clone();
        let current: HashSet<NodeId> = group.replicas.iter().cloned().collect();

        // 5 total, 3 excluded → 2 eligible. Ask for 5; expect 2.
        let picks = topology.ring_replacements_for(group.id, &current, 5);
        assert_eq!(picks.len(), 2);
    }

    #[test]
    fn ring_replacements_returns_empty_when_all_excluded() {
        let topology = five_node_topology();
        let group = topology.shard_group_for(b"topic-blue").unwrap().clone();
        let all: HashSet<NodeId> = topology.live_nodes().into_iter().collect();

        let picks = topology.ring_replacements_for(group.id, &all, 1);
        assert!(picks.is_empty());
    }

    #[test]
    fn ring_replacements_empty_for_count_zero() {
        let topology = five_node_topology();
        let group = topology.shard_group_for(b"topic-blue").unwrap().clone();
        let picks = topology.ring_replacements_for(group.id, &HashSet::new(), 0);
        assert!(picks.is_empty());
    }

    #[test]
    fn ring_replacements_empty_when_ring_empty() {
        let topology = topology_from(
            &[],
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 3,
            },
        );
        // Group id chosen arbitrarily; with an empty ring nothing is eligible.
        let picks = topology.ring_replacements_for(ShardGroupId(123), &HashSet::new(), 3);
        assert!(picks.is_empty());
    }

    #[test]
    fn ring_replacements_deterministic_across_calls() {
        let topology = five_node_topology();
        let group = topology.shard_group_for(b"topic-blue").unwrap().clone();
        let current: HashSet<NodeId> = group.replicas.iter().cloned().collect();

        let a = topology.ring_replacements_for(group.id, &current, 2);
        let b = topology.ring_replacements_for(group.id, &current, 2);
        assert_eq!(a, b, "same inputs must yield identical results");
    }

    #[test]
    fn ring_replacements_deterministic_across_instances() {
        // Two topologies built from the same node set must agree on replacement.
        // This is what lets the leader propose and other replicas accept without
        // recomputing — but is also a useful property in its own right.
        let config = TopologyConfig {
            vnodes_per_pnode: 64,
            replication_factor: 3,
        };
        let t1 = topology_from(
            &["node-0", "node-1", "node-2", "node-3", "node-4"],
            config.clone(),
        );
        let t2 = topology_from(
            &["node-4", "node-3", "node-2", "node-1", "node-0"], // different insert order
            config,
        );

        let g1 = t1.shard_group_for(b"topic-blue").unwrap().clone();
        let g2 = t2.shard_group_for(b"topic-blue").unwrap();
        assert_eq!(g1.id, g2.id, "group id must be deterministic");

        let excluded: HashSet<NodeId> = g1.replicas.iter().cloned().collect();
        let p1 = t1.ring_replacements_for(g1.id, &excluded, 2);
        let p2 = t2.ring_replacements_for(g2.id, &excluded, 2);
        assert_eq!(p1, p2);
    }

    #[test]
    fn ring_replacements_matches_ring_continuation_past_current_members() {
        // The semantic claim of "ring-aware": replacements are exactly the nodes
        // the ring would yield if we kept walking past the current member count.
        // Verifies we're picking the ring's natural successors, not arbitrary nodes.
        let topology = five_node_topology();
        let group = topology.shard_group_for(b"topic-blue").unwrap().clone();
        assert_eq!(group.replicas.len(), 3);

        // Owners at RF=5 are: the 3 current members, then the 2 ring successors.
        let owners_5 = topology.token_owners_at(group.id.0 as u32, 5);

        assert_eq!(owners_5.len(), 5);
        assert_eq!(&owners_5[..3], group.replicas.as_slice());
        let expected_successors = &owners_5[3..];

        let current: HashSet<NodeId> = group.replicas.iter().cloned().collect();
        let picks = topology.ring_replacements_for(group.id, &current, 2);
        assert_eq!(&*picks, expected_successors);
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
            leader_addr: NodeAddress::test(
                "127.0.0.1:8080".parse().unwrap(),
                "127.0.0.1:7080".parse().unwrap(),
            ),
            term: 1,
        };
        assert!(topology.update_shard_leader(&info));

        let entry = topology.shard_leader(ShardGroupId(42)).unwrap();
        assert_eq!(entry.leader.node_id, NodeId::new("node-0"));
        assert_eq!(
            entry.leader.cluster_addr(),
            "127.0.0.1:8080".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            entry.leader.client_addr(),
            "127.0.0.1:7080".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(entry.term, 1);
        assert!(
            topology.take_dirty(),
            "accepted shard-leader update must mark dirty"
        );
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
            leader_addr: NodeAddress::test(
                "127.0.0.1:8080".parse().unwrap(),
                "127.0.0.1:8080".parse().unwrap(),
            ),
            term: 1,
        };
        topology.update_shard_leader(&info1);
        assert!(topology.take_dirty(), "first update must mark dirty");

        let info2 = ShardLeaderInfo {
            shard_group_id: ShardGroupId(42),
            leader_node_id: NodeId::new("node-1"),
            leader_addr: NodeAddress::test(
                "127.0.0.1:8081".parse().unwrap(),
                "127.0.0.1:8081".parse().unwrap(),
            ),
            term: 3,
        };
        assert!(topology.update_shard_leader(&info2));
        assert!(topology.take_dirty(), "higher-term update must mark dirty");

        let entry = topology.shard_leader(ShardGroupId(42)).unwrap();
        assert_eq!(entry.leader.node_id, NodeId::new("node-1"));
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
            leader_addr: NodeAddress::test(
                "127.0.0.1:8080".parse().unwrap(),
                "127.0.0.1:8080".parse().unwrap(),
            ),
            term: 5,
        };
        topology.update_shard_leader(&info1);
        // Clear the dirty bit from the seeding update so the next two
        // rejected updates have a clean baseline to assert against.
        let _ = topology.take_dirty();

        let stale = ShardLeaderInfo {
            shard_group_id: ShardGroupId(42),
            leader_node_id: NodeId::new("node-1"),
            leader_addr: NodeAddress::test(
                "127.0.0.1:8081".parse().unwrap(),
                "127.0.0.1:8081".parse().unwrap(),
            ),
            term: 2,
        };
        assert!(!topology.update_shard_leader(&stale));
        assert!(
            !topology.take_dirty(),
            "lower-term rejection must not mark dirty"
        );

        let equal = ShardLeaderInfo {
            shard_group_id: ShardGroupId(42),
            leader_node_id: NodeId::new("node-1"),
            leader_addr: NodeAddress::test(
                "127.0.0.1:8081".parse().unwrap(),
                "127.0.0.1:8081".parse().unwrap(),
            ),
            term: 5,
        };
        assert!(!topology.update_shard_leader(&equal));
        assert!(
            !topology.take_dirty(),
            "same-term rejection must not mark dirty"
        );

        let entry = topology.shard_leader(ShardGroupId(42)).unwrap();
        assert_eq!(entry.leader.node_id, NodeId::new("node-0"));
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
            leader_addr: NodeAddress::test(
                "127.0.0.1:8080".parse().unwrap(),
                "127.0.0.1:8080".parse().unwrap(),
            ),
            term: 1,
        };
        topology.update_shard_leader(&info);

        topology.update(NodeId::new("node-0"), &SwimNodeState::Dead);

        let entry = topology.shard_leader(ShardGroupId(42));
        assert!(
            entry.is_some(),
            "shard leader entry must survive node death"
        );
        assert_eq!(entry.unwrap().leader.node_id, NodeId::new("node-0"));
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
            leader_addr: NodeAddress::test(
                "127.0.0.1:8080".parse().unwrap(),
                "127.0.0.1:8080".parse().unwrap(),
            ),
            term: 1,
        };
        let info2 = ShardLeaderInfo {
            shard_group_id: ShardGroupId(20),
            leader_node_id: NodeId::new("node-0"),
            leader_addr: NodeAddress::test(
                "127.0.0.1:8080".parse().unwrap(),
                "127.0.0.1:8080".parse().unwrap(),
            ),
            term: 2,
        };
        topology.update_shard_leader(&info1);
        topology.update_shard_leader(&info2);

        let leaders = topology.all_shard_leaders();
        assert_eq!(leaders.len(), 2);
        assert!(leaders.contains_key(&ShardGroupId(10)));
        assert!(leaders.contains_key(&ShardGroupId(20)));
    }

    #[test]
    fn deterministic_across_separate_instances() {
        let config = TopologyConfig {
            vnodes_per_pnode: 64,
            replication_factor: 3,
        };
        let t1 = topology_from(&["node-0", "node-1", "node-2"], config.clone());
        let t2 = topology_from(&["node-0", "node-1", "node-2"], config);

        let g1 = t1.shard_group_for(b"topic-blue").unwrap();
        let g2 = t2.shard_group_for(b"topic-blue").unwrap();
        assert_eq!(g1.id, g2.id);
        assert_eq!(g1.replicas, g2.replicas);
    }
}
