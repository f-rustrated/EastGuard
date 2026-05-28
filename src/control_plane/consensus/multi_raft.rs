use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};

use tokio::sync::oneshot;

use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::{
    ConsensusCommand, CoordinatorSealRequest, DeferredReply, LogMutation, MetadataCommitted,
    MultiRaftActorCommand, PacketReceived, ProposeError, RaftEvent, RaftPropose,
    RaftTimeoutCallback, SealContext,
};
use crate::control_plane::consensus::raft::state::{Raft, TimerSeqs};
use crate::control_plane::metadata::TopicId;
use crate::control_plane::metadata::command::RollSegment;
use crate::control_plane::metadata::types::TopicStats;

use crate::control_plane::consensus::raft::storage::RaftStorage;
use crate::control_plane::membership::{ShardGroup, ShardGroupId};
use crate::data_plane::SegmentKey;

// dirty is owned here because it's a persistence concern — flush() drains it.
pub(crate) struct MultiRaft {
    node_id: NodeId,
    election_jitter_seed: u64,
    storage: Box<dyn RaftStorage>,
    groups: BTreeMap<ShardGroupId, Raft>,
    seq_counter: RaftTimerTokenGenerator,
    dirty: BTreeSet<ShardGroupId>,
    pending_events: Vec<RaftEvent>,
    deferred: Vec<DeferredReply>,
    /// Senders waiting for a specific (shard, log index) to be committed and applied.
    pending_proposes: BTreeMap<(ShardGroupId, u64), oneshot::Sender<Result<(), ProposeError>>>,
    pending_seals: PendingSealTracker,
}

#[derive(Default)]
struct PendingSealTracker {
    by_index: HashMap<(ShardGroupId, u64), PendingSeal>,
    by_key: HashSet<SegmentKey>,
}

impl PendingSealTracker {
    fn contains(&self, key: &SegmentKey) -> bool {
        self.by_key.contains(key)
    }

    fn insert(&mut self, group_id: ShardGroupId, log_index: u64, seal: PendingSeal) {
        self.by_key.insert(seal.segment_key);
        self.by_index.insert((group_id, log_index), seal);
    }

    fn remove(&mut self, group_id: ShardGroupId, log_index: u64) -> Option<PendingSeal> {
        let seal = self.by_index.remove(&(group_id, log_index))?;
        self.by_key.remove(&seal.segment_key);
        Some(seal)
    }

    fn attach_seal_context(&mut self, group_id: ShardGroupId, committed: &mut MetadataCommitted) {
        if let Some(seal) = self.remove(group_id, committed.log_index) {
            committed.seal_context = Some(SealContext {
                requester: seal.requester,
                old_segment_key: seal.segment_key,
            });
        }
    }
}

struct PendingSeal {
    requester: NodeId,
    segment_key: SegmentKey,
}

impl MultiRaft {
    pub(crate) fn new(
        node_id: NodeId,
        election_jitter_seed: u64,
        storage: Box<dyn RaftStorage>,
    ) -> Self {
        Self {
            node_id,
            election_jitter_seed,
            storage,
            groups: BTreeMap::new(),
            seq_counter: RaftTimerTokenGenerator::default(),
            dirty: BTreeSet::new(),
            pending_events: Vec::new(),
            deferred: Vec::new(),
            pending_proposes: BTreeMap::new(),
            pending_seals: PendingSealTracker::default(),
        }
    }

    pub(crate) fn process(&mut self, cmd: MultiRaftActorCommand) {
        match cmd {
            MultiRaftActorCommand::ConsensusCommand(c) => {
                self.handle_command(c);
            }
            MultiRaftActorCommand::GetLeader { group_id, reply } => {
                let result = self.get_leader(group_id);
                self.deferred.push(DeferredReply::GetLeader(reply, result));
            }
            MultiRaftActorCommand::Propose { propose, reply } => {
                self.propose(propose, reply);
            }

            MultiRaftActorCommand::GetTopics { reply } => {
                let topics = self.get_topics();
                self.deferred.push(DeferredReply::GetTopics(reply, topics));
            }
            MultiRaftActorCommand::GetTopicStats { reply } => {
                let stats = self.get_topic_stats();
                self.deferred
                    .push(DeferredReply::GetTopicStats(reply, stats));
            }
            MultiRaftActorCommand::Coordinator(req) => {
                self.handle_seal_request(req);
            }
        }
    }

    pub(crate) fn fire_deferred(&mut self) {
        for reply in self.deferred.drain(..) {
            match reply {
                DeferredReply::GetLeader(sender, v) => {
                    let _ = sender.send(v);
                }
                DeferredReply::Propose(sender, v) => {
                    let _ = sender.send(v);
                }
                DeferredReply::GetTopics(sender, v) => {
                    let _ = sender.send(v);
                }
                DeferredReply::GetTopicStats(sender, v) => {
                    let _ = sender.send(v);
                }
            }
        }
    }

    pub(crate) fn handle_command(&mut self, cmd: impl Into<ConsensusCommand>) {
        match cmd.into() {
            ConsensusCommand::PacketReceived(cmd) => self.step(cmd),
            ConsensusCommand::Timeout(cb) => self.handle_timeout(cb),
            ConsensusCommand::EnsureGroup(cmd) => self.add_group(cmd.group),
            ConsensusCommand::RemoveGroup(cmd) => self.remove_group(cmd.group_id),
            ConsensusCommand::HandleNodeDeath(cmd) => {
                self.handle_node_death(cmd.dead_node_id, cmd.live_nodes)
            }
            ConsensusCommand::HandleNodeJoin(cmd) => {
                self.add_node(cmd.new_node_id, cmd.affected_groups)
            }
        }
    }

    fn add_group(&mut self, group: ShardGroup) {
        if self.groups.contains_key(&group.id) {
            return;
        }
        if !group.members.contains(&self.node_id) {
            return;
        }

        let peers: HashSet<NodeId> = group
            .members
            .iter()
            .filter(|id| *id != &self.node_id)
            .cloned()
            .collect();

        let election_jitter_seed = self.create_election_jitter_seed(&group);

        let persistent = self.storage.load_state(group.id.0);

        let timer_seqs = TimerSeqs {
            election: self.seq_counter.generate(),
            heartbeat: self.seq_counter.generate(),
            merge_check: self.seq_counter.generate(),
        };

        let raft = Raft::new(
            self.node_id.clone(),
            peers,
            persistent,
            election_jitter_seed,
            group.id,
            timer_seqs,
        );

        tracing::info!(
            "[{}] Created Raft group {:?} with {} peers",
            self.node_id,
            group.id,
            raft.peers_count()
        );

        self.groups.insert(group.id, raft);
        self.dirty.insert(group.id);
    }

    fn create_election_jitter_seed(&mut self, group: &ShardGroup) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.election_jitter_seed.hash(&mut hasher);
        group.id.hash(&mut hasher);
        hasher.finish()
    }

    fn remove_group(&mut self, group_id: ShardGroupId) {
        let Some(mut raft) = self.groups.remove(&group_id) else {
            return;
        };
        raft.cancel_all_timers();
        self.pending_events.extend(raft.take_events());

        let pending_keys: Vec<_> = self
            .pending_proposes
            .keys()
            .filter(|(gid, _)| *gid == group_id)
            .cloned()
            .collect();
        for key in pending_keys {
            if let Some(sender) = self.pending_proposes.remove(&key) {
                let _ = sender.send(Err(ProposeError::ShardGroupRemoved));
            }
        }

        self.storage.delete_group(group_id);

        tracing::info!("[{}] Removed Raft group {:?}", self.node_id, group_id);
    }

    fn step(&mut self, cmd: PacketReceived) {
        if let Some(raft) = self.groups.get_mut(&cmd.shard_group_id) {
            raft.step(cmd.from, cmd.rpc);
            self.dirty.insert(cmd.shard_group_id);
        }
    }

    fn handle_timeout(&mut self, cb: RaftTimeoutCallback) {
        let shard_id = match &cb {
            RaftTimeoutCallback::Ignored => return,
            RaftTimeoutCallback::ElectionTimeout { shard_group_id }
            | RaftTimeoutCallback::HeartbeatTimeout { shard_group_id }
            | RaftTimeoutCallback::MergeCheckTimeout { shard_group_id, .. } => *shard_group_id,
        };
        if let Some(raft) = self.groups.get_mut(&shard_id) {
            raft.handle_timeout(cb);
            self.dirty.insert(shard_id);
        }
    }

    fn get_leader(&self, group_id: ShardGroupId) -> Option<NodeId> {
        self.groups
            .get(&group_id)
            .and_then(|r| r.current_leader().cloned())
    }

    fn get_topics(&self) -> Vec<String> {
        self.groups
            .values()
            .flat_map(|raft| raft.topic_names())
            .collect()
    }

    fn get_topic_stats(&self) -> Vec<TopicStats> {
        self.groups
            .values()
            .flat_map(|raft| raft.topic_stats())
            .collect()
    }

    // Full scan over groups is acceptable — node death is rare (~6-7s SWIM detection)
    // and the scan is O(groups) with cheap has_peer/is_leader checks.
    fn handle_node_death(&mut self, dead_node_id: NodeId, live_nodes: Vec<NodeId>) {
        for (group_id, raft) in self.groups.iter_mut() {
            if raft.has_peer(&dead_node_id) {
                raft.remove_peer(&dead_node_id);
                self.dirty.insert(*group_id);
            }

            if !raft.is_leader() {
                continue;
            }

            // ? how does this work when we have 3 nodes while replication factor is being three?
            for (segment_key, old_replica_set) in raft.active_segments_for_node(&dead_node_id) {
                let new_replica_set = compute_replacement_replica_set(
                    &old_replica_set,
                    std::slice::from_ref(&dead_node_id),
                    &live_nodes,
                );
                let cmd = RollSegment {
                    segment_key,
                    sealed_at: now_ms(),
                    new_replica_set,
                    end_entry_id: None,
                };

                // ! Consider retry logic as for leader's this is only path
                if let Err(e) = raft.propose(cmd.into()) {
                    tracing::warn!(
                        "Death-triggered RollSegment for {:?} failed: {:?}",
                        segment_key,
                        e
                    );
                }
            }
        }
    }

    fn handle_seal_request(&mut self, req: CoordinatorSealRequest) {
        let seal = &req.request;
        if self.pending_seals.contains(&seal.segment_key) {
            return;
        }

        let Some(group_id) = self.find_group_for_topic(&seal.segment_key.topic_id) else {
            tracing::warn!(
                "SealRequest for unknown topic {:?}",
                seal.segment_key.topic_id
            );
            return;
        };
        let Some(raft) = self.groups.get_mut(&group_id) else {
            return;
        };

        let Some(old_replica_set) = raft.get_replica_set(&seal.segment_key) else {
            tracing::warn!("SealRequest for unknown segment {:?}", seal.segment_key);
            return;
        };

        let new_replica_set =
            compute_replacement_replica_set(&old_replica_set, &seal.failed_nodes, &req.live_nodes);

        let cmd = RollSegment {
            segment_key: seal.segment_key,
            sealed_at: now_ms(),
            new_replica_set,
            end_entry_id: Some(seal.end_entry_id),
        };

        match raft.propose(cmd.into()) {
            Ok(log_index) => {
                self.pending_seals.insert(
                    group_id,
                    log_index,
                    PendingSeal {
                        requester: seal.from.clone(),
                        segment_key: seal.segment_key,
                    },
                );
                self.dirty.insert(group_id);
            }
            Err(e) => {
                tracing::debug!("SealRequest proposal rejected: {:?}", e);
            }
        }
    }

    fn find_group_for_topic(&self, topic_id: &TopicId) -> Option<ShardGroupId> {
        self.groups
            .iter()
            .find_map(|(gid, raft)| raft.has_topic(topic_id).then_some(*gid))
    }

    fn add_node(&mut self, node_id: NodeId, affected_groups: Vec<ShardGroup>) {
        self.add_peer_to_existing_groups(&node_id, &affected_groups);
        self.ensure_new_groups(affected_groups);
    }

    fn add_peer_to_existing_groups(&mut self, node_id: &NodeId, groups: &[ShardGroup]) {
        for group in groups {
            if let Some(raft) = self.groups.get_mut(&group.id)
                && !raft.has_peer(node_id)
            {
                raft.add_peer(node_id.clone());
            }
        }
    }

    fn ensure_new_groups(&mut self, groups: Vec<ShardGroup>) {
        for group in groups {
            if !self.groups.contains_key(&group.id) && group.members.contains(&self.node_id) {
                self.add_group(group);
            }
        }
    }

    fn propose(&mut self, cmd: RaftPropose, reply: oneshot::Sender<Result<(), ProposeError>>) {
        let gid = cmd.shard_group_id;
        match self.propose_internal(cmd) {
            Ok(index) => {
                self.pending_proposes.insert((gid, index), reply);
            }
            Err(e) => {
                self.deferred.push(DeferredReply::Propose(reply, Err(e)));
            }
        }
    }

    fn propose_internal(&mut self, cmd: RaftPropose) -> Result<u64, ProposeError> {
        match self.groups.get_mut(&cmd.shard_group_id) {
            Some(raft) => {
                let result = raft.propose(cmd.command);
                self.dirty.insert(cmd.shard_group_id);
                result
            }
            None => Err(ProposeError::ShardNotFound),
        }
    }

    pub(crate) fn flush(&mut self) -> Vec<RaftEvent> {
        let dirty: Vec<ShardGroupId> = std::mem::take(&mut self.dirty).into_iter().collect();
        self.flush_auto_proposals(&dirty);
        let (mutations, last_indices) = self.collect_mutations(&dirty);
        self.persist_and_advance(mutations, last_indices);
        self.resolve_pending_proposes(&dirty);
        std::mem::take(&mut self.pending_events)
    }

    fn flush_auto_proposals(&mut self, dirty: &[ShardGroupId]) {
        for id in dirty {
            let Some(raft) = self.groups.get_mut(id) else {
                continue;
            };
            for cmd in raft.take_pending_proposals() {
                if let Err(e) = raft.propose(cmd) {
                    tracing::warn!("Auto-proposal rejected for {:?}: {:?}", id, e);
                }
            }
        }
    }

    fn collect_mutations(
        &mut self,
        dirty: &[ShardGroupId],
    ) -> (
        Vec<(ShardGroupId, LogMutation)>,
        BTreeMap<ShardGroupId, u64>,
    ) {
        let mut mutations = vec![];
        let mut last_indices = BTreeMap::new();
        for id in dirty {
            let Some(raft) = self.groups.get_mut(id) else {
                continue;
            };

            last_indices.insert(*id, raft.log_last_index());
            for log in raft.take_log_mutations() {
                mutations.push((*id, log));
            }
            for event in raft.take_events() {
                match event {
                    RaftEvent::MetadataCommitted(_) if !raft.is_leader() => {}
                    RaftEvent::MetadataCommitted(mut committed) => {
                        self.pending_seals.attach_seal_context(*id, &mut committed);
                        self.pending_events
                            .push(RaftEvent::MetadataCommitted(committed));
                    }
                    other => self.pending_events.push(other),
                }
            }
        }
        (mutations, last_indices)
    }

    fn resolve_pending_proposes(&mut self, dirty: &[ShardGroupId]) {
        for id in dirty {
            let Some(raft) = self.groups.get(id) else {
                continue;
            };
            let last_applied = raft.last_applied_index();
            let is_leader = raft.is_leader(); // NLL: borrow of `raft` ends here

            let to_fire: Vec<_> = self
                .pending_proposes
                .range((*id, 0)..=(*id, last_applied))
                .map(|(k, _)| *k)
                .collect();
            for key in to_fire {
                if let Some(sender) = self.pending_proposes.remove(&key) {
                    let _ = sender.send(Ok(()));
                }
            }

            // Reject senders for entries that will never be applied because
            // this node is no longer the leader.
            if !is_leader {
                let to_reject: Vec<_> = self
                    .pending_proposes
                    .range((*id, last_applied.saturating_add(1))..)
                    .take_while(|(k, _)| k.0 == *id)
                    .map(|(k, _)| *k)
                    .collect();
                for key in to_reject {
                    if let Some(sender) = self.pending_proposes.remove(&key) {
                        let _ = sender.send(Err(ProposeError::NotLeader(None)));
                    }
                }
            }
        }
    }

    fn persist_and_advance(
        &mut self,
        mutations: Vec<(ShardGroupId, LogMutation)>,
        last_indices: BTreeMap<ShardGroupId, u64>,
    ) {
        if mutations.is_empty() {
            return;
        }
        self.storage.persist_mutations(mutations);
        for (id, last_log_index) in last_indices {
            if let Some(raft) = self.groups.get_mut(&id) {
                raft.advance_stabled_index(last_log_index);
            }
        }
    }
}

#[derive(Debug, Default)]
struct RaftTimerTokenGenerator(u64);
impl RaftTimerTokenGenerator {
    fn generate(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(1);
        self.0
    }
}

fn compute_replacement_replica_set(
    old: &[NodeId],
    dead_nodes: &[NodeId],
    live_nodes: &[NodeId],
) -> Vec<NodeId> {
    let rf = old.len();
    let mut new_set: Vec<NodeId> = old
        .iter()
        .filter(|n| !dead_nodes.contains(n))
        .cloned()
        .collect();
    for candidate in live_nodes {
        if new_set.len() >= rf {
            break;
        }
        if !new_set.contains(candidate) && !dead_nodes.contains(candidate) {
            new_set.push(candidate.clone());
        }
    }
    if new_set.is_empty() {
        tracing::error!("All replicas dead for segment — empty replica set");
    }
    new_set
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
impl MultiRaft {
    fn stabled_for(&self, id: ShardGroupId) -> u64 {
        self.groups.get(&id).map_or(0, |r| r.stabled_index())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::control_plane::consensus::raft::storage::RaftPersistentState;
    use crate::impls::metadata_storage::MetadataStorage;
    use crate::schedulers::ticker_message::TimerCommand;
    use std::collections::BTreeSet;

    fn node(id: &str) -> NodeId {
        NodeId::new(id)
    }

    fn shard(id: u64, members: Vec<NodeId>) -> ShardGroup {
        ShardGroup {
            id: ShardGroupId(id),
            members,
        }
    }

    fn timer_seqs(events: &[RaftEvent]) -> Vec<u64> {
        events
            .iter()
            .filter_map(|e| match e {
                RaftEvent::Timer(TimerCommand::SetSchedule { seq, .. }) => Some(*seq),
                _ => None,
            })
            .collect()
    }

    fn temp_storage() -> (Box<dyn RaftStorage>, std::path::PathBuf) {
        let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let db = MetadataStorage::open(path.clone());
        (Box::new(db), path)
    }

    fn new_store(node_id: NodeId, storage: Box<dyn RaftStorage>) -> MultiRaft {
        MultiRaft::new(node_id, 0, storage)
    }

    #[test]
    fn timer_seqs_are_unique_across_groups() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);

        store.add_group(shard(1, vec![me.clone(), node("n2")]));
        store.add_group(shard(2, vec![me.clone(), node("n2")]));

        let events = store.flush();
        let seqs = timer_seqs(&events);
        let unique: BTreeSet<u64> = seqs.iter().cloned().collect();
        assert_eq!(seqs.len(), unique.len());
    }

    #[test]
    fn add_group_registers_in_memory() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);

        store.add_group(shard(42, vec![me.clone(), node("n2")]));

        assert!(store.groups.contains_key(&ShardGroupId(42)));
    }

    #[test]
    fn remove_group_deletes_data() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);

        store.add_group(shard(42, vec![me.clone()]));
        store.handle_command(ConsensusCommand::Timeout(
            RaftTimeoutCallback::ElectionTimeout {
                shard_group_id: ShardGroupId(42),
            },
        ));
        store.flush();
        assert!(store.storage.load_state(42) != Default::default());

        store.remove_group(ShardGroupId(42));
        assert!(store.storage.load_state(42) == Default::default());
    }

    #[test]
    fn add_group_noop_when_not_member() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);

        store.add_group(shard(42, vec![node("n2"), node("n3")]));

        assert!(!store.groups.contains_key(&ShardGroupId(42)));
    }

    #[test]
    fn restart_recovers_persisted_state() {
        let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let me = node("n1");

        {
            let db = MetadataStorage::open(path.clone());
            let mut store = new_store(me.clone(), Box::new(db));
            store.add_group(shard(1, vec![me.clone()]));
            store.add_group(shard(2, vec![me.clone()]));
            store.handle_command(ConsensusCommand::Timeout(
                RaftTimeoutCallback::ElectionTimeout {
                    shard_group_id: ShardGroupId(1),
                },
            ));
            store.handle_command(ConsensusCommand::Timeout(
                RaftTimeoutCallback::ElectionTimeout {
                    shard_group_id: ShardGroupId(2),
                },
            ));
            store.flush();
        }

        let db = MetadataStorage::open(path);
        assert!(db.load_state(1) != Default::default());
        assert!(db.load_state(2) != Default::default());
    }

    // -----------------------------------------------------------------------
    // Unit 2 — flush() writes to RocksDB
    // -----------------------------------------------------------------------

    use crate::control_plane::consensus::messages::{AppendEntries, RaftRpc};
    use crate::control_plane::consensus::raft::log::LogEntry;

    const TEST_GROUP_ID: ShardGroupId = ShardGroupId(42);

    /// Elect n1 as leader of a single-node shard group. Single-node clusters
    /// become leader immediately on ElectionTimeout (no peers to wait for).
    fn elect_leader(store: &mut MultiRaft) {
        store.handle_command(ConsensusCommand::Timeout(
            RaftTimeoutCallback::ElectionTimeout {
                shard_group_id: TEST_GROUP_ID,
            },
        ));
    }

    fn propose_noop(store: &mut MultiRaft) {
        let raft = store.groups.get_mut(&TEST_GROUP_ID).unwrap();
        raft.propose_noop().expect("propose_noop failed");
        store.dirty.insert(TEST_GROUP_ID);
    }

    fn read_entry(store: &MultiRaft, index: u64) -> Option<LogEntry> {
        let state = store.storage.load_state(TEST_GROUP_ID.0);
        state.log.into_iter().find(|e| e.index == index)
    }

    fn read_hard_state(store: &MultiRaft) -> RaftPersistentState {
        store.storage.load_state(TEST_GROUP_ID.0)
    }

    #[test]
    fn flush_persists_log_entry() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

        elect_leader(&mut store);
        store.flush(); // persist election HardState

        propose_noop(&mut store);
        store.flush();

        // The noop appended by become_leader is index 1; our proposal is index 2.
        let entry = read_entry(&store, 2).expect("entry 2 must be in RocksDB after flush");
        assert_eq!(entry.index, 2);
    }

    #[test]
    fn flush_persists_hard_state() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

        elect_leader(&mut store);
        store.flush();

        let state = read_hard_state(&store);
        assert_eq!(state.term, 1, "term must be 1 after first election");
    }

    #[test]
    fn stabled_advances_after_flush() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

        assert_eq!(store.stabled_for(TEST_GROUP_ID), 0);

        elect_leader(&mut store);
        store.flush();
        let after_election = store.stabled_for(TEST_GROUP_ID);

        propose_noop(&mut store);
        store.flush();
        let after_propose = store.stabled_for(TEST_GROUP_ID);

        assert!(
            after_propose > after_election,
            "stabled must advance after appending an entry"
        );
    }

    #[test]
    fn flush_noop_when_not_dirty() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));
        store.flush(); // consume dirty from add_group

        // No state changes — flush should be a no-op and return no events.
        let events = store.flush();
        assert!(events.is_empty());
    }

    #[test]
    fn flush_truncate_removes_entries_from_rocksdb() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let n2 = node("n2");
        let mut store = new_store(me.clone(), storage);
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone(), n2.clone()]));
        store.flush(); // consume add_group dirty

        // n2 is acting as leader at term 1.  Send two entries to n1 (follower).
        store.handle_command(PacketReceived {
            shard_group_id: TEST_GROUP_ID,
            from: n2.clone(),
            rpc: RaftRpc::AppendEntries(AppendEntries {
                term: 1,
                leader_id: n2.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![
                    LogEntry {
                        term: 1,
                        index: 1,
                        command: None,
                    },
                    LogEntry {
                        term: 1,
                        index: 2,
                        command: None,
                    },
                ],
                leader_commit: 0,
            }),
        });
        store.flush();
        assert!(
            read_entry(&store, 1).is_some(),
            "entry 1 must exist before truncation"
        );
        assert!(
            read_entry(&store, 2).is_some(),
            "entry 2 must exist before truncation"
        );

        // n2 re-sends from index 1 at term 2, conflicting with the existing term-1 entries.
        // Raft truncates from index 1 and replaces with the new entry.
        store.handle_command(PacketReceived {
            shard_group_id: TEST_GROUP_ID,
            from: n2.clone(),
            rpc: RaftRpc::AppendEntries(AppendEntries {
                term: 2,
                leader_id: n2.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![LogEntry {
                    term: 2,
                    index: 1,
                    command: None,
                }],
                leader_commit: 0,
            }),
        });
        store.flush();

        let new_entry = read_entry(&store, 1).expect("replacement entry 1 must be in RocksDB");
        assert_eq!(new_entry.term, 2, "entry 1 must be the term-2 replacement");
        assert!(
            read_entry(&store, 2).is_none(),
            "truncated entry 2 must be gone from RocksDB"
        );
    }

    #[test]
    fn restart_restores_log_entries_and_stabled_index() {
        let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let me = node("n1");

        let expected_last_index;
        {
            let db = MetadataStorage::open(path.clone());
            let mut store = new_store(me.clone(), Box::new(db));
            store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));
            elect_leader(&mut store);
            store.flush();
            propose_noop(&mut store);
            store.flush();
            expected_last_index = store.groups.get(&TEST_GROUP_ID).unwrap().log_last_index();
        }

        let db = MetadataStorage::open(path);
        let mut store = new_store(me.clone(), Box::new(db));
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

        let raft = store.groups.get(&TEST_GROUP_ID).unwrap();
        assert_eq!(
            raft.log_last_index(),
            expected_last_index,
            "log must be restored from RocksDB after restart"
        );
        assert_eq!(
            raft.stabled_index(),
            expected_last_index,
            "stabled_index must equal last restored log entry"
        );
    }

    #[test]
    fn restart_restores_hard_state() {
        let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let me = node("n1");
        {
            let db = MetadataStorage::open(path.clone());
            let mut store = new_store(me.clone(), Box::new(db));
            store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));
            elect_leader(&mut store);
            store.flush();
        }

        let db = MetadataStorage::open(path);
        let mut store = new_store(me.clone(), Box::new(db));
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));
        let raft = store.groups.get(&TEST_GROUP_ID).unwrap();
        assert_eq!(raft.current_term(), 1);
        assert_eq!(raft.voted_for(), Some(node("n1")));
    }

    #[test]
    fn propose_metadata_command_via_multi_raft() {
        use crate::control_plane::metadata::command::{CreateTopic, MetadataCommand};
        use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};

        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

        elect_leader(&mut store);
        store.flush();

        let cmd = MetadataCommand::CreateTopic(CreateTopic {
            name: "test-topic".to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
            replica_set: vec![node("n1"), node("n2"), node("n3")],
            created_at: 1000,
        });

        store
            .propose_internal(RaftPropose {
                shard_group_id: TEST_GROUP_ID,
                command: cmd,
            })
            .expect("metadata propose failed");
        store.flush();

        let entry = read_entry(&store, 2).expect("metadata command entry must be persisted");
        assert_eq!(entry.index, 2);
    }

    #[test]
    fn groups_are_independent_no_cross_contamination() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);

        // Single-node groups so election succeeds immediately
        store.add_group(shard(1, vec![me.clone()]));
        store.add_group(shard(2, vec![me.clone()]));
        store.flush();

        // Elect leader in group 1 only
        store.handle_command(ConsensusCommand::Timeout(
            RaftTimeoutCallback::ElectionTimeout {
                shard_group_id: ShardGroupId(1),
            },
        ));
        store.flush();

        // Propose noop to group 1
        let raft = store.groups.get_mut(&ShardGroupId(1)).unwrap();
        raft.propose_noop().unwrap();
        store.dirty.insert(ShardGroupId(1));
        store.flush();

        let g1 = store.groups.get(&ShardGroupId(1)).unwrap();
        assert_eq!(g1.current_term(), 1);
        assert!(g1.log_last_index() >= 2);

        let g2 = store.groups.get(&ShardGroupId(2)).unwrap();
        assert_eq!(g2.current_term(), 0, "group 2 term must be unaffected");
        assert_eq!(g2.log_last_index(), 0, "group 2 log must be empty");
    }

    #[test]
    fn stale_metadata_proposal_rejected_gracefully() {
        use crate::control_plane::metadata::command::{CreateTopic, MetadataCommand};
        use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};

        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

        elect_leader(&mut store);
        store.flush();

        let make_cmd = || {
            MetadataCommand::CreateTopic(CreateTopic {
                name: "blue".to_string(),
                storage_policy: StoragePolicy {
                    retention_ms: 3_600_000,
                    replication_factor: 3,
                    partition_strategy: PartitionStrategy::AutoSplit,
                },
                replica_set: vec![node("n1"), node("n2"), node("n3")],
                created_at: 1000,
            })
        };

        // First proposal succeeds
        let _ = store.propose_internal(RaftPropose {
            shard_group_id: TEST_GROUP_ID,
            command: make_cmd(),
        });
        store.flush();

        // Duplicate proposal: must not panic, state machine rejects at apply
        let _ = store.propose_internal(RaftPropose {
            shard_group_id: TEST_GROUP_ID,
            command: make_cmd(),
        });
        store.flush();

        let raft = store.groups.get(&TEST_GROUP_ID).unwrap();
        assert_eq!(
            raft.state_machine().topic_count(),
            1,
            "duplicate must not create a second topic"
        );
    }
}
