use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::clusters::NodeId;
use crate::clusters::raft::messages::{
    LogMutation, MultiRaftCommand, MultiRaftReply, ProposeError, RaftCommand, RaftEvent, RaftRpc,
    RaftTimeoutCallback,
};
use crate::clusters::raft::state::Raft;

use crate::clusters::raft::storage::RaftStorage;
use crate::clusters::swims::{ShardGroup, ShardGroupId};

// dirty is owned here because it's a persistence concern — flush() drains it.
pub(crate) struct MultiRaft {
    node_id: NodeId,
    storage: Box<dyn RaftStorage>,
    groups: HashMap<ShardGroupId, Raft>,
    seq_counter: RaftTimerTokenGenerator,
    dirty: HashSet<ShardGroupId>,
    pending_events: Vec<RaftEvent>,
}

impl MultiRaft {
    pub(crate) fn new(node_id: NodeId, storage: Box<dyn RaftStorage>) -> Self {
        Self {
            node_id,
            storage,
            groups: HashMap::new(),
            seq_counter: RaftTimerTokenGenerator::default(),
            dirty: HashSet::new(),
            pending_events: Vec::new(),
        }
    }

    pub(crate) fn handle_command(&mut self, cmd: MultiRaftCommand) -> MultiRaftReply {
        use MultiRaftCommand::*;
        match cmd {
            PacketReceived {
                shard_group_id,
                from,
                rpc,
            } => {
                self.step(shard_group_id, from, rpc);
                MultiRaftReply::None
            }
            Timeout(cb) => {
                self.handle_timeout(cb);
                MultiRaftReply::None
            }
            EnsureGroup { group } => {
                self.add_group(group);
                MultiRaftReply::None
            }
            RemoveGroup { group_id } => {
                self.remove_group(group_id);
                MultiRaftReply::None
            }
            GetLeader { group_id } => MultiRaftReply::GetLeader(self.get_leader(group_id)),
            Propose {
                shard_group_id,
                command,
            } => MultiRaftReply::Propose(self.propose(shard_group_id, command)),
            HandleNodeDeath { dead_node_id } => {
                self.remove_node(dead_node_id);
                MultiRaftReply::None
            }
            HandleNodeJoin {
                new_node_id,
                affected_groups,
            } => {
                self.add_node(new_node_id, affected_groups);
                MultiRaftReply::None
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

        let jitter = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            self.node_id.hash(&mut hasher);
            group.id.0.hash(&mut hasher);
            (hasher.finish() % 20) as u32
        };

        let election_seq = self.seq_counter.generate();
        let heartbeat_seq = self.seq_counter.generate();

        let persistent = self.storage.load_state(group.id.0);

        let raft = Raft::new(
            self.node_id.clone(),
            peers,
            persistent,
            jitter,
            group.id,
            election_seq,
            heartbeat_seq,
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

    fn remove_group(&mut self, group_id: ShardGroupId) {
        let Some(mut raft) = self.groups.remove(&group_id) else {
            return;
        };
        raft.cancel_all_timers();
        self.pending_events.extend(raft.take_events());

        self.storage.delete_group(group_id);

        tracing::info!("[{}] Removed Raft group {:?}", self.node_id, group_id);
    }

    fn step(&mut self, shard_id: ShardGroupId, from: NodeId, rpc: RaftRpc) {
        if let Some(raft) = self.groups.get_mut(&shard_id) {
            raft.step(from, rpc);
            self.dirty.insert(shard_id);
        }
    }

    fn handle_timeout(&mut self, cb: RaftTimeoutCallback) {
        let shard_id = match &cb {
            RaftTimeoutCallback::Ignored => return,
            RaftTimeoutCallback::ElectionTimeout { shard_group_id } => *shard_group_id,
            RaftTimeoutCallback::HeartbeatTimeout { shard_group_id } => *shard_group_id,
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

    fn remove_node(&mut self, node_id: NodeId) {
        for (group_id, raft) in self.groups.iter_mut() {
            if raft.has_peer(&node_id) {
                raft.remove_peer(&node_id);
                self.dirty.insert(*group_id);
            }
        }
    }

    fn add_node(&mut self, node_id: NodeId, affected_groups: Vec<ShardGroup>) {
        for group in &affected_groups {
            if let Some(raft) = self.groups.get_mut(&group.id)
                && !raft.has_peer(&node_id)
            {
                raft.add_peer(node_id.clone());
            }
        }

        for group in affected_groups {
            if !self.groups.contains_key(&group.id) && group.members.contains(&self.node_id) {
                self.add_group(group);
            }
        }
    }

    fn propose(
        &mut self,
        shard_id: ShardGroupId,
        command: RaftCommand,
    ) -> Result<(), ProposeError> {
        match self.groups.get_mut(&shard_id) {
            Some(raft) => {
                let result = raft.propose(command);
                self.dirty.insert(shard_id);
                result
            }
            None => Err(ProposeError::NotLeader(None)),
        }
    }

    pub(crate) fn flush(&mut self) -> Vec<RaftEvent> {
        let dirty: Vec<ShardGroupId> = std::mem::take(&mut self.dirty).into_iter().collect();
        let mut mutations: Vec<(ShardGroupId, LogMutation)> = vec![];
        let mut last_indices: HashMap<ShardGroupId, u64> = HashMap::new();

        for id in &dirty {
            let Some(raft) = self.groups.get_mut(id) else {
                continue;
            };

            last_indices.insert(*id, raft.log_last_index());
            for log in raft.take_log_mutations() {
                mutations.push((*id, log));
            }
            self.pending_events.extend(raft.take_events());
        }

        if !mutations.is_empty() {
            self.storage.persist_mutations(mutations);

            for (id, last_log_index) in last_indices {
                if let Some(raft) = self.groups.get_mut(&id) {
                    raft.advance_stabled_index(last_log_index)
                }
            }
        }

        std::mem::take(&mut self.pending_events)
    }
}

#[derive(Debug, Default)]
struct RaftTimerTokenGenerator(u32);
impl RaftTimerTokenGenerator {
    fn generate(&mut self) -> u32 {
        self.0 = self.0.wrapping_add(1);
        self.0
    }
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

    use crate::clusters::raft::storage::RaftPersistentState;
    use crate::impls::metadata_storage::MetadataStorage;
    use crate::schedulers::ticker_message::TimerCommand;
    use std::collections::HashSet;

    fn node(id: &str) -> NodeId {
        NodeId::new(id)
    }

    fn shard(id: u64, members: Vec<NodeId>) -> ShardGroup {
        ShardGroup {
            id: ShardGroupId(id),
            members,
        }
    }

    fn timer_seqs(events: &[RaftEvent]) -> Vec<u32> {
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
        MultiRaft::new(node_id, storage)
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
        let unique: HashSet<u32> = seqs.iter().cloned().collect();
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
        store.handle_command(MultiRaftCommand::Timeout(
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
            store.handle_command(MultiRaftCommand::Timeout(
                RaftTimeoutCallback::ElectionTimeout {
                    shard_group_id: ShardGroupId(1),
                },
            ));
            store.handle_command(MultiRaftCommand::Timeout(
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

    use crate::clusters::raft::log::LogEntry;
    use crate::clusters::raft::messages::{AppendEntries, RaftRpc};

    const TEST_GROUP_ID: ShardGroupId = ShardGroupId(42);

    /// Elect n1 as leader of a single-node shard group. Single-node clusters
    /// become leader immediately on ElectionTimeout (no peers to wait for).
    fn elect_leader(store: &mut MultiRaft) {
        store.handle_command(MultiRaftCommand::Timeout(
            RaftTimeoutCallback::ElectionTimeout {
                shard_group_id: TEST_GROUP_ID,
            },
        ));
    }

    fn propose_noop(store: &mut MultiRaft) -> MultiRaftReply {
        store.handle_command(MultiRaftCommand::Propose {
            shard_group_id: TEST_GROUP_ID,
            command: RaftCommand::Noop,
        })
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
        store.handle_command(MultiRaftCommand::PacketReceived {
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
                        command: RaftCommand::Noop,
                    },
                    LogEntry {
                        term: 1,
                        index: 2,
                        command: RaftCommand::Noop,
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
        store.handle_command(MultiRaftCommand::PacketReceived {
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
                    command: RaftCommand::Noop,
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
        use crate::clusters::metadata::command::{CreateTopic, MetadataCommand};
        use crate::clusters::metadata::strategy::{PartitionStrategy, StoragePolicy};

        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

        elect_leader(&mut store);
        store.flush();

        let cmd = RaftCommand::Metadata(MetadataCommand::CreateTopic(CreateTopic {
            name: "test-topic".to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
            replica_set: vec![node("n1"), node("n2"), node("n3")],
            created_at: 1000,
        }));

        let reply = store.handle_command(MultiRaftCommand::Propose {
            shard_group_id: TEST_GROUP_ID,
            command: cmd,
        });
        assert!(matches!(reply, MultiRaftReply::Propose(Ok(()))));
        store.flush();

        let entry = read_entry(&store, 2).expect("metadata command entry must be persisted");
        assert_eq!(entry.index, 2);
    }
}
