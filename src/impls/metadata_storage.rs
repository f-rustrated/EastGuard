use crate::control_plane::{
    NodeId,
    consensus::messages::LogMutation,
    consensus::raft::{
        log::LogEntry,
        storage::{RaftPersistentState, RaftSnapshot, RaftSnapshotHeader, RaftStorage},
    },
    membership::ShardGroupId,
};
#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;

/// Key encoder for per-group entries in the default column family.
/// Layout: `[group_id: u8×8][type_tag: u8]` for fixed-size keys (HardState, AppliedIndex, …)
///          `[group_id: u8×8][0x01][index: u8×8]` for LogEntry.
enum GroupKey {
    LogEntry(u64),
    Fixed(u8),
}

impl GroupKey {
    fn hard_state() -> Self {
        Self::Fixed(0x02)
    }
    fn snap_meta() -> Self {
        Self::Fixed(0x03)
    }
    fn snap_data() -> Self {
        Self::Fixed(0x04)
    }
    fn applied_index() -> Self {
        Self::Fixed(0x05)
    }
    #[allow(dead_code)]
    fn epoch() -> Self {
        Self::Fixed(0x06)
    }

    fn encode(&self) -> Vec<u8> {
        match self {
            GroupKey::LogEntry(index) => Self::encode_log_entry(*index),
            GroupKey::Fixed(tag) => vec![*tag],
        }
    }

    fn encode_log_entry(index: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(9);
        key.push(0x01);
        key.extend_from_slice(&index.to_be_bytes());
        key
    }

    fn encode_for(&self, group_id: u64) -> Vec<u8> {
        let suffix = self.encode();
        let mut key = Vec::with_capacity(8 + suffix.len());
        key.extend_from_slice(&group_id.to_be_bytes());
        key.extend_from_slice(&suffix);
        key
    }
}

enum DbOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    DeleteRange { start: Vec<u8>, end: Vec<u8> },
}
impl DbOp {
    fn from_log(id: &ShardGroupId, log: LogMutation) -> Vec<Self> {
        fn put_log_entry(group_id: u64, entry: &LogEntry) -> DbOp {
            let key = GroupKey::LogEntry(entry.index).encode_for(group_id);
            let value = borsh::to_vec(entry).expect("encode LogEntry failed");
            DbOp::Put { key, value }
        }

        fn put_hard_state(group_id: u64, term: u64, voted_for: Option<NodeId>) -> DbOp {
            let key = GroupKey::hard_state().encode_for(group_id);
            let value = borsh::to_vec(&(term, voted_for)).expect("encode HardState failed");
            DbOp::Put { key, value }
        }

        fn delete_from(group_id: u64, from_index: u64) -> DbOp {
            let start = GroupKey::LogEntry(from_index).encode_for(group_id);
            let end = GroupKey::hard_state().encode_for(group_id);
            DbOp::DeleteRange { start, end }
        }

        fn put_snapshot(group_id: u64, snapshot: RaftSnapshot) -> Vec<DbOp> {
            let meta_key = GroupKey::snap_meta().encode_for(group_id);
            let data_key = GroupKey::snap_data().encode_for(group_id);
            let applied_key = GroupKey::applied_index().encode_for(group_id);
            let compact_start = GroupKey::LogEntry(1).encode_for(group_id);
            let compact_end =
                GroupKey::LogEntry(snapshot.header.last_included_index + 1).encode_for(group_id);
            vec![
                DbOp::Put {
                    key: meta_key,
                    value: borsh::to_vec(&snapshot.header).expect("encode SnapshotMeta failed"),
                },
                DbOp::Put {
                    key: data_key,
                    value: snapshot.encoded_data().to_vec(),
                },
                DbOp::Put {
                    key: applied_key,
                    value: snapshot.header.last_included_index.to_be_bytes().to_vec(),
                },
                DbOp::DeleteRange {
                    start: compact_start,
                    end: compact_end,
                },
            ]
        }

        match log {
            LogMutation::Append(entry) => vec![put_log_entry(**id, &entry)],
            LogMutation::TruncateFrom(index) => vec![delete_from(**id, index)],
            LogMutation::HardState { term, voted_for } => {
                vec![put_hard_state(**id, term, voted_for)]
            }
            LogMutation::Snapshot(snapshot) => put_snapshot(**id, snapshot),
        }
    }
}

/// Opaque handle to the node's RocksDB instance.
/// Callers interact only through this API — `rocksdb` does not leak outside this module.
pub(crate) struct MetadataStorage {
    db: rocksdb::DB,
    sync_opts: rocksdb::WriteOptions,
}

impl MetadataStorage {
    pub(crate) fn open(path: std::path::PathBuf) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open(&opts, &path).expect("failed to open RocksDB");
        let mut sync_opts = rocksdb::WriteOptions::default();
        sync_opts.set_sync(true);
        Self { db, sync_opts }
    }

    fn write_batch(&self, operations: &[DbOp]) {
        let mut batch = rocksdb::WriteBatch::default();
        for operation in operations {
            match operation {
                DbOp::Put { key, value } => {
                    batch.put(key, value);
                }
                DbOp::DeleteRange { start, end } => {
                    batch.delete_range(start, end);
                }
            }
        }

        self.db
            .write_opt(batch, &self.sync_opts)
            .expect("failed to write batch");
    }

    fn scan_range(&self, start: &[u8], end: &[u8]) -> Vec<Vec<u8>> {
        //ReadOptions mutated per-call — set_iterate_upper_bound(end.to_vec()) changes each time. Can't reuse.
        let mut opts = rocksdb::ReadOptions::default();

        opts.set_iterate_upper_bound(end.to_vec());
        self.db
            .iterator_opt(
                rocksdb::IteratorMode::From(start, rocksdb::Direction::Forward),
                opts,
            )
            .flatten()
            .map(|(_, v)| v.to_vec())
            .collect()
    }

    fn take_persistent_state_for(&self, group_id: u64) -> RaftPersistentState {
        let Some(bytes) = self
            .db
            .get(GroupKey::hard_state().encode_for(group_id))
            .unwrap_or_default()
        else {
            return RaftPersistentState::default();
        };

        let (term, voted_for) =
            borsh::from_slice::<(u64, Option<NodeId>)>(&bytes).expect("corrupt HardState");

        let snapshot = self.load_snapshot(group_id);
        let log = self.list_log_entires(group_id);
        let first_retained = snapshot
            .as_ref()
            .map_or(1, |snapshot| snapshot.header.last_included_index + 1);

        // ! Failing fast in corruption
        for (offset, entry) in log.iter().enumerate() {
            assert_eq!(
                entry.index,
                first_retained + offset as u64,
                "non-contiguous retained Raft log"
            );
        }
        RaftPersistentState {
            term,
            voted_for,
            log,
            snapshot,
        }
    }

    fn load_snapshot(&self, group_id: u64) -> Option<RaftSnapshot> {
        // ! Err means RocksDB read failure;
        let meta_bytes = self
            .db
            .get(GroupKey::snap_meta().encode_for(group_id))
            .expect("read SnapshotMeta failed");
        let data_bytes = self
            .db
            .get(GroupKey::snap_data().encode_for(group_id))
            .expect("read SnapshotData failed");
        let applied_bytes = self
            .db
            .get(GroupKey::applied_index().encode_for(group_id))
            .expect("read AppliedIndex failed");

        let (meta_bytes, data_bytes, applied_bytes) = match (meta_bytes, data_bytes, applied_bytes)
        {
            (Some(meta), Some(data), Some(applied)) => (meta, data, applied),
            (None, None, None) => return None,
            _ => panic!("incomplete durable snapshot"),
        };
        let meta =
            borsh::from_slice::<RaftSnapshotHeader>(&meta_bytes).expect("corrupt SnapshotMeta");
        let applied_index = u64::from_be_bytes(
            applied_bytes
                .as_slice()
                .try_into()
                .expect("corrupt AppliedIndex"),
        );
        assert_eq!(
            applied_index, meta.last_included_index,
            "snapshot applied index mismatch"
        );
        Some(RaftSnapshot::from_encoded(
            meta,
            data_bytes.into_boxed_slice(),
        ))
    }

    fn list_log_entires(&self, group_id: u64) -> Vec<LogEntry> {
        let start = GroupKey::LogEntry(0).encode_for(group_id);
        let end = GroupKey::hard_state().encode_for(group_id);
        self.scan_range(&start, &end)
            .into_iter()
            .map(|bytes| {
                borsh::from_slice::<LogEntry>(&bytes).expect("corrupt LogEntry in RocksDB")
            })
            .collect()
    }

    fn delete_range(&self, group_id: ShardGroupId) {
        let start: &[u8] = &group_id.to_be_bytes();
        let end: &[u8] = &(*group_id + 1).to_be_bytes();

        let mut batch = rocksdb::WriteBatch::default();
        batch.delete_range(start, end);
        self.db
            .write_opt(batch, &self.sync_opts)
            .expect("failed to delete range");
    }
}

impl RaftStorage for MetadataStorage {
    fn load_state(&self, group_id: u64) -> RaftPersistentState {
        self.take_persistent_state_for(group_id)
    }

    fn persist_mutations(&self, mutations: Box<[(ShardGroupId, LogMutation)]>) {
        let batch: Box<[DbOp]> = mutations
            .into_iter()
            .flat_map(|(id, log)| DbOp::from_log(&id, log))
            .collect();
        self.write_batch(&batch);
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    fn delete_group(&self, group_id: ShardGroupId) {
        self.delete_range(group_id);
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }
}

#[cfg(any(test, debug_assertions))]
impl TAssertInvariant for MetadataStorage {
    fn assert_invariants(&self) {
        let mut prev_key: Option<Vec<u8>> = None;
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, _) = item.expect("corrupt RocksDB iterator");
            let key = key.to_vec();

            // Keys must be in strictly ascending order (RocksDB guarantee,
            // but validates our encoding doesn't produce duplicates)
            if let Some(ref prev) = prev_key {
                assert!(
                    key > *prev,
                    "keys not in ascending order: {:?} >= {:?}",
                    key,
                    prev,
                );
            }

            // Every key must be at least 9 bytes (8-byte group_id + 1-byte type tag)
            assert!(
                key.len() >= 9,
                "key too short ({} bytes): {:?}",
                key.len(),
                key,
            );

            let type_tag = key[8];
            assert!(
                (0x01..=0x06).contains(&type_tag),
                "unknown type tag 0x{:02x} in key {:?}",
                type_tag,
                key,
            );

            // LogEntry keys must be exactly 17 bytes (8 group + 1 tag + 8 index)
            // and the index portion must be >= 1 (invariant 5: log index is 1-based;
            // index 0 is never stored).
            if type_tag == 0x01 {
                assert_eq!(
                    key.len(),
                    17,
                    "LogEntry key must be 17 bytes, got {}",
                    key.len(),
                );
                let index = u64::from_be_bytes(key[9..17].try_into().unwrap());
                assert!(
                    index >= 1,
                    "LogEntry index must be >= 1 (1-based), got {}",
                    index,
                );
            }

            prev_key = Some(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::consensus::raft::command::RaftCommand;
    use crate::control_plane::consensus::raft::states::metadata_state::MetadataState;
    use crate::control_plane::consensus::raft::storage::SnapshotData;

    fn temp_db() -> (MetadataStorage, std::path::PathBuf) {
        let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let db = MetadataStorage::open(path.clone());
        (db, path)
    }

    fn noop_entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            command: RaftCommand::Noop,
        }
    }

    #[test]
    fn key_encoding_preserves_sort_order() {
        let g1_log1 = GroupKey::LogEntry(1).encode_for(1);
        let g1_log2 = GroupKey::LogEntry(2).encode_for(1);
        let g1_hard = GroupKey::hard_state().encode_for(1);
        let g2_log1 = GroupKey::LogEntry(1).encode_for(2);

        assert!(g1_log1 < g1_log2, "log entries must sort by index");
        assert!(g1_log2 < g1_hard, "log entries must sort before HardState");
        assert!(g1_hard < g2_log1, "group 1 keys must sort before group 2");
    }

    #[test]
    fn group_isolation_no_cross_leakage() {
        let (db, _path) = temp_db();
        let g1 = ShardGroupId(1);
        let g2 = ShardGroupId(2);

        db.persist_mutations(Box::new([
            (g1, LogMutation::Append(noop_entry(1, 1))),
            (
                g1,
                LogMutation::HardState {
                    term: 1,
                    voted_for: Some(NodeId::new("n1")),
                },
            ),
            (g2, LogMutation::Append(noop_entry(1, 1))),
            (g2, LogMutation::Append(noop_entry(2, 1))),
            (
                g2,
                LogMutation::HardState {
                    term: 1,
                    voted_for: Some(NodeId::new("n2")),
                },
            ),
        ]));

        let state1 = db.load_state(1);
        assert_eq!(state1.log.len(), 1, "group 1 must have exactly 1 entry");
        assert_eq!(state1.term, 1);

        let state2 = db.load_state(2);
        assert_eq!(state2.log.len(), 2, "group 2 must have exactly 2 entries");

        db.delete_group(g1);

        let state1_after = db.load_state(1);
        assert_eq!(
            state1_after.log.len(),
            0,
            "group 1 must be empty after delete"
        );
        assert_eq!(state1_after.term, 0);

        let state2_after = db.load_state(2);
        assert_eq!(state2_after.log.len(), 2, "group 2 must be unaffected");
        assert_eq!(state2_after.term, 1);
    }

    #[test]
    fn truncation_preserves_hard_state() {
        let (db, _path) = temp_db();
        let g = ShardGroupId(1);

        db.persist_mutations(Box::new([
            (g, LogMutation::Append(noop_entry(1, 5))),
            (g, LogMutation::Append(noop_entry(2, 5))),
            (g, LogMutation::Append(noop_entry(3, 5))),
            (
                g,
                LogMutation::HardState {
                    term: 5,
                    voted_for: Some(NodeId::new("n1")),
                },
            ),
        ]));

        let before = db.load_state(1);
        assert_eq!(before.log.len(), 3);
        assert_eq!(before.term, 5);
        assert_eq!(before.voted_for, Some(NodeId::new("n1")));

        db.persist_mutations(Box::new([(g, LogMutation::TruncateFrom(2))]));

        let after = db.load_state(1);
        assert_eq!(after.log.len(), 1, "only entry 1 should remain");
        assert_eq!(after.log[0].index, 1);
        assert_eq!(after.term, 5, "HardState term must survive truncation");
        assert_eq!(
            after.voted_for,
            Some(NodeId::new("n1")),
            "HardState voted_for must survive truncation"
        );
    }

    #[test]
    fn snapshot_persistence_atomically_compacts_covered_prefix() {
        let (db, _path) = temp_db();
        let group = ShardGroupId(7);
        db.persist_mutations(Box::new([
            (group, LogMutation::Append(noop_entry(1, 1))),
            (group, LogMutation::Append(noop_entry(2, 1))),
            (group, LogMutation::Append(noop_entry(3, 2))),
            (
                group,
                LogMutation::HardState {
                    term: 2,
                    voted_for: None,
                },
            ),
        ]));
        let snapshot = RaftSnapshot::new(
            2,
            1,
            SnapshotData {
                metadata: MetadataState::new(group).snapshot(),
                peers: vec![NodeId::new("n1"), NodeId::new("n2")].into_boxed_slice(),
            },
        );
        db.persist_mutations(Box::new([(group, LogMutation::Snapshot(snapshot.clone()))]));

        let restored = db.load_state(group.0);
        assert_eq!(restored.snapshot, Some(snapshot));
        assert_eq!(restored.log, vec![noop_entry(3, 2)]);
        assert_eq!(restored.stabled_index(), 3);
    }

    #[test]
    #[should_panic(expected = "snapshot checksum mismatch")]
    fn corrupt_snapshot_fails_recovery_explicitly() {
        let (db, _path) = temp_db();
        let group = ShardGroupId(8);
        let snapshot = RaftSnapshot::new(
            1,
            1,
            SnapshotData {
                metadata: MetadataState::new(group).snapshot(),
                peers: Box::new([]),
            },
        );
        let snapshot_size = snapshot.header.size_bytes as usize;
        db.persist_mutations(Box::new([
            (
                group,
                LogMutation::HardState {
                    term: 1,
                    voted_for: None,
                },
            ),
            (group, LogMutation::Snapshot(snapshot)),
        ]));
        db.write_batch(&[DbOp::Put {
            key: GroupKey::snap_data().encode_for(group.0),
            value: vec![0xff; snapshot_size],
        }]);

        let _ = db.load_state(group.0);
    }
}
