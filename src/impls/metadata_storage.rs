use crate::clusters::{
    BINCODE_CONFIG, NodeId,
    raft::{
        log::LogEntry,
        messages::LogMutation,
        storage::{RaftPersistentState, RaftStorage},
    },
    swims::ShardGroupId,
};

/// Key encoder for per-group entries in the default column family.
/// Layout: `[group_id: u8×8][type_tag: u8]` for fixed-size keys (HardState, AppliedIndex, …)
///          `[group_id: u8×8][0x01][index: u8×8]` for LogEntry.
#[allow(dead_code)]
enum GroupKey {
    LogEntry(u64),
    HardState,
    SnapMeta,
    SnapData,
    AppliedIndex,
    Epoch,
}

impl GroupKey {
    fn encode(&self) -> Vec<u8> {
        match self {
            GroupKey::LogEntry(index) => {
                let mut key = Vec::with_capacity(9);
                key.push(0x01);
                key.extend_from_slice(&index.to_be_bytes());
                key
            }
            GroupKey::HardState => vec![0x02],
            GroupKey::SnapMeta => vec![0x03],
            GroupKey::SnapData => vec![0x04],
            GroupKey::AppliedIndex => vec![0x05],
            GroupKey::Epoch => vec![0x06],
        }
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
    fn from_log(id: &ShardGroupId, log: LogMutation) -> Self {
        fn put_log_entry(group_id: u64, entry: &LogEntry) -> DbOp {
            let key = GroupKey::LogEntry(entry.index).encode_for(group_id);
            let value =
                bincode::encode_to_vec(entry, BINCODE_CONFIG).expect("encode LogEntry failed");
            DbOp::Put { key, value }
        }

        fn put_hard_state(group_id: u64, term: u64, voted_for: Option<NodeId>) -> DbOp {
            let key = GroupKey::HardState.encode_for(group_id);
            let value = bincode::encode_to_vec(&(term, voted_for), BINCODE_CONFIG)
                .expect("encode HardState failed");
            DbOp::Put { key, value }
        }

        fn delete_from(group_id: u64, from_index: u64) -> DbOp {
            let start = GroupKey::LogEntry(from_index).encode_for(group_id);
            let end = GroupKey::HardState.encode_for(group_id);
            DbOp::DeleteRange { start, end }
        }

        match log {
            LogMutation::Append(entry) => put_log_entry(id.0, &entry),
            LogMutation::TruncateFrom(index) => delete_from(id.0, index),
            LogMutation::HardState { term, voted_for } => put_hard_state(id.0, term, voted_for),
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
            .get(GroupKey::HardState.encode_for(group_id))
            .unwrap_or_default()
        else {
            return RaftPersistentState::default();
        };

        let ((term, voted_for), _) =
            bincode::decode_from_slice::<(u64, Option<NodeId>), _>(&bytes, BINCODE_CONFIG)
                .expect("corrupt HardState");

        RaftPersistentState {
            term,
            voted_for,
            log: self.list_log_entires(group_id),
        }
    }

    fn list_log_entires(&self, group_id: u64) -> Vec<LogEntry> {
        let start = GroupKey::LogEntry(0).encode_for(group_id);
        let end = GroupKey::HardState.encode_for(group_id);
        self.scan_range(&start, &end)
            .into_iter()
            .map(|bytes| {
                let (entry, _) = bincode::decode_from_slice::<LogEntry, _>(&bytes, BINCODE_CONFIG)
                    .expect("corrupt LogEntry in RocksDB");
                entry
            })
            .collect()
    }

    fn delete_range(&self, group_id: ShardGroupId) {
        let start: &[u8] = &group_id.0.to_be_bytes();
        let end: &[u8] = &(group_id.0 + 1).to_be_bytes();

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

    fn persist_mutations(&self, mutations: Vec<(ShardGroupId, LogMutation)>) {
        let batch: Vec<DbOp> = mutations
            .into_iter()
            .map(|(id, log)| DbOp::from_log(&id, log))
            .collect();
        self.write_batch(&batch);
        #[cfg(test)]
        self.assert_invariants();
    }

    fn delete_group(&self, group_id: ShardGroupId) {
        self.delete_range(group_id);
        #[cfg(test)]
        self.assert_invariants();
    }

    #[cfg(test)]
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
            if type_tag == 0x01 {
                assert_eq!(
                    key.len(),
                    17,
                    "LogEntry key must be 17 bytes, got {}",
                    key.len(),
                );
            }

            prev_key = Some(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> (MetadataStorage, std::path::PathBuf) {
        let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let db = MetadataStorage::open(path.clone());
        (db, path)
    }

    fn noop_entry(index: u64, term: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            command: crate::clusters::raft::messages::RaftCommand::Noop,
        }
    }

    #[test]
    fn key_encoding_preserves_sort_order() {
        let g1_log1 = GroupKey::LogEntry(1).encode_for(1);
        let g1_log2 = GroupKey::LogEntry(2).encode_for(1);
        let g1_hard = GroupKey::HardState.encode_for(1);
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

        db.persist_mutations(vec![
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
        ]);

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

        db.persist_mutations(vec![
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
        ]);

        let before = db.load_state(1);
        assert_eq!(before.log.len(), 3);
        assert_eq!(before.term, 5);
        assert_eq!(before.voted_for, Some(NodeId::new("n1")));

        db.persist_mutations(vec![(g, LogMutation::TruncateFrom(2))]);

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
}
