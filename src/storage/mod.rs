use crate::clusters::{
    BINCODE_CONFIG, NodeId,
    raft::{
        log::LogEntry,
        messages::LogMutation,
        storage::{RaftPersistentState, RaftStorage},
    },
    swims::ShardGroupId,
};

/// Per-shard key space within the default column family.
/// read storage-key-layout.md for more information
#[allow(dead_code)]
pub(crate) enum ShardCfKey {
    LogEntry(u64),
    HardState,
    SnapMeta,
    SnapData,
    AppliedIndex,
    Epoch,
}

impl ShardCfKey {
    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            ShardCfKey::LogEntry(index) => {
                let mut key = Vec::with_capacity(9);
                key.push(0x01);
                key.extend_from_slice(&index.to_be_bytes());
                key
            }
            ShardCfKey::HardState => vec![0x02],
            ShardCfKey::SnapMeta => vec![0x03],
            ShardCfKey::SnapData => vec![0x04],
            ShardCfKey::AppliedIndex => vec![0x05],
            ShardCfKey::Epoch => vec![0x06],
        }
    }

    pub(crate) fn encode_for(&self, group_id: u64) -> Vec<u8> {
        let suffix = self.encode();
        let mut key = Vec::with_capacity(8 + suffix.len());
        key.extend_from_slice(&group_id.to_be_bytes());
        key.extend_from_slice(&suffix);
        key
    }
}

pub(crate) enum DbOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    DeleteRange { start: Vec<u8>, end: Vec<u8> },
}
impl DbOp {
    pub(crate) fn from_log(id: &ShardGroupId, log: LogMutation) -> Self {
        fn put_log_entry(group_id: u64, entry: &LogEntry) -> DbOp {
            let key = ShardCfKey::LogEntry(entry.index).encode_for(group_id);
            let value =
                bincode::encode_to_vec(entry, BINCODE_CONFIG).expect("encode LogEntry failed");
            DbOp::Put { key, value }
        }

        fn put_hard_state(group_id: u64, term: u64, voted_for: Option<NodeId>) -> DbOp {
            let key = ShardCfKey::HardState.encode_for(group_id);
            let value = bincode::encode_to_vec(&(term, voted_for), BINCODE_CONFIG)
                .expect("encode HardState failed");
            DbOp::Put { key, value }
        }

        fn delete_from(group_id: u64, from_index: u64) -> DbOp {
            let start = ShardCfKey::LogEntry(from_index).encode_for(group_id);
            let end = ShardCfKey::HardState.encode_for(group_id);
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
pub(crate) struct Db(rocksdb::DB);

impl Db {
    pub(crate) fn open(path: std::path::PathBuf) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = rocksdb::DB::open(&opts, &path).expect("failed to open RocksDB");
        Self(db)
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

        // Raft safety requires WAL sync before acknowledging writes
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(true);
        self.0
            .write_opt(batch, &write_opts)
            .expect("failed to write batch");
    }

    fn scan_range(&self, start: &[u8], end: &[u8]) -> Vec<Vec<u8>> {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_iterate_upper_bound(end.to_vec());
        self.0
            .iterator_opt(
                rocksdb::IteratorMode::From(start, rocksdb::Direction::Forward),
                opts,
            )
            .flatten()
            .map(|(_, v)| v.to_vec())
            .collect()
    }

    pub(crate) fn take_persistent_state_for(&self, group_id: u64) -> RaftPersistentState {
        let Some(bytes) = self
            .0
            .get(ShardCfKey::HardState.encode_for(group_id))
            .unwrap_or_else(|e| {
                tracing::error!("RocksDB get error: {}", e);
                None
            })
        else {
            return RaftPersistentState::default();
        };

        let ((term, voted_for), _) =
            bincode::decode_from_slice::<(u64, Option<NodeId>), _>(&bytes, BINCODE_CONFIG)
                .expect("corrupt HardState");

        let log = self.get_log_entries(group_id);
        RaftPersistentState {
            term,
            voted_for,
            log,
        }
    }

    fn get_log_entries(&self, group_id: u64) -> Vec<LogEntry> {
        let start = ShardCfKey::LogEntry(0).encode_for(group_id);
        let end = ShardCfKey::HardState.encode_for(group_id);
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
        self.0.write(batch).expect("failed to delete range");
    }
}

impl RaftStorage for Db {
    fn load_state(&self, group_id: u64) -> RaftPersistentState {
        self.take_persistent_state_for(group_id)
    }

    fn persist_mutations(&self, mutations: Vec<(ShardGroupId, LogMutation)>) {
        let batch: Vec<DbOp> = mutations
            .into_iter()
            .map(|(id, log)| DbOp::from_log(&id, log))
            .collect();
        self.write_batch(&batch);
    }

    fn delete_group(&self, group_id: ShardGroupId) {
        self.delete_range(group_id);
    }
}
