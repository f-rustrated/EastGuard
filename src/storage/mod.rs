#[allow(dead_code)]
pub(crate) const CF_META: &str = "meta";
pub(crate) const CF_SHARD_PREFIX: &str = "shard-";

/// Per-shard column family key space.
///
/// Layout (one CF per shard group):
///   0x01 + u64 BE  →  LogEntry        (9 bytes; lexicographic = numeric order)
///   0x02           →  HardState       (1 byte)
///   0x03           →  SnapMeta        (1 byte)
///   0x04           →  SnapData        (1 byte)
///   0x05           →  AppliedIndex    (1 byte)
///   0x06           →  Epoch           (1 byte)
///
/// Log entries (0x01…) sort strictly before metadata keys (0x02–0x06).
/// delete_range on 0x01..0x02 safely compacts only log entries.
#[allow(dead_code)]
pub(crate) enum ShardCfKey {
    /// Raft log entry at the given 1-based index.
    LogEntry(u64),
    /// Durable Raft state (current_term, voted_for) — must survive restarts.
    HardState,
    /// Snapshot descriptor (last_included_index/term, peers, epoch).
    /// Leader writes on checkpoint; follower writes on InstallSnapshot RPC arrival.
    /// Survives crashes mid-transfer — missing SnapData means transfer must restart.
    SnapMeta,
    /// Full state machine payload (topics, ranges, segments) — only sent to followers too far behind for log replication.
    /// Written after full out-of-band stream is received and checksum-verified.
    SnapData,
    /// Highest log index applied to the state machine — determines replay boundary on restart.
    AppliedIndex,
    /// Shard membership + topology version — incremented on AddPeer/RemovePeer to detect stale routing.
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
}

const LOG_ENTRY_RANGE_END: &[u8] = &[0x02];

pub(crate) fn shard_cf_name(id: u64) -> String {
    format!("{CF_SHARD_PREFIX}{id:016x}")
}

pub(crate) enum DbOp {
    Put {
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    DeleteFrom {
        cf: String,
        index: Vec<u8>,
    },
}

/// Opaque handle to the node's RocksDB instance.
/// Callers interact only through this API — `rocksdb` does not leak outside this module.
pub(crate) struct Db {
    db: rocksdb::DB,
}

impl Db {
    pub(crate) fn open(path: std::path::PathBuf) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_names = rocksdb::DB::list_cf(&opts, &path).unwrap_or_default();
        let db = rocksdb::DB::open_cf(&opts, &path, &cf_names).expect("failed to open RocksDB");
        Self { db }
    }

    pub(crate) fn create_cf(&mut self, name: &str) {
        self.db
            .create_cf(name, &rocksdb::Options::default())
            .expect("failed to create column family");
    }

    pub(crate) fn drop_cf(&mut self, name: &str) {
        let _ = self.db.drop_cf(name);
    }

    pub(crate) fn has_cf(&self, name: &str) -> bool {
        self.db.cf_handle(name).is_some()
    }

    pub(crate) fn get_value(&self, cf_name: &str, key: &[u8]) -> Option<Vec<u8>> {
        let cf = self.db.cf_handle(cf_name)?;
        self.db.get_cf(cf, key).unwrap_or_else(|e| {
            tracing::error!("Error retrieving: {}", e);
            None
        })
    }

    pub(crate) fn write_batch(&self, operations: &[DbOp]) {
        let mut batch = rocksdb::WriteBatch::default();
        for operation in operations {
            match operation {
                DbOp::Put { cf, key, value } => {
                    let cf = self
                        .db
                        .cf_handle(cf)
                        .unwrap_or_else(|| panic!("CF {cf} not found"));
                    batch.put_cf(cf, key, value)
                }
                DbOp::DeleteFrom { cf, index: start } => {
                    let cf = self
                        .db
                        .cf_handle(cf)
                        .unwrap_or_else(|| panic!("CF {cf} not found"));
                    batch.delete_range_cf(&cf, start.as_slice(), LOG_ENTRY_RANGE_END);
                }
            }
        }

        // ! SAFETY: without the following sync option, Raft's safety is not guaranteed
        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(true);
        self.db
            .write_opt(batch, &write_opts)
            .expect("failed to write batch");
    }

    pub(crate) fn scan_range(&self, cf_name: &str, start: &[u8], end: &[u8]) -> Vec<Vec<u8>> {
        let Some(cf) = self.db.cf_handle(cf_name) else {
            return vec![];
        };
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_iterate_upper_bound(end.to_vec());
        self.db
            .iterator_cf_opt(
                &cf,
                opts,
                rocksdb::IteratorMode::From(start, rocksdb::Direction::Forward),
            )
            .flatten()
            .map(|(_, v)| v.to_vec())
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn get_cf(&self, cf_name: &str, key: &[u8]) -> Option<Vec<u8>> {
        let cf = self.db.cf_handle(cf_name)?;
        self.db.get_cf(cf, key).ok().flatten()
    }
}
