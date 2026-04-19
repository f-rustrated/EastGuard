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
/// Metadata keys (0x02–0x06) sort strictly before all log entries (0x01…).
/// delete_range on 0x01..0x01+MAX safely compacts only log entries.
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
    #[allow(dead_code)]
    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            ShardCfKey::LogEntry(index) => {
                let mut key = Vec::with_capacity(9);
                key.push(0x01);
                key.extend_from_slice(&index.to_be_bytes());
                key
            }
            ShardCfKey::HardState    => vec![0x02],
            ShardCfKey::SnapMeta     => vec![0x03],
            ShardCfKey::SnapData     => vec![0x04],
            ShardCfKey::AppliedIndex => vec![0x05],
            ShardCfKey::Epoch        => vec![0x06],
        }
    }
}

pub(crate) fn shard_cf_name(id: u64) -> String {
    format!("{CF_SHARD_PREFIX}{id:016x}")
}

/// Opaque handle to the node's RocksDB instance.
/// Callers interact only through this API — `rocksdb` does not leak outside this module.
pub(crate) struct RaftDb {
    db: rocksdb::DB
}

impl RaftDb {
    pub(crate) fn open(path: std::path::PathBuf) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_names = rocksdb::DB::list_cf(&opts, &path).unwrap_or_default();
        let db = rocksdb::DB::open_cf(&opts, &path, &cf_names)
            .expect("failed to open RocksDB");
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
}
