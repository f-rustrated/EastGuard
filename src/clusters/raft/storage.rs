use crate::clusters::{NodeId, BINCODE_CONFIG};
use crate::clusters::raft::log::LogEntry;
use crate::storage::{DbOp, ShardCfKey};

impl DbOp {
    pub(crate) fn put_log_entry(cf: &str, entry: &LogEntry) -> Self {
        let key = ShardCfKey::LogEntry(entry.index).encode();
        let value = bincode::encode_to_vec(entry, BINCODE_CONFIG).expect("encode LogEntry failed");
        DbOp::Put { cf: cf.to_owned(), key, value }
    }

    pub(crate) fn put_hard_state(cf: &str, term: u64, voted_for: Option<NodeId>) -> Self {
        let key = ShardCfKey::HardState.encode();
        let value = bincode::encode_to_vec(&(term, voted_for), BINCODE_CONFIG)
            .expect("encode HardState failed");
        DbOp::Put { cf: cf.to_owned(), key, value }
    }

    pub(crate) fn delete_from(cf: &str, from_index: u64) -> Self {
        let index = ShardCfKey::LogEntry(from_index).encode();
        DbOp::DeleteFrom { cf: cf.to_owned(), index }
    }
}