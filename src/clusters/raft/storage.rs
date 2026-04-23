use crate::clusters::raft::log::LogEntry;
use crate::clusters::{BINCODE_CONFIG, NodeId};
use crate::storage::{Db, DbOp, ShardCfKey};

pub(crate) fn put_log_entry(cf: &str, entry: &LogEntry) -> DbOp {
    let key = ShardCfKey::LogEntry(entry.index).encode();
    let value = bincode::encode_to_vec(entry, BINCODE_CONFIG).expect("encode LogEntry failed");
    DbOp::Put {
        cf: cf.to_owned(),
        key,
        value,
    }
}

pub(crate) fn put_hard_state(cf: &str, term: u64, voted_for: Option<NodeId>) -> DbOp {
    let key = ShardCfKey::HardState.encode();
    let value = bincode::encode_to_vec(&(term, voted_for), BINCODE_CONFIG)
        .expect("encode HardState failed");
    DbOp::Put {
        cf: cf.to_owned(),
        key,
        value,
    }
}

pub(crate) fn get_hard_state(db: &Db, cf: &str) -> Option<(u64, Option<NodeId>)> {
    let bytes = db.get_value(cf, &ShardCfKey::HardState.encode())?;
    let (hard_state, _) =
        bincode::decode_from_slice(&bytes, BINCODE_CONFIG).expect("corrupt HardState");
    Some(hard_state)
}

pub(crate) fn delete_from(cf: &str, from_index: u64) -> DbOp {
    let index = ShardCfKey::LogEntry(from_index).encode();
    DbOp::DeleteFrom {
        cf: cf.to_owned(),
        index,
    }
}
