#![allow(dead_code)]

use crate::clusters::raft::interface::LogStore;
use crate::clusters::raft::messages::RaftCommand;

/// Bytes reserved for the command discriminant.
pub(crate) const COMMAND_LEN: usize = 1;
/// Bytes reserved for the term (u64 little-endian).
pub(crate) const TERM_LEN: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: RaftCommand,
}

#[derive(Debug)]
pub struct RaftLog<S: LogStore> {
    store: S,
}

impl<S: LogStore> RaftLog<S> {
    pub fn with_store(store: S) -> Self {
        Self { store }
    }

    pub fn last_index(&self) -> u64 {
        self.store.get_last().ok().flatten().map_or(0, |e| e.index)
    }

    pub fn last_term(&self) -> u64 {
        self.store.get_last().ok().flatten().map_or(0, |e| e.term)
    }

    pub fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        self.store.get(index).ok().flatten().map_or(0, |e| e.term)
    }

    pub fn get(&self, index: u64) -> Option<LogEntry> {
        // TODO: propagate error
        self.store.get(index).ok()?
    }

    pub fn entries_from(&self, start_index: u64) -> Vec<LogEntry> {
        let last = self.last_index();
        if start_index == 0 || start_index > last {
            return vec![];
        }
        // TODO: propagate error
        self.store.get_range(start_index, last).unwrap_or_default()
    }

    pub fn append(&mut self, entry: LogEntry) {
        debug_assert_eq!(
            entry.index,
            self.last_index() + 1,
            "log entries must be appended in order"
        );
        // TODO: propagate error
        let _ = self.store.append_log(entry);
    }

    pub fn truncate_from(&mut self, from_index: u64) {
        // TODO: propagate error
        let _ = self.store.truncate_log(from_index);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryLogStore;

    fn entry(term: u64, index: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            command: RaftCommand::Noop,
        }
    }

    #[test]
    fn empty_log_defaults() {
        let log = RaftLog::with_store(MemoryLogStore::default());
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert_eq!(log.term_at(0), 0);
        assert_eq!(log.term_at(1), 0);
        assert!(log.get(0).is_none());
        assert!(log.get(1).is_none());
        assert!(log.entries_from(1).is_empty());
    }

    #[test]
    fn append_and_get() {
        let mut log = RaftLog::with_store(MemoryLogStore::default());
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        log.append(entry(2, 3));

        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.term_at(1), 1);
        assert_eq!(log.term_at(2), 1);
        assert_eq!(log.term_at(3), 2);
        assert_eq!(log.get(1).unwrap().term, 1);
        assert_eq!(log.get(3).unwrap().term, 2);
    }

    #[test]
    fn entries_from_returns_slice() {
        let mut log = RaftLog::with_store(MemoryLogStore::default());
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        log.append(entry(2, 3));

        let slice = log.entries_from(2);
        assert_eq!(slice.len(), 2);
        assert_eq!(slice[0].index, 2);
        assert_eq!(slice[1].index, 3);
    }

    #[test]
    fn truncate_from_removes_tail() {
        let mut log = RaftLog::with_store(MemoryLogStore::default());
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        log.append(entry(2, 3));

        log.truncate_from(2);
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.last_term(), 1);
        assert!(log.get(2).is_none());
    }

    #[test]
    fn truncate_from_beyond_end_is_noop() {
        let mut log = RaftLog::with_store(MemoryLogStore::default());
        log.append(entry(1, 1));
        log.truncate_from(5);
        assert_eq!(log.last_index(), 1);
    }
}
