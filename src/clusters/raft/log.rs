#![allow(dead_code)]

use crate::clusters::raft::messages::LogEntry;

/// In-memory log store. Will be replaced with RocksDB-backed storage later.
#[derive(Debug, Default)]
pub struct MemLog {
    entries: Vec<LogEntry>,
}

impl MemLog {
    pub fn last_index(&self) -> u64 {
        self.entries.last().map_or(0, |e| e.index)
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().map_or(0, |e| e.term)
    }

    pub fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        self.get(index).map_or(0, |e| e.term)
    }

    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    pub fn entries_from(&self, start_index: u64) -> &[LogEntry] {
        if start_index == 0 || start_index > self.last_index() {
            return &[];
        }
        &self.entries[(start_index - 1) as usize..]
    }

    pub fn append(&mut self, entry: LogEntry) {
        debug_assert_eq!(entry.index, self.last_index() + 1);
        self.entries.push(entry);
    }

    pub fn truncate_from(&mut self, from_index: u64) {
        if from_index == 0 || from_index > self.last_index() {
            return;
        }
        self.entries.truncate((from_index - 1) as usize);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clusters::raft::messages::RaftCommand;

    fn entry(term: u64, index: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            command: RaftCommand::Noop,
        }
    }

    #[test]
    fn empty_log_defaults() {
        let log = MemLog::default();
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
        let mut log = MemLog::default();
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
        let mut log = MemLog::default();
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
        let mut log = MemLog::default();
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
        let mut log = MemLog::default();
        log.append(entry(1, 1));
        log.truncate_from(5);
        assert_eq!(log.last_index(), 1);
    }
}
