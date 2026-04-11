#![allow(dead_code)]

use crate::raft::messages::{LogEntry, RaftCommand};
use crate::storage::{Entry, MemEngine, StorageEngine};

// ---------------------------------------------------------------------------
// Serialization — lives here because only the Raft layer knows the layout
//
// Entry.data layout: [ command discriminant (1 byte) | term: u64 LE (8 bytes) ]
// ---------------------------------------------------------------------------

fn serialize_command(cmd: &RaftCommand) -> Vec<u8> {
    match cmd {
        RaftCommand::Noop => vec![0x00],
    }
}

fn deserialize_command(data: &[u8]) -> RaftCommand {
    match data.first() {
        Some(0x00) | None => RaftCommand::Noop,
        _ => RaftCommand::Noop,
    }
}

fn to_entry(entry: &LogEntry) -> Entry {
    let mut data = Vec::with_capacity(9);
    data.extend_from_slice(&serialize_command(&entry.command));
    data.extend_from_slice(&entry.term.to_le_bytes());
    Entry { index: entry.index, data }
}

fn from_entry(entry: Entry) -> LogEntry {
    let command = deserialize_command(&entry.data[0..1]);
    let term = u64::from_le_bytes(entry.data[1..9].try_into().unwrap());
    LogEntry { term, index: entry.index, command }
}

fn deserialize_term(data: &[u8]) -> u64 {
    u64::from_le_bytes(data[1..9].try_into().unwrap())
}

// ---------------------------------------------------------------------------
// RaftLog — the struct Raft interacts with.
// Backed by MemEngine for Phase 1; will use DiskEngine in Phase 2.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct RaftLog {
    engine: MemEngine,
}

impl RaftLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn last_index(&self) -> u64 {
        self.engine
            .get_last()
            .ok()
            .flatten()
            .map_or(0, |e| e.index)
    }

    pub fn last_term(&self) -> u64 {
        self.engine
            .get_last()
            .ok()
            .flatten()
            .map_or(0, |e| deserialize_term(&e.data))
    }

    pub fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        self.engine
            .get(index)
            .ok()
            .flatten()
            .map_or(0, |e| deserialize_term(&e.data))
    }

    pub fn get(&self, index: u64) -> Option<LogEntry> {
        self.engine.get(index).ok()?.map(from_entry)
    }

    pub fn entries_from(&self, start_index: u64) -> Vec<LogEntry> {
        let last = self.last_index();
        if start_index == 0 || start_index > last {
            return vec![];
        }
        self.engine
            .get_range(start_index, last)
            .unwrap_or_default()
            .into_iter()
            .map(from_entry)
            .collect()
    }

    pub fn append(&mut self, entry: LogEntry) {
        let _ = self.engine.append_log(to_entry(&entry));
    }

    pub fn truncate_from(&mut self, from_index: u64) {
        let _ = self.engine.truncate_log(from_index);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(term: u64, index: u64) -> LogEntry {
        LogEntry { term, index, command: RaftCommand::Noop }
    }

    #[test]
    fn empty_log_defaults() {
        let log = RaftLog::new();
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
        let mut log = RaftLog::new();
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
        let mut log = RaftLog::new();
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
        let mut log = RaftLog::new();
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
        let mut log = RaftLog::new();
        log.append(entry(1, 1));
        log.truncate_from(5);
        assert_eq!(log.last_index(), 1);
    }
}
