#![allow(dead_code)]

use crate::raft::messages::{LogEntry, RaftCommand};
use crate::storage::{Entry, LogStore, MemoryLogStore, StorageError};

// ---------------------------------------------------------------------------
// Serialization — lives here because only the Raft layer knows the layout
//
// Entry.data layout: [ command: COMMAND_LEN bytes | term: TERM_LEN bytes (u64 LE) ]
// ---------------------------------------------------------------------------

/// Bytes reserved for the command discriminant.
const COMMAND_LEN: usize = 1;
/// Bytes reserved for the term (u64 little-endian).
const TERM_LEN: usize = 8;

fn serialize_command(cmd: &RaftCommand) -> Vec<u8> {
    match cmd {
        RaftCommand::Noop => vec![0x00],
    }
}

fn deserialize_command(data: &[u8]) -> Result<RaftCommand, StorageError> {
    match data.first() {
        Some(0x00) | None => Ok(RaftCommand::Noop),
        Some(&b) => Err(StorageError::Corruption {
            reason: format!("unknown command discriminant: 0x{b:02x}"),
        }),
    }
}

fn to_entry(entry: &LogEntry) -> Entry {
    let cmd_bytes = serialize_command(&entry.command);
    debug_assert_eq!(cmd_bytes.len(), COMMAND_LEN, "serialize_command must produce exactly COMMAND_LEN bytes");
    let mut data = Vec::with_capacity(COMMAND_LEN + TERM_LEN);
    data.extend_from_slice(&cmd_bytes);
    data.extend_from_slice(&entry.term.to_le_bytes());
    Entry { index: entry.index, data }
}

fn from_entry(entry: Entry) -> Result<LogEntry, StorageError> {
    if entry.data.len() < COMMAND_LEN + TERM_LEN {
        return Err(StorageError::Corruption {
            reason: format!(
                "entry {} payload too short: expected >= {} bytes, got {}",
                entry.index,
                COMMAND_LEN + TERM_LEN,
                entry.data.len()
            ),
        });
    }
    let command = deserialize_command(&entry.data[..COMMAND_LEN])?;
    let term = u64::from_le_bytes(
        entry.data[COMMAND_LEN..COMMAND_LEN + TERM_LEN].try_into().unwrap(),
    );
    Ok(LogEntry { term, index: entry.index, command })
}

fn deserialize_term(data: &[u8]) -> u64 {
    debug_assert!(data.len() >= COMMAND_LEN + TERM_LEN, "entry data too short to deserialize term");
    u64::from_le_bytes(data[COMMAND_LEN..COMMAND_LEN + TERM_LEN].try_into().unwrap())
}

// TODO: use FileLogStore
#[derive(Debug, Default)]
pub struct RaftLog {
    store: MemoryLogStore,
}

impl RaftLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn last_index(&self) -> u64 {
        self.store
            .get_last()
            .ok()
            .flatten()
            .map_or(0, |e| e.index)
    }

    pub fn last_term(&self) -> u64 {
        self.store
            .get_last()
            .ok()
            .flatten()
            .map_or(0, |e| deserialize_term(&e.data))
    }

    pub fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        self.store
            .get(index)
            .ok()
            .flatten()
            .map_or(0, |e| deserialize_term(&e.data))
    }

    pub fn get(&self, index: u64) -> Option<LogEntry> {
        // TODO: propagate corruption error
        self.store.get(index).ok()?.map(from_entry).transpose().ok()?
    }

    pub fn entries_from(&self, start_index: u64) -> Vec<LogEntry> {
        let last = self.last_index();
        if start_index == 0 || start_index > last {
            return vec![];
        }
        // TODO: propagate corruption error
        self.store
            .get_range(start_index, last)
            .unwrap_or_default()
            .into_iter()
            .map(from_entry)
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_default()
    }

    pub fn append(&mut self, entry: LogEntry) {
        // TODO: propagate error
        let _ = self.store.append_log(to_entry(&entry));
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
