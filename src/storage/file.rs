#![allow(dead_code)]

use std::fs::{self, File, OpenOptions};
use std::mem::size_of;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use crate::clusters::raft::interface::{LogError, LogStore};
use crate::clusters::raft::log::LogEntry;
use crate::storage::{Entry, Index};

// ── On-disk format ────────────────────────────────────────────────────────────
//
// <base_dir>/
//     data.log    — [entry_len: u32 LE (4 B) | entry_data (entry_len B)]*
//     index.log   — [byte_offset: u64 LE (8 B)]*
//
// index.log[i] = byte offset in data.log of entry (first_index + i).
// ─────────────────────────────────────────────────────────────────────────────

// TODO: CRC, RocksDB for key-value lookup

type EntryLen = u32;

const DATA_LOG: &str = "data.log";
const INDEX_LOG: &str = "index.log";
const INDEX_ENTRY_SIZE: u64 = size_of::<u64>() as u64;

pub struct FileLogStore {
    data_file: File,
    index_file: File,
    // TODO: first_index > 1 once snapshot support is added (Phase 2).
    first_index: Index,
    num_entries: u64,
    data_bytes: u64, // mirrors data.log size; verified by debug_assert in append_log
}

impl FileLogStore {
    pub fn open(base_dir: impl Into<PathBuf>) -> Result<Self, LogError> {
        let base_dir = base_dir.into();
        fs::create_dir_all(&base_dir)?;
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(base_dir.join(DATA_LOG))?;
        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(base_dir.join(INDEX_LOG))?;
        let num_entries = index_file.metadata()?.len() / INDEX_ENTRY_SIZE;
        let data_bytes = data_file.metadata()?.len();
        Ok(Self {
            data_file,
            index_file,
            first_index: 1,
            num_entries,
            data_bytes,
        })
    }

    fn last_index(&self) -> Option<Index> {
        (self.num_entries > 0).then(|| self.first_index + self.num_entries - 1)
    }

    fn read_offset_at(&self, pos: u64) -> Result<u64, LogError> {
        let mut buf = [0u8; size_of::<u64>()];
        self.index_file
            .read_at(&mut buf, pos * INDEX_ENTRY_SIZE)
            .map_err(|e| LogError::Corruption(format!("index.log: {e}")))?;
        Ok(u64::from_le_bytes(buf))
    }
}

impl LogStore for FileLogStore {
    fn append_log(&mut self, entry: LogEntry) -> Result<(), LogError> {
        let raw = Entry::from_entry(&entry);
        let byte_offset = self.data_bytes;
        debug_assert_eq!(
            self.data_bytes,
            self.data_file.metadata().map(|m| m.len()).unwrap_or(0),
            "data_bytes cache diverged from actual data.log size"
        );
        self.data_file
            .write_at(&(raw.data.len() as EntryLen).to_le_bytes(), byte_offset)?;
        self.data_file
            .write_at(&raw.data, byte_offset + size_of::<EntryLen>() as u64)?;
        self.index_file
            .write_at(&byte_offset.to_le_bytes(), self.num_entries * INDEX_ENTRY_SIZE)?;
        self.data_bytes += size_of::<EntryLen>() as u64 + raw.data.len() as u64;
        self.num_entries += 1;
        Ok(())
    }

    fn get_range(&self, start: Index, end: Index) -> Result<Vec<LogEntry>, LogError> {
        if start == 0 || start > end {
            return Ok(vec![]);
        }
        let Some(last) = self.last_index() else {
            return Ok(vec![]);
        };
        if start > last {
            return Ok(vec![]);
        }
        let end = end.min(last);
        let count = (end - start + 1) as usize;

        // One read from index.log to find the byte offset of `start` in data.log,
        // then sequential positioned reads through data.log — O(1) fd opens regardless of range size.
        let start_pos = start - self.first_index;
        let mut current_offset = self.read_offset_at(start_pos)?;

        let mut entries = Vec::with_capacity(count);
        for i in 0..count as u64 {
            let mut len_buf = [0u8; size_of::<EntryLen>()];
            self.data_file
                .read_at(&mut len_buf, current_offset)
                .map_err(|e| {
                    LogError::Corruption(format!("data.log len at index {}: {e}", start + i))
                })?;
            let entry_len = EntryLen::from_le_bytes(len_buf) as usize;
            let mut data = vec![0u8; entry_len];
            self.data_file
                .read_at(&mut data, current_offset + size_of::<EntryLen>() as u64)
                .map_err(|e| {
                    LogError::Corruption(format!("data.log data at index {}: {e}", start + i))
                })?;
            current_offset += size_of::<EntryLen>() as u64 + entry_len as u64;
            entries.push(Entry { index: start + i, data }.into_log_entry()?);
        }

        Ok(entries)
    }

    fn get(&self, index: Index) -> Result<Option<LogEntry>, LogError> {
        if index == 0 || index < self.first_index {
            return Ok(None);
        }
        let pos = index - self.first_index;
        if pos >= self.num_entries {
            return Ok(None);
        }
        let byte_offset = self.read_offset_at(pos)?;
        let mut len_buf = [0u8; size_of::<EntryLen>()];
        self.data_file
            .read_at(&mut len_buf, byte_offset)
            .map_err(|e| LogError::Corruption(format!("data.log len: {e}")))?;
        let entry_len = EntryLen::from_le_bytes(len_buf) as usize;
        let mut data = vec![0u8; entry_len];
        self.data_file
            .read_at(&mut data, byte_offset + size_of::<EntryLen>() as u64)
            .map_err(|e| LogError::Corruption(format!("data.log data: {e}")))?;
        Ok(Some(Entry { index, data }.into_log_entry()?))
    }

    fn get_last(&self) -> Result<Option<LogEntry>, LogError> {
        let Some(last) = self.last_index() else {
            return Ok(None);
        };
        self.get(last)
    }

    fn truncate_log(&mut self, from: Index) -> Result<(), LogError> {
        if from == 0 || self.num_entries == 0 {
            return Ok(());
        }
        let last = self.first_index + self.num_entries - 1;
        if from > last {
            return Ok(());
        }
        // pos = number of entries to keep; 0 means truncate everything.
        let pos = from - self.first_index;
        let new_data_size = if pos == 0 { 0 } else { self.read_offset_at(pos)? };
        self.data_file.set_len(new_data_size)?;
        self.index_file.set_len(pos * INDEX_ENTRY_SIZE)?;
        self.num_entries = pos;
        self.data_bytes = new_data_size;
        Ok(())
    }

    fn sync(&mut self) -> Result<(), LogError> {
        self.data_file.sync_all()?;
        self.index_file.sync_all()?;
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clusters::raft::messages::RaftCommand;
    use tempfile::TempDir;

    fn entry(term: u64, index: Index) -> LogEntry {
        LogEntry {
            term,
            index,
            command: RaftCommand::Noop,
        }
    }

    fn open(dir: &TempDir) -> FileLogStore {
        FileLogStore::open(dir.path()).unwrap()
    }

    // ── Empty engine ─────────────────────────────────────────────────────────

    #[test]
    fn empty_engine_get_last_is_none() {
        let dir = TempDir::new().unwrap();
        assert!(open(&dir).get_last().unwrap().is_none());
    }

    #[test]
    fn empty_engine_get_is_none() {
        let dir = TempDir::new().unwrap();
        assert!(open(&dir).get(1).unwrap().is_none());
    }

    #[test]
    fn empty_engine_get_range_is_empty() {
        let dir = TempDir::new().unwrap();
        assert!(open(&dir).get_range(1, 5).unwrap().is_empty());
    }

    #[test]
    fn get_index_zero_is_none() {
        let dir = TempDir::new().unwrap();
        // Index 0 is reserved; must return None regardless of log state.
        assert!(open(&dir).get(0).unwrap().is_none());
    }

    // ── Append and read ──────────────────────────────────────────────────────

    #[test]
    fn append_and_get_by_index() {
        let dir = TempDir::new().unwrap();
        let mut eng = open(&dir);
        eng.append_log(entry(1, 1)).unwrap();
        eng.append_log(entry(2, 2)).unwrap();
        eng.append_log(entry(3, 3)).unwrap();

        assert_eq!(eng.get(1).unwrap().unwrap().term, 1);
        assert_eq!(eng.get(2).unwrap().unwrap().term, 2);
        assert_eq!(eng.get(3).unwrap().unwrap().term, 3);
        assert!(eng.get(4).unwrap().is_none());
    }

    #[test]
    fn get_last_returns_last_appended() {
        let dir = TempDir::new().unwrap();
        let mut eng = open(&dir);
        eng.append_log(entry(1, 1)).unwrap();
        eng.append_log(entry(2, 2)).unwrap();

        let last = eng.get_last().unwrap().unwrap();
        assert_eq!(last.index, 2);
        assert_eq!(last.term, 2);
    }

    #[test]
    fn get_range_returns_correct_slice() {
        let dir = TempDir::new().unwrap();
        let mut eng = open(&dir);
        for i in 1..=5u64 {
            eng.append_log(entry(i, i)).unwrap();
        }

        let range = eng.get_range(2, 4).unwrap();
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].index, 2);
        assert_eq!(range[1].index, 3);
        assert_eq!(range[2].index, 4);
    }

    #[test]
    fn get_range_clamps_end_to_last_index() {
        let dir = TempDir::new().unwrap();
        let mut eng = open(&dir);
        eng.append_log(entry(1, 1)).unwrap();
        eng.append_log(entry(2, 2)).unwrap();

        let range = eng.get_range(1, 99).unwrap();
        assert_eq!(range.len(), 2);
    }

    #[test]
    fn get_range_start_beyond_end_is_empty() {
        let dir = TempDir::new().unwrap();
        let eng = open(&dir);
        assert!(eng.get_range(5, 3).unwrap().is_empty());
        assert!(eng.get_range(0, 3).unwrap().is_empty());
    }

    // ── Truncation ───────────────────────────────────────────────────────────

    #[test]
    fn truncate_from_middle_removes_tail() {
        let dir = TempDir::new().unwrap();
        let mut eng = open(&dir);
        for i in 1..=5u64 {
            eng.append_log(entry(i, i)).unwrap();
        }

        eng.truncate_log(3).unwrap();

        assert_eq!(eng.get_last().unwrap().unwrap().index, 2);
        assert!(eng.get(3).unwrap().is_none());
        assert!(eng.get(4).unwrap().is_none());
    }

    #[test]
    fn truncate_from_first_entry_clears_log() {
        let dir = TempDir::new().unwrap();
        let mut eng = open(&dir);
        eng.append_log(entry(1, 1)).unwrap();

        eng.truncate_log(1).unwrap();

        assert!(eng.get_last().unwrap().is_none());
        assert!(eng.get(1).unwrap().is_none());
    }

    #[test]
    fn truncate_beyond_last_is_noop() {
        let dir = TempDir::new().unwrap();
        let mut eng = open(&dir);
        eng.append_log(entry(1, 1)).unwrap();

        eng.truncate_log(999).unwrap();

        assert_eq!(eng.get_last().unwrap().unwrap().index, 1);
    }

    #[test]
    fn truncate_index_zero_is_noop() {
        let dir = TempDir::new().unwrap();
        let mut eng = open(&dir);
        eng.append_log(entry(1, 1)).unwrap();

        eng.truncate_log(0).unwrap();

        assert_eq!(eng.get_last().unwrap().unwrap().index, 1);
    }

    // ── Durability: close and reopen ─────────────────────────────────────────

    #[test]
    fn reopen_preserves_entries() {
        let dir = TempDir::new().unwrap();
        {
            let mut eng = open(&dir);
            eng.append_log(entry(1, 1)).unwrap();
            eng.append_log(entry(2, 2)).unwrap();
        }

        let eng = open(&dir);
        assert_eq!(eng.get(1).unwrap().unwrap().term, 1);
        assert_eq!(eng.get_last().unwrap().unwrap().index, 2);
    }

    #[test]
    fn reopen_after_truncate_reflects_truncation() {
        let dir = TempDir::new().unwrap();
        {
            let mut eng = open(&dir);
            for i in 1..=4u64 {
                eng.append_log(entry(i, i)).unwrap();
            }
            eng.truncate_log(3).unwrap();
        }

        let eng = open(&dir);
        assert_eq!(eng.get_last().unwrap().unwrap().index, 2);
        assert!(eng.get(3).unwrap().is_none());
    }
}
