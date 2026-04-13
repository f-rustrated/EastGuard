#![allow(dead_code)]

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};

use crate::storage::{Entry, Index, StorageEngine, StorageError};

// ── On-disk format ────────────────────────────────────────────────────────────
//
// <base_dir>/
//     data.log    — [entry_len: u32 LE (4 B) | entry_data (entry_len B)]*
//     index.log   — [byte_offset: u64 LE (8 B)]*
//
// index.log[i] = byte offset in data.log of entry (first_index + i).
// ─────────────────────────────────────────────────────────────────────────────

// TODO: CRC, RocksDB for key-value lookup 

const DATA_LOG: &str = "data.log";
const INDEX_LOG: &str = "index.log";
const INDEX_ENTRY_SIZE: u64 = size_of::<u64>() as u64;

// No file handles kept open; the OS page cache handles read-after-write.
pub struct DiskEngine {
    base_dir: PathBuf,
    // TODO: first_index > 1 once snapshot support is added (Phase 2).
    first_index: Index,
    num_entries: u64,
    data_bytes: u64, // mirrors data.log size; verified by debug_assert in append_log
}

impl DiskEngine {
    pub fn open(base_dir: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let base_dir = base_dir.into();
        fs::create_dir(&base_dir)?;
        let num_entries = {
            let p = base_dir.join(INDEX_LOG);
            if p.exists() {
                fs::metadata(&p)?.len() / INDEX_ENTRY_SIZE
            } else {
                0
            }
        };
        let data_bytes = {
            let p = base_dir.join(DATA_LOG);
            if p.exists() {
                fs::metadata(&p)?.len()
            } else {
                0
            }
        };
        Ok(Self {
            base_dir,
            first_index: 1,
            num_entries,
            data_bytes,
        })
    }

    fn data_path(&self) -> PathBuf {
        self.base_dir.join(DATA_LOG)
    }
    
    fn index_path(&self) -> PathBuf {
        self.base_dir.join(INDEX_LOG)
    }

    fn last_index(&self) -> Option<Index> {
        (self.num_entries > 0).then(|| self.first_index + self.num_entries - 1)
    }
}

/// Reads the byte offset stored at position `pos` in `index.log`.
fn read_offset_at(index_path: &Path, pos: u64) -> Result<u64, StorageError> {
    let mut f = File::open(index_path)?;
    f.seek(SeekFrom::Start(pos * INDEX_ENTRY_SIZE))?;
    let mut buf = [0u8; size_of::<u64>()];
    f.read_exact(&mut buf)
        .map_err(|e| StorageError::Corruption {
            reason: format!("index.log: {e}"),
        })?;
    Ok(u64::from_le_bytes(buf))
}

impl StorageEngine for DiskEngine {
    fn append_log(&mut self, entry: Entry) -> Result<(), StorageError> {
        let byte_offset = self.data_bytes;
        debug_assert_eq!(
            self.data_bytes,
            if self.data_path().exists() {
                fs::metadata(self.data_path()).unwrap().len()
            } else {
                0
            },
            "data_bytes cache diverged from actual data.log size"
        );
        // Write data.log before index.log: orphaned data tail is detectable;
        // dangling index pointer is not.
        let mut dat = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.data_path())?;
        dat.write_all(&(entry.data.len() as u32).to_le_bytes())?;
        dat.write_all(&entry.data)?;
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.index_path())?
            .write_all(&byte_offset.to_le_bytes())?;
        self.data_bytes += size_of::<u32>() as u64 + entry.data.len() as u64;
        self.num_entries += 1;
        Ok(())
    }

    fn get_range(&self, start: Index, end: Index) -> Result<Vec<Entry>, StorageError> {
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
        (start..=end)
            .map(|i| {
                self.get(i)?
                    .ok_or(StorageError::IndexOutOfRange { requested: i, last })
            })
            .collect()
    }

    fn get(&self, index: Index) -> Result<Option<Entry>, StorageError> {
        if index == 0 || index < self.first_index {
            return Ok(None);
        }
        let pos = index - self.first_index;
        if pos >= self.num_entries {
            return Ok(None);
        }
        let byte_offset = read_offset_at(&self.index_path(), pos)?;
        let mut f = File::open(self.data_path())?;
        f.seek(SeekFrom::Start(byte_offset))?;
        let mut len_buf = [0u8; size_of::<u32>()];
        f.read_exact(&mut len_buf)
            .map_err(|e| StorageError::Corruption {
                reason: format!("data.log len: {e}"),
            })?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut data = vec![0u8; len];
        f.read_exact(&mut data)
            .map_err(|e| StorageError::Corruption {
                reason: format!("data.log data: {e}"),
            })?;
        Ok(Some(Entry { index, data }))
    }

    fn get_last(&self) -> Result<Option<Entry>, StorageError> {
        let Some(last) = self.last_index() else {
            return Ok(None);
        };
        self.get(last)
    }

    fn truncate_log(&mut self, from: Index) -> Result<(), StorageError> {
        if from == 0 || self.num_entries == 0 {
            return Ok(());
        }
        let last = self.first_index + self.num_entries - 1;
        if from > last {
            return Ok(());
        }
        // pos = number of entries to keep; 0 means truncate everything.
        let pos = from - self.first_index;
        let new_data_size = if pos == 0 {
            0
        } else {
            read_offset_at(&self.index_path(), pos)?
        };
        // Disk before memory: a failed set_len after memory update would leave
        // the engine believing fewer entries than are actually on disk.
        OpenOptions::new()
            .write(true)
            .open(self.data_path())?
            .set_len(new_data_size)?;
        OpenOptions::new()
            .write(true)
            .open(self.index_path())?
            .set_len(pos * INDEX_ENTRY_SIZE)?;
        self.num_entries = pos;
        self.data_bytes = new_data_size;
        Ok(())
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        for path in [self.data_path(), self.index_path()] {
            if path.exists() {
                OpenOptions::new().write(true).open(path)?.sync_all()?;
            }
        }
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn entry(index: Index, data: &[u8]) -> Entry {
        Entry {
            index,
            data: data.to_vec(),
        }
    }

    fn open(dir: &TempDir) -> DiskEngine {
        DiskEngine::open(dir.path()).unwrap()
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
        eng.append_log(entry(1, b"alpha")).unwrap();
        eng.append_log(entry(2, b"beta")).unwrap();
        eng.append_log(entry(3, b"gamma")).unwrap();

        assert_eq!(eng.get(1).unwrap().unwrap().data, b"alpha");
        assert_eq!(eng.get(2).unwrap().unwrap().data, b"beta");
        assert_eq!(eng.get(3).unwrap().unwrap().data, b"gamma");
        assert!(eng.get(4).unwrap().is_none());
    }

    #[test]
    fn get_last_returns_last_appended() {
        let dir = TempDir::new().unwrap();
        let mut eng = open(&dir);
        eng.append_log(entry(1, b"x")).unwrap();
        eng.append_log(entry(2, b"y")).unwrap();

        let last = eng.get_last().unwrap().unwrap();
        assert_eq!(last.index, 2);
        assert_eq!(last.data, b"y");
    }

    #[test]
    fn get_range_returns_correct_slice() {
        let dir = TempDir::new().unwrap();
        let mut eng = open(&dir);
        for i in 1..=5u64 {
            eng.append_log(entry(i, &[i as u8])).unwrap();
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
        eng.append_log(entry(1, b"a")).unwrap();
        eng.append_log(entry(2, b"b")).unwrap();

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
            eng.append_log(entry(i, &[i as u8])).unwrap();
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
        eng.append_log(entry(1, b"only")).unwrap();

        eng.truncate_log(1).unwrap();

        assert!(eng.get_last().unwrap().is_none());
        assert!(eng.get(1).unwrap().is_none());
    }

    #[test]
    fn truncate_beyond_last_is_noop() {
        let dir = TempDir::new().unwrap();
        let mut eng = open(&dir);
        eng.append_log(entry(1, b"a")).unwrap();

        eng.truncate_log(999).unwrap();

        assert_eq!(eng.get_last().unwrap().unwrap().index, 1);
    }

    #[test]
    fn truncate_index_zero_is_noop() {
        let dir = TempDir::new().unwrap();
        let mut eng = open(&dir);
        eng.append_log(entry(1, b"a")).unwrap();

        eng.truncate_log(0).unwrap();

        assert_eq!(eng.get_last().unwrap().unwrap().index, 1);
    }

    // ── Durability: close and reopen ─────────────────────────────────────────

    #[test]
    fn reopen_preserves_entries() {
        let dir = TempDir::new().unwrap();
        {
            let mut eng = open(&dir);
            eng.append_log(entry(1, b"persisted")).unwrap();
            eng.append_log(entry(2, b"data")).unwrap();
        }

        let eng = open(&dir);
        assert_eq!(eng.get(1).unwrap().unwrap().data, b"persisted");
        assert_eq!(eng.get_last().unwrap().unwrap().index, 2);
    }

    #[test]
    fn reopen_after_truncate_reflects_truncation() {
        let dir = TempDir::new().unwrap();
        {
            let mut eng = open(&dir);
            for i in 1..=4u64 {
                eng.append_log(entry(i, &[i as u8])).unwrap();
            }
            eng.truncate_log(3).unwrap();
        }

        let eng = open(&dir);
        assert_eq!(eng.get_last().unwrap().unwrap().index, 2);
        assert!(eng.get(3).unwrap().is_none());
    }
}
