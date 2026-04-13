mod memory;
mod disk;

pub use memory::MemEngine;

/// Append-only log storage — one instance per shard group.
///
/// # Directory layout (DiskEngine)
///
/// ```text
/// <base_dir>/
///     data.log     ← [entry_len: u32 LE | entry_data]*
///     index.log    ← [byte_offset: u64 LE]*  (one entry per log entry)
///     snapshot     ← Phase 2 (not yet implemented)
/// ```
pub trait StorageEngine {
    /// Appends an entry to the end of the log.
    fn append_log(&mut self, entry: Entry) -> Result<(), StorageError>;

    /// Returns all entries in the closed range `[start, end]`.
    fn get_range(&self, start: Index, end: Index) -> Result<Vec<Entry>, StorageError>;

    /// Returns the entry at `index`, or `None` if it does not exist.
    fn get(&self, index: Index) -> Result<Option<Entry>, StorageError>;

    /// Returns the last stored entry, or `None` if the log is empty.
    fn get_last(&self) -> Result<Option<Entry>, StorageError>;

    /// Removes all entries at and after `from` (inclusive).
    fn truncate_log(&mut self, from: Index) -> Result<(), StorageError>;

    /// Flushes buffered writes to durable storage.
    #[allow(dead_code)]
    fn sync(&mut self) -> Result<(), StorageError>;
}

/// Position in the log. 1-based; `0` is reserved to mean "before the log begins".
pub type Index = u64;

#[derive(Debug, Clone)]
pub struct Entry {
    pub index: Index,
    pub data: Vec<u8>,
}

/// Errors that can be returned by a [`StorageEngine`].
#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    /// The data on disk is unreadable or inconsistent.
    Corruption {
        reason: String,
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io(e) => write!(f, "storage I/O error: {e}"),
            StorageError::Corruption { reason } => write!(f, "storage corruption: {reason}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        StorageError::Io(e)
    }
}
