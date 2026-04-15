use crate::raft::log::LogEntry;
use crate::storage::Index;

/// Append-only log storage — one instance per shard group.
///
/// # Directory layout (FileLogStore)
///
/// ```text
/// <base_dir>/
///     data.log     ← [entry_len: u32 LE | entry_data]*
///     index.log    ← [byte_offset: u64 LE]*  (one entry per log entry)
///     snapshot     ← Phase 2 (not yet implemented)
/// ```
pub trait LogStore {
    /// Appends an entry to the end of the log.
    fn append_log(&mut self, entry: LogEntry) -> Result<(), LogError>;

    /// Returns all entries in the closed range `[start, end]`.
    fn get_range(&self, start: Index, end: Index) -> Result<Vec<LogEntry>, LogError>;

    /// Returns the entry at `index`, or `None` if it does not exist.
    fn get(&self, index: Index) -> Result<Option<LogEntry>, LogError>;

    /// Returns the last stored entry, or `None` if the log is empty.
    fn get_last(&self) -> Result<Option<LogEntry>, LogError>;

    /// Removes all entries at and after `from` (inclusive).
    fn truncate_log(&mut self, from: Index) -> Result<(), LogError>;

    /// Flushes buffered writes to durable storage.
    #[allow(dead_code)]
    fn sync(&mut self) -> Result<(), LogError>;
}

/// Errors that can be returned by a [`LogStore`].
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The data on disk is unreadable or inconsistent.
    #[error("storage corruption: {0}")]
    Corruption(String),
}
