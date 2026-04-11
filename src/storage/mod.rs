#![allow(dead_code)]

/// Position in the log. 1-based; `0` is reserved to mean "before the log begins".
pub type Index = u64;

/// A single entry in the log.
///
/// `data` is an opaque byte payload. The caller is responsible for encoding
/// and decoding whatever domain type (e.g. a serialized Raft command) lives
/// inside it.
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
    Corruption { reason: String },
    /// The requested index range falls outside the stored log.
    IndexOutOfRange { requested: Index, last: Index },
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io(e) => write!(f, "storage I/O error: {e}"),
            StorageError::Corruption { reason } => write!(f, "storage corruption: {reason}"),
            StorageError::IndexOutOfRange { requested, last } => write!(
                f,
                "index out of range: requested {requested}, last stored {last}"
            ),
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

/// Append-only log storage — one instance per directory (shard or meta).
///
/// # Directory layout
///
/// ```text
/// <base_dir>/
///     meta/                ← meta store (shard group registry)
///     shards/
///         <shard_id>/      ← per-shard store
///             log/
///             snapshot/    (Phase 2)
/// ```
///
/// # Ordering invariant
///
/// Entries must be appended in strictly ascending index order
/// (`entry.index == last_index() + 1`). The only mutation allowed beyond
/// appending is [`StorageEngine::truncate_log`], which removes entries from a
/// given position onward.
pub trait StorageEngine {
    /// Appends a single entry.
    ///
    /// The caller must ensure `entry.index == last_index() + 1`.
    fn append_log(&mut self, entry: Entry) -> Result<(), StorageError>;

    /// Returns all entries in the closed range `[start, end]`.
    ///
    /// `get_log(start, start + limit - 1)` gives limit-based semantics.
    /// Returns an empty `Vec` when the range is empty or inverted.
    fn get_log(&self, start: Index, end: Index) -> Result<Vec<Entry>, StorageError>;

    /// Returns the index of the last stored entry, or `0` if the log is empty.
    fn last_index(&self) -> Result<Index, StorageError>;
}
