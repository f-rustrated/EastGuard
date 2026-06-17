//! The single segment-file writer.
//!
//! Appends entries in the `(Data, BatchEnd)` framing with **bare** payloads (the
//! routing header is WAL-only) and emits the sparse-index anchor for each batch
//! that lands on an index interval. Shared by the checkpoint worker (cache →
//! file), recovery replay (WAL → file), and the catch-up receive (network →
//! file) — the one place segment files are written, so the framing and the
//! anchor predicate live in exactly one spot.

use crate::data_plane::SegmentKey;
use crate::data_plane::sparse_index::{SparseEntry, is_index_anchor};
use crate::data_plane::wal::WalRecord;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

/// One open segment file plus the absolute byte offset of its next write — the
/// position an index anchor would point at.
pub(crate) struct SegmentAppender {
    segment_key: SegmentKey,
    writer: BufWriter<File>,
    next_pos: u64,
}

impl SegmentAppender {
    /// Fresh file from offset 0, truncating any stale partial left by an earlier
    /// interrupted write. Used by the catch-up receive, which streams a whole
    /// sealed segment.
    pub(crate) fn create(segment_key: SegmentKey, path: &Path) -> io::Result<Self> {
        Self::ensure_parent(path)?;
        let file = File::create(path)?;
        Ok(Self {
            segment_key,
            writer: BufWriter::new(file),
            next_pos: 0,
        })
    }

    /// Open an existing file (creating it if absent) and append at its current
    /// end. Used by the checkpoint worker — its own writes are always clean, so
    /// the file end is the append point.
    pub(crate) fn open_append(segment_key: SegmentKey, path: &Path) -> io::Result<Self> {
        Self::ensure_parent(path)?;
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let next_pos = file.metadata()?.len();
        Ok(Self {
            segment_key,
            writer: BufWriter::new(file),
            next_pos,
        })
    }

    /// Open an existing file, truncate any torn tail past `valid_len`, and append
    /// from there. Used by recovery replay to repair a crash-damaged segment.
    /// `truncate(false)` is deliberate — the open must not zero the file.
    pub(crate) fn open_truncating(
        segment_key: SegmentKey,
        path: &Path,
        valid_len: u64,
    ) -> io::Result<Self> {
        Self::ensure_parent(path)?;
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        file.set_len(valid_len)?;
        let mut writer = BufWriter::new(file);
        writer.seek(SeekFrom::Start(valid_len))?;
        Ok(Self {
            segment_key,
            writer,
            next_pos: valid_len,
        })
    }

    fn ensure_parent(path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    /// Flush the buffer and fsync the file — the durability boundary.
    pub(crate) fn flush_and_sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }

    /// Like [`flush_and_sync`](Self::flush_and_sync), then hint the OS to drop the
    /// file's pages from cache — checkpoint output is cold once durable.
    pub(crate) fn flush_sync_and_release(&mut self) -> io::Result<()> {
        self.flush_and_sync()?;
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = self.writer.get_ref().as_raw_fd();
            // SAFETY: `fd` is valid while `self.writer` owns the file; a failing
            // advisory hint is harmless.
            unsafe {
                libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED);
            }
        }
        Ok(())
    }

    /// Write one `(Data, BatchEnd)` batch with the bare `data` payload, returning
    /// the sparse-index anchor for this batch when its start offset / entry id
    /// lands on an index interval — callers just collect the `Some`s. The anchor
    /// is keyed to the appender's own segment. Takes an owned `Bytes` so callers
    /// pass their cheapest form (an Arc-backed clone) rather than forcing a copy.
    pub(crate) fn append_batch(
        &mut self,
        entry_id: u64,
        record: WalRecord,
    ) -> io::Result<Option<SparseEntry>> {
        let data_start = self.next_pos;
        record.encode_to(&mut self.writer)?;
        let end = WalRecord::batch_end();
        end.encode_to(&mut self.writer)?;
        self.next_pos += record.encoded_size() as u64 + end.encoded_size() as u64;
        Ok(is_index_anchor(data_start, entry_id)
            .then(|| SparseEntry::new(self.segment_key, entry_id, data_start.to_be_bytes())))
    }
}
