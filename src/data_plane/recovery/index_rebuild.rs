//! Sparse-index rebuild: re-derive a segment file's index entries by re-walking
//! it, for when the index RocksDB is missing or doubtful.
//!
//! Mirrors the checkpoint worker's live bookkeeping (`checkpoint.rs`): one
//! [`SparseEntry`] per `(Data, BatchEnd)` batch, keyed by the entry id and
//! pointing at the byte offset where the batch's `Data` record begins. Segment
//! records are bare, so entry ids are positional — `start_offset` (parsed from
//! the filename) plus the batch's index. Only complete batches are indexed, so
//! a damaged tail is left out. Feeding the result through
//! [`SparseIndex::put_batch`](crate::data_plane::sparse_index::SparseIndex) is
//! idempotent by construction: the same keys and byte positions the live index
//! holds.

use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

use crate::data_plane::SegmentKey;
use crate::data_plane::parse_segment_file;
use crate::data_plane::sparse_index::SparseEntry;
use crate::data_plane::wal::{WalRecord, WalRecordType};

/// Re-derives the sparse-index entries for the segment file at `path`
pub(crate) fn rebuild_entries(path: &Path, key: SegmentKey) -> io::Result<Vec<SparseEntry>> {
    let (_segment_id, start_offset) = parse_segment_file(path)?;
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut reader = BufReader::new(file);

    let mut entries = Vec::new();
    let mut position: u64 = 0;
    let mut completed: u64 = 0;

    // The open batch's (Data-record byte start, positional entry id), pending
    // until its `BatchEnd` confirms the batch is durable.
    let mut pending: Option<(u64, u64)> = None;

    loop {
        match WalRecord::decode_from(&mut reader) {
            Ok(record) => {
                match record.record_type {
                    WalRecordType::Data => pending = Some((position, start_offset + completed)),
                    WalRecordType::BatchEnd => {
                        if let Some((byte_start, entry_id)) = pending.take() {
                            entries.push(SparseEntry::new(key, entry_id, byte_start.to_be_bytes()));
                            completed += 1;
                        }
                    }
                }
                position += record.encoded_size() as u64;
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
                ) =>
            {
                break;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use bytes::Bytes;

    use super::*;
    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
    use crate::data_plane::sparse_index::SparseIndex;

    fn key() -> SegmentKey {
        SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0))
    }

    /// One `(Data, BatchEnd)` batch with a bare payload, like the checkpoint
    /// worker writes.
    fn encode_batch(payload: &str, record_count: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        WalRecord::data(Bytes::copy_from_slice(payload.as_bytes()), record_count)
            .encode_to(&mut buf)
            .unwrap();
        WalRecord::batch_end().encode_to(&mut buf).unwrap();
        buf
    }

    fn write_segment(path: &Path, bytes: &[u8]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }

    fn open_db() -> (tempfile::TempDir, rocksdb::DB) {
        let dir = tempfile::tempdir().unwrap();
        let db = rocksdb::DB::open_default(dir.path()).unwrap();
        (dir, db)
    }

    #[test]
    fn rebuilt_index_resolves_to_each_batch_start() {
        let dir = tempfile::tempdir().unwrap();
        let b0 = encode_batch("e0", 1);
        let b1 = encode_batch("e1", 1);
        let b2 = encode_batch("e2", 1);
        let pos1 = b0.len() as u64;
        let pos2 = (b0.len() + b1.len()) as u64;
        let path = key().file_path(dir.path(), 0);
        write_segment(&path, &[b0, b1, b2].concat());

        let entries = rebuild_entries(&path, key()).unwrap();
        assert_eq!(entries.len(), 3);

        let (_db_dir, db) = open_db();
        db.put_batch(entries).unwrap();
        assert_eq!(db.seek_index(key(), 0), 0);
        assert_eq!(db.seek_index(key(), 1), pos1);
        assert_eq!(db.seek_index(key(), 2), pos2);
        // An offset past the last indexed entry resolves to the last batch.
        assert_eq!(db.seek_index(key(), 99), pos2);
    }

    #[test]
    fn entry_ids_are_the_filename_base_plus_position() {
        let dir = tempfile::tempdir().unwrap();
        let b0 = encode_batch("e0", 1);
        let pos1 = b0.len() as u64;
        let path = key().file_path(dir.path(), 50); // base 50
        write_segment(&path, &[b0, encode_batch("e1", 1)].concat());

        let (_db_dir, db) = open_db();
        db.put_batch(rebuild_entries(&path, key()).unwrap())
            .unwrap();
        // Indexed at ids 50 and 51, not 0 and 1.
        assert_eq!(db.seek_index(key(), 50), 0);
        assert_eq!(db.seek_index(key(), 51), pos1);
    }

    #[test]
    fn a_damaged_tail_is_not_indexed() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = encode_batch("e0", 1);
        bytes.extend_from_slice(b"torn-tail-without-a-batchend");
        let path = key().file_path(dir.path(), 0);
        write_segment(&path, &bytes);

        let entries = rebuild_entries(&path, key()).unwrap();
        assert_eq!(entries.len(), 1); // only the complete batch
    }
}
