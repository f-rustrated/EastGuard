//! Sparse-index rebuild: re-derive a segment file's index anchors by re-walking
//! it, for when the index RocksDB is missing or doubtful.
//!
//! Mirrors the checkpoint worker's live bookkeeping (`checkpoint.rs`): both gate
//! their anchor writes on [`is_index_anchor`], so the rebuilt index is
//! byte-identical to the live one — an anchor for the segment's first batch plus
//! one every `INDEX_INTERVAL_ENTRIES` entries, each pointing at the byte offset
//! where its `Data` record begins. Segment records are bare, so entry ids are
//! positional — `start_offset` (parsed from the filename) plus the batch's
//! index. Only complete batches are counted, so a damaged tail is left out.
//! Feeding the result through
//! [`SparseIndex::put_batch`](crate::data_plane::sparse_index::SparseIndex) is
//! idempotent by construction: the same keys and byte positions the live index
//! holds.

use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

use crate::control_plane::metadata::EntryId;
use crate::data_plane::SegmentKey;
use crate::data_plane::parse_segment_file;
use crate::data_plane::sparse_index::{SparseEntry, is_index_anchor};
use crate::data_plane::wal::{WalRecord, WalRecordType};

pub(crate) fn rebuild_sparse_entries(path: &Path, key: SegmentKey) -> io::Result<Vec<SparseEntry>> {
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

    // The open batch's (Data-record byte start, positional entry id, record count), pending
    // until its `BatchEnd` confirms the batch is durable.
    let mut pending: Option<(u64, EntryId, u32)> = None;

    loop {
        match WalRecord::decode_from(&mut reader) {
            Ok(record) => {
                match record.record_type {
                    WalRecordType::Data => {
                        pending = Some((position, start_offset + completed, record.record_count))
                    }
                    WalRecordType::BatchEnd => {
                        if let Some((byte_start, entry_id, _)) = pending.take() {
                            if is_index_anchor(byte_start, entry_id) {
                                let entry =
                                    SparseEntry::new(key, entry_id, byte_start.to_be_bytes());
                                entries.push(entry);
                            }
                            completed += 1_u64;
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
    use crate::control_plane::metadata::{EntryId, RangeId, SegmentId, TopicId};
    use crate::data_plane::sparse_index::{INDEX_INTERVAL_ENTRIES, SparseIndex};

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
    fn anchors_the_base_and_every_interval() {
        let dir = tempfile::tempdir().unwrap();
        let n = INDEX_INTERVAL_ENTRIES;
        // One single-entry batch per id 0..=n+1.
        let batches: Vec<Vec<u8>> = (0..=n + 1)
            .map(|i| encode_batch(&format!("e{i}"), 1))
            .collect();
        // Byte offset where the id-n batch starts (batches vary in size).
        let pos_n: u64 = batches[..n as usize].iter().map(|b| b.len() as u64).sum();
        let path = key().file_path(dir.path(), EntryId(0));
        write_segment(&path, &batches.concat());

        let entries = rebuild_sparse_entries(&path, key()).unwrap();
        // Anchored: id 0 (the base) and id n (a multiple of the interval). The
        // ids in between, and id n+1, are skipped.
        assert_eq!(entries.len(), 2);

        let (_db_dir, db) = open_db();
        db.put_batch(entries).unwrap();
        // Anything below the first interval resolves to the base anchor.
        assert_eq!(db.seek_index(key(), 0), (0, 0));
        assert_eq!(db.seek_index(key(), n - 1), (0, 0));
        // At or after the interval, the id-n anchor.
        assert_eq!(db.seek_index(key(), n), (n, pos_n));
        assert_eq!(db.seek_index(key(), n + 1), (n, pos_n));
    }

    #[test]
    fn the_segment_base_is_always_anchored() {
        let dir = tempfile::tempdir().unwrap();
        // Base 50 isn't a multiple of the interval, and neither are 51/52, so
        // only the base gets an anchor.
        let path = key().file_path(dir.path(), 50);
        write_segment(
            &path,
            &[
                encode_batch("e50", 1),
                encode_batch("e51", 1),
                encode_batch("e52", 1),
            ]
            .concat(),
        );

        let entries = rebuild_sparse_entries(&path, key()).unwrap();
        assert_eq!(entries.len(), 1);

        let (_db_dir, db) = open_db();
        db.put_batch(entries).unwrap();
        // Reads anywhere in [base, base + interval) fall back to the base at
        // byte 0 — and carry the base id, not the requested one.
        assert_eq!(db.seek_index(key(), 50), (50, 0));
        assert_eq!(db.seek_index(key(), 52), (50, 0));
    }

    #[test]
    fn a_damaged_tail_is_not_indexed() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = encode_batch("e0", 1);
        bytes.extend_from_slice(b"torn-tail-without-a-batchend");
        let path = key().file_path(dir.path(), 0);
        write_segment(&path, &bytes);

        let entries = rebuild_sparse_entries(&path, key()).unwrap();
        assert_eq!(entries.len(), 1); // the base batch; torn tail ignored
    }
}
