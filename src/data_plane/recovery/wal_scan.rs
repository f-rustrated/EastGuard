//! WAL directory discovery for crash recovery
//!
//! Read-only: lists the files replay will scan, in replay order. The oldest
//! surviving file is the checkpoint boundary — WAL files are deleted only
//! once every segment has checkpointed past them, so "still on disk" means
//! "may contain entries the segment files lack".

use std::fs::{self, File};
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use crate::data_plane::wal::{WalRecord, WalRecordType, parse_wal_filename};

/// One WAL file, as replay will visit it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalFileRef {
    pub(crate) seq: u64,
    pub(crate) path: PathBuf,
}

/// Lists the WAL files under `data_dir/wal`, sorted by sequence number —
/// the order the live writer created them, hence replay order. Gaps in the
/// sequence are normal: checkpoint-driven deletion removes old files.
///
/// A missing WAL directory yields an empty result, not an error: a node
/// that never wrote (or a wiped disk) has nothing to replay. Files that do
/// not match the WAL naming scheme are skipped with a debug log — foreign
/// files must not abort recovery.
pub(crate) fn discover_wal_files(data_dir: &Path) -> io::Result<Box<[WalFileRef]>> {
    let wal_dir = data_dir.join("wal");
    let read_dir = match fs::read_dir(&wal_dir) {
        Ok(read_dir) => read_dir,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Box::new([])),
        Err(e) => return Err(e),
    };

    let mut files = Vec::new();
    for entry in read_dir {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        match parse_wal_filename(&name_str) {
            Some(seq) => files.push(WalFileRef {
                seq,
                path: entry.path(),
            }),
            None => {
                tracing::debug!(file = %name_str, "skipping non-WAL file in WAL directory");
            }
        }
    }

    files.sort_by_key(|f| f.seq);
    Ok(files.into_boxed_slice())
}

/// One durable batch: the data records between two batch-end markers.
/// Batches were fsynced as a unit, so the batch is the durability granule —
/// replay credits whole batches or nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalBatch {
    pub(crate) records: Box<[WalRecord]>,
}

/// How a WAL file ended.
///
/// The reader reports *where* credit stops, not *why* — whether damage is a
/// torn tail (normal crash artifact) or real corruption depends on which
/// file it is in (last vs earlier), which only the multi-file scanner knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileEnd {
    /// The file ends exactly on a batch boundary.
    Clean,
    /// Credit stops at `valid_bytes`: everything beyond the last complete is uncredited.
    Damaged { valid_bytes: u64 },
}

/// Streaming reader over one WAL file, yielding complete batches.
pub(crate) struct WalFileReader {
    reader: BufReader<File>,
    file_len: u64,
    /// Bytes covered by complete batches so far.
    credited: u64,
    /// Bytes covered by successfully decoded records (≥ `credited` while a
    /// batch is open).
    consumed: u64,
    pending: Vec<WalRecord>,
    end: Option<FileEnd>,
}

impl WalFileReader {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        Ok(Self {
            reader: BufReader::new(file),
            file_len,
            credited: 0,
            consumed: 0,
            pending: Vec::new(),
            end: None,
        })
    }

    // Returns `Ok(None)` when the file is exhausted; `end_state`
    // then says whether it ended cleanly. Genuine I/O failures (permissions, hardware) propagate as errors;
    // data-level damage (`InvalidData`, `UnexpectedEof` from the record decoder) ends iteration and is reported
    // through `end_state` instead — damage is an answer, not a failure.
    pub(crate) fn next_batch(&mut self) -> io::Result<Option<WalBatch>> {
        if self.end.is_some() {
            return Ok(None);
        }
        loop {
            match WalRecord::decode_from(&mut self.reader) {
                Ok(record) => {
                    self.consumed += record.encoded_size() as u64;
                    match record.record_type {
                        WalRecordType::Data => self.pending.push(record),
                        WalRecordType::BatchEnd => {
                            self.credited = self.consumed;
                            let records = std::mem::take(&mut self.pending).into_boxed_slice();
                            return Ok(Some(WalBatch { records }));
                        }
                    }
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData
                    ) =>
                {
                    // The decoder has no EOF signal other than an error: at
                    // the end of a well-formed file, the next header read returns UnexpectedEof.
                    // So even a clean file ends up in this branch. `file_len == credited` is what tells a
                    // mere end-of-file (every byte belongs to a complete batch) apart from a torn or corrupt tail
                    // (leftover bytes that no complete batch accounts for).
                    self.end = Some(if self.file_len == self.credited {
                        FileEnd::Clean
                    } else {
                        FileEnd::Damaged {
                            valid_bytes: self.credited,
                        }
                    });
                    self.pending.clear();
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// `None` until the reader has been driven to the end of the file.
    pub(crate) fn end_state(&self) -> Option<FileEnd> {
        self.end
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    fn touch(dir: &Path, name: &str) {
        File::create(dir.join(name)).unwrap();
    }

    fn make_wal_dir(root: &Path) -> PathBuf {
        let wal_dir = root.join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        wal_dir
    }

    #[test]
    fn missing_wal_dir_yields_empty() {
        let dir = tempfile::tempdir().unwrap();
        let files = discover_wal_files(dir.path()).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn empty_wal_dir_yields_empty() {
        let dir = tempfile::tempdir().unwrap();
        make_wal_dir(dir.path());
        let files = discover_wal_files(dir.path()).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn files_sorted_by_seq_with_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = make_wal_dir(dir.path());
        // Gaps are normal: checkpoint-driven deletion removed 1 and 3–6.
        touch(&wal_dir, "wal-000007.log");
        touch(&wal_dir, "wal-000002.log");
        touch(&wal_dir, "wal-000010.log");

        let files = discover_wal_files(dir.path()).unwrap();
        let seqs: Vec<u64> = files.iter().map(|f| f.seq).collect();
        assert_eq!(seqs, vec![2, 7, 10]);
        assert!(files.iter().all(|f| f.path.starts_with(&wal_dir)));
    }

    #[test]
    fn foreign_files_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let wal_dir = make_wal_dir(dir.path());
        touch(&wal_dir, "wal-000001.log");
        touch(&wal_dir, "notes.txt");
        touch(&wal_dir, "wal-abc.log");
        touch(&wal_dir, "wal-000003.log.tmp");

        let files = discover_wal_files(dir.path()).unwrap();
        let seqs: Vec<u64> = files.iter().map(|f| f.seq).collect();
        assert_eq!(seqs, vec![1]);
    }

    // ---------------------------------------------------------------
    // WalFileReader
    // ---------------------------------------------------------------

    fn data_record(payload: &str, record_count: u32) -> WalRecord {
        WalRecord::data(
            bytes::Bytes::copy_from_slice(payload.as_bytes()),
            record_count,
        )
    }

    /// Encodes `records` followed by a batch-end marker — the same shape
    /// the live write path stages into the WAL buffer per flush.
    fn encode_batch(records: &[WalRecord]) -> Vec<u8> {
        let mut buf = Vec::new();
        for record in records {
            record.encode_to(&mut buf).unwrap();
        }
        WalRecord::batch_end().encode_to(&mut buf).unwrap();
        buf
    }

    fn read_all(path: &Path) -> (Vec<WalBatch>, FileEnd) {
        let mut reader = WalFileReader::open(path).unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = reader.next_batch().unwrap() {
            batches.push(batch);
        }
        (batches, reader.end_state().expect("driven to end"))
    }

    #[test]
    fn clean_file_two_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-000001.log");
        let b1 = encode_batch(&[data_record("alpha", 1), data_record("beta", 2)]);
        let b2 = encode_batch(&[data_record("gamma", 3)]);
        fs::write(&path, [b1, b2].concat()).unwrap();

        let (batches, end) = read_all(&path);
        assert_eq!(end, FileEnd::Clean);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].records.len(), 2);
        assert_eq!(batches[1].records.len(), 1);
        assert_eq!(&batches[1].records[0].payload[..], b"gamma");
        assert_eq!(batches[1].records[0].record_count, 3);
    }

    #[test]
    fn empty_file_is_clean() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-000001.log");
        fs::write(&path, []).unwrap();

        let (batches, end) = read_all(&path);
        assert!(batches.is_empty());
        assert_eq!(end, FileEnd::Clean);
    }

    #[test]
    fn live_writer_file_reads_clean() {
        // Round-trip against the real writer: stage record + marker into the
        // batch buffer exactly as the produce path does, flush, read back.
        use crate::data_plane::wal::{WalStorage, WalWriter};

        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        data_record("hello", 4).encode_to(wal.buf()).unwrap();
        WalRecord::batch_end().encode_to(wal.buf()).unwrap();
        wal.flush_batch().unwrap();

        let files = discover_wal_files(dir.path()).unwrap();
        let (batches, end) = read_all(&files[0].path);
        assert_eq!(end, FileEnd::Clean);
        assert_eq!(batches.len(), 1);
        assert_eq!(&batches[0].records[0].payload[..], b"hello");
    }

    #[test]
    fn truncated_mid_record_keeps_prior_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-000001.log");
        let b1 = encode_batch(&[data_record("alpha", 1)]);
        let b2 = encode_batch(&[data_record("beta", 1)]);
        let valid = b1.len() as u64;
        let mut bytes = [b1, b2].concat();
        bytes.truncate(bytes.len() - 3); // tear inside the second batch
        fs::write(&path, bytes).unwrap();

        let (batches, end) = read_all(&path);
        assert_eq!(batches.len(), 1, "first batch is intact");
        assert_eq!(end, FileEnd::Damaged { valid_bytes: valid });
    }

    #[test]
    fn missing_batch_end_is_uncredited() {
        // Data records without their marker: the fsync covering this batch
        // never completed, so nothing in it was ever ACKed.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-000001.log");
        let mut bytes = Vec::new();
        data_record("orphan", 1).encode_to(&mut bytes).unwrap();
        fs::write(&path, bytes).unwrap();

        let (batches, end) = read_all(&path);
        assert!(batches.is_empty());
        assert_eq!(end, FileEnd::Damaged { valid_bytes: 0 });
    }

    #[test]
    fn crc_flip_stops_credit_at_first_batch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-000001.log");
        let b1 = encode_batch(&[data_record("alpha", 1)]);
        let b2 = encode_batch(&[data_record("beta", 1)]);
        let mut bytes = [b1, b2].concat();
        bytes[14] ^= 0xFF; // inside the first record's payload → CRC mismatch
        fs::write(&path, bytes).unwrap();

        let (batches, end) = read_all(&path);
        assert!(batches.is_empty(), "credit stops at the first damage");
        assert_eq!(end, FileEnd::Damaged { valid_bytes: 0 });
    }

    #[test]
    fn trailing_garbage_after_clean_batch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-000001.log");
        let b1 = encode_batch(&[data_record("alpha", 1)]);
        let valid = b1.len() as u64;
        let mut bytes = b1;
        bytes.extend_from_slice(b"junk");
        fs::write(&path, bytes).unwrap();

        let (batches, end) = read_all(&path);
        assert_eq!(batches.len(), 1);
        assert_eq!(end, FileEnd::Damaged { valid_bytes: valid });
    }

    #[test]
    fn live_writer_files_are_discovered() {
        // Round-trip against the real writer: whatever WalWriter creates,
        // discovery must list, in creation order.
        use crate::data_plane::wal::{WalStorage, WalWriter};

        let dir = tempfile::tempdir().unwrap();
        let mut wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        wal.buf().extend_from_slice(b"data");
        wal.flush_batch().unwrap();

        let files = discover_wal_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].seq, 1);
    }
}
