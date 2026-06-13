//! Segment file scanning for crash recovery.
use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;

use crate::data_plane::parse_segment_file;
use crate::data_plane::wal::{WalRecord, WalRecordType};

/// The CRC-verified prefix of one segment file.
///
/// Built only by [`scan_segment_file`]; callers read the fields, never set
/// them — the pair always reflects one scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // staged: first consumed by the cursor map (commit 5) and replay (commit 7)
pub(crate) struct SegmentScan {
    /// Highest entry id covered by a complete batch, or `None` when no
    /// complete batch survived (empty, missing, or torn-from-the-start file).
    pub(crate) last_entry_id: Option<u64>,
    /// Byte offset where credit stops — the end of the last complete batch.
    /// Replay truncates the file to this length before appending, so a torn
    /// tail is overwritten rather than appended after.
    pub(crate) valid_len: u64,
}

/// Scans a segment file to find its last verified cursor position.
///
/// Reads the stream of `(Data, BatchEnd)` batches via [`WalRecord::decode_from`],
/// crediting only data records that belong to a fully written batch (one whose
/// `BatchEnd` marker decodes).
///
/// # Partial writes & error handling
/// * The scan stops at the first undecodable point — an incomplete or corrupt
///   record (`UnexpectedEof` or `InvalidData`).
/// * Data records in a half-written trailing batch are ignored: a batch missing
///   its `BatchEnd` never finished its `fsync`. Replay re-appends them from the
///   WAL (deleted only after a checkpoint completes).
///
/// # The base entry id
/// The segment's first entry id (`start_offset`) is parsed from the filename
/// (`{segment_id}-{start_offset}.seg`) — segment records are bare, so the name
/// is the only place the base lives. Entry ids are contiguous, so the last
/// verified id is computed positionally from it.
///
/// # Returns / errors
/// * A missing file is an empty scan (`{ None, 0 }`), not an error — a segment
///   never checkpointed has nothing on disk.
/// * A path whose filename is not a segment name is an `InvalidInput` error.
/// * Genuine I/O failures propagate.
#[allow(dead_code)]
pub(crate) fn scan_segment_file(path: &Path) -> io::Result<SegmentScan> {
    let (_segment_id, start_offset) = parse_segment_file(path)?;
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(SegmentScan {
                last_entry_id: None,
                valid_len: 0,
            });
        }
        Err(e) => return Err(e),
    };
    let mut reader = BufReader::new(file);

    let mut consumed: u64 = 0; // bytes of records decoded so far
    let mut valid_len: u64 = 0; // bytes through the last complete batch
    let mut verified: u64 = 0; // data records in complete batches
    let mut pending: u64 = 0; // data records in the batch currently open

    loop {
        match WalRecord::decode_from(&mut reader) {
            Ok(record) => {
                consumed += record.encoded_size() as u64;
                match record.record_type {
                    WalRecordType::Data => pending += 1,
                    WalRecordType::BatchEnd => {
                        verified += pending;
                        valid_len = consumed;
                        pending = 0;
                    }
                }
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

    // Entry ids are contiguous within a segment, so the last verified id is the
    // base plus the verified count, less one. No complete batch -> no id.
    let last_entry_id = verified.checked_sub(1).map(|n| start_offset + n);
    Ok(SegmentScan {
        last_entry_id,
        valid_len,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use bytes::Bytes;

    use super::*;

    /// Encodes entries the way `CheckpointWorker::process_job` (`checkpoint.rs`)
    /// does: each entry is a bare-payload `Data` record followed by a `BatchEnd`
    /// marker. Kept byte-faithful so a framing change in the real writer breaks
    /// these tests too.
    fn encode_segment(entries: &[(&str, u32)]) -> Vec<u8> {
        let mut buf = Vec::new();
        for &(payload, record_count) in entries {
            WalRecord::data(Bytes::copy_from_slice(payload.as_bytes()), record_count)
                .encode_to(&mut buf)
                .unwrap();
            WalRecord::batch_end().encode_to(&mut buf).unwrap();
        }
        buf
    }

    /// Writes `bytes` to a segment-named file (`{segment_id}-{start_offset}.seg`)
    /// so `scan_segment_file` can parse the base from the path.
    fn write_named(
        segment_id: u64,
        start_offset: u64,
        bytes: &[u8],
    ) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("{segment_id}-{start_offset}.seg"));
        fs::write(&path, bytes).unwrap();
        (dir, path)
    }

    #[test]
    fn clean_file_credits_every_entry() {
        let bytes = encode_segment(&[("alpha", 1), ("beta", 2), ("gamma", 1)]);
        let len = bytes.len() as u64;
        let (_dir, path) = write_named(3, 100, &bytes);

        let scan = scan_segment_file(&path).unwrap();
        assert_eq!(scan.last_entry_id, Some(102)); // 100, 101, 102
        assert_eq!(scan.valid_len, len);
    }

    #[test]
    fn base_comes_from_the_filename() {
        // Same body, different start_offset in the name ⇒ different last id.
        let bytes = encode_segment(&[("a", 1), ("b", 1)]);
        let (_d0, p0) = write_named(0, 0, &bytes);
        let (_d5, p5) = write_named(0, 500, &bytes);

        assert_eq!(scan_segment_file(&p0).unwrap().last_entry_id, Some(1));
        assert_eq!(scan_segment_file(&p5).unwrap().last_entry_id, Some(501));
    }

    #[test]
    fn empty_file_has_no_verified_id() {
        let (_dir, path) = write_named(7, 42, &[]);
        let scan = scan_segment_file(&path).unwrap();
        assert_eq!(scan.last_entry_id, None);
        assert_eq!(scan.valid_len, 0);
    }

    #[test]
    fn missing_file_is_an_empty_scan_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("9-0.seg"); // valid name, file absent
        let scan = scan_segment_file(&path).unwrap();
        assert_eq!(scan.last_entry_id, None);
        assert_eq!(scan.valid_len, 0);
    }

    #[test]
    fn unparseable_filename_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("garbage.seg"); // no `-{start_offset}`
        fs::write(&path, encode_segment(&[("a", 1)])).unwrap();
        assert!(scan_segment_file(&path).is_err());
    }

    #[test]
    fn truncated_mid_record_keeps_prior_batches() {
        let intact = encode_segment(&[("alpha", 1), ("beta", 1)]);
        let valid = intact.len() as u64;
        let mut bytes = intact;
        bytes.extend_from_slice(&encode_segment(&[("gamma", 1)]));
        bytes.truncate(bytes.len() - 3); // tear inside the third batch
        let (_dir, path) = write_named(2, 0, &bytes);

        let scan = scan_segment_file(&path).unwrap();
        assert_eq!(scan.last_entry_id, Some(1)); // gamma uncredited
        assert_eq!(scan.valid_len, valid);
    }

    #[test]
    fn data_without_batch_end_is_uncredited() {
        // A data record whose BatchEnd never reached disk: the batch's fsync
        // did not complete, so the entry is not durable.
        let intact = encode_segment(&[("alpha", 1)]);
        let valid = intact.len() as u64;
        let mut bytes = intact;
        WalRecord::data(Bytes::from_static(b"orphan"), 1)
            .encode_to(&mut bytes)
            .unwrap(); // no trailing BatchEnd
        let (_dir, path) = write_named(1, 10, &bytes);

        let scan = scan_segment_file(&path).unwrap();
        assert_eq!(scan.last_entry_id, Some(10)); // base 10 + 1 verified - 1
        assert_eq!(scan.valid_len, valid);
    }

    #[test]
    fn crc_flip_stops_credit_at_first_damage() {
        let b1 = encode_segment(&[("alpha", 1)]);
        let valid = b1.len() as u64;
        let b2 = encode_segment(&[("beta", 1)]);
        let mut bytes = [b1, b2].concat();
        // Flip a byte inside the second batch's data payload (header is 13
        // bytes) → CRC mismatch on that record.
        bytes[valid as usize + 14] ^= 0xFF;
        let (_dir, path) = write_named(5, 0, &bytes);

        let scan = scan_segment_file(&path).unwrap();
        assert_eq!(scan.last_entry_id, Some(0)); // only the first batch
        assert_eq!(scan.valid_len, valid);
    }
}
