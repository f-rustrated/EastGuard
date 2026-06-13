//! Segment file scanning for crash recovery.
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use crate::control_plane::metadata::{RangeId, TopicId};
use crate::data_plane::SegmentKey;
use crate::data_plane::parse_segment_file;
use crate::data_plane::states::segment::record::RoutingHeader;
use crate::data_plane::wal::{WalRecord, WalRecordType};

/// The CRC-verified prefix of one segment file.
///
/// Built only by [`scan_segment_file`]; callers read the fields, never set
/// them — the pair always reflects one scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // staged: first consumed by the cursor map and replay
pub(crate) struct SegmentScan {
    /// The segment's first entry id, parsed from its filename. The replay
    /// appender uses it to build the file path and as the contiguity base.
    pub(crate) start_offset: u64,
    /// Highest entry id covered by a complete batch, or `None` when no
    /// complete batch survived (empty, missing, or torn-from-the-start file).
    pub(crate) last_entry_id: Option<u64>,
    /// Byte offset where credit stops — the end of the last complete batch.
    /// Replay truncates the file to this length before appending, so a torn
    /// tail is overwritten rather than appended after.
    pub(crate) valid_len: u64,
}

/// Whether a replayed WAL record should be appended to its segment file or
/// skipped as already present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // staged: consumed by the replay appender
pub(crate) enum Decision {
    /// The segment file already holds this entry (id ≤ its verified cursor).
    Skip,
    /// The entry is beyond the verified prefix, or the segment has no local
    /// data at all — append it.
    Append,
}

/// Every locally-scanned segment, keyed by [`SegmentKey`] with its verified
/// [`SegmentScan`] — the output of walking the data dir and the input to
/// replay. `decide` reads each segment's cursor; the appender reads
/// `valid_len` to truncate a damaged tail before its first write.
#[derive(Debug, Default)]
#[allow(dead_code)] // staged: built by scan_data_dir, consumed by replay, finalized as the inventory (commit 11)
pub(crate) struct RecoveredSegments(HashMap<SegmentKey, SegmentScan>);

impl RecoveredSegments {
    /// Every `{topic}/{range}/` segment file is scanned and keyed by its
    /// [`SegmentKey`] — the inverse of [`SegmentKey::file_path`]'s layout.
    ///
    /// Tolerant of junk so recovery never aborts on foreign content:
    /// - a missing `data_dir` yields an empty map;
    /// - the `wal/` sibling and any non-id directories are skipped (`debug!`);
    /// - a file whose name is not `{segment_id}-{start_offset}.seg` is skipped (`warn!`).
    ///
    /// Only genuine I/O failures and unreadable real segments propagate.
    #[allow(dead_code)]
    pub(crate) fn scan_data_dir(data_dir: &Path) -> io::Result<Self> {
        let mut cursors = HashMap::new();
        for (topic_id, topic_path) in numeric_child_dirs(data_dir)? {
            for (range_id, range_path) in numeric_child_dirs(&topic_path)? {
                for entry in fs::read_dir(&range_path)? {
                    let path = entry?.path();
                    let segment_id = match parse_segment_file(&path) {
                        Ok((segment_id, _start_offset)) => segment_id,
                        Err(e) => {
                            tracing::warn!(
                                file = %path.display(),
                                error = %e,
                                "skipping non-segment file in data dir"
                            );
                            continue;
                        }
                    };
                    let key = SegmentKey::new(TopicId(topic_id), RangeId(range_id), segment_id);
                    cursors.insert(key, scan_segment_file(&path)?);
                }
            }
        }
        Ok(RecoveredSegments(cursors))
    }

    /// Skip if the segment file already holds `header`'s record (its id is at or
    /// below the verified cursor); append otherwise — including every record of
    /// a segment that is absent or has no verified prefix.
    #[allow(dead_code)]
    pub(crate) fn decide(&self, header: &RoutingHeader) -> Decision {
        match self
            .0
            .get(&header.segment_key())
            .and_then(|scan| scan.last_entry_id)
        {
            Some(cursor) if header.entry_id <= cursor => Decision::Skip,
            _ => Decision::Append,
        }
    }

    pub(crate) fn valid_len(&self, key: &SegmentKey) -> u64 {
        self.0.get(key).map_or(0, |scan| scan.valid_len)
    }

    pub(crate) fn start_offset(&self, key: &SegmentKey) -> Option<u64> {
        self.0.get(key).map(|scan| scan.start_offset)
    }

    /// Records that `entry_id` was just appended to `key`'s segment file,
    /// advancing its cursor.
    ///
    /// Enforces the contiguous-prefix invariant: appends are gapless, so `entry_id` must be exactly one past the cursor
    /// or the segment's `start_offset` for the first append.
    ///
    /// A segment absent from the map is inserted, taking `entry_id` as its base.
    #[allow(dead_code)]
    pub(crate) fn advance(&mut self, key: SegmentKey, entry_id: u64) {
        let scan = self.0.entry(key).or_insert(SegmentScan {
            start_offset: entry_id,
            last_entry_id: None,
            valid_len: 0,
        });
        let expected = scan
            .last_entry_id
            .map_or(scan.start_offset, |last| last + 1);
        debug_assert_eq!(
            entry_id, expected,
            "non-contiguous replay append to {key:?}: expected {expected}, got {entry_id}"
        );
        scan.last_entry_id = Some(entry_id);

        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    #[cfg(any(test, debug_assertions))]
    fn assert_invariants(&self) {
        for (key, scan) in &self.0 {
            if let Some(last) = scan.last_entry_id {
                debug_assert!(
                    last >= scan.start_offset,
                    "{key:?}: cursor {last} precedes start_offset {}",
                    scan.start_offset
                );
            }
        }
    }
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
                start_offset,
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
        start_offset,
        last_entry_id,
        valid_len,
    })
}

/// Immediate child directories of `dir` whose names parse as a `u64` (topic or
/// range ids). A missing `dir` is an empty list; non-directory entries and
/// non-numeric names (e.g. the `wal/` sibling) are skipped with a `debug!`.
fn numeric_child_dirs(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let read_dir = match fs::read_dir(dir) {
        Ok(read_dir) => read_dir,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut dirs = Vec::new();
    for entry in read_dir {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_dir = entry.file_type()?.is_dir();
        match name.parse::<u64>() {
            Ok(id) if is_dir => dirs.push((id, entry.path())),
            _ => tracing::debug!(entry = %name, "skipping non-id entry in data dir"),
        }
    }
    Ok(dirs)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use bytes::Bytes;

    use super::*;
    use crate::control_plane::metadata::SegmentId;

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
        assert_eq!(scan.start_offset, 100);
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

    /// Creates parent dirs then writes a segment file at `path` (built from
    /// `SegmentKey::file_path`), matching the layout recovery walks.
    fn write_segment_file(path: &Path, bytes: &[u8]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn scan_data_dir_walks_nested_layout() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path();

        let k1 = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));
        let k2 = SegmentKey::new(TopicId(1), RangeId(2), SegmentId(5));
        let k3 = SegmentKey::new(TopicId(7), RangeId(0), SegmentId(3));
        write_segment_file(
            &k1.file_path(data_dir, 0),
            &encode_segment(&[("a", 1), ("b", 1)]),
        );
        write_segment_file(&k2.file_path(data_dir, 100), &encode_segment(&[("c", 1)]));
        write_segment_file(
            &k3.file_path(data_dir, 50),
            &encode_segment(&[("d", 1), ("e", 1), ("f", 1)]),
        );

        let recovered = RecoveredSegments::scan_data_dir(data_dir).unwrap();
        assert_eq!(recovered.0.len(), 3);
        assert_eq!(recovered.0[&k1].last_entry_id, Some(1)); // 0, 1
        assert_eq!(recovered.0[&k2].last_entry_id, Some(100)); // 100
        assert_eq!(recovered.0[&k3].last_entry_id, Some(52)); // 50, 51, 52
    }

    #[test]
    fn scan_data_dir_skips_foreign_entries() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path();

        let real = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));
        write_segment_file(&real.file_path(data_dir, 0), &encode_segment(&[("a", 1)]));

        // A WAL sibling dir, a non-id topic dir, and a foreign file in a real
        // range dir — none should abort the walk or land in the map.
        fs::create_dir_all(data_dir.join("wal")).unwrap();
        fs::write(data_dir.join("wal").join("wal-000001.log"), b"x").unwrap();
        fs::create_dir_all(data_dir.join("not-a-topic")).unwrap();
        fs::write(data_dir.join("1").join("0").join("notes.txt"), b"x").unwrap();

        let recovered = RecoveredSegments::scan_data_dir(data_dir).unwrap();
        assert_eq!(recovered.0.len(), 1);
        assert!(recovered.0.contains_key(&real));
    }

    #[test]
    fn scan_data_dir_missing_dir_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let recovered = RecoveredSegments::scan_data_dir(&dir.path().join("nonexistent")).unwrap();
        assert!(recovered.0.is_empty());
    }

    fn seg_key() -> SegmentKey {
        SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0))
    }

    fn routing(entry_id: u64) -> RoutingHeader {
        RoutingHeader::new(seg_key(), entry_id, 1)
    }

    fn scanned(start_offset: u64, last_entry_id: Option<u64>, valid_len: u64) -> SegmentScan {
        SegmentScan {
            start_offset,
            last_entry_id,
            valid_len,
        }
    }

    fn recovered_with(entries: &[(SegmentKey, SegmentScan)]) -> RecoveredSegments {
        RecoveredSegments(entries.iter().copied().collect())
    }

    #[test]
    fn decide_appends_beyond_cursor_skips_within() {
        let r = recovered_with(&[(seg_key(), scanned(0, Some(5), 0))]);
        assert_eq!(r.decide(&routing(6)), Decision::Append);
        assert_eq!(r.decide(&routing(5)), Decision::Skip);
        assert_eq!(r.decide(&routing(3)), Decision::Skip);
    }

    #[test]
    fn decide_absent_or_no_prefix_appends_from_zero() {
        // Absent segment → append everything, including entry id 0.
        assert_eq!(
            RecoveredSegments::default().decide(&routing(0)),
            Decision::Append
        );
        // Present but no verified prefix (last_entry_id None) — same as absent.
        let r = recovered_with(&[(seg_key(), scanned(0, None, 0))]);
        assert_eq!(r.decide(&routing(0)), Decision::Append);
    }

    #[test]
    fn decide_cursor_zero_is_distinct_from_absent() {
        let r = recovered_with(&[(seg_key(), scanned(0, Some(0), 0))]);
        assert_eq!(r.decide(&routing(0)), Decision::Skip);
        assert_eq!(r.decide(&routing(1)), Decision::Append);
    }

    #[test]
    fn valid_len_is_the_scans_or_zero_when_absent() {
        let r = recovered_with(&[(seg_key(), scanned(0, Some(2), 128))]);
        assert_eq!(r.valid_len(&seg_key()), 128);
        let absent = SegmentKey::new(TopicId(9), RangeId(0), SegmentId(0));
        assert_eq!(r.valid_len(&absent), 0);
    }

    #[test]
    fn advance_walks_contiguously() {
        let mut r = recovered_with(&[(seg_key(), scanned(0, Some(4), 0))]);
        r.advance(seg_key(), 5);
        r.advance(seg_key(), 6);
        assert_eq!(r.0[&seg_key()].last_entry_id, Some(6));
    }

    #[test]
    fn advance_inserts_an_absent_segment_at_its_base() {
        let mut r = RecoveredSegments::default();
        r.advance(seg_key(), 7); // first record of a never-checkpointed segment
        let scan = r.0[&seg_key()];
        assert_eq!(scan.start_offset, 7);
        assert_eq!(scan.last_entry_id, Some(7));
        r.advance(seg_key(), 8);
        assert_eq!(r.0[&seg_key()].last_entry_id, Some(8));
    }

    #[test]
    #[should_panic(expected = "non-contiguous")]
    fn advance_rejects_a_gap() {
        let mut r = recovered_with(&[(seg_key(), scanned(0, Some(4), 0))]);
        r.advance(seg_key(), 6); // skips 5
    }
}
