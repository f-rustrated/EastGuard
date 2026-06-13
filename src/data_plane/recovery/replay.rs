//! Replay appender: writes the WAL records recovery is missing into their
//! segment files, then advances the cursors.
//!
//! [`RecoveredSegments::decide`] (in `segment_scan.rs`) says whether a replayed
//! record is already on disk; this module does the I/O for the ones that
//! aren't. It opens one appender per segment lazily — truncating any damaged
//! tail to `valid_len` before the first write — and re-encodes each record in
//! the checkpoint's `(Data, BatchEnd)` framing with the **bare** entry payload
//! (the routing header is WAL-only). [`ReplayWriter::finish`] fsyncs every file
//! before the cursors become the inventory: nothing is reported durable until
//! it is on disk.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;

use bytes::Bytes;

use super::segment_scan::{Decision, RecoveredSegments};
use crate::data_plane::SegmentKey;
use crate::data_plane::states::segment::record::RoutingHeader;
use crate::data_plane::wal::WalRecord;

/// Appends the missing suffix of each segment during recovery.
///
/// Holds one open appender per segment (created on first write) and owns the
/// [`RecoveredSegments`] cursors, so dedup ([`RecoveredSegments::decide`]) and
/// advancement stay in sync — duplicates *within* the WAL stream are skipped
/// too, not just WAL-vs-file.
pub(crate) struct ReplayWriter {
    data_dir: PathBuf,
    recovered: RecoveredSegments,
    writers: HashMap<SegmentKey, BufWriter<File>>,
}

impl ReplayWriter {
    pub(crate) fn new(data_dir: PathBuf, recovered: RecoveredSegments) -> Self {
        Self {
            data_dir,
            recovered,
            writers: HashMap::new(),
        }
    }

    /// Replays one WAL data record into its segment file. Returns
    /// [`Decision::Skip`] without writing if the segment already holds the
    /// entry; otherwise appends `entry_data` (the bare payload, no routing
    /// header) as a `(Data, BatchEnd)` batch, advances the cursor, and returns
    /// [`Decision::Append`].
    pub(crate) fn replay(
        &mut self,
        header: &RoutingHeader,
        entry_data: &[u8],
    ) -> io::Result<Decision> {
        if self.recovered.decide(header) == Decision::Skip {
            return Ok(Decision::Skip);
        }
        let key = header.segment_key();
        let writer = self.writer_for(key, header.entry_id)?;
        WalRecord::data(Bytes::copy_from_slice(entry_data), header.record_count)
            .encode_to(writer)?;
        WalRecord::batch_end().encode_to(writer)?;
        self.recovered.advance(key, header.entry_id);
        Ok(Decision::Append)
    }

    /// Flushes and fsyncs every open segment file, then returns the advanced
    /// cursors (which become the local inventory). Nothing replay appended is
    /// durable until this returns `Ok`.
    pub(crate) fn finish(mut self) -> io::Result<RecoveredSegments> {
        for writer in self.writers.values_mut() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        Ok(self.recovered)
    }

    /// The open appender for `key`, created on first use:
    /// builds the path from the segment's `start_offset` (or `first_entry_id` for a segment with no local file — the base of a never-checkpointed segment),
    /// creates parent dirs, and truncates a damaged tail to `valid_len` before seeking to the append point.
    fn writer_for(
        &mut self,
        key: SegmentKey,
        first_entry_id: u64,
    ) -> io::Result<&mut BufWriter<File>> {
        if !self.writers.contains_key(&key) {
            let start_offset = self.recovered.start_offset(&key).unwrap_or(first_entry_id);
            let valid_len = self.recovered.valid_len(&key);
            let path = key.file_path(&self.data_dir, start_offset);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            // `truncate(false)` on purpose: opening must NOT zero an existing segment
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&path)?;
            file.set_len(valid_len)?;
            let mut writer = BufWriter::new(file);
            writer.seek(SeekFrom::Start(valid_len))?;
            self.writers.insert(key, writer);
        }

        Ok(self
            .writers
            .get_mut(&key)
            .expect("writer inserted just above"))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
    use crate::data_plane::recovery::segment_scan::scan_segment_file;

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
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn creates_a_new_segment_file_from_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();

        let mut w = ReplayWriter::new(data_dir.clone(), RecoveredSegments::default());
        assert_eq!(
            w.replay(&RoutingHeader::new(key(), 0, 1), b"alpha")
                .unwrap(),
            Decision::Append
        );
        assert_eq!(
            w.replay(&RoutingHeader::new(key(), 1, 1), b"beta").unwrap(),
            Decision::Append
        );
        w.finish().unwrap();

        // Named from the first record's id (the segment's base).
        let scan = scan_segment_file(&key().file_path(&data_dir, 0)).unwrap();
        assert_eq!(scan.last_entry_id, Some(1)); // 0, 1
    }

    #[test]
    fn appends_the_missing_suffix_and_skips_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        write_segment(
            &key().file_path(&data_dir, 0),
            &[encode_batch("alpha", 1), encode_batch("beta", 1)].concat(),
        );

        let recovered = RecoveredSegments::scan_data_dir(&data_dir).unwrap();
        let mut w = ReplayWriter::new(data_dir.clone(), recovered);
        assert_eq!(
            w.replay(&RoutingHeader::new(key(), 1, 1), b"beta-again")
                .unwrap(),
            Decision::Skip
        );
        assert_eq!(
            w.replay(&RoutingHeader::new(key(), 2, 1), b"gamma")
                .unwrap(),
            Decision::Append
        );
        w.finish().unwrap();

        let scan = scan_segment_file(&key().file_path(&data_dir, 0)).unwrap();
        assert_eq!(scan.last_entry_id, Some(2)); // 0, 1, 2
    }

    #[test]
    fn truncates_a_damaged_tail_before_appending() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let mut bytes = encode_batch("alpha", 1); // verified prefix: entry 0
        bytes.extend_from_slice(b"torn-uncommitted-tail");
        write_segment(&key().file_path(&data_dir, 0), &bytes);

        let recovered = RecoveredSegments::scan_data_dir(&data_dir).unwrap();
        let mut w = ReplayWriter::new(data_dir.clone(), recovered);
        assert_eq!(
            w.replay(&RoutingHeader::new(key(), 1, 1), b"beta").unwrap(),
            Decision::Append
        );
        w.finish().unwrap();

        // The torn tail was truncated to valid_len, so the file re-scans clean.
        let scan = scan_segment_file(&key().file_path(&data_dir, 0)).unwrap();
        assert_eq!(scan.last_entry_id, Some(1)); // 0, 1 — tail gone, beta clean
    }

    /// Scans `data_dir` then replays an interleaved record stream over it,
    /// fsyncing at the end. Each `(segment, entry_id, payload)` stands in for a
    /// WAL record the scanner would hand replay.
    fn replay_stream(data_dir: &Path, stream: &[(SegmentKey, u64, &str)]) {
        let recovered = RecoveredSegments::scan_data_dir(data_dir).unwrap();
        let mut w = ReplayWriter::new(data_dir.to_path_buf(), recovered);
        for &(seg, entry_id, payload) in stream {
            w.replay(&RoutingHeader::new(seg, entry_id, 1), payload.as_bytes())
                .unwrap();
        }
        w.finish().unwrap();
    }

    #[test]
    fn replay_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let a = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));
        let b = SegmentKey::new(TopicId(1), RangeId(1), SegmentId(0));
        // Each segment already has its entry 0 checkpointed on disk.
        write_segment(&a.file_path(&data_dir, 0), &encode_batch("a0", 1));
        write_segment(&b.file_path(&data_dir, 0), &encode_batch("b0", 1));

        // Interleaved across two notional WAL files: the checkpointed entries
        // reappear (overlap), plus new suffix entries.
        let stream = [
            (a, 0u64, "a0"),
            (b, 0, "b0"),
            (a, 1, "a1"),
            (b, 1, "b1"),
            (a, 2, "a2"),
        ];

        replay_stream(&data_dir, &stream);
        // Each segment's prefix is reconstructed contiguously.
        let scan_a = scan_segment_file(&a.file_path(&data_dir, 0)).unwrap();
        let scan_b = scan_segment_file(&b.file_path(&data_dir, 0)).unwrap();
        assert_eq!(scan_a.last_entry_id, Some(2)); // a0, a1, a2
        assert_eq!(scan_b.last_entry_id, Some(1)); // b0, b1

        let a_bytes = fs::read(a.file_path(&data_dir, 0)).unwrap();
        let b_bytes = fs::read(b.file_path(&data_dir, 0)).unwrap();

        // Re-running scan + replay over the same disk changes nothing — every
        // record now dedups against the higher cursors.
        replay_stream(&data_dir, &stream);
        assert_eq!(fs::read(a.file_path(&data_dir, 0)).unwrap(), a_bytes);
        assert_eq!(fs::read(b.file_path(&data_dir, 0)).unwrap(), b_bytes);
    }

    #[test]
    fn overlap_produces_zero_appends() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().to_path_buf();
        let s = key();
        write_segment(
            &s.file_path(&data_dir, 0),
            &[
                encode_batch("e0", 1),
                encode_batch("e1", 1),
                encode_batch("e2", 1),
            ]
            .concat(),
        );
        let before = fs::read(s.file_path(&data_dir, 0)).unwrap();

        let recovered = RecoveredSegments::scan_data_dir(&data_dir).unwrap();
        let mut w = ReplayWriter::new(data_dir.clone(), recovered);
        // The WAL re-presents entries already checkpointed → all skipped.
        for entry_id in 0u64..=2 {
            assert_eq!(
                w.replay(&RoutingHeader::new(s, entry_id, 1), b"dup")
                    .unwrap(),
                Decision::Skip
            );
        }
        w.finish().unwrap();

        assert_eq!(fs::read(s.file_path(&data_dir, 0)).unwrap(), before);
    }
}
