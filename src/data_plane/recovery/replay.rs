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
            let file = OpenOptions::new().create(true).write(true).open(&path)?;
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
}
