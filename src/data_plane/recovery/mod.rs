//! Crash recovery: rebuild local state from disk before serving.
//!
//! Design: `diagrams/data-plane/d5_crash_recovery.md`. Commit-by-commit
//! plan: `diagrams/data-plane/d5_implementation_plan.md`.
//!
//! Staged module: pieces land bottom-up (scanners → replay → orchestrator)
//! and stay unused by the running system until the orchestrator wires them
//! into node startup.

pub(crate) mod index_rebuild;
pub(crate) mod inventory;
pub(crate) mod orphan;
pub(crate) mod replay;
pub(crate) mod segment_scan;
pub(crate) mod wal_scan;
use self::index_rebuild::rebuild_sparse_entries;
use self::inventory::{LocalInventory, RecoveryOutput};
use self::replay::ReplayWriter;
use self::segment_scan::RecoveredSegments;
use self::wal_scan::{ScanError, WalScanner};
use crate::data_plane::sparse_index::SparseIndex;
use crate::data_plane::states::segment::record::RoutingHeader;
use std::io;
use std::path::PathBuf;

/// Fatal recovery failure. WAL *corruption* is deliberately not a variant: it is
/// not fatal — `run` stops replay at the last verified batch and recovers what
/// was already durable. Only genuine I/O failures abort recovery.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RecoveryError {
    #[error("recovery I/O failure: {0}")]
    Io(#[from] std::io::Error),
}

/// Runs crash recovery and returns the verified local inventory.
/// It is a small type-state pipeline so the order is enforced by the compiler, not by convention:
///
// ! The safety-critical edge is that the WAL must not be deleted before the segment files are fsynced.
// ! ```text
// ! Replaying --replay_wal--> Durable --commit--> RecoveryOutput
// ! ```
// ! `commit` is the only thing that deletes the WAL - lives on [`Durable`], and the only way to reach `Durable` is `replay_wal`,
// ! which fsyncs via `ReplayWriter::finish`. So "delete only after durable" is a compile-time fact, and each state has a narrow API: `Replaying` cannot
// ! delete, `Durable` cannot replay.
pub(crate) fn run(
    data_dir: PathBuf,
    index: &dyn SparseIndex,
) -> Result<RecoveryOutput, RecoveryError> {
    Ok(Replaying::start(data_dir)?.replay_wal()?.commit(index)?)
}

struct Replaying {
    data_dir: PathBuf,
    writer: ReplayWriter,
    scanner: WalScanner,
}

impl Replaying {
    fn start(data_dir: PathBuf) -> io::Result<Self> {
        let recovered = RecoveredSegments::scan_data_dir(&data_dir)?;
        let writer = ReplayWriter::new(data_dir.clone(), recovered);
        let scanner = WalScanner::open(&data_dir)?;
        Ok(Self {
            data_dir,
            writer,
            scanner,
        })
    }

    /// Replays every WAL batch into the segment files (skipping duplicates) and
    /// fsyncs them via [`ReplayWriter::finish`] — only after which is the WAL
    /// deletable.
    // ! WAL corruption is not fatal: it stops replay at the last verified batch
    // ! and proceeds with what was already durable.
    fn replay_wal(mut self) -> io::Result<Durable> {
        loop {
            match self.scanner.next_batch() {
                Ok(Some(batch)) => {
                    for record in &batch.records {
                        // Fails only if a record that already passed the WAL CRC
                        // has a payload shorter than the 36-byte routing header.
                        let (header, entry_data) =
                            RoutingHeader::split_wal_payload(&record.payload)?;
                        // Fails only on a real disk error.
                        self.writer.replay(&header, entry_data)?;
                    }
                }
                Ok(None) => break,
                Err(ScanError::Corrupt { file, valid_bytes }) => {
                    // Expected crash artifact (torn / CRC-bad tail) — the only
                    // "recover what you can" case.
                    tracing::warn!(
                        file = %file.display(),
                        valid_bytes,
                        "WAL corruption: stopping replay, keeping what was verified before it",
                    );
                    break;
                }
                // Genuine I/O failure — fatal.
                Err(ScanError::Io(e)) => return Err(e),
            }
        }

        let recovered = self.writer.finish()?;
        Ok(Durable {
            data_dir: self.data_dir,
            scanner: self.scanner,
            recovered,
        })
    }
}

struct Durable {
    data_dir: PathBuf,
    scanner: WalScanner,
    recovered: RecoveredSegments,
}

impl Durable {
    /// Re-indexes the replayed suffix, freezes the cursors into a
    /// [`LocalInventory`], then DELETEs the now-superseded WAL. The fresh WAL is
    /// the serving side's job (commit 14), leaving the dir empty in between — an
    /// empty WAL just means nothing to replay.
    fn commit(self, index: &dyn SparseIndex) -> io::Result<RecoveryOutput> {
        // The sparse index is derived, and recovered segments are served only as
        // cold reads — which depend on it. Rebuild each one from its now-final,
        // fsynced segment file rather than trusting whatever index survived the
        // crash; recovery already read these files to find the cursors.
        for (key, _last) in self.recovered.cursors() {
            let Some(start_offset) = self.recovered.start_entry_id(&key) else {
                continue;
            };
            let path = key.file_path(&self.data_dir, start_offset);
            index.put_batch(rebuild_sparse_entries(&path, key)?)?;
        }

        let inventory = LocalInventory::from_recovered(&self.recovered);
        self.scanner.delete_files()?;
        Ok(RecoveryOutput {
            inventory,
            data_dir: self.data_dir,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use bytes::Bytes;

    use super::segment_scan::scan_segment_file;
    use super::*;
    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
    use crate::data_plane::SegmentKey;
    use crate::data_plane::wal::{WalRecord, WalStorage, WalWriter};

    /// One bare-payload `(Data, BatchEnd)` batch — the segment-file framing the
    /// checkpoint worker writes (no routing header).
    fn seg_batch(payload: &str, record_count: u32) -> Vec<u8> {
        let mut buf = Vec::new();
        WalRecord::data(Bytes::copy_from_slice(payload.as_bytes()), record_count)
            .encode_to(&mut buf)
            .unwrap();
        WalRecord::batch_end().encode_to(&mut buf).unwrap();
        buf
    }

    /// One WAL data record: a routing header followed by the bare payload,
    /// exactly as the live produce path (`tracker.rs`) stages it.
    fn wal_data(key: SegmentKey, entry_id: u64, record_count: u32, payload: &str) -> WalRecord {
        let wal_payload =
            RoutingHeader::new(key, entry_id, record_count).build_wal_payload(payload.as_bytes());
        WalRecord::data(wal_payload, record_count)
    }

    /// Encodes `records` followed by a batch-end marker — one fsynced WAL batch.
    fn wal_batch(records: &[WalRecord]) -> Vec<u8> {
        let mut buf = Vec::new();
        for record in records {
            record.encode_to(&mut buf).unwrap();
        }
        WalRecord::batch_end().encode_to(&mut buf).unwrap();
        buf
    }

    fn write_segment(data_dir: &Path, key: SegmentKey, start_offset: u64, bytes: &[u8]) {
        let path = key.file_path(data_dir, start_offset);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    fn write_wal(data_dir: &Path, seq: u64, bytes: &[u8]) {
        let wal_dir = data_dir.join("wal");
        fs::create_dir_all(&wal_dir).unwrap();
        fs::write(wal_dir.join(format!("wal-{seq:06}.log")), bytes).unwrap();
    }

    fn open_db() -> (tempfile::TempDir, rocksdb::DB) {
        let dir = tempfile::tempdir().unwrap();
        let db = rocksdb::DB::open_default(dir.path()).unwrap();
        (dir, db)
    }

    fn last_id(data_dir: &Path, key: SegmentKey, start_offset: u64) -> Option<u64> {
        scan_segment_file(&key.file_path(data_dir, start_offset))
            .unwrap()
            .last_entry_id
    }

    #[test]
    fn runs_full_pipeline_and_builds_inventory() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let a = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));
        let b = SegmentKey::new(TopicId(1), RangeId(1), SegmentId(0));

        // Checkpointed prefix already on disk: entry 0 of each segment.
        write_segment(&data_dir, a, 0, &seg_batch("a0", 1));
        write_segment(&data_dir, b, 0, &seg_batch("b0", 1));

        // WAL re-presents the checkpointed entries (overlap → skipped) plus the
        // un-checkpointed suffix, interleaved across the two segments.
        write_wal(
            &data_dir,
            1,
            &wal_batch(&[
                wal_data(a, 0, 1, "a0"),
                wal_data(b, 0, 1, "b0"),
                wal_data(a, 1, 1, "a1"),
                wal_data(b, 1, 1, "b1"),
                wal_data(a, 2, 1, "a2"),
            ]),
        );

        let (_db_dir, db) = open_db();
        let output = run(data_dir.clone(), &db).unwrap();

        // Inventory reflects the post-replay cursors.
        assert_eq!(output.inventory.get(&a), Some(2)); // a0, a1, a2
        assert_eq!(output.inventory.get(&b), Some(1)); // b0, b1
        assert_eq!(output.data_dir, data_dir);

        // The segment files actually gained the replayed suffix.
        assert_eq!(last_id(&data_dir, a, 0), Some(2));
        assert_eq!(last_id(&data_dir, b, 0), Some(1));
    }

    #[test]
    fn re_running_recovery_converges() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let a = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));
        write_segment(&data_dir, a, 0, &seg_batch("a0", 1));
        write_wal(
            &data_dir,
            1,
            &wal_batch(&[
                wal_data(a, 0, 1, "a0"),
                wal_data(a, 1, 1, "a1"),
                wal_data(a, 2, 1, "a2"),
            ]),
        );

        let (_db_dir, db) = open_db();
        run(data_dir.clone(), &db).unwrap();
        let after_first = fs::read(a.file_path(&data_dir, 0)).unwrap();

        // Commit 13 deletes the WAL after the first run, so the second run finds
        // an empty WAL dir and rebuilds the inventory from the segment files
        // alone — still byte-identical, still cursor 2.
        let output = run(data_dir.clone(), &db).unwrap();
        assert_eq!(fs::read(a.file_path(&data_dir, 0)).unwrap(), after_first);
        assert_eq!(output.inventory.get(&a), Some(2));
    }

    #[test]
    fn corruption_is_not_fatal_and_keeps_the_verified_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let a = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));

        // Earlier file: a clean batch (entry 0) then a torn one (entry 1).
        // Damage in a non-last file is corruption, not a tolerable torn tail.
        let intact = wal_batch(&[wal_data(a, 0, 1, "a0")]);
        let mut damaged = wal_batch(&[wal_data(a, 1, 1, "a1")]);
        damaged.truncate(damaged.len() - 3);
        write_wal(&data_dir, 1, &[intact, damaged].concat());
        write_wal(&data_dir, 2, &wal_batch(&[wal_data(a, 2, 1, "a2")]));

        let (_db_dir, db) = open_db();
        // Recovery returns Ok, having recovered only the pre-corruption entry.
        let output = run(data_dir.clone(), &db).unwrap();
        assert_eq!(output.inventory.get(&a), Some(0));
        assert_eq!(last_id(&data_dir, a, 0), Some(0));
    }

    #[test]
    fn recovery_clears_the_wal_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let a = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));
        write_wal(&data_dir, 1, &wal_batch(&[wal_data(a, 0, 1, "a0")]));
        write_wal(&data_dir, 2, &wal_batch(&[wal_data(a, 1, 1, "a1")]));

        let (_db_dir, db) = open_db();
        run(data_dir.clone(), &db).unwrap();

        // Every discovered WAL file is deleted; the dir is left empty (the fresh
        // WAL is the serving side's job, commit 14).
        let remaining: Vec<_> = fs::read_dir(data_dir.join("wal"))
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert!(remaining.is_empty(), "WAL dir not empty: {remaining:?}");
        // The data itself survived in the segment file.
        assert_eq!(last_id(&data_dir, a, 0), Some(1));
    }

    #[test]
    fn converges_after_a_partial_wal_deletion() {
        // Simulates a crash mid-deletion: the segment already holds entries 0 and
        // 1, one old WAL file was already deleted, and only the survivor remains.
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let a = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));
        write_segment(
            &data_dir,
            a,
            0,
            &[seg_batch("a0", 1), seg_batch("a1", 1)].concat(),
        );
        // The survivor re-presents entry 1 — already on disk, so it dedups.
        write_wal(&data_dir, 2, &wal_batch(&[wal_data(a, 1, 1, "a1")]));
        let before = fs::read(a.file_path(&data_dir, 0)).unwrap();

        let (_db_dir, db) = open_db();
        let output = run(data_dir.clone(), &db).unwrap();

        // Zero appends (the survivor dedups), the segment is byte-identical, and
        // the already-deleted half's data was safe in the segment all along.
        assert_eq!(fs::read(a.file_path(&data_dir, 0)).unwrap(), before);
        assert_eq!(output.inventory.get(&a), Some(1));
        assert!(fs::read_dir(data_dir.join("wal")).unwrap().next().is_none());
    }

    #[test]
    fn recovery_rebuilds_the_sparse_index_from_segment_files() {
        use crate::data_plane::recovery::index_rebuild::rebuild_sparse_entries;
        use crate::data_plane::sparse_index::{INDEX_INTERVAL_ENTRIES, SparseIndex};

        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let a = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));

        // A checkpointed segment spanning more than one index interval (so a full
        // rebuild yields a base AND an interior anchor), with no WAL and an empty
        // index — recovery rebuilds the index from the segment file.
        let n = INDEX_INTERVAL_ENTRIES;
        let body: Vec<u8> = (0..=n + 2)
            .flat_map(|i| seg_batch(&format!("e{i}"), 1))
            .collect();
        write_segment(&data_dir, a, 0, &body);

        let (_db_dir, db) = open_db();
        let output = run(data_dir.clone(), &db).unwrap();
        assert_eq!(output.inventory.get(&a), Some(n + 2));

        // The rebuilt index resolves byte-identically to a from-scratch rebuild for
        // every id — in particular the interior anchor resolves, instead of
        // collapsing to the byte-0 fallback that would mislabel a cold read.
        let (_full_dir, full) = open_db();
        full.put_batch(rebuild_sparse_entries(&a.file_path(&data_dir, 0), a).unwrap())
            .unwrap();
        for id in 0..=n + 2 {
            assert_eq!(db.seek_index(a, id), full.seek_index(a, id));
        }
    }

    // ---------------------------------------------------------------
    // Crash simulation: produce through the real WAL writer,
    // "kill" the process at a chosen point, recover, and assert the recovered
    // state equals exactly what was ACKed — no more, no less.
    // ---------------------------------------------------------------

    /// Produces entries through the real `WalWriter` — one fsynced batch per
    /// inner slice, exactly like the live produce path — then drops it. Dropping
    /// with no graceful shutdown or WAL cleanup *is* the crash: the fsynced files
    /// stay on disk as they were the instant the process died.
    fn produce_and_crash(data_dir: &Path, batches: &[&[(SegmentKey, u64, &str)]]) {
        let mut wal = WalWriter::new(data_dir.to_path_buf()).unwrap();
        for batch in batches {
            for &(key, entry_id, payload) in *batch {
                let wp = RoutingHeader::new(key, entry_id, 1).build_wal_payload(payload.as_bytes());
                WalRecord::data(wp, 1).encode_to(wal.buf()).unwrap();
            }
            WalRecord::batch_end().encode_to(wal.buf()).unwrap();
            wal.flush_batch().unwrap();
        }
    }

    /// Appends a half-written record to the single WAL file — bytes that were in
    /// flight when the crash struck and never reached their fsync.
    fn append_torn_tail(data_dir: &Path, key: SegmentKey, entry_id: u64) {
        let mut torn = wal_batch(&[wal_data(key, entry_id, 1, "torn")]);
        torn.truncate(torn.len() - 4);
        let path = data_dir.join("wal").join("wal-000001.log");
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(&mut f, &torn).unwrap();
    }

    #[test]
    fn crash_mid_batch_recovers_acked_and_drops_the_torn_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let a = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));

        // ACKed: entries 0 and 1 (two fsynced batches). Then a torn write of 2
        // that the crash interrupted before its fsync.
        produce_and_crash(&data_dir, &[&[(a, 0, "a0")], &[(a, 1, "a1")]]);
        append_torn_tail(&data_dir, a, 2);

        let (_db, db) = open_db();
        let output = run(data_dir.clone(), &db).unwrap();

        // Exactly the ACKed prefix survives; the never-fsynced entry 2 is gone.
        assert_eq!(output.inventory.get(&a), Some(1));
        assert_eq!(last_id(&data_dir, a, 0), Some(1));
    }

    #[test]
    fn crash_after_checkpoint_recovers_the_uncheckpointed_suffix() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let a = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));

        // The checkpoint worker had flushed entry 0 to the segment file...
        write_segment(&data_dir, a, 0, &seg_batch("a0", 1));
        // ...but the WAL (entries 0..=2, all ACKed) was not yet deleted at the crash.
        produce_and_crash(&data_dir, &[&[(a, 0, "a0"), (a, 1, "a1"), (a, 2, "a2")]]);

        let (_db, db) = open_db();
        let output = run(data_dir.clone(), &db).unwrap();

        // Entry 0 dedups against the checkpoint; 1 and 2 are replayed from the WAL.
        assert_eq!(output.inventory.get(&a), Some(2));
        assert_eq!(last_id(&data_dir, a, 0), Some(2));
        assert!(fs::read_dir(data_dir.join("wal")).unwrap().next().is_none());
    }

    #[test]
    fn crash_mid_recovery_truncates_torn_segment_then_replays() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let a = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));

        // The WAL holds ACKed entries 0 and 1.
        produce_and_crash(&data_dir, &[&[(a, 0, "a0"), (a, 1, "a1")]]);
        // A previous recovery had begun appending to the segment but crashed
        // mid-write: entry 0 is complete, followed by a half-written fragment.
        let mut seg = seg_batch("a0", 1);
        seg.extend_from_slice(b"torn-half-written-entry");
        write_segment(&data_dir, a, 0, &seg);

        let (_db, db) = open_db();
        let output = run(data_dir.clone(), &db).unwrap();

        // The torn segment tail is truncated to the verified prefix, then entry 1
        // is re-replayed from the WAL — converging to the full ACKed sequence.
        assert_eq!(output.inventory.get(&a), Some(1));
        assert_eq!(last_id(&data_dir, a, 0), Some(1));
    }
}
