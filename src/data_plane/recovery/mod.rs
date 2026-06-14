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
pub(crate) mod replay;
pub(crate) mod segment_scan;
pub(crate) mod wal_scan;
use self::inventory::{LocalInventory, RecoveryOutput};
use self::replay::ReplayWriter;
use self::segment_scan::RecoveredSegments;
use self::wal_scan::{ScanError, WalScanner};
use crate::data_plane::sparse_index::SparseIndex;
use crate::data_plane::states::segment::record::RoutingHeader;
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
///
/// This is the non-destructive half: safe on any disk, since it only reads the WAL and appends the records
/// the segment files are missing. Pipeline:
///
/// 1. scan the segment files into a cursor map — what is already durable,
/// 2. replay the WAL's un-checkpointed suffix into them, skipping duplicates,
/// 3. re-index just that appended suffix (full rebuild stays the fallback),
/// 4. freeze the post-replay cursors into a [`LocalInventory`].
///
/// WAL corruption is not fatal: a [`ScanError::Corrupt`] stops replay at the
/// last verified batch, and recovery proceeds with what was already durable —
/// it never aborts.
pub(crate) fn run(
    data_dir: PathBuf,
    index: &dyn SparseIndex,
) -> Result<RecoveryOutput, RecoveryError> {
    let recovered = RecoveredSegments::scan_data_dir(&data_dir)?;
    let mut writer = ReplayWriter::new(data_dir.clone(), recovered);
    let mut scanner = WalScanner::open(&data_dir)?;

    loop {
        match scanner.next_batch() {
            Ok(Some(batch)) => {
                for record in &batch.records {
                    // ! Fail only if a record that already passed the WAL CRC has a payload shorter than the 36-byte header
                    let (header, entry_data) = RoutingHeader::split_wal_payload(&record.payload)?;

                    // ! fails only on a real disk error
                    writer.replay(&header, entry_data)?;
                }
            }
            Ok(None) => break,
            Err(ScanError::Corrupt { file, valid_bytes }) => {
                tracing::warn!(
                    file = %file.display(),
                    valid_bytes,
                    "WAL corruption: stopping replay, keeping what was verified before it",
                );
                // ! expected crash artifact (torn/CRC-bad) this is only "recover... only what you can" case
                break;
            }
            // ! Genuine I/O failure, fatal
            Err(ScanError::Io(e)) => return Err(e.into()),
        }
    }

    let output = writer.finish()?;
    index.put_batch(output.suffix_index.into_vec())?;
    let inventory = LocalInventory::from_recovered(&output.recovered);

    // The WAL's contents are now durable in the segment files (finish fsynced
    // them), so the WAL is superseded — delete it. The fresh WAL is created
    // later by the serving side (DataPlane, commit 14), leaving the dir empty in
    // between (an empty WAL just means nothing to replay).
    scanner.delete_files()?;

    Ok(RecoveryOutput {
        inventory,
        data_dir,
    })
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
    use crate::data_plane::wal::WalRecord;

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
}
