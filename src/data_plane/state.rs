use super::messages::*;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::data_plane::messages::event::DataPlaneEvent;
use crate::data_plane::record::BufferedRecord;

use crate::data_plane::states::segment::tracker::SegmentTracker;
use crate::schedulers::ticker_message::TimerCommand;
#[cfg(test)]
use crate::test_traits::TAssertInvariant;

use super::record::{Record, SegmentKey, SegmentRecordBatch};
use super::timer::DataPlaneTimeoutCallback;
use super::wal::WalStorage;

const BATCH_MAX_RECORDS: usize = 20_000;
const BATCH_MAX_BYTES: usize = 10 * 1024 * 1024; // 10MB
const SEGMENT_SIZE_LIMIT: u64 = 1024 * 1024 * 1024; // 1GB

pub struct DataPlane<W: WalStorage> {
    wal: W,
    segments: BTreeMap<SegmentKey, SegmentTracker>,
    accumulation_buffers: BTreeMap<SegmentKey, Vec<BufferedRecord>>,
    pending_events: Vec<DataPlaneEvent>,
    buffer_record_count: usize,
    buffer_byte_count: usize,
    data_dir: PathBuf,
}

impl<W: WalStorage> DataPlane<W> {
    pub fn new(wal: W, data_dir: PathBuf) -> Self {
        DataPlane {
            wal,
            segments: BTreeMap::new(),
            accumulation_buffers: BTreeMap::new(),
            pending_events: Vec::new(),
            buffer_record_count: 0,
            buffer_byte_count: 0,
            data_dir,
        }
    }

    pub fn take_events(&mut self) -> Vec<DataPlaneEvent> {
        std::mem::take(&mut self.pending_events)
    }

    pub fn process(&mut self, cmd: DataPlaneCommand) {
        match cmd {
            DataPlaneCommand::Produce {
                segment_key,
                records,
                reply,
            } => {
                if !self.segments.contains_key(&segment_key) {
                    let _ = reply.send(ProduceAck::Err("segment not found".into()));
                } else {
                    self.pending_events
                        .push(DataPlaneEvent::ProducePending(reply));
                    self.handle_produce(segment_key, records);
                }
            }
            DataPlaneCommand::SegmentAssignment {
                segment_key,
                replica_set: _,
            } => self.handle_segment_assignment(segment_key),
            DataPlaneCommand::SealSegment { segment_key } => {
                self.handle_seal_segment(segment_key);
            }
            DataPlaneCommand::CheckpointComplete(complete) => {
                self.handle_checkpoint_complete(complete);
            }
            DataPlaneCommand::Timeout(callback) => {
                self.handle_timeout(callback);
            }
        }

        #[cfg(test)]
        self.assert_invariants();
    }

    pub fn handle_produce(&mut self, segment_key: SegmentKey, records: Vec<Bytes>) {
        let Some(tracker) = self.segments.get_mut(&segment_key) else {
            return;
        };

        for user_data in records {
            let offset = tracker.size_bytes;
            let len = user_data.len() as u64;
            self.buffer_byte_count += user_data.len();
            self.buffer_record_count += 1;

            self.accumulation_buffers
                .entry(segment_key)
                .or_default()
                .push(BufferedRecord::new(user_data, segment_key, offset));

            tracker.size_bytes += len;
        }

        if self.should_flush_by_batch_size() {
            self.flush_batch();
        }
    }

    fn handle_timeout(&mut self, callback: DataPlaneTimeoutCallback) {
        match callback {
            DataPlaneTimeoutCallback::PeriodicTick => {
                if self.has_buffered_data() {
                    self.flush_batch();
                }
            }
        }
    }

    fn should_flush_by_batch_size(&self) -> bool {
        self.buffer_record_count >= BATCH_MAX_RECORDS || self.buffer_byte_count >= BATCH_MAX_BYTES
    }

    fn has_buffered_data(&self) -> bool {
        self.buffer_record_count > 0
    }

    fn flush_batch(&mut self) {
        if self.buffer_record_count == 0 {
            return;
        }

        let mut wal_buf = Vec::new();
        let mut per_segment: BTreeMap<SegmentKey, Vec<(u64, Bytes)>> = BTreeMap::new();

        for (key, records) in std::mem::take(&mut self.accumulation_buffers) {
            for rec in records {
                let wal_record = { Record::data(rec.build_wal_payload()) };
                let _ = wal_record.encode_to(&mut wal_buf);

                per_segment
                    .entry(key)
                    .or_default()
                    .push((rec.logical_offset(), rec.user_data));
            }
        }

        let batch_end = Record::batch_end();
        let _ = batch_end.encode_to(&mut wal_buf);

        let lsn = match self.wal.write_batch(&wal_buf) {
            Ok(lsn) => lsn,
            Err(e) => {
                tracing::error!("WAL write failed: {e}");
                self.pending_events
                    .push(DataPlaneEvent::WalBatchFailed(e.to_string()));
                return;
            }
        };

        if let Err(e) = self.wal.fsync() {
            tracing::error!("WAL fsync failed: {e}");
            self.pending_events
                .push(DataPlaneEvent::WalBatchFailed(e.to_string()));
            return;
        }

        for (key, records) in per_segment {
            let Some(tracker) = self.segments.get(&key) else {
                continue;
            };

            let segment_records: Vec<Record> = records
                .iter()
                .map(|(_, data)| Record::data(data.clone()))
                .collect();

            let start_offset = records.first().map(|(o, _)| *o).unwrap_or(0);
            let end_offset = records.last().map(|(o, _)| *o).unwrap_or(0);

            let batch = SegmentRecordBatch {
                records: segment_records,
                start_offset,
                end_offset,
                lsn,
            };

            tracker.publish(batch);

            if tracker.size_bytes >= SEGMENT_SIZE_LIMIT {
                self.pending_events
                    .push(DataPlaneEvent::SubmitCheckpoint(tracker.checkpoint(key)));
            }
        }

        self.pending_events
            .push(DataPlaneEvent::WalBatchComplete { lsn });

        self.buffer_record_count = 0;
        self.buffer_byte_count = 0;

        self.pending_events
            .push(DataPlaneEvent::Timer(TimerCommand::ResetPeriodic));
    }

    fn handle_segment_assignment(&mut self, segment_key: SegmentKey) {
        if self.segments.contains_key(&segment_key) {
            return;
        }

        let (shard_group_id, range_id, segment_id) = segment_key;
        let seg_dir = self
            .data_dir
            .join(shard_group_id.0.to_string())
            .join(range_id.0.to_string());
        let seg_path = seg_dir.join(format!("{}.seg", segment_id.0));

        self.segments
            .insert(segment_key, SegmentTracker::new(seg_path));
    }

    // TODO D3: after final checkpoint completes, remove SegmentTracker entry
    fn handle_seal_segment(&mut self, segment_key: SegmentKey) {
        if let Some(tracker) = self.segments.get(&segment_key) {
            self.pending_events.push(DataPlaneEvent::SubmitCheckpoint(
                tracker.checkpoint(segment_key),
            ));
        }
    }

    fn handle_checkpoint_complete(&mut self, complete: CheckpointComplete) {
        if let Some(tracker) = self.segments.get_mut(&complete.segment_key) {
            tracker.advance_checkpoint(complete.checkpointed_lsn);
        }

        let watermark = self.compute_checkpoint_watermark();
        if watermark > 0 {
            self.wal.delete_below(watermark);
        }
    }

    fn compute_checkpoint_watermark(&self) -> u64 {
        self.segments
            .values()
            .map(|t| t.checkpoint_lsn())
            .min()
            .unwrap_or(0)
    }

    // Sort segments by most uncheckpointed batches, pick the top one, submit a checkpoint for it.
    // uncheckpointed() is used to sort candidates and segment with the most uncheckpointed batches gets checkpointed first to relieve cache pressure
    // TODO not wired up yet
    pub fn submit_checkpoint_for_pressure(&mut self) {
        let mut candidates: Vec<(SegmentKey, u64)> = self
            .segments
            .iter()
            .map(|(key, tracker)| (*key, tracker.uncheckpointed()))
            .collect();

        candidates.sort_by_key(|b| std::cmp::Reverse(b.1));

        if let Some((key, _)) = candidates.first()
            && let Some(tracker) = self.segments.get(key)
        {
            self.pending_events
                .push(DataPlaneEvent::SubmitCheckpoint(tracker.checkpoint(*key)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clusters::metadata::{RangeId, SegmentId};
    use crate::clusters::swims::ShardGroupId;
    use crate::data_plane::wal::WalWriter;
    use tokio::sync::oneshot;

    impl<T: WalStorage> TAssertInvariant for DataPlane<T> {
        fn assert_invariants(&self) {
            self.wal.assert_invariants();

            for tracker in self.segments.values() {
                tracker.assert_invariants();
            }

            let actual_record_count: usize =
                self.accumulation_buffers.values().map(|v| v.len()).sum();
            assert_eq!(
                self.buffer_record_count, actual_record_count,
                "buffer_record_count ({}) != actual accumulated records ({actual_record_count})",
                self.buffer_record_count
            );

            let actual_byte_count: usize = self
                .accumulation_buffers
                .values()
                .flat_map(|v| v.iter())
                .map(|r| r.user_data.len())
                .sum();
            assert_eq!(
                self.buffer_byte_count, actual_byte_count,
                "buffer_byte_count ({}) != actual buffered bytes ({actual_byte_count})",
                self.buffer_byte_count
            );

            for key in self.accumulation_buffers.keys() {
                assert!(
                    self.segments.contains_key(key),
                    "accumulation buffer for unknown segment {key:?}"
                );
            }
        }
    }

    fn test_key() -> SegmentKey {
        (ShardGroupId(1), RangeId(0), SegmentId(0))
    }

    fn make_data_plane(dir: &tempfile::TempDir) -> DataPlane<WalWriter> {
        let wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        DataPlane::new(wal, dir.path().to_path_buf())
    }

    #[test]
    fn has_segment_returns_false_for_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let dp = make_data_plane(&dir);
        assert!(!dp.segments.contains_key(&test_key()));
    }

    #[test]
    fn has_segment_returns_true_after_assignment() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.process(DataPlaneCommand::SegmentAssignment {
            segment_key: test_key(),
            replica_set: vec![],
        });
        assert!(dp.segments.contains_key(&test_key()));
    }

    #[test]
    fn produce_to_unknown_segment_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let (tx, mut rx) = oneshot::channel();

        dp.process(DataPlaneCommand::Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("hello")],
            reply: tx,
        });

        assert!(!dp.has_buffered_data());
        let ack = rx.try_recv().unwrap();
        assert!(matches!(ack, ProduceAck::Err(_)));
    }

    #[test]
    fn produce_emits_pending_then_ok_on_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.process(DataPlaneCommand::SegmentAssignment {
            segment_key: test_key(),
            replica_set: vec![],
        });

        let (tx, _rx) = oneshot::channel();
        dp.process(DataPlaneCommand::Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });

        let produce_events = dp.take_events();
        assert!(
            produce_events
                .iter()
                .any(|e| matches!(e, DataPlaneEvent::ProducePending(_)))
        );

        dp.process(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::PeriodicTick,
        ));

        let flush_events = dp.take_events();
        assert!(
            flush_events
                .iter()
                .any(|e| matches!(e, DataPlaneEvent::WalBatchComplete { .. }))
        );
    }

    #[test]
    fn produce_tracks_size_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.process(DataPlaneCommand::SegmentAssignment {
            segment_key: test_key(),
            replica_set: vec![],
        });

        dp.handle_produce(
            test_key(),
            vec![Bytes::from("hello"), Bytes::from("world!")],
        );

        let tracker = dp.segments.get(&test_key()).unwrap();
        assert_eq!(
            tracker.size_bytes,
            5 + 6,
            "size_bytes should track actual data bytes"
        );
    }

    #[test]
    fn segment_assignment_creates_tracker() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.process(DataPlaneCommand::SegmentAssignment {
            segment_key: test_key(),
            replica_set: vec![],
        });

        assert!(dp.segments.contains_key(&test_key()));
    }

    #[test]
    fn periodic_tick_flushes_buffered_data() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.process(DataPlaneCommand::SegmentAssignment {
            segment_key: test_key(),
            replica_set: vec![],
        });

        let (tx, _) = oneshot::channel();
        dp.process(DataPlaneCommand::Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });
        dp.take_events();

        assert!(dp.has_buffered_data());

        dp.process(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::PeriodicTick,
        ));

        assert!(!dp.has_buffered_data());

        let events = dp.take_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, DataPlaneEvent::WalBatchComplete { .. }))
        );

        let cache = dp.segments.get(&test_key()).unwrap().cache();
        assert_eq!(cache.load_commit_offset(), 1);
    }

    #[test]
    fn periodic_tick_noop_when_no_data() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.process(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::PeriodicTick,
        ));

        assert!(!dp.has_buffered_data());
    }

    #[test]
    fn flush_emits_reset_periodic() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.process(DataPlaneCommand::SegmentAssignment {
            segment_key: test_key(),
            replica_set: vec![],
        });

        let (tx, _) = oneshot::channel();
        dp.process(DataPlaneCommand::Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });
        dp.take_events();

        dp.process(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::PeriodicTick,
        ));

        let has_reset = dp
            .take_events()
            .into_iter()
            .any(|e| matches!(e, DataPlaneEvent::Timer(TimerCommand::ResetPeriodic)));
        assert!(has_reset, "flush should emit ResetPeriodic");
    }

    #[test]
    fn volume_trigger_flushes_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.process(DataPlaneCommand::SegmentAssignment {
            segment_key: test_key(),
            replica_set: vec![],
        });

        let big_records: Vec<Bytes> = (0..BATCH_MAX_RECORDS)
            .map(|i| Bytes::from(format!("r{i}")))
            .collect();

        dp.handle_produce(test_key(), big_records);

        assert!(
            !dp.has_buffered_data(),
            "volume trigger should have flushed inline"
        );

        let events = dp.take_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, DataPlaneEvent::WalBatchComplete { .. }))
        );
    }

    #[test]
    fn volume_trigger_resets_periodic() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.process(DataPlaneCommand::SegmentAssignment {
            segment_key: test_key(),
            replica_set: vec![],
        });

        let big_records: Vec<Bytes> = (0..BATCH_MAX_RECORDS)
            .map(|i| Bytes::from(format!("r{i}")))
            .collect();

        dp.handle_produce(test_key(), big_records);

        let has_reset = dp
            .take_events()
            .into_iter()
            .any(|e| matches!(e, DataPlaneEvent::Timer(TimerCommand::ResetPeriodic)));
        assert!(has_reset, "volume flush should also reset periodic timer");
    }

    #[test]
    fn checkpoint_complete_advances_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let key1 = (ShardGroupId(1), RangeId(0), SegmentId(0));
        let key2 = (ShardGroupId(2), RangeId(0), SegmentId(0));

        dp.process(DataPlaneCommand::SegmentAssignment {
            segment_key: key1,
            replica_set: vec![],
        });
        dp.process(DataPlaneCommand::SegmentAssignment {
            segment_key: key2,
            replica_set: vec![],
        });

        let wal_file_count =
            || -> usize { std::fs::read_dir(dir.path().join("wal")).unwrap().count() };
        let initial_count = wal_file_count();

        dp.process(DataPlaneCommand::CheckpointComplete(CheckpointComplete {
            segment_key: key1,
            checkpointed_lsn: 10,
        }));
        assert_eq!(
            wal_file_count(),
            initial_count,
            "no WAL deletion yet (key2 not checkpointed)"
        );

        dp.process(DataPlaneCommand::CheckpointComplete(CheckpointComplete {
            segment_key: key2,
            checkpointed_lsn: 5,
        }));
    }

    #[test]
    fn duplicate_segment_assignment_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let key = test_key();

        dp.process(DataPlaneCommand::SegmentAssignment {
            segment_key: key,
            replica_set: vec![],
        });
        dp.process(DataPlaneCommand::SegmentAssignment {
            segment_key: key,
            replica_set: vec![],
        });

        assert_eq!(dp.segments.len(), 1);
    }
}
