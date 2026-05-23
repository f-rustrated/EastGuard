use super::SegmentKey;
use super::messages::command::DataPlaneInterNodeCommand;
use super::messages::command::*;
use super::messages::event;
use super::states::replication::ReplicationState;
use super::timer::DataPlaneTimeoutCallback;
use super::transport::command::DataTransportCommand;
use super::wal::WalRecord;
use super::wal::WalStorage;
use crate::clusters::NodeId;
use crate::data_plane::checkpoint::CheckpointJob;
use crate::data_plane::messages::event::DataPlaneEvent;
use crate::data_plane::states::segment::tracker::{SegmentRole, SegmentTracker};
use crate::data_plane::timer::{BatchFlushTimer, ReplicationTimer};
use crate::schedulers::ticker_message::{SchedulerSender, TimerCommand};
#[cfg(test)]
use bytes::Bytes;
use crossbeam_channel::Sender;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::mpsc as tokio_mpsc;

#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;

const BATCH_MAX_RECORDS: usize = 20_000;
const BATCH_MAX_BYTES: usize = 10 * 1024 * 1024; // 10MB

pub struct DataPlane<W: WalStorage> {
    node_id: NodeId,
    wal: W,
    segments: HashMap<SegmentKey, SegmentTracker>,
    dirty_segments: Vec<SegmentKey>,
    pending_events: Vec<DataPlaneEvent>,
    buffer_record_count: usize,
    buffer_byte_count: usize,

    //  `needs_flush` means "flush immediately after this mailbox drain, don't wait for the timer." It exists because follower records
    // (process_replica_append) and seal replays (handle_seal_response) need immediate flush — they can't wait 10ms for BatchFlushDeadline.
    needs_flush: bool,
    data_dir: PathBuf,
    replication: ReplicationState,
}

impl<W: WalStorage> DataPlane<W> {
    pub(crate) fn new(node_id: NodeId, wal: W, data_dir: PathBuf) -> Self {
        DataPlane {
            node_id,
            wal,
            segments: HashMap::new(),
            dirty_segments: Vec::new(),
            pending_events: Vec::new(),
            buffer_record_count: 0,
            buffer_byte_count: 0,
            needs_flush: false,
            data_dir,
            replication: ReplicationState::default(),
        }
    }

    pub(crate) fn handle_command(&mut self, cmd: impl Into<DataPlaneCommand>) {
        let cmd = cmd.into();
        match cmd {
            DataPlaneCommand::Produce(cmd) => self.handle_produce(cmd),
            DataPlaneCommand::CheckpointComplete(complete) => {
                self.handle_checkpoint_complete(complete);
            }
            DataPlaneCommand::Timeout(callback) => {
                self.handle_timeout(callback);
            }
            DataPlaneCommand::InterNode(inter) => {
                self.process_inter_node(inter);
            }
        }

        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    pub(crate) fn flush_and_dispatch(
        &mut self,
        checkpoint_tx: &Sender<CheckpointJob>,
        batch_scheduler: &SchedulerSender<BatchFlushTimer>,
        repl_scheduler: &SchedulerSender<ReplicationTimer>,
        transport_tx: &tokio_mpsc::Sender<DataTransportCommand>,
    ) {
        if self.should_flush() {
            self.flush_batch();
        }
        self.dispatch_events(checkpoint_tx, batch_scheduler, repl_scheduler, transport_tx);
    }

    // --- Command handlers (called from handle_command) ---
    fn handle_produce(&mut self, cmd: Produce) {
        let reject = match self.segments.get(&cmd.segment_key) {
            Some(t) if t.role() == SegmentRole::Follower => Some("not leader"),
            None => Some("segment not found"),
            _ => None,
        };
        if let Some(reason) = reject {
            let _ = cmd.reply.send(ProduceAck::Err(reason.into()));
            return;
        }

        self.replication.enqueue_reply(cmd.segment_key, cmd.reply);

        let Some(tracker) = self.segments.get_mut(&cmd.segment_key) else {
            return;
        };

        for user_data in cmd.records {
            self.buffer_byte_count += user_data.len();
            self.buffer_record_count += 1;
            tracker.buffer_record(cmd.segment_key, user_data);
        }
        self.dirty_segments.push(cmd.segment_key);

        // Ticker rejects duplicate seqs, so this is a no-op if a flush timer
        // is already registered for the current LSN. After a flush, next_lsn
        // advances and the next produce registers a new timer.
        self.raise_event(TimerCommand::SetSchedule {
            seq: self.wal.next_lsn(),
            timer: BatchFlushTimer::deadline(),
        });
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

    fn handle_timeout(&mut self, callback: DataPlaneTimeoutCallback) {
        match callback {
            DataPlaneTimeoutCallback::BatchFlushDeadline => {
                self.flush_batch();
            }
            DataPlaneTimeoutCallback::ReplicationTimeout { seq, segment_key } => {
                if !self.replication.is_active_timer(&segment_key, seq) {
                    return;
                }

                let committed_end_offset = self
                    .segments
                    .get(&segment_key)
                    .map(|t| t.committed_end_offset())
                    .unwrap_or(0);

                self.raise_event(event::ReplicationTimedOut {
                    segment_key,
                    committed_end_offset,
                });
            }
        }
    }

    fn process_inter_node(&mut self, cmd: DataPlaneInterNodeCommand) {
        use DataPlaneInterNodeCommand as C;
        match cmd {
            C::SegmentAssignment(cmd) => self.handle_segment_assignment(cmd),
            C::ReplicaAppend(cmd) => self.process_replica_append(cmd),
            C::ReplicaAck(cmd) => {
                self.raise_event(event::ReplicaAckReceived {
                    segment_key: cmd.segment_key,
                    end_offset: cmd.end_offset,
                    from: cmd.from,
                });
            }
            C::CommitAdvance(cmd) => self.handle_commit_advance(cmd),
            C::SealRequest(_) => {
                tracing::info!("SealRequest received (coordinator routing in D3)");
            }
            C::SealResponse(cmd) => self.handle_seal_response(cmd),
            C::SegmentSealed(cmd) => self.handle_segment_sealed(cmd.segment_key),
        }
    }

    // --- Inter-node handlers (called from process_inter_node) ---
    fn handle_segment_assignment(&mut self, cmd: SegmentAssignment) {
        if self.segments.contains_key(&cmd.segment_key) {
            return;
        }

        let tracker = SegmentTracker::new_with_offset(
            cmd.segment_key.file_path(&self.data_dir),
            SegmentRole::Leader,
            cmd.replica_set,
            cmd.start_offset,
        );

        self.segments.insert(cmd.segment_key, tracker);
    }

    fn process_replica_append(&mut self, cmd: ReplicaAppend) {
        // Self-authorizing segment
        if !self.segments.contains_key(&cmd.segment_key) {
            if !cmd.replica_set.contains(&self.node_id) {
                tracing::warn!(
                    "ReplicaAppend for segment I'm not in: {:?}",
                    cmd.segment_key
                );
                return;
            }
            if cmd.replica_set.first() == Some(&self.node_id) {
                tracing::warn!("ReplicaAppend from self as leader: {:?}", cmd.segment_key);
                return;
            }

            self.segments.insert(
                cmd.segment_key,
                SegmentTracker::new_with_offset(
                    cmd.segment_key.file_path(&self.data_dir),
                    SegmentRole::Follower,
                    cmd.replica_set,
                    cmd.start_offset,
                ),
            );
        }

        let Some(tracker) = self.segments.get_mut(&cmd.segment_key) else {
            return;
        };

        tracker.buffer_record_in_bulk(cmd.segment_key, cmd.records, cmd.start_offset);
        self.dirty_segments.push(cmd.segment_key);

        self.needs_flush = true;
    }

    /// Leader -> follower: advance read_cursor after all replicas ACKed.
    fn handle_commit_advance(&mut self, cmd: CommitAdvance) {
        let Some(tracker) = self.segments.get_mut(&cmd.segment_key) else {
            return;
        };

        // Defensive. In practice, it should never fire.
        if tracker.role() != SegmentRole::Follower {
            tracing::warn!(
                "CommitAdvance received by non-follower: {:?}",
                cmd.segment_key
            );
            return;
        }
        tracker.commit_batch(cmd.committed_end_offset);
    }

    /// Coordinator -> leader: seal approved. Create new segment, replay uncommitted tail.
    fn handle_seal_response(&mut self, cmd: SealResponse) {
        let Some(old_tracker) = self.segments.get(&cmd.old_segment_key) else {
            return;
        };

        let new_segment_key = cmd.old_segment_key.with_segment_id(cmd.new_segment_id);

        let mut new_tracker = SegmentTracker::new_with_offset(
            new_segment_key.file_path(&self.data_dir),
            SegmentRole::Leader,
            cmd.new_replica_set,
            old_tracker.committed_end_offset() + 1,
        );

        // Replay uncommitted records into the new tracker's staged_records.
        // They reach cache via the normal flush path: staged_records → WAL → publish_uncommitted.
        // Records are re-WAL'd with new routing headers; old WAL entries are cleaned up
        // by normal file deletion once the checkpoint watermark advances past them.
        //
        // D5 crash recovery: duplicate WAL data is safe. Old entries route to the sealed
        // segment (bounded by metadata end_offset), new entries route to the new segment.
        // Metadata-first recovery (Raft log before WAL) provides the seal boundary.
        for payload in old_tracker.uncommitted_records() {
            new_tracker.buffer_record(new_segment_key, payload);
        }

        self.replication
            .segment_handoff(cmd.old_segment_key, new_segment_key);

        if !old_tracker.followers().is_empty() {
            self.raise_event(event::InterNodeCommandQueued::new(
                old_tracker.followers().to_vec(),
                SegmentSealed {
                    segment_key: cmd.old_segment_key,
                },
            ));
        }

        self.segments.insert(new_segment_key, new_tracker);
        self.dirty_segments.push(new_segment_key);
        self.needs_flush = true;
    }

    /// Leader -> follower: segment is sealed. Submit final checkpoint and drop tracker.
    fn handle_segment_sealed(&mut self, segment_key: SegmentKey) {
        if let Some(tracker) = self.segments.remove(&segment_key) {
            self.raise_event(tracker.checkpoint(segment_key));
        }
    }

    // --- Replication ---
    fn commit_segment(&mut self, segment_key: SegmentKey, end_offset: u64) {
        let Some(tracker) = self.segments.get_mut(&segment_key) else {
            return;
        };

        tracker.commit_batch(end_offset);

        let followers = tracker.followers().to_vec();
        if !followers.is_empty() {
            self.raise_event(event::InterNodeCommandQueued::new(
                followers,
                CommitAdvance {
                    segment_key,
                    committed_end_offset: end_offset,
                },
            ));
        }
    }

    fn should_flush(&self) -> bool {
        self.needs_flush
            || self.buffer_record_count >= BATCH_MAX_RECORDS
            || self.buffer_byte_count >= BATCH_MAX_BYTES
    }

    fn flush_batch(&mut self) {
        let dirty = std::mem::take(&mut self.dirty_segments);

        for &key in &dirty {
            if let Some(tracker) = self.segments.get_mut(&key) {
                tracker.stage_to_wal(self.wal.buf());
            }
        }

        if !dirty
            .iter()
            .any(|k| self.segments.get(k).is_some_and(|t| t.has_staged()))
        {
            self.needs_flush = false;
            return;
        }

        let batch_end = WalRecord::batch_end();
        let _ = batch_end.encode_to(self.wal.buf());

        let lsn = match self.wal.flush_batch() {
            Ok(lsn) => lsn,
            Err(e) => {
                // staged_records are NOT cleared — they remain in each tracker
                // and will be re-encoded on the next flush attempt. Duplicate WAL
                // entries are safe: D5 replay deduplicates by logical_offset per segment.
                tracing::error!("WAL flush failed: {e}");
                self.replication.fail_all(e.to_string());
                self.needs_flush = false;
                return;
            }
        };

        // -- Post flush
        let mut segment_batches: Vec<event::PendingReplicationBatch> = Vec::new();

        for key in dirty {
            let Some(tracker) = self.segments.get_mut(&key) else {
                continue;
            };
            if !tracker.has_staged() {
                continue;
            }

            let batch = tracker.publish_staged(lsn);
            let end_offset = batch.end_offset;

            match tracker.role() {
                SegmentRole::Leader if tracker.followers().is_empty() => {
                    tracker.commit_batch(end_offset);
                }
                SegmentRole::Leader => {
                    segment_batches.push(event::PendingReplicationBatch {
                        segment_key: key,
                        batch,
                        replica_set: tracker.replica_set(),
                        followers: tracker.followers().to_vec(),
                    });
                }
                SegmentRole::Follower => {
                    self.pending_events.push(
                        event::InterNodeCommandQueued::new(
                            vec![tracker.leader_node()],
                            ReplicaAck {
                                segment_key: key,
                                end_offset,
                                from: self.node_id.clone(),
                            },
                        )
                        .into(),
                    );
                }
            }

            if tracker.size_limit_reached() {
                self.pending_events.push(tracker.checkpoint(key).into());
            }
        }

        if !segment_batches.is_empty() {
            self.raise_event(event::BatchPublished {
                lsn,
                segment_batches,
            });
        }

        // No-follower fast path: replies for segments without followers were left
        // in pending_replies (not moved to in_flight). Drain them now with Ok.
        for reply in self.replication.drain_all_pending_replies() {
            let _ = reply.send(ProduceAck::Ok);
        }

        self.buffer_record_count = 0;
        self.buffer_byte_count = 0;
        self.needs_flush = false;
    }

    // --- Checkpoint ---

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
    fn submit_checkpoint_for_pressure(&mut self) {
        let mut candidates: Vec<(SegmentKey, u64)> = self
            .segments
            .iter()
            .map(|(key, tracker)| (*key, tracker.uncheckpointed()))
            .collect();

        candidates.sort_by_key(|b| std::cmp::Reverse(b.1));

        if let Some((key, _)) = candidates.first()
            && let Some(tracker) = self.segments.get(key)
        {
            self.raise_event(tracker.checkpoint(*key));
        }
    }

    // --- Event dispatch (called from flush_and_dispatch) ---

    fn dispatch_events(
        &mut self,
        checkpoint_tx: &Sender<CheckpointJob>,
        batch_scheduler: &SchedulerSender<BatchFlushTimer>,
        repl_scheduler: &SchedulerSender<ReplicationTimer>,
        transport_tx: &tokio_mpsc::Sender<DataTransportCommand>,
    ) {
        use DataPlaneEvent as E;
        while !self.pending_events.is_empty() {
            let events = self.take_events();
            for event in events {
                match event {
                    E::CheckpointRequired(job) => {
                        let _ = checkpoint_tx.send(job);
                    }
                    E::BatchPublished(evt) => {
                        for pending_repl in evt.segment_batches {
                            if let Some(seq) = self.replication.begin_replication(&pending_repl) {
                                repl_scheduler.schedule(
                                    seq,
                                    ReplicationTimer::timeout(pending_repl.segment_key),
                                );
                            }
                            let (targets, message) = pending_repl.into_replica_append();
                            let _ = transport_tx
                                .blocking_send(DataTransportCommand::send(targets, message));
                        }
                    }
                    E::BatchFlushTimerScheduled(cmd) => {
                        batch_scheduler.send(cmd);
                    }
                    E::InterNodeCommandQueued(evt) => {
                        let _ = transport_tx
                            .blocking_send(DataTransportCommand::send(evt.targets, evt.message));
                    }
                    E::ReplicaAckReceived(evt) => {
                        let Some(committed) =
                            self.replication.process_ack(&evt.segment_key, &evt.from)
                        else {
                            continue;
                        };

                        self.commit_segment(evt.segment_key, committed.batch_end_offset);

                        for reply in committed.replies {
                            let _ = reply.send(ProduceAck::Ok);
                        }

                        if let Some(seq) = committed.reset_timer_seq {
                            repl_scheduler
                                .schedule(seq, ReplicationTimer::timeout(evt.segment_key));
                        }
                    }
                    E::ReplicationTimedOut(evt) => {
                        // In-flight state intentionally NOT cleared. A late ack may
                        // commit the batch while SealRequest is in transit — safe
                        // because apply_roll_segment() is idempotent (DS-RSM invariant 9).

                        let Some(nodes) = self.replication.in_flight_pending_acks(&evt.segment_key)
                        else {
                            continue;
                        };

                        // ! Locate Coordinator
                        let targets = vec![];
                        let _ = transport_tx.blocking_send(DataTransportCommand::send(
                            targets,
                            SealRequest {
                                segment_key: evt.segment_key,
                                failed_nodes: nodes,
                                end_offset: evt.committed_end_offset,
                            },
                        ));
                    }
                }
            }
        }

        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    // --- Internal utilities ---

    fn raise_event(&mut self, event: impl Into<DataPlaneEvent>) {
        self.pending_events.push(event.into());
    }

    fn take_events(&mut self) -> Vec<DataPlaneEvent> {
        std::mem::take(&mut self.pending_events)
    }

    #[cfg(test)]
    fn has_buffered_data(&self) -> bool {
        self.buffer_record_count > 0
    }
}

#[cfg(any(test, debug_assertions))]
impl<T: WalStorage> TAssertInvariant for DataPlane<T> {
    fn assert_invariants(&self) {
        self.wal.assert_invariants();

        for tracker in self.segments.values() {
            tracker.assert_invariants();
        }

        let actual_record_count: usize = self
            .segments
            .values()
            .filter(|t| t.role() == SegmentRole::Leader)
            .map(|t| t.staged_records().len())
            .sum();
        assert_eq!(
            self.buffer_record_count, actual_record_count,
            "buffer_record_count ({}) != actual leader accumulated records ({actual_record_count})",
            self.buffer_record_count
        );

        let actual_byte_count: usize = self
            .segments
            .values()
            .filter(|t| t.role() == SegmentRole::Leader)
            .flat_map(|t| t.staged_records().iter())
            .map(|r| r.user_data.len())
            .sum();
        assert_eq!(
            self.buffer_byte_count, actual_byte_count,
            "buffer_byte_count ({}) != actual leader buffered bytes ({actual_byte_count})",
            self.buffer_byte_count
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clusters::metadata::{RangeId, SegmentId};
    use crate::clusters::swims::ShardGroupId;
    use crate::data_plane::wal::WalWriter;
    use tokio::sync::oneshot;

    fn test_key() -> SegmentKey {
        SegmentKey::new(ShardGroupId(1), RangeId(0), SegmentId(0))
    }

    fn test_node_id() -> NodeId {
        NodeId::new("test-node")
    }

    fn make_data_plane(dir: &tempfile::TempDir) -> DataPlane<WalWriter> {
        let wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        DataPlane::new(test_node_id(), wal, dir.path().to_path_buf())
    }

    fn process_and_flush(dp: &mut DataPlane<WalWriter>, cmd: DataPlaneCommand) {
        dp.handle_command(cmd);
        if dp.should_flush() {
            dp.flush_batch();
        }
    }

    fn assign_segment(key: SegmentKey, replica_set: Vec<NodeId>) -> DataPlaneCommand {
        DataPlaneCommand::InterNode(
            SegmentAssignment {
                segment_key: key,
                replica_set,
                start_offset: 0,
            }
            .into(),
        )
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
        dp.handle_command(assign_segment(test_key(), vec![]));
        assert!(dp.segments.contains_key(&test_key()));
    }

    #[test]
    fn produce_to_unknown_segment_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let (tx, mut rx) = oneshot::channel();

        dp.handle_command(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("hello")],
            reply: tx,
        });

        assert!(!dp.has_buffered_data());
        let ack = rx.try_recv().unwrap();
        assert!(matches!(ack, ProduceAck::Err(_)));
    }

    #[test]
    fn produce_replies_ok_on_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        let (tx, rx) = oneshot::channel();
        dp.handle_command(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });
        dp.take_events();

        dp.handle_command(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));

        let ack = rx.blocking_recv().unwrap();
        assert!(matches!(ack, ProduceAck::Ok));
    }

    #[test]
    fn produce_tracks_size_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        dp.handle_produce(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("hello"), Bytes::from("world!")],
            reply: oneshot::channel().0,
        });

        let tracker = dp.segments.get(&test_key()).unwrap();
        assert_eq!(
            tracker.size_bytes(),
            5 + 6,
            "size_bytes should track actual data bytes"
        );
        assert_eq!(
            tracker.next_offset(),
            2,
            "next_offset should track logical record count"
        );
    }

    #[test]
    fn segment_assignment_creates_tracker() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        assert!(dp.segments.contains_key(&test_key()));
    }

    #[test]
    fn batch_deadline_flushes_buffered_data() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        let (tx, _) = oneshot::channel();
        dp.handle_command(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });
        dp.take_events();

        assert!(dp.has_buffered_data());

        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );

        assert!(!dp.has_buffered_data());

        let cache = dp.segments.get(&test_key()).unwrap().cache();
        assert_eq!(cache.load_read_cursor(), 1);
    }

    #[test]
    fn produce_schedules_flush_timer() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        let (tx, _) = oneshot::channel();
        dp.handle_command(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });

        let events = dp.take_events();
        let has_set_schedule = events.iter().any(|e| {
            matches!(
                e,
                DataPlaneEvent::BatchFlushTimerScheduled(TimerCommand::SetSchedule { .. })
            )
        });
        assert!(
            has_set_schedule,
            "first produce should schedule flush timer"
        );
    }

    #[test]
    fn stale_flush_timer_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        let (tx, _) = oneshot::channel();
        dp.handle_command(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });
        dp.take_events();

        // Flush via size trigger (not the timer)
        dp.flush_batch();
        dp.take_events();

        // Stale timer fires — should be a no-op (no WAL write)
        dp.handle_command(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));
        assert!(
            !dp.has_buffered_data(),
            "stale flush timer should not buffer anything"
        );
    }

    #[test]
    fn batch_flush_deadline_flushes_inline() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        let (tx, _) = oneshot::channel();
        dp.handle_command(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });
        dp.take_events();

        dp.handle_command(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));

        assert!(
            !dp.has_buffered_data(),
            "BatchFlushDeadline should flush immediately"
        );
    }

    #[test]
    fn volume_trigger_flushes_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![]));

        let big_records: Vec<Bytes> = (0..BATCH_MAX_RECORDS)
            .map(|i| Bytes::from(format!("r{i}")))
            .collect();

        dp.handle_produce(Produce {
            segment_key: test_key(),
            records: big_records,
            reply: oneshot::channel().0,
        });

        assert!(
            dp.buffer_record_count >= BATCH_MAX_RECORDS,
            "buffer_record_count ({}) should exceed threshold ({})",
            dp.buffer_record_count,
            BATCH_MAX_RECORDS
        );
        dp.flush_batch();
        assert!(!dp.has_buffered_data(), "flush should drain all records");
    }

    #[test]
    fn volume_trigger_flush_leaves_stale_timer_harmless() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![]));

        let big_records: Vec<Bytes> = (0..BATCH_MAX_RECORDS)
            .map(|i| Bytes::from(format!("r{i}")))
            .collect();

        dp.handle_produce(Produce {
            segment_key: test_key(),
            records: big_records,
            reply: oneshot::channel().0,
        });
        assert!(dp.buffer_record_count >= BATCH_MAX_RECORDS);
        dp.flush_batch();
        dp.take_events();

        // Stale timer fires — no-op because buffer is empty
        dp.handle_command(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));
        assert!(
            !dp.has_buffered_data(),
            "stale flush timer after volume flush should be no-op"
        );
    }

    #[test]
    fn checkpoint_complete_advances_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let key1 = SegmentKey::new(ShardGroupId(1), RangeId(0), SegmentId(0));
        let key2 = SegmentKey::new(ShardGroupId(2), RangeId(0), SegmentId(0));

        dp.handle_command(assign_segment(key1, vec![]));
        dp.handle_command(assign_segment(key2, vec![]));

        let wal_file_count =
            || -> usize { std::fs::read_dir(dir.path().join("wal")).unwrap().count() };
        let initial_count = wal_file_count();

        dp.handle_command(DataPlaneCommand::CheckpointComplete(CheckpointComplete {
            segment_key: key1,
            checkpointed_lsn: 10,
        }));
        assert_eq!(
            wal_file_count(),
            initial_count,
            "no WAL deletion yet (key2 not checkpointed)"
        );

        dp.handle_command(DataPlaneCommand::CheckpointComplete(CheckpointComplete {
            segment_key: key2,
            checkpointed_lsn: 5,
        }));
    }

    #[test]
    fn duplicate_segment_assignment_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let key = test_key();

        dp.handle_command(assign_segment(key, vec![]));
        dp.handle_command(assign_segment(key, vec![]));

        assert_eq!(dp.segments.len(), 1);
    }

    // ---- D2 replication tests ----

    #[test]
    fn produce_with_followers_creates_replication_gap() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower-1");

        dp.handle_command(assign_segment(test_key(), vec![test_node_id(), follower]));

        let (tx, _rx) = oneshot::channel();
        dp.handle_command(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });
        dp.take_events();

        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );

        let tracker = dp.segments.get(&test_key()).unwrap();
        let cache = tracker.cache();
        assert_eq!(
            cache.load_write_cursor(),
            1,
            "tail should advance after publish"
        );
        assert_eq!(
            cache.load_read_cursor(),
            0,
            "commit should NOT advance — waiting for replication"
        );
    }

    #[test]
    fn produce_without_followers_commits_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        let (tx, _rx) = oneshot::channel();
        dp.handle_command(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });
        dp.take_events();

        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );

        let cache = dp.segments.get(&test_key()).unwrap().cache();
        assert_eq!(cache.load_write_cursor(), 1);
        assert_eq!(
            cache.load_read_cursor(),
            1,
            "no followers = immediate commit"
        );
    }

    #[test]
    fn flush_emits_replication_ready_for_replicated_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower-1");

        dp.handle_command(assign_segment(
            test_key(),
            vec![test_node_id(), follower.clone()],
        ));

        let (tx, _rx) = oneshot::channel();
        dp.handle_command(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });
        dp.take_events();

        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );

        let events = dp.take_events();
        let has_replication_ready = events
            .iter()
            .any(|e| matches!(e, DataPlaneEvent::BatchPublished(..)));
        assert!(
            has_replication_ready,
            "should emit ReplicationReady for replicated segment"
        );
    }

    #[test]
    fn commit_segment_advances_read_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower-1");

        dp.handle_command(assign_segment(test_key(), vec![test_node_id(), follower]));

        let (tx, _rx) = oneshot::channel();
        dp.handle_command(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });
        dp.take_events();
        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );
        dp.take_events();

        dp.commit_segment(test_key(), 0);

        let cache = dp.segments.get(&test_key()).unwrap().cache();
        assert_eq!(
            cache.load_read_cursor(),
            1,
            "commit should advance after all replicas ACK"
        );

        let events = dp.take_events();
        let has_commit_advance = events
            .iter()
            .any(|e| matches!(e, DataPlaneEvent::InterNodeCommandQueued(..)));
        assert!(has_commit_advance, "should emit SendInterNode after commit");
    }

    #[test]
    fn follower_rejects_produce() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader-node");

        dp.handle_command(DataPlaneCommand::InterNode(
            ReplicaAppend {
                segment_key: test_key(),
                replica_set: vec![leader, test_node_id()],
                records: vec![b"setup".to_vec()],
                start_offset: 0,
            }
            .into(),
        ));
        dp.take_events();
        dp.flush_batch();
        dp.take_events();

        let (tx, rx) = oneshot::channel();
        dp.handle_command(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: tx,
        });

        let ack = rx.blocking_recv().unwrap();
        assert!(
            matches!(ack, ProduceAck::Err(_)),
            "follower should reject produce"
        );
    }

    #[test]
    fn replica_append_self_authorization() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader-node");

        dp.handle_command(DataPlaneCommand::InterNode(
            ReplicaAppend {
                segment_key: test_key(),
                replica_set: vec![leader.clone(), test_node_id()],
                records: vec![b"data".to_vec()],
                start_offset: 0,
            }
            .into(),
        ));

        assert!(
            dp.segments.contains_key(&test_key()),
            "self-authorization should create tracker"
        );
        let tracker = dp.segments.get(&test_key()).unwrap();
        assert_eq!(tracker.role(), SegmentRole::Follower);
    }

    #[test]
    fn replica_append_rejected_if_not_in_replica_set() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(DataPlaneCommand::InterNode(
            ReplicaAppend {
                segment_key: test_key(),
                replica_set: vec![NodeId::new("other-leader"), NodeId::new("other-follower")],
                records: vec![b"data".to_vec()],
                start_offset: 0,
            }
            .into(),
        ));

        assert!(
            !dp.segments.contains_key(&test_key()),
            "should reject — not in replica_set"
        );
    }

    #[test]
    fn commit_advance_advances_follower() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader-node");

        dp.handle_command(DataPlaneCommand::InterNode(
            ReplicaAppend {
                segment_key: test_key(),
                replica_set: vec![leader.clone(), test_node_id()],
                records: vec![b"data".to_vec()],
                start_offset: 0,
            }
            .into(),
        ));
        dp.take_events();
        dp.flush_batch();
        dp.take_events();

        dp.handle_command(DataPlaneCommand::InterNode(
            CommitAdvance {
                segment_key: test_key(),
                committed_end_offset: 0,
            }
            .into(),
        ));

        let tracker = dp.segments.get(&test_key()).unwrap();
        assert_eq!(tracker.committed_end_offset(), 0);
        assert_eq!(tracker.cache().load_read_cursor(), 1);
    }

    #[test]
    fn replication_timeout_emits_event() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        let seq = dp.replication.alloc_timer_seq();
        dp.replication.set_timer_seq(test_key(), seq);

        dp.handle_command(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::ReplicationTimeout {
                seq,
                segment_key: test_key(),
            },
        ));

        let events = dp.take_events();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, DataPlaneEvent::ReplicationTimedOut(..)))
        );
    }

    #[test]
    fn needs_flush_set_by_replica_append() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader-node");

        assert!(!dp.needs_flush);

        dp.handle_command(DataPlaneCommand::InterNode(
            ReplicaAppend {
                segment_key: test_key(),
                replica_set: vec![leader, test_node_id()],
                records: vec![b"data".to_vec()],
                start_offset: 0,
            }
            .into(),
        ));

        assert!(dp.needs_flush, "replica_append should set needs_flush");
    }

    #[test]
    fn seal_response_no_uncommitted_skips_wal_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower");
        let rs = vec![test_node_id(), follower.clone()];

        process_and_flush(&mut dp, assign_segment(test_key(), rs.clone()));
        dp.take_events();

        // Produce + flush + commit so read_cursor == write_cursor
        dp.handle_command(Produce {
            segment_key: test_key(),
            records: vec![Bytes::from("data")],
            reply: oneshot::channel().0,
        });
        dp.handle_command(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));
        dp.take_events();
        dp.segments.get_mut(&test_key()).unwrap().commit_batch(0);

        // SealResponse with no uncommitted records to replay
        dp.handle_command(DataPlaneCommand::InterNode(
            SealResponse {
                old_segment_key: test_key(),
                new_segment_id: SegmentId(1),
                new_replica_set: vec![test_node_id(), NodeId::new("new-follower")],
            }
            .into(),
        ));

        assert!(dp.needs_flush, "seal_response always sets needs_flush");

        let new_key = test_key().with_segment_id(SegmentId(1));
        let new_tracker = dp.segments.get(&new_key).unwrap();
        assert!(
            new_tracker.staged_records().is_empty(),
            "no uncommitted records to replay"
        );

        // flush_batch should be a no-op (no staged records)
        dp.flush_batch();
        assert!(!dp.needs_flush, "needs_flush cleared after flush");
    }
}
