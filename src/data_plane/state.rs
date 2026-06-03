use super::SegmentKey;
use super::messages::command::DataPlaneInterNodeCommand;
use super::messages::command::*;
use super::messages::pending::DataPlaneOutputs;
use super::states::replication::PendingReplicationBatch;
use super::states::replication::ReplicationState;
use super::states::segment_store::SegmentStore;
use super::timer::DataPlaneTimeoutCallback;
use super::transport::command::DataTransportCommand;
use super::wal::WalRecord;
use super::wal::WalStorage;
use crate::config::DataNodeConfig;
use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::{CoordinatorSealRequest, MultiRaftActorCommand};
use crate::control_plane::membership::ShardGroupId;
use crate::data_plane::states::segment::tracker::{SegmentRole, SegmentTracker};
use crate::data_plane::timer::{BatchFlushTimer, ReplicationTimer};
use crate::data_plane::transport::command::DataTransportSendToCoordinator;
use crate::schedulers::ticker_message::TimerCommand;
#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;
use std::collections::HashMap;

struct PendingSealRequest {
    sent_at: std::time::Instant,
    failed_nodes: Vec<NodeId>,
}

pub struct DataPlane<W: WalStorage> {
    node_id: NodeId,
    config: DataNodeConfig,
    wal: W,
    /// All locally-resident segments + the secondary indexes the consume
    /// read path needs. See `states::segment_store::SegmentStore`.
    segments: SegmentStore,
    dirty_segments: Vec<SegmentKey>,
    buffer_byte_count: usize,
    needs_flush: bool,

    replication: ReplicationState,
    pending_seal_requests: HashMap<SegmentKey, PendingSealRequest>,

    out: DataPlaneOutputs,
}

impl<W: WalStorage> DataPlane<W> {
    pub(crate) fn new(
        node_id: NodeId,
        config: DataNodeConfig,
        wal: W,

        out: DataPlaneOutputs,
    ) -> Self {
        DataPlane {
            node_id,
            config,
            wal,
            segments: SegmentStore::new(),
            dirty_segments: Vec::new(),
            buffer_byte_count: 0,
            needs_flush: false,

            replication: ReplicationState::default(),
            pending_seal_requests: HashMap::new(),
            out,
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

    pub(crate) fn flush_and_dispatch(&mut self) {
        if self.should_flush() {
            self.flush_batch();
        }
        self.enqueue_timed_out_seal_retries();
        self.out.flush();

        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

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

        self.buffer_byte_count += cmd.data.len();
        tracker.stage_entry(cmd.segment_key, cmd.data, cmd.record_count);
        self.dirty_segments.push(cmd.segment_key);

        self.out
            .store_batch_produce_timer(TimerCommand::SetSchedule {
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
                self.handle_replication_timeout(segment_key, seq);
            }
            DataPlaneTimeoutCallback::SegmentAgeCheck => {
                self.enqueue_seal_for_aged_segments(self.config.max_segment_age);
            }
        }
    }

    fn process_inter_node(&mut self, cmd: DataPlaneInterNodeCommand) {
        use DataPlaneInterNodeCommand as C;
        match cmd {
            C::SegmentAssignment(cmd) => self.handle_segment_assignment(cmd),
            C::ReplicaAppend(cmd) => self.process_replica_append(cmd),
            C::ReplicaAck(cmd) => self.handle_replica_ack(cmd),
            C::CommitAdvance(cmd) => self.handle_commit_advance(cmd),
            C::SealResponse(cmd) => self.handle_seal_response(cmd),
            C::SegmentSealed(cmd) => self.handle_segment_sealed(cmd.segment_key),

            // Pass-through to MultiRaftActor — SealRequest is a control plane
            // message that shares the data transport wire format. The
            // coordinator loads its own live-nodes view from the shared
            // topology snapshot, so the data plane just forwards the request.
            C::SealRequest(cmd) => {
                self.out
                    .store_coordinator_cmd(MultiRaftActorCommand::Coordinator(
                        CoordinatorSealRequest { request: cmd },
                    ));
            }
        }
    }

    fn handle_replica_ack(&mut self, cmd: ReplicaAck) {
        let Some(committed) = self.replication.process_ack(&cmd.segment_key, &cmd.from) else {
            return;
        };

        self.commit_segment(cmd.segment_key, committed.entry_id);

        self.out.produce_replies.extend(committed.replies);

        if let Some(seq) = committed.reset_timer_seq {
            self.out
                .repl_schedules
                .push((seq, ReplicationTimer::timeout(cmd.segment_key)));
        }
    }

    fn handle_segment_assignment(&mut self, cmd: SegmentAssignment) {
        if self.segments.contains_key(&cmd.segment_key) {
            return;
        }
        let tracker = SegmentTracker::new_with_start_entry_id(
            cmd.segment_key.file_path(&self.config.data_dir),
            SegmentRole::Leader,
            cmd.replica_set,
            cmd.shard_group_id,
            cmd.start_entry_id,
        );
        self.segments.insert_active(cmd.segment_key, tracker);
    }

    fn process_replica_append(&mut self, cmd: ReplicaAppend) {
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

            self.segments.insert_active(
                cmd.segment_key,
                SegmentTracker::new_with_start_entry_id(
                    cmd.segment_key.file_path(&self.config.data_dir),
                    SegmentRole::Follower,
                    cmd.replica_set,
                    ShardGroupId(0),
                    cmd.entry_id,
                ),
            );
        }

        let Some(tracker) = self.segments.get_mut(&cmd.segment_key) else {
            return;
        };

        tracker.stage_entry_from_replica(cmd.segment_key, cmd.data, cmd.record_count, cmd.entry_id);
        self.dirty_segments.push(cmd.segment_key);

        self.needs_flush = true;
    }

    fn handle_commit_advance(&mut self, cmd: CommitAdvance) {
        let Some(tracker) = self.segments.get_mut(&cmd.segment_key) else {
            return;
        };

        if tracker.role() != SegmentRole::Follower {
            tracing::warn!(
                "CommitAdvance received by non-follower: {:?}",
                cmd.segment_key
            );
            return;
        }
        tracker.commit_entry(cmd.committed_entry_id);
    }

    fn handle_seal_response(&mut self, cmd: SealResponse) {
        self.pending_seal_requests.remove(&cmd.old_segment_key);
        let Some(old_tracker) = self.segments.get(&cmd.old_segment_key) else {
            return;
        };

        let new_segment_key = cmd.old_segment_key.with_segment_id(cmd.new_segment_id);

        let shard_group_id = old_tracker.shard_group_id();
        let old_start = old_tracker.start_entry_id();
        let old_end = old_tracker.committed_entry_id();
        let new_start_entry_id = old_end + 1;
        let mut new_tracker = SegmentTracker::new_with_start_entry_id(
            new_segment_key.file_path(&self.config.data_dir),
            SegmentRole::Leader,
            cmd.new_replica_set,
            shard_group_id,
            new_start_entry_id,
        );

        // Replay uncommitted records into the new tracker's staged_records.
        // They reach cache via the normal flush path: staged_records → WAL → publish_uncommitted.
        // Records are re-WAL'd with new routing headers; old WAL entries are cleaned up
        // by normal file deletion once the checkpoint watermark advances past them.
        //
        // D5 crash recovery: duplicate WAL data is safe. Old entries route to the sealed
        // segment (bounded by metadata end_offset), new entries route to the new segment.
        // Metadata-first recovery (Raft log before WAL) provides the seal boundary.
        for (data, record_count) in old_tracker.uncommitted_entries() {
            new_tracker.stage_entry(new_segment_key, data, record_count);
        }

        self.replication
            .segment_handoff(cmd.old_segment_key, new_segment_key);

        if !old_tracker.followers().is_empty() {
            self.out
                .store_transport_cmd(DataTransportCommand::send_to_targets(
                    old_tracker.followers().to_vec(),
                    SegmentSealed {
                        segment_key: cmd.old_segment_key,
                    },
                ));
        }

        self.segments.insert_active(new_segment_key, new_tracker);
        // The leader's old tracker stays in self.segments (no remove here —
        // that happens via the follower-side SegmentSealed path). But the
        // resolver index now points at the new active segment for the range;
        // the old one moves to sealed so cold reads still find it.
        self.segments
            .move_active_to_sealed(cmd.old_segment_key, old_start, old_end);
        self.dirty_segments.push(new_segment_key);
        self.needs_flush = true;
    }

    fn handle_segment_sealed(&mut self, segment_key: SegmentKey) {
        // On followers this is where the seal is first observed; the store
        // captures end_entry_id from the tracker's own committed boundary.
        // a duplicate migrate would be harmless (the sealed entry is immutable by construction).
        if let Some(tracker) = self.segments.take_active_and_seal(segment_key) {
            self.out.store_checkpoint(tracker.checkpoint(segment_key));
        }
    }

    fn commit_segment(&mut self, segment_key: SegmentKey, entry_id: u64) {
        let Some(tracker) = self.segments.get_mut(&segment_key) else {
            return;
        };

        tracker.commit_entry(entry_id);

        let followers = tracker.followers().to_vec();
        if !followers.is_empty() {
            self.out
                .transport_cmds
                .push(DataTransportCommand::send_to_targets(
                    followers,
                    CommitAdvance {
                        segment_key,
                        committed_entry_id: entry_id,
                    },
                ));
        }
    }

    pub(crate) fn enqueue_seal_for_aged_segments(&mut self, max_age: std::time::Duration) {
        let aged: Vec<SegmentKey> = self
            .segments
            .iter()
            .filter(|(_, t)| t.role() == SegmentRole::Leader && t.age_limit_reached(max_age))
            .map(|(&k, _)| k)
            .collect();

        for key in aged {
            self.enqueue_seal_request(key);
        }
    }

    fn handle_replication_timeout(&mut self, segment_key: SegmentKey, seq: u64) {
        if !self.replication.is_active_timer(&segment_key, seq) {
            return;
        }
        let Some(failed_nodes) = self.replication.in_flight_pending_acks(&segment_key) else {
            return;
        };
        let Some(tracker) = self.segments.get(&segment_key) else {
            return;
        };

        self.out
            .store_transport_cmd(DataTransportSendToCoordinator {
                shard_group_id: tracker.shard_group_id(),
                message: SealRequest {
                    from: self.node_id.clone(),
                    segment_key,
                    failed_nodes: failed_nodes.clone(),
                    end_entry_id: tracker.committed_entry_id(),
                }
                .into(),
            });
        self.pending_seal_requests.insert(
            segment_key,
            PendingSealRequest {
                sent_at: std::time::Instant::now(),
                failed_nodes,
            },
        );
    }

    fn enqueue_seal_request(&mut self, segment_key: SegmentKey) {
        if self.pending_seal_requests.contains_key(&segment_key) {
            return;
        }
        let Some(tracker) = self.segments.get(&segment_key) else {
            return;
        };
        if tracker.role() != SegmentRole::Leader {
            return;
        }
        self.out
            .store_transport_cmd(DataTransportSendToCoordinator {
                shard_group_id: tracker.shard_group_id(),
                message: SealRequest {
                    from: self.node_id.clone(),
                    segment_key,
                    failed_nodes: vec![],
                    end_entry_id: tracker.committed_entry_id(),
                }
                .into(),
            });
        self.pending_seal_requests.insert(
            segment_key,
            PendingSealRequest {
                sent_at: std::time::Instant::now(),
                failed_nodes: vec![],
            },
        );
    }

    fn should_flush(&self) -> bool {
        self.needs_flush || self.buffer_byte_count >= self.config.batch_max_bytes
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
                tracing::error!("WAL flush failed: {e}");
                self.replication.fail_all(e.to_string());
                self.needs_flush = false;
                return;
            }
        };

        let mut segment_batches: Vec<PendingReplicationBatch> = Vec::new();

        for key in dirty {
            let Some(tracker) = self.segments.get_mut(&key) else {
                continue;
            };
            if !tracker.has_staged() {
                continue;
            }

            for entry in tracker.publish_staged(lsn) {
                match tracker.role() {
                    SegmentRole::Leader => {
                        if tracker.followers().is_empty() {
                            tracker.commit_entry(entry.entry_id);
                        } else {
                            segment_batches.push(PendingReplicationBatch {
                                segment_key: key,
                                entry,
                                replica_set: tracker.replica_set(),
                                followers: tracker.followers().to_vec(),
                            });
                        }
                    }
                    SegmentRole::Follower => {
                        self.out
                            .store_transport_cmd(DataTransportCommand::send_to_targets(
                                vec![tracker.leader_node()],
                                ReplicaAck {
                                    segment_key: key,
                                    entry_id: entry.entry_id,
                                    from: self.node_id.clone(),
                                },
                            ));
                    }
                }
            }

            if tracker.size_limit_reached(self.config.segment_size_limit) {
                self.out.checkpoint_jobs.push(tracker.checkpoint(key));
                self.enqueue_seal_request(key);
            }
        }

        for pending_repl in segment_batches {
            if let Some(seq) = self.replication.begin_replication(&pending_repl) {
                self.out
                    .repl_schedules
                    .push((seq, ReplicationTimer::timeout(pending_repl.segment_key)));
            }
            let (targets, message) = pending_repl.into_replica_append();
            self.out
                .store_transport_cmd(DataTransportCommand::send_to_targets(targets, message));
        }

        for reply in self.replication.drain_all_pending_replies() {
            let _ = reply.send(ProduceAck::Ok);
        }

        self.buffer_byte_count = 0;
        self.needs_flush = false;
    }

    fn compute_checkpoint_watermark(&self) -> u64 {
        self.segments
            .values()
            .map(|t| t.checkpoint_lsn())
            .min()
            .unwrap_or(0)
    }

    #[allow(dead_code)]
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
            self.out.checkpoint_jobs.push(tracker.checkpoint(*key));
        }
    }

    fn enqueue_timed_out_seal_retries(&mut self) {
        let now = std::time::Instant::now();
        let timeout = self.config.seal_request_timeout;

        for (key, pending) in &mut self.pending_seal_requests {
            if now.duration_since(pending.sent_at) < timeout {
                continue;
            }
            let Some(tracker) = self.segments.get(key) else {
                continue;
            };
            if tracker.role() != SegmentRole::Leader {
                continue;
            }

            self.out
                .store_transport_cmd(DataTransportSendToCoordinator {
                    shard_group_id: tracker.shard_group_id(),
                    message: SealRequest {
                        from: self.node_id.clone(),
                        segment_key: *key,
                        failed_nodes: pending.failed_nodes.clone(),
                        end_entry_id: tracker.committed_entry_id(),
                    }
                    .into(),
                });
            pending.sent_at = now;
        }
    }
}

#[cfg(any(test, debug_assertions))]
impl<T: WalStorage> TAssertInvariant for DataPlane<T> {
    fn assert_invariants(&self) {
        self.wal.assert_invariants();

        for tracker in self.segments.values() {
            tracker.assert_invariants();
        }

        let actual_byte_count: usize = self
            .segments
            .values()
            .filter(|t| t.role() == SegmentRole::Leader)
            .flat_map(|t| t.staged_entries())
            .map(|e| e.byte_len())
            .sum();
        assert_eq!(
            self.buffer_byte_count, actual_byte_count,
            "buffer_byte_count ({}) != actual leader staged bytes ({actual_byte_count})",
            self.buffer_byte_count
        );

        self.segments.assert_invariants();
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::control_plane::membership::ShardGroupId;
    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
    use crate::data_plane::wal::WalWriter;
    use tokio::sync::oneshot;

    use bytes::Bytes;

    impl<T: WalStorage> DataPlane<T> {
        fn has_buffered_data(&self) -> bool {
            self.buffer_byte_count > 0
        }
    }

    fn test_key() -> SegmentKey {
        SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0))
    }

    fn test_node_id() -> NodeId {
        NodeId::new("test-node")
    }

    const TEST_BATCH_MAX_BYTES: usize = 10 * 1024 * 1024;

    fn test_config(dir: PathBuf) -> DataNodeConfig {
        DataNodeConfig {
            max_segment_age: std::time::Duration::from_secs(3600),
            age_check_interval: std::time::Duration::from_secs(60),
            segment_size_limit: 1024 * 1024 * 1024,
            batch_max_bytes: TEST_BATCH_MAX_BYTES,
            seal_request_timeout: std::time::Duration::from_secs(5),
            data_dir: dir,
        }
    }

    fn make_data_plane(dir: &tempfile::TempDir) -> DataPlane<WalWriter> {
        let wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        let out = DataPlaneOutputs::test();
        DataPlane::new(
            test_node_id(),
            test_config(dir.path().to_path_buf()),
            wal,
            out,
        )
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
                shard_group_id: ShardGroupId(1),
                replica_set,
                start_entry_id: 0,
            }
            .into(),
        )
    }

    fn produce(key: SegmentKey) -> (DataPlaneCommand, oneshot::Receiver<ProduceAck>) {
        let (tx, rx) = oneshot::channel();
        let cmd = Produce {
            segment_key: key,
            data: Bytes::from("data").into(),
            record_count: 1,
            reply: tx,
        };
        (cmd.into(), rx)
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
        let (cmd, mut rx) = produce(test_key());

        dp.handle_command(cmd);

        assert!(!dp.has_buffered_data());
        let ack = rx.try_recv().unwrap();
        assert!(matches!(ack, ProduceAck::Err(_)));
    }

    #[test]
    fn produce_replies_ok_on_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        let (cmd, rx) = produce(test_key());
        dp.handle_command(cmd);

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

        let (tx, _) = oneshot::channel();
        dp.handle_command(Produce {
            segment_key: test_key(),
            data: Bytes::from("hello world!").into(),
            record_count: 2,
            reply: tx,
        });

        let tracker = dp.segments.get(&test_key()).unwrap();
        assert_eq!(tracker.size_bytes(), 12);
        assert_eq!(tracker.next_entry_id(), 0);
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

        let (cmd, _) = produce(test_key());
        dp.handle_command(cmd);

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

        let (cmd, _) = produce(test_key());
        dp.handle_command(cmd);

        assert!(
            dp.out
                .batch_timer_cmds
                .iter()
                .any(|c| matches!(c, TimerCommand::SetSchedule { .. }))
        );
    }

    #[test]
    fn stale_flush_timer_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        let (cmd, _) = produce(test_key());
        dp.handle_command(cmd);

        dp.flush_batch();

        dp.handle_command(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));
        assert!(!dp.has_buffered_data());
    }

    #[test]
    fn batch_flush_deadline_flushes_inline() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        let (cmd, _) = produce(test_key());
        dp.handle_command(cmd);

        dp.handle_command(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));

        assert!(!dp.has_buffered_data());
    }

    #[test]
    fn volume_trigger_flushes_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![]));

        dp.handle_command(Produce {
            segment_key: test_key(),
            data: Bytes::from(vec![0u8; TEST_BATCH_MAX_BYTES]).into(),
            record_count: 1,
            reply: oneshot::channel().0,
        });

        assert!(dp.buffer_byte_count >= TEST_BATCH_MAX_BYTES);
        dp.flush_batch();
        assert!(!dp.has_buffered_data());
    }

    #[test]
    fn volume_trigger_flush_leaves_stale_timer_harmless() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![]));

        dp.handle_command(Produce {
            segment_key: test_key(),
            data: Bytes::from(vec![0u8; TEST_BATCH_MAX_BYTES]).into(),
            record_count: 1,
            reply: oneshot::channel().0,
        });
        assert!(dp.buffer_byte_count >= TEST_BATCH_MAX_BYTES);
        dp.flush_batch();

        dp.handle_command(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));
        assert!(!dp.has_buffered_data());
    }

    #[test]
    fn checkpoint_complete_advances_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let key1 = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));
        let key2 = SegmentKey::new(TopicId(2), RangeId(0), SegmentId(0));

        dp.handle_command(assign_segment(key1, vec![]));
        dp.handle_command(assign_segment(key2, vec![]));

        let wal_file_count =
            || -> usize { std::fs::read_dir(dir.path().join("wal")).unwrap().count() };
        let initial_count = wal_file_count();

        dp.handle_command(DataPlaneCommand::CheckpointComplete(CheckpointComplete {
            segment_key: key1,
            checkpointed_lsn: 10,
        }));
        assert_eq!(wal_file_count(), initial_count);

        dp.handle_command(DataPlaneCommand::CheckpointComplete(CheckpointComplete {
            segment_key: key2,
            checkpointed_lsn: 5,
        }));
    }

    #[test]
    fn duplicate_segment_assignment_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));
        dp.handle_command(assign_segment(test_key(), vec![]));

        assert_eq!(dp.segments.len(), 1);
    }

    #[test]
    fn multiple_produces_accumulate_in_tracker() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        let (cmd1, _rx1) = produce(test_key());
        let (cmd2, _rx2) = produce(test_key());
        let (cmd3, _rx3) = produce(test_key());
        dp.handle_command(cmd1);
        dp.handle_command(cmd2);
        dp.handle_command(cmd3);

        assert!(dp.has_buffered_data());
        assert!(!dp.should_flush());

        {
            let t = dp.segments.get(&test_key()).unwrap();
            assert_eq!(t.staged_entries().len(), 3);
            assert_eq!(t.cache_write_cursor(), 0);
            assert_eq!(t.next_entry_id(), 0);
        }

        dp.flush_batch();

        let t = dp.segments.get(&test_key()).unwrap();
        assert!(t.staged_entries().is_empty());
        assert_eq!(t.cache_write_cursor(), 3);
        assert_eq!(t.next_entry_id(), 3);
        assert_eq!(t.cache_read_cursor(), 3);
    }

    #[test]
    fn produce_with_followers_creates_replication_gap() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower-1");

        dp.handle_command(assign_segment(test_key(), vec![test_node_id(), follower]));

        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);

        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );

        let tracker = dp.segments.get(&test_key()).unwrap();
        let cache = tracker.cache();
        assert_eq!(cache.load_write_cursor(), 1);
        assert_eq!(cache.load_read_cursor(), 0);
    }

    #[test]
    fn produce_without_followers_commits_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![]));

        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);

        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );

        let cache = dp.segments.get(&test_key()).unwrap().cache();
        assert_eq!(cache.load_write_cursor(), 1);
        assert_eq!(cache.load_read_cursor(), 1);
    }

    #[test]
    fn flush_emits_transport_cmds_for_replicated_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower-1");

        dp.handle_command(assign_segment(
            test_key(),
            vec![test_node_id(), follower.clone()],
        ));

        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);

        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );

        assert!(!dp.out.transport_cmds.is_empty());
    }

    #[test]
    fn commit_segment_advances_read_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower-1");

        dp.handle_command(assign_segment(test_key(), vec![test_node_id(), follower]));

        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);
        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );
        dp.out.transport_cmds.clear();

        dp.commit_segment(test_key(), 0);

        let cache = dp.segments.get(&test_key()).unwrap().cache();
        assert_eq!(cache.load_read_cursor(), 1);

        assert!(!dp.out.transport_cmds.is_empty());
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
                data: b"setup".to_vec().into(),
                record_count: 1,
                entry_id: 0,
            }
            .into(),
        ));
        dp.flush_batch();

        let (cmd, rx) = produce(test_key());
        dp.handle_command(cmd);

        let ack = rx.blocking_recv().unwrap();
        assert!(matches!(ack, ProduceAck::Err(_)));
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
                data: b"data".to_vec().into(),
                record_count: 1,
                entry_id: 0,
            }
            .into(),
        ));

        assert!(dp.segments.contains_key(&test_key()));
        assert_eq!(
            dp.segments.get(&test_key()).unwrap().role(),
            SegmentRole::Follower
        );
    }

    #[test]
    fn replica_append_rejected_if_not_in_replica_set() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(DataPlaneCommand::InterNode(
            ReplicaAppend {
                segment_key: test_key(),
                replica_set: vec![NodeId::new("other-leader"), NodeId::new("other-follower")],
                data: b"data".to_vec().into(),
                record_count: 1,
                entry_id: 0,
            }
            .into(),
        ));

        assert!(!dp.segments.contains_key(&test_key()));
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
                data: b"data".to_vec().into(),
                record_count: 1,
                entry_id: 0,
            }
            .into(),
        ));
        dp.flush_batch();

        dp.handle_command(DataPlaneCommand::InterNode(
            CommitAdvance {
                segment_key: test_key(),
                committed_entry_id: 0,
            }
            .into(),
        ));

        let tracker = dp.segments.get(&test_key()).unwrap();
        assert_eq!(tracker.committed_entry_id(), 0);
        assert_eq!(tracker.cache().load_read_cursor(), 1);
    }

    #[test]
    fn replication_timeout_generates_seal_request() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower-1");

        dp.handle_command(assign_segment(test_key(), vec![test_node_id(), follower]));

        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);
        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );

        let seq = dp.out.repl_schedules[0].0;
        dp.out.repl_schedules.clear();
        dp.out.transport_cmds.clear();

        dp.handle_command(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::ReplicationTimeout {
                seq,
                segment_key: test_key(),
            },
        ));

        assert!(
            dp.out
                .transport_cmds
                .iter()
                .any(|c| matches!(c, DataTransportCommand::SendToCoordinator(..)))
        );
    }

    #[test]
    fn replication_timeout_stale_seq_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower-1");

        dp.handle_command(assign_segment(test_key(), vec![test_node_id(), follower]));

        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);
        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );
        dp.out.transport_cmds.clear();

        dp.handle_replication_timeout(test_key(), 9999);

        assert!(dp.out.transport_cmds.is_empty());
    }

    #[test]
    fn replication_timeout_stores_failed_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower-1");

        dp.handle_command(assign_segment(
            test_key(),
            vec![test_node_id(), follower.clone()],
        ));

        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);
        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );

        let seq = dp.out.repl_schedules[0].0;
        dp.handle_replication_timeout(test_key(), seq);

        let pending = dp.pending_seal_requests.get(&test_key()).unwrap();
        assert!(pending.failed_nodes.contains(&follower));
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
                data: b"data".to_vec().into(),
                record_count: 1,
                entry_id: 0,
            }
            .into(),
        ));

        assert!(dp.needs_flush);
    }

    #[test]
    fn seal_response_no_uncommitted_skips_wal_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower");
        let rs = vec![test_node_id(), follower.clone()];

        process_and_flush(&mut dp, assign_segment(test_key(), rs.clone()));

        let (cmd, _) = produce(test_key());
        dp.handle_command(cmd);
        dp.handle_command(DataPlaneCommand::Timeout(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));
        dp.segments.get_mut(&test_key()).unwrap().commit_entry(0);

        dp.handle_command(DataPlaneCommand::InterNode(
            SealResponse {
                old_segment_key: test_key(),
                new_segment_id: SegmentId(1),
                new_replica_set: vec![test_node_id(), NodeId::new("new-follower")],
            }
            .into(),
        ));

        assert!(dp.needs_flush);

        let new_key = test_key().with_segment_id(SegmentId(1));
        let new_tracker = dp.segments.get(&new_key).unwrap();
        assert!(new_tracker.staged_entries().is_empty());

        dp.flush_batch();
        assert!(!dp.needs_flush);
    }

    #[test]
    fn enqueue_seal_request_sends_to_coordinator() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        dp.enqueue_seal_request(test_key());

        assert!(
            dp.out
                .transport_cmds
                .iter()
                .any(|c| matches!(c, DataTransportCommand::SendToCoordinator(..)))
        );
        assert!(dp.pending_seal_requests.contains_key(&test_key()));
    }

    #[test]
    fn enqueue_seal_request_deduplicates() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        dp.enqueue_seal_request(test_key());
        dp.enqueue_seal_request(test_key());

        assert_eq!(
            dp.out
                .transport_cmds
                .iter()
                .filter(|c| matches!(c, DataTransportCommand::SendToCoordinator(..)))
                .count(),
            1
        );
    }

    #[test]
    fn enqueue_seal_request_skips_follower() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader-node");

        dp.handle_command(DataPlaneCommand::InterNode(
            ReplicaAppend {
                segment_key: test_key(),
                replica_set: vec![leader, test_node_id()],
                data: b"data".to_vec().into(),
                record_count: 1,
                entry_id: 0,
            }
            .into(),
        ));

        dp.enqueue_seal_request(test_key());

        assert!(dp.out.transport_cmds.is_empty());
        assert!(!dp.pending_seal_requests.contains_key(&test_key()));
    }

    #[test]
    fn enqueue_seal_request_skips_unknown_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.enqueue_seal_request(test_key());

        assert!(dp.out.transport_cmds.is_empty());
    }

    // ── D4 integration tests ──────────────────────────────────────────
    //
    // These exercise the resolver end-to-end via `DataPlane` commands —
    // SegmentAssignment routes through `handle_segment_assignment`, seal
    // routes through `handle_seal_response`, etc. The unit-level tests for
    // the index itself live in `states/segment_store.rs`.

    use crate::data_plane::states::segment_store::SegmentReadState;

    #[test]
    fn segment_assignment_indexes_for_resolver() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![]));

        let resolved = dp
            .segments
            .resolve(TopicId(1), RangeId(0), 0)
            .expect("active segment should resolve");
        assert!(matches!(resolved, SegmentReadState::Active(k) if k == test_key()));
    }

    /// Verifies the leader-side seal path: after `SealResponse`, the old
    /// segment migrates to the sealed table with `end_entry_id` set from
    /// the tracker's `committed_entry_id`, and the new segment becomes the
    /// active index entry for the range.
    #[test]
    fn seal_response_routes_old_offsets_through_sealed_table() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        // Single-replica produce auto-commits on flush, so entry 0 is
        // committed at the time we issue the seal. After seal: end=0 on the
        // sealed entry, new segment starts at offset 1.
        dp.handle_command(assign_segment(test_key(), vec![]));
        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);
        process_and_flush(
            &mut dp,
            DataPlaneCommand::Timeout(DataPlaneTimeoutCallback::BatchFlushDeadline),
        );

        dp.handle_command(DataPlaneCommand::InterNode(
            SealResponse {
                old_segment_key: test_key(),
                new_segment_id: SegmentId(1),
                new_replica_set: vec![],
            }
            .into(),
        ));

        let s0 = test_key();
        let s1 = s0.with_segment_id(SegmentId(1));

        let lookup = dp.segments.resolve(TopicId(1), RangeId(0), 0).unwrap();
        match lookup {
            SegmentReadState::Sealed { key, end_entry_id } => {
                assert_eq!(key, s0);
                assert_eq!(end_entry_id, 0);
            }
            other => panic!("expected Sealed, got {other:?}"),
        }

        let active = dp.segments.resolve(TopicId(1), RangeId(0), 1).unwrap();
        assert!(matches!(active, SegmentReadState::Active(k) if k == s1));
    }
}
