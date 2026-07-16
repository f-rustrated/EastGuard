use super::SegmentKey;
use super::actor::DataPlaneSender;
use super::cold_read::{CatchUpReadReply, ColdReadReply, ColdReadRequest};
use super::messages::DataPlaneMessage;
use super::messages::command::DataPlanePeerMessage;
use super::messages::command::*;
use super::messages::pending::DataPlaneOutputs;
use super::messages::query::{
    DataPlaneQuery, Fetch, FetchResult, ListOffsets, ListOffsetsResult, ReadConsumerOffsetResult,
};
use super::recovery::inventory::{LocalInventory, RecoveryOutput};
use super::recovery::orphan::OrphanCandidate;
use super::recovery::segment_scan::scan_segment_file;
use super::segment_writer::SegmentAppender;

use super::states::replication::PendingReplicationBatch;
use super::states::replication::ReplicationState;
use super::states::seal_request::PendingSegmentRollRequests;
use super::states::segment_store::SegmentStore;
use super::timer::DataPlaneTimeoutCallback;
use super::transport::command::DataTransportCommand;
use super::wal::WalRecord;
use super::wal::WalStorage;
use crate::config::DataNodeConfig;
use crate::control_plane::consensus::messages::{MultiRaftActorCommand, ProposeSegmentRoll};
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::EntryId;
use crate::control_plane::{NodeId, Replicas};
use crate::data_plane::EntryPayload;
use crate::data_plane::consumer_offset_management::ConsumerOffsetManager;
use crate::data_plane::consumer_offset_management::ledger::{EpochSeal, StaleEpoch};

use crate::data_plane::consumer_offset_management::types::*;
use crate::data_plane::messages::query::ReadConsumerOffset;
use crate::data_plane::states::segment::tracker::{SegmentRole, SegmentTracker};
use crate::data_plane::states::segment_store::SegmentReadState;
use crate::data_plane::timer::{BatchFlushTimer, ReplicationTimer};
use crate::data_plane::transport::command::DataTransportSendToCoordinator;

use crate::schedulers::ticker_message::TimerCommand;
#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;
use std::collections::{BTreeSet, HashMap};

/// Same rational as the Raft transport's 4MiB cap, Per-`CatchUpEntries` read cap.
/// A large segment streams as several chunks via the read-complete re-arm loop.
const CATCH_UP_CHUNK_MAX_BYTES: u64 = 4 * 1024 * 1024;

/// Re-drives without an append before the receiver treats a receive as stalled
/// and re-requests from a fresh source. A grace window for slow transfers.
const CATCH_UP_IDLE_REDRIVES: u32 = 2;

/// Max stray files deleted per orphan-GC sweep — a large backlog drains over several
/// sweeps, not one burst.
const ORPHAN_GC_BATCH: usize = 64;

/// An in-progress catch-up receive (replacement side): the open segment-file
/// appender, the sealed bounds being filled toward, and the sparse anchors
/// accumulated as chunks land — flushed to the index once the file verifies.
use super::states::types::*;

pub struct DataPlane<W: WalStorage> {
    node_id: NodeId,
    config: DataNodeConfig,
    wal: W,
    /// All locally-resident segments + the secondary indexes the consume
    /// read path needs. See `states::segment_store::SegmentStore`.
    segments: SegmentStore,
    dirty_segments: BTreeSet<SegmentKey>,
    buffer_byte_count: usize,
    needs_flush: bool,

    replication: ReplicationState,

    pending_seal_requests: PendingSegmentRollRequests,
    /// In-progress catch-up receives (replacement side), keyed by segment.
    pending_catch_ups: HashMap<SegmentKey, PendingCatchUp>,

    /// Hand-off channel to the cold-read thread pool. A fetch that resolves to a
    /// sealed segment (whose live tracker is gone) is forwarded here; the pool
    /// reads the segment file and fulfils the consumer's reply directly, so this
    /// synchronous worker never blocks on disk I/O.
    cold_read_handoff_sender: flume::Sender<ColdReadRequest>,
    self_tx: DataPlaneSender,
    out: DataPlaneOutputs,

    /// The verified local inventory recovery handed us (segment → highest durable entry id).
    /// Consulted on a catch-up assignment to decide full-match / delta /
    /// full-copy and to skip transfer when we already hold the segment.
    recovered: LocalInventory,
    pending_seals: std::collections::HashSet<SegmentKey>,
    pressure_checkpoints_in_flight: BTreeSet<SegmentKey>,
    consumer_offsets: ConsumerOffsetManager,
}

impl<W: WalStorage> DataPlane<W> {
    pub(crate) fn new(
        node_id: NodeId,
        config: DataNodeConfig,
        wal: W,
        cold_read_handoff_sender: flume::Sender<ColdReadRequest>,
        self_tx: DataPlaneSender,
        out: DataPlaneOutputs,
        recovery: RecoveryOutput,
    ) -> Self {
        let RecoveryOutput {
            inventory: recovered,
            data_dir: _,
            offsets: offset_ledger,
        } = recovery;
        DataPlane {
            node_id,
            config,
            wal,
            segments: SegmentStore::new(),
            dirty_segments: BTreeSet::new(),
            buffer_byte_count: 0,
            needs_flush: false,
            replication: ReplicationState::default(),
            pending_seal_requests: PendingSegmentRollRequests::default(),
            pending_catch_ups: HashMap::new(),
            cold_read_handoff_sender,
            self_tx,
            out,
            recovered,
            pending_seals: std::collections::HashSet::new(),
            pressure_checkpoints_in_flight: BTreeSet::new(),
            consumer_offsets: ConsumerOffsetManager::new(offset_ledger),
        }
    }

    pub(crate) fn process(&mut self, msg: impl Into<DataPlaneMessage>) {
        match msg.into() {
            DataPlaneMessage::Command(cmd) => self.handle_command(cmd),
            DataPlaneMessage::Query(q) => self.handle_query(q),
        }
    }

    fn handle_command(&mut self, cmd: impl Into<DataPlaneCommand>) {
        let cmd = cmd.into();
        match cmd {
            DataPlaneCommand::Produce(cmd) => self.handle_produce(cmd),
            DataPlaneCommand::SegmentCheckpointComplete(complete) => {
                self.handle_checkpoint_complete(complete);
            }
            DataPlaneCommand::OffsetCheckpointComplete(complete) => {
                self.handle_offset_checkpoint_complete(complete);
            }
            DataPlaneCommand::DataPlaneTimeoutCallback(callback) => {
                self.handle_timeout(callback);
            }
            DataPlaneCommand::ReceivePeerMessage(received) => {
                self.receive_peer_message(received);
            }
            DataPlaneCommand::CatchUpReadComplete(cmd) => {
                self.handle_catch_up_read_complete(cmd);
            }
            DataPlaneCommand::OrphanGcCheck(check) => self.handle_orphan_gc_check(check),
            DataPlaneCommand::CommitConsumerOffset(cmd) => self.commit_consumer_offset(cmd),
        }

        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    fn handle_query(&self, q: impl Into<DataPlaneQuery>) {
        match q.into() {
            DataPlaneQuery::Fetch(f) => self.handle_fetch(f),
            DataPlaneQuery::ListOffsets(lo) => self.handle_list_offsets(lo),
            DataPlaneQuery::ReadConsumerOffset(query) => self.read_consumer_offset(query),
        }
    }

    pub(crate) async fn flush_and_dispatch(&mut self) {
        if self.should_flush() {
            self.flush_batch();
        }
        self.enqueue_timed_out_seal_retries();
        self.out.flush().await;

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
        let entry_id = tracker.next_staged_entry_id();
        tracker.stage_entry(cmd.segment_key, cmd.data, cmd.record_count, entry_id);
        self.dirty_segments.insert(cmd.segment_key);

        self.out
            .store_batch_produce_timer(TimerCommand::SetSchedule {
                seq: self.wal.next_lsn(),
                timer: BatchFlushTimer::deadline(),
            });
    }

    /// Consume fetch. Resolves `(topic, range, entry_id)` through the segment
    /// store and routes to one of two read paths:
    /// - **Active** segment → hot read from the live tracker's tail cache.
    /// - **Sealed** segment → hand off to the cold-read pool, which reads the
    ///   segment file off disk and fulfils the consumer's reply itself.
    ///
    /// Returns `SegmentNotLocal` when no locally-hosted segment of the range
    /// covers the offset (the consumer re-resolves via `DescribeTopic`).
    fn handle_fetch(&self, cmd: Fetch) {
        let Some(read_stat) = self
            .segments
            .resolve(cmd.topic_id, cmd.range_id, cmd.entry_id)
        else {
            let _ = cmd.reply.send(FetchResult::SegmentNotLocal);
            return;
        };

        match read_stat {
            SegmentReadState::Active(key) => self.handle_hot_fetch(cmd, key),
            SegmentReadState::Sealed {
                key,
                start_entry_id,
                end_entry_id,
            } => self.dispatch_cold_fetch(cmd, key, start_entry_id, end_entry_id),
        }
    }

    /// Read committed records from a live (active) segment's tail cache.
    fn handle_hot_fetch(&self, cmd: Fetch, key: SegmentKey) {
        let Some(tracker) = self.segments.get(&key) else {
            // Resolver and store invariants should keep these in sync(just a guard)
            let _ = cmd.reply.send(FetchResult::SegmentNotLocal);
            return;
        };

        let cache = tracker.cache();
        let read_cursor = cache.load_read_cursor();
        let start_entry_id = tracker.start_entry_id();

        // Translate the absolute entry_id to a 0-based cache position.
        let cache_position = cmd.entry_id.saturating_sub(*start_entry_id);

        // The active tail can have `committed_entry_id = 0` and no entries
        // yet committed; distinguish "no data committed at all" by also
        // checking against the cache read cursor.
        if *cache_position >= read_cursor {
            // Past the tail — nothing to return.
            let _ = cmd.reply.send(FetchResult::Records {
                entries: Vec::new(),
                next_entry_id: cmd.entry_id,
                progress_signal: cmd.progress_signal,
            });
            return;
        }

        let mut entries = Vec::new();
        let mut next_entry_id = cmd.entry_id;
        let mut bytes_read: usize = 0;
        let max_bytes = cmd.max_bytes as usize;

        while let Some(entry_arc) =
            cache.read_committed(*next_entry_id.saturating_sub(*start_entry_id))
        {
            let payload_len = entry_arc.data.len();
            // Always include at least one entry (even if max_bytes is small).
            if !entries.is_empty() && bytes_read + payload_len > max_bytes {
                break;
            }
            next_entry_id = entry_arc.entry_id + 1u64;
            entries.push(entry_arc);
            bytes_read += payload_len;

            if bytes_read >= max_bytes {
                break;
            }
        }

        let _ = cmd.reply.send(FetchResult::Records {
            entries,
            next_entry_id,
            progress_signal: cmd.progress_signal,
        });
    }

    /// Hand a sealed-segment read off to the cold-read pool. The pool owns the
    /// reply channel from here on, so this returns immediately without blocking.
    fn dispatch_cold_fetch(
        &self,
        cmd: Fetch,
        key: SegmentKey,
        start_entry_id: EntryId,
        end_entry_id: EntryId,
    ) {
        if cmd.entry_id > end_entry_id {
            let _ = cmd.reply.send(FetchResult::EntryIdOutOfRange);
            return;
        }

        let req = ColdReadRequest {
            segment_key: key,
            segment_file_path: key.file_path(&self.config.data_dir, start_entry_id),
            start_entry_offset: cmd.entry_id,
            end_entry_id,
            max_bytes: cmd.max_bytes as u64,
            reply: ColdReadReply::Consumer {
                reply: cmd.reply,
                progress_signal: cmd.progress_signal,
            },
        };
        if let Err(flume::SendError(req)) = self.cold_read_handoff_sender.send(req)
            && let ColdReadReply::Consumer { reply, .. } = req.reply
        {
            let _ = reply.send(FetchResult::InternalError(
                "cold-read pool unavailable".into(),
            ));
        }
    }

    /// Returns the start and currently-committed entry IDs for the range's
    /// active segment on this node. Used by the consumer's `ListOffsets`
    /// query to bound its read window.
    fn handle_list_offsets(&self, cmd: ListOffsets) {
        // The active write head sits at the largest `start_entry_id` in the
        // active index. Resolve via the highest indexable offset (u64::MAX)
        // to land on whatever's currently active.
        let Some(SegmentReadState::Active(key)) =
            self.segments
                .resolve(cmd.topic_id, cmd.range_id, EntryId(u64::MAX))
        else {
            let _ = cmd.reply.send(ListOffsetsResult::SegmentNotLocal);
            return;
        };

        let Some(tracker) = self.segments.get(&key) else {
            let _ = cmd.reply.send(ListOffsetsResult::SegmentNotLocal);
            return;
        };
        let _ = cmd.reply.send(ListOffsetsResult::RangeOffsets {
            start_entry_id: tracker.start_entry_id(),
            next_entry_id: tracker.successor_start_entry_id(),
        });
    }

    fn read_consumer_offset(&self, query: ReadConsumerOffset) {
        let result = match self
            .consumer_offsets
            .get_replica_set_if_leader(&query.key, &self.node_id)
        {
            Ok(_) => ReadConsumerOffsetResult::Offset(self.consumer_offsets.offset(&query.key)),
            Err(leader) => ReadConsumerOffsetResult::NotLeader(leader),
        };
        let _ = query.reply.send(result);
    }

    fn handle_checkpoint_complete(&mut self, complete: SegmentCheckpointComplete) {
        self.pressure_checkpoints_in_flight
            .remove(&complete.segment_key);
        if let Some(tracker) = self.segments.get_mut(&complete.segment_key) {
            tracker.advance_checkpoint(
                complete.checkpointed_lsn,
                complete.new_frontier,
                complete.checkpointed_bytes,
            );
        }
        self.reclaim_checkpointed_wal();
    }

    fn handle_offset_checkpoint_complete(&mut self, complete: OffsetCheckpointComplete) {
        self.consumer_offsets
            .handle_offset_checkpoint_complete(complete.checkpointed_lsn);
        self.reclaim_checkpointed_wal();
    }

    // Garbage collect old Write-Ahead Log (WAL) files to free up disk space.
    // It determines how much of the WAL is no longer needed because the corresponding state has been safely persisted to disk,
    // and then it deletes those obsolete WAL sections.
    fn reclaim_checkpointed_wal(&mut self) {
        let watermark = self.compute_safe_deletion_lsn();
        if watermark > 0 {
            self.wal.delete_below(watermark);
        }

        if self.consumer_offsets.offset_checkpoint_in_flight() {
            return;
        }
        let Some(reclaimable_lsn) = self.wal.reclaimable_lsn() else {
            return;
        };
        let Some(oldest_offset_lsn) = self.consumer_offsets.oldest_uncheckpointed_lsn() else {
            return;
        };
        if self.segments.reclamation_watermark(reclaimable_lsn) < *oldest_offset_lsn {
            return;
        }

        // At this point, lsn must exist in consumer_offset so the following is just safeguard.
        if let Some(job) = self
            .consumer_offsets
            .raise_offset_checkpoint_job(self.config.data_dir.clone())
        {
            self.out.store_offset_checkpoint(job);
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

    /// Handle the orphan-GC ticker's prompt: sweep, then tell the ticker whether to keep
    /// going. The ticker stops once we report `Stop` (nothing left to reclaim), so it goes
    /// quiet when the work is done — recovery fills `recovered` once; it only shrinks.
    fn handle_orphan_gc_check(&mut self, check: OrphanGcCheck) {
        // Reclaim recovered segments the cluster never made this node a data replica of.
        //
        // we classify each recovered segment by how the owning group's decision shows up locally:
        // - registered (`sealed_bounds`) → reused via catch-up, the lottery → keep, prune;
        // - mid-catch-up (`pending_catch_ups`) → about to register → skip, don't race it;
        // - neither → never assigned here → stray → delete, prune.
        //
        // Deletes are bounded per sweep; a large backlog drains over several.
        let mut deletes = 0usize;
        for (key, start_entry) in self.recovered.orphan_candidates().collect::<Vec<_>>() {
            if self.segments.sealed_bounds(&key).is_some() {
                self.recovered.remove(&key); // reused (lottery won) → live, no longer an orphan
            } else if self.pending_catch_ups.contains_key(&key) {
                continue; // catch-up in flight → it will register; don't delete under it
            } else if deletes < ORPHAN_GC_BATCH
                && OrphanCandidate::new(key, start_entry)
                    .delete(&self.config.data_dir)
                    .inspect_err(|e| tracing::warn!("orphan GC: deleting {key:?} failed: {e}"))
                    .is_ok()
            {
                self.recovered.remove(&key);
                deletes += 1;
            }
        }

        let signal = if self.recovered.is_empty() {
            OrphanGcSignal::Stop
        } else {
            OrphanGcSignal::KeepTicking
        };
        let _ = check.reply.try_send(signal);
    }

    fn receive_peer_message(&mut self, received: ReceivePeerMessage) {
        use DataPlanePeerMessage as C;
        let ReceivePeerMessage { from, message } = received;

        match *message {
            C::PlaceSegment(cmd) => self.place_segment(cmd),
            C::ReplicateSegmentEntries(cmd) => self.replicate_segment_entries(&from, cmd),
            C::ReplicaEntriesAppended(cmd) => self.handle_replica_ack(cmd),
            C::ReplicateConsumerOffset(cmd) => self.replicate_consumer_offset(cmd),
            C::ConsumerOffsetReplicated(cmd) => self.handle_replica_offset_ack(cmd),
            C::InstallConsumerOffsetSnapshot(cmd) => {
                self.install_consumer_offset_snapshot(&from, cmd)
            }
            C::RequestConsumerOffsetSnapshot(cmd) => {
                self.request_consumer_offset_snapshot(&from, cmd)
            }
            C::ConsumerOffsetSnapshotInstalled(cmd) => {
                self.handle_consumer_offset_snapshot_installed(cmd)
            }
            C::AdvanceReplicaCommit(cmd) => self.handle_commit_advance(cmd),
            C::SegmentRollCommitted(message) => self.handle_segment_roll_committed(message),
            C::SegmentSealed(cmd) => self.handle_segment_sealed(cmd),

            // Pass-through to MultiRaftActor — RequestSegmentRoll is a control plane
            // message that shares the data transport wire format. The
            // coordinator loads its own live-nodes view from the shared
            // topology snapshot, so the data plane just forwards the request.
            C::RequestSegmentRoll(cmd) => {
                self.out
                    .store_coordinator_cmd(MultiRaftActorCommand::ProposeSegmentRoll(
                        ProposeSegmentRoll { request: cmd },
                    ));
            }

            // Assignment confirmation from a data-leader — forward to the local
            // MultiRaftActor (we are this shard's coordinator), which marks the
            // segment confirmed so the heartbeat sweep stops re-driving it.
            C::SegmentPlaced(cmd) => {
                self.out
                    .store_coordinator_cmd(MultiRaftActorCommand::AssignmentAck(cmd));
            }

            // Catch-ups: re-replicate a sealed segment to a newly assigned replica.
            C::AssignSegmentCatchUp(cmd) => self.handle_catch_up_assignment(cmd),
            C::RequestCatchUpEntries(cmd) => self.handle_catch_up_request(cmd),
            C::CatchUpEntries(cmd) => self.handle_catch_up_chunk(cmd),
            C::CatchUpEntriesSent(cmd) => self.handle_catch_up_stream_end(cmd),
            // Pass-through to the local MultiRaftActor (we coordinate this group):
            // a replica's confirmation that it finished catching up.
            C::SegmentCaughtUp(cmd) => {
                self.out
                    .store_coordinator_cmd(MultiRaftActorCommand::SegmentCaughtUp(cmd));
            }

            C::RequestDurableSegmentEnd(cmd) => self.handle_seal_boundary_query(cmd),
            // Pass-through to the local MultiRaftActor
            C::DurableSegmentEndReported(cmd) => {
                self.out
                    .store_coordinator_cmd(MultiRaftActorCommand::DurableSegmentEndReported(cmd));
            }
            C::DeleteSegments(cmd) => self.handle_delete_segments(cmd),
            C::ConsumerGroupEpochSealed(cmd) => self.handle_consumer_group_epoch_seal(cmd),
        }
    }

    /// Retention (D7): reclaim the named sealed segments the coordinator expired — for
    /// each, drop the tracker/sealed-index slot, delete the file, and remove its
    /// sparse-index entries. Idempotent per key: a segment this node no longer holds
    /// (already gone, never had it) is skipped, and orphan GC is the eventual backstop
    /// for a missed delete.
    fn handle_delete_segments(&mut self, cmd: DeleteSegments) {
        for key in cmd.segment_keys {
            let Some(start_offset) = self.segments.remove_segment(&key) else {
                continue;
            };
            self.recovered.remove(&key);
            if let Err(e) = OrphanCandidate::new(key, start_offset).delete(&self.config.data_dir) {
                tracing::warn!("retention: deleting segment file {key:?} failed: {e}");
            }
            self.out.store_delete_segment_index(key);
        }
    }

    fn handle_consumer_group_epoch_seal(&mut self, cmd: EpochSeal) {
        let Some(parked_commits) = self.consumer_offsets.handle_consumer_group_epoch_seal(cmd)
        else {
            return;
        };

        self.needs_flush = true;
        self.resume_future_offset_commits(parked_commits);
    }

    fn resume_future_offset_commits(&mut self, parked_commits: Vec<FutureOffsetCommit>) {
        for parked in parked_commits {
            match parked {
                FutureOffsetCommit::Client(pending) => {
                    self.commit_consumer_offset(pending);
                }
                FutureOffsetCommit::Replica(pending) => {
                    self.replicate_consumer_offset(pending);
                }
            }
        }
    }

    fn handle_seal_boundary_query(&mut self, cmd: RequestDurableSegmentEnd) {
        let durable_end = self.durable_end(&cmd.segment_key);
        self.out
            .store_transport_cmd(DataTransportCommand::send_to_targets(
                vec![cmd.coordinator],
                DurableSegmentEndReported {
                    segment_key: cmd.segment_key,
                    from: self.node_id.clone(),
                    durable_end,
                },
            ));
    }

    // ── Replacement-side catch-up: receive → verify → register ────────────

    /// The coordinator announced our (new) membership of this sealed segment.
    /// Reconcile against local state — declarative and idempotent:
    /// - already hold it through `sealed_end` → register if needed, transfer nothing;
    /// - already reconciling → no-op while progressing, resume if stalled;
    /// - else fetch `(local, sealed_end]` from a peer — a delta or a full copy.
    fn handle_catch_up_assignment(&mut self, cmd: AssignSegmentCatchUp) {
        if let Some(pending) = self.pending_catch_ups.get_mut(&cmd.segment_key) {
            // A receive is already in flight: no-op while it appends; once stalled
            // (no append across `CATCH_UP_IDLE_REDRIVES` re-drives) re-request the
            // rest from a fresh source, resuming from `last_written`.
            pending.idle_rounds += 1;
            if pending.idle_rounds < CATCH_UP_IDLE_REDRIVES {
                return;
            }
            pending.idle_rounds = 0;
            let local_end = pending.last_written;
            let Some(source) = Self::pick_catch_up_source(&cmd.replica_set, &self.node_id) else {
                return;
            };
            self.out
                .store_transport_cmd(DataTransportCommand::send_to_targets(
                    vec![source],
                    RequestCatchUpEntries {
                        segment_key: cmd.segment_key,
                        from: self.node_id.clone(),
                        local_end,
                    },
                ));
            return;
        }
        let local = self.local_sealed_progress(&cmd.segment_key);
        if local.is_some_and(|end| end >= cmd.sealed_end_entry_id) {
            // Full match: already hold it through `sealed_end`. Register it if the
            // resolver doesn't have it yet, ack the coordinator, transfer nothing.
            // The ack fires even if already registered, so a re-drive re-confirms.
            if self.segments.sealed_bounds(&cmd.segment_key).is_none() {
                self.segments.insert_sealed_from_catch_up(
                    cmd.segment_key,
                    cmd.start_entry_id,
                    cmd.sealed_end_entry_id,
                );
            }
            self.send_catch_up_ack(cmd.shard_group_id, cmd.segment_key);
            return;
        }
        let Some(source) = Self::pick_catch_up_source(&cmd.replica_set, &self.node_id) else {
            tracing::warn!(
                "catch-up: no source peer in {:?} for {:?}",
                cmd.replica_set,
                cmd.segment_key
            );
            return;
        };
        let path = cmd
            .segment_key
            .file_path(&self.config.data_dir, cmd.start_entry_id);
        let appender = match Self::open_catch_up_appender(cmd.segment_key, &path, local) {
            Ok(appender) => appender,
            Err(e) => {
                tracing::warn!("catch-up: cannot open segment file {path:?}: {e}");
                return;
            }
        };
        self.pending_catch_ups.insert(
            cmd.segment_key,
            PendingCatchUp {
                appender,
                shard_group_id: cmd.shard_group_id,
                start_offset: cmd.start_entry_id,
                sealed_end: cmd.sealed_end_entry_id,
                last_written: local,
                idle_rounds: 0,
                anchors: Vec::new(),
            },
        );
        self.out
            .store_transport_cmd(DataTransportCommand::send_to_targets(
                vec![source],
                RequestCatchUpEntries {
                    segment_key: cmd.segment_key,
                    from: self.node_id.clone(),
                    local_end: local,
                },
            ));
    }

    /// Highest verified entry id we already hold for this sealed segment — from the
    /// live sealed store (a survivor or a prior catch-up) or the recovered inventory
    /// (a restarted node's on-disk data). `None` if we hold nothing.
    fn local_sealed_progress(&self, key: &SegmentKey) -> Option<EntryId> {
        let registered = self.segments.sealed_bounds(key).map(|(_, end)| end);
        let recovered = self.recovered.get(key);
        registered.max(recovered)
    }

    /// Highest durable (fsync'd) entry id this node holds for `key`, across every
    /// place a copy can live: a live tracker (the active segment a survivor is
    /// still following), the sealed store, or the recovered inventory. `None` if
    /// we hold nothing. Reported in a `DurableSegmentEndReported` so the coordinator can seal
    /// a leader-crashed segment at the `min` of survivors' durable extents.
    fn durable_end(&self, key: &SegmentKey) -> Option<EntryId> {
        let live = self
            .segments
            .get(key)
            .and_then(|t| t.durable_end_entry_id());
        [live, self.local_sealed_progress(key)]
            .into_iter()
            .flatten()
            .max()
    }

    /// A peer to fetch from: any replica that isn't us, preferring one that isn't
    /// the (former) write leader at `replica_set[0]` so repair reads don't pile on
    /// it. Falls back to any non-self peer; `None` if we're the only member.
    fn pick_catch_up_source(replica_set: &[NodeId], me: &NodeId) -> Option<NodeId> {
        let leader = replica_set.first();
        replica_set
            .iter()
            .find(|n| *n != me && Some(*n) != leader)
            .or_else(|| replica_set.iter().find(|n| *n != me))
            .cloned()
    }

    /// Open the appender for an incoming catch-up: keep the verified prefix and
    /// append the delta when we hold a partial copy, or create a fresh file when we
    /// hold nothing.
    fn open_catch_up_appender(
        key: SegmentKey,
        path: &std::path::Path,
        local: Option<EntryId>,
    ) -> std::io::Result<SegmentAppender> {
        match local {
            None => SegmentAppender::create(key, path),
            Some(_) => {
                let valid_len = scan_segment_file(path)?.valid_len;
                SegmentAppender::open_truncating(key, path, valid_len)
            }
        }
    }

    /// Append a streamed batch to the in-progress receive, building sparse-index
    /// anchors as we go (same predicate the checkpoint/recovery paths use).
    /// A write failure aborts the receive — the source/coordinator re-drive.
    fn handle_catch_up_chunk(&mut self, cmd: CatchUpEntries) {
        let Some(pending) = self.pending_catch_ups.get_mut(&cmd.segment_key) else {
            tracing::debug!(
                "catch-up chunk for an unknown receive {:?}; dropping",
                cmd.segment_key
            );
            return;
        };
        let mut failed = false;
        for entry in &cmd.entries {
            // Append only the strictly-next id; skip duplicates / out-of-order. A
            // resume can overlap a stalled stream's tail, and a double append would
            // overstate the verified prefix.
            let next = pending
                .last_written
                .map_or(pending.start_offset, |w| w + 1u64);
            if entry.entry_id != next {
                continue;
            }
            match pending.appender.append_entry(
                entry.entry_id,
                WalRecord::data((*entry.data).clone(), entry.record_count),
            ) {
                Ok(anchor) => {
                    pending.anchors.extend(anchor);
                    pending.last_written = Some(entry.entry_id);
                    pending.idle_rounds = 0;
                }
                Err(e) => {
                    tracing::warn!(
                        "catch-up: append failed for {:?}: {e}; aborting receive",
                        cmd.segment_key
                    );
                    failed = true;
                    break;
                }
            }
        }
        if failed {
            self.pending_catch_ups.remove(&cmd.segment_key);
        }
    }

    /// End of stream. Fsync, re-scan to verify it reaches `sealed_end`, then on
    /// success index the anchors, register the segment, and ack the coordinator.
    fn handle_catch_up_stream_end(&mut self, cmd: CatchUpEntriesSent) {
        let Some(mut pending) = self.pending_catch_ups.remove(&cmd.segment_key) else {
            tracing::debug!(
                "catch-up done for an unknown receive {:?}; dropping",
                cmd.segment_key
            );
            return;
        };
        if let Err(e) = pending.appender.flush_and_sync() {
            tracing::warn!("catch-up: fsync failed for {:?}: {e}", cmd.segment_key);
            return;
        }
        let path = cmd
            .segment_key
            .file_path(&self.config.data_dir, pending.start_offset);
        let scan = match scan_segment_file(&path) {
            Ok(scan) => scan,
            Err(e) => {
                tracing::warn!(
                    "catch-up: verify scan failed for {:?}: {e}",
                    cmd.segment_key
                );
                return;
            }
        };
        if scan
            .last_entry_id
            .is_none_or(|last| last < pending.sealed_end)
        {
            tracing::warn!(
                "catch-up incomplete for {:?}: have {:?}, need {}; not registering",
                cmd.segment_key,
                scan.last_entry_id,
                pending.sealed_end
            );
            return;
        }
        // Verified through `sealed_end`. Hand the suffix anchors to the checkpoint
        // worker (the sole runtime sparse-index writer) and register the segment
        // so the cold-read resolver can serve it. The anchor write is async — cold
        // reads fall back to a byte-0 scan until it lands.
        self.out.store_put_anchors(pending.anchors);
        self.segments.insert_sealed_from_catch_up(
            cmd.segment_key,
            pending.start_offset,
            pending.sealed_end,
        );
        self.send_catch_up_ack(pending.shard_group_id, cmd.segment_key);
    }

    /// Tell the coordinator we hold this segment, so its sweep stops re-announcing.
    /// Routed via `SendToCoordinator`, mirroring `SegmentPlaced`.
    fn send_catch_up_ack(&mut self, shard_group_id: ShardGroupId, segment_key: SegmentKey) {
        self.out
            .store_transport_cmd(DataTransportSendToCoordinator {
                shard_group_id,
                message: SegmentCaughtUp {
                    segment_key,
                    shard_group_id,
                    from: self.node_id.clone(),
                }
                .into(),
            });
    }

    /// Source side: a peer wants `cmd.segment_key` brought up to its local end.
    fn handle_catch_up_request(&mut self, cmd: RequestCatchUpEntries) {
        let Some((start_offset, sealed_end)) = self.segments.sealed_bounds(&cmd.segment_key) else {
            tracing::warn!(
                "catch-up source has no sealed segment {:?}; dropping request",
                cmd.segment_key
            );
            return;
        };
        let start = cmd.local_end.map_or(start_offset, |held| held + 1u64);
        if start > sealed_end {
            // The requester is already at or past OUR committed end — nothing to
            // stream. Confirm completion immediately and skip a pointless cold-read
            // dispatch.
            self.send_catch_up_stream_end(cmd.from, cmd.segment_key);
            return;
        }
        self.dispatch_catch_up_read(cmd.from, cmd.segment_key, start_offset, sealed_end, start);
    }

    /// Hand one bounded segment read to the cold-read pool, routed back to this
    /// worker (not a consumer) via `ColdReadReply::CatchUp`.
    fn dispatch_catch_up_read(
        &self,
        requester: NodeId,
        segment_key: SegmentKey,
        start_offset: EntryId, // Segment base — names the file (`{segment_id}-{start_offset}.seg`)
        sealed_end: EntryId,
        read_from: EntryId,
    ) {
        let req = ColdReadRequest {
            segment_key,
            segment_file_path: segment_key.file_path(&self.config.data_dir, start_offset),
            start_entry_offset: read_from,
            end_entry_id: sealed_end,
            max_bytes: CATCH_UP_CHUNK_MAX_BYTES,
            reply: ColdReadReply::CatchUp(CatchUpReadReply {
                requester,
                start_offset,
                sealed_end,
                mailbox: self.self_tx.clone(),
            }),
        };
        if self.cold_read_handoff_sender.send(req).is_err() {
            tracing::warn!("cold-read pool unavailable; catch-up for {segment_key:?} dropped");
        }
    }

    /// The cold-read pool finished one batch for a catch-up source read. Emit it
    /// as a `CatchUpEntries` to the requester, then re-arm the next read until the
    /// sealed end is reached, at which point send `CatchUpEntriesSent`.
    fn handle_catch_up_read_complete(&mut self, cmd: CatchUpReadComplete) {
        let has_entries = !cmd.entries.is_empty();
        if has_entries {
            let entries = cmd
                .entries
                .into_iter()
                .map(CatchUpEntry::from_cache)
                .collect();
            self.out
                .store_transport_cmd(DataTransportCommand::send_to_targets(
                    vec![cmd.requester.clone()],
                    CatchUpEntries {
                        segment_key: cmd.segment_key,
                        entries,
                    },
                ));
        }

        // Re-arm only while a non-empty batch left more below the sealed end;
        // `next_offset` strictly advances per non-empty batch, so this terminates.
        if has_entries && cmd.next_offset <= cmd.sealed_end {
            self.dispatch_catch_up_read(
                cmd.requester,
                cmd.segment_key,
                cmd.start_offset,
                cmd.sealed_end,
                cmd.next_offset,
            );
        } else {
            self.send_catch_up_stream_end(cmd.requester, cmd.segment_key);
        }
    }

    fn send_catch_up_stream_end(&mut self, requester: NodeId, segment_key: SegmentKey) {
        self.out
            .store_transport_cmd(DataTransportCommand::send_to_targets(
                vec![requester],
                CatchUpEntriesSent { segment_key },
            ));
    }

    fn handle_replica_ack(&mut self, cmd: ReplicaEntriesAppended) {
        let Some(committed) = self.replication.process_ack(&cmd.segment_key, &cmd.from) else {
            return;
        };

        self.commit_segment(cmd.segment_key, committed.entry_id);
        self.out.produce_replies.extend(
            committed
                .replies
                .into_iter()
                .map(|reply| (committed.entry_id, reply)),
        );

        if let Some(seq) = committed.reset_timer_seq {
            self.out
                .repl_schedules
                .push((seq, ReplicationTimer::timeout(cmd.segment_key)));
        }

        self.check_pending_seal(cmd.segment_key);
    }

    fn commit_consumer_offset(&mut self, cmd: CommitConsumerOffset) {
        self.needs_flush |= self
            .consumer_offsets
            .commit_consumer_offset(cmd, &self.node_id);
    }

    fn replicate_consumer_offset(&mut self, cmd: ReplicateConsumerOffset) {
        if !self.is_follower_of(&cmd.replica_set) {
            return;
        }

        let Some(leader) = cmd.replica_set.leader().cloned() else {
            return;
        };

        let sealed_generation = self.consumer_offsets.latest_generation(&cmd.update.key);
        if cmd.update.generation < sealed_generation {
            let transport = DataTransportCommand::send_to_targets(
                vec![leader],
                ConsumerOffsetReplicated {
                    seq: cmd.seq,
                    update: cmd.update,
                    from: self.node_id.clone(),
                    result: ConsumerOffsetReplicationResult::StaleEpoch(StaleEpoch(
                        sealed_generation,
                    )),
                },
            );
            self.out.store_transport_cmd(transport);
            return;
        }

        if cmd.update.generation > sealed_generation {
            let targets = cmd.replica_set.except_for(&self.node_id);
            if !targets.is_empty() {
                let transport = DataTransportCommand::send_to_targets(
                    targets,
                    RequestConsumerOffsetSnapshot {
                        segment_key: cmd.segment_key,
                        replicas: cmd.replica_set.clone(),
                        requester: self.node_id.clone(),
                    },
                );
                self.out.store_transport_cmd(transport)
            }

            self.consumer_offsets
                .future_entry(cmd.update.key.clone())
                .push(FutureOffsetCommit::Replica(cmd));
            return;
        }

        self.consumer_offsets.push_pending_offset_mutation(cmd);
        self.needs_flush = true;
    }

    fn handle_replica_offset_ack(&mut self, cmd: ConsumerOffsetReplicated) {
        self.consumer_offsets.handle_replica_offset_ack(cmd);
        self.ack_ready_offset_placements();
    }

    fn install_consumer_offset_snapshot(
        &mut self,
        from: &NodeId,
        cmd: InstallConsumerOffsetSnapshot,
    ) {
        if !cmd.replica_set.contains(&self.node_id) || !cmd.replica_set.contains(from) {
            return;
        }
        let Some(leader) = cmd.replica_set.leader() else {
            return;
        };
        if leader != from && leader != &self.node_id {
            return;
        };

        let parked = self
            .consumer_offsets
            .install_consumer_offsets(cmd, &self.node_id);
        self.resume_future_offset_commits(parked);

        self.needs_flush = true;
    }

    fn request_consumer_offset_snapshot(
        &mut self,
        from: &NodeId,
        cmd: RequestConsumerOffsetSnapshot,
    ) {
        if &cmd.requester != from || !cmd.replicas.contains(&self.node_id) {
            return;
        }
        if !cmd.replicas.contains(&cmd.requester) {
            return;
        }

        if let Some(bootstrap) = self
            .consumer_offsets
            .create_offset_snapshot(cmd, &self.node_id)
        {
            self.out.store_transport_cmd(bootstrap);
        }
    }

    fn handle_consumer_offset_snapshot_installed(&mut self, cmd: ConsumerOffsetSnapshotInstalled) {
        if self.node_id != cmd.leader {
            return;
        }

        self.consumer_offsets.handle_bootstrap_ack(&cmd);

        if let Some(bootstrap) = self
            .consumer_offsets
            .create_offset_snapshot_for_unready_replicas(cmd.segment_key)
        {
            self.out.store_transport_cmd(bootstrap);
        }
        self.ack_ready_offset_placements();
    }

    fn handle_commit_advance(&mut self, cmd: AdvanceReplicaCommit) {
        let Some(tracker) = self.segments.get_mut(&cmd.segment_key) else {
            return;
        };

        if tracker.role() != SegmentRole::Follower {
            tracing::warn!(
                "AdvanceReplicaCommit received by non-follower: {:?}",
                cmd.segment_key
            );
            return;
        }
        tracker.commit_entry(cmd.committed_entry_id);
    }

    fn place_segment(&mut self, cmd: PlaceSegment) {
        if !self.is_leader_of(&cmd.replica_set) {
            return;
        }

        if let Some(transport) = self.consumer_offsets.install_leader_placement(
            cmd.segment_key,
            cmd.shard_group_id,
            cmd.replica_set.clone(),
        ) {
            self.out.store_transport_cmd(transport);
        }

        if !self.segments.contains_key(&cmd.segment_key) {
            let tracker = SegmentTracker::new_with_start_entry_id(
                cmd.segment_key
                    .file_path(&self.config.data_dir, cmd.start_entry_id),
                SegmentRole::Leader,
                cmd.replica_set,
                cmd.shard_group_id,
                cmd.start_entry_id,
            );
            self.segments.insert_active(cmd.segment_key, tracker);
        }

        self.ack_ready_offset_placements();
    }

    /// Checks if there are any consumer offset placements that have reached full readiness (all replicas in the set
    /// have successfully bootstrapped and are up-to-date)
    /// If then sends [`SegmentPlaced`](self::SegmentPlaced) to coordinator responsible for the shard
    fn ack_ready_offset_placements(&mut self) {
        for ack in self.consumer_offsets.drain_ready_placements(&self.node_id) {
            self.out
                .store_transport_cmd(DataTransportSendToCoordinator {
                    shard_group_id: ack.shard_group_id,
                    message: ack.into(),
                });
        }
    }

    fn replicate_segment_entries(&mut self, from: &NodeId, cmd: ReplicateSegmentEntries) {
        if cmd.replicas.leader() != Some(from) {
            return;
        }
        if !self.is_follower_of(&cmd.replicas) {
            tracing::warn!("{} is not follower for {:?}", self.node_id, cmd.segment_key);
            return;
        }

        if matches!(
            self.consumer_offsets.observe_placement(
                cmd.segment_key,
                ShardGroupId(0),
                &cmd.replicas,
                &self.node_id
            ),
            PlacementObservation::Stale | PlacementObservation::Conflict
        ) {
            return;
        };

        // If it sees this segment for the first time
        if !self.segments.contains_key(&cmd.segment_key) {
            self.segments.insert_active(
                cmd.segment_key,
                SegmentTracker::new_with_start_entry_id(
                    cmd.segment_key
                        .file_path(&self.config.data_dir, cmd.entry_id),
                    SegmentRole::Follower,
                    cmd.replicas,
                    ShardGroupId(0),
                    cmd.entry_id,
                ),
            );
        }
        let Some(tracker) = self.segments.get_mut(&cmd.segment_key) else {
            return;
        };

        tracker.stage_entry(cmd.segment_key, cmd.data, cmd.record_count, cmd.entry_id);
        self.dirty_segments.insert(cmd.segment_key);

        self.needs_flush = true;
    }

    // TODO refactor
    fn handle_segment_roll_committed(&mut self, cmd: SegmentRollCommitted) {
        self.pending_seal_requests.clear(&cmd.old_segment_key);
        let Some(old_tracker) = self.segments.get(&cmd.old_segment_key) else {
            return;
        };

        let shard_group_id = old_tracker.shard_group_id();
        let new_start_entry_id = old_tracker.successor_start_entry_id();

        let replayed_bytes = old_tracker.replayable_bytes();
        let committed_entry_id = old_tracker.last_committed_entry_id();

        let uncommitted_entry = old_tracker
            .uncommitted_entries()
            .chain(old_tracker.staged_for_replay())
            .collect::<Box<[(EntryPayload, u32)]>>();

        if !old_tracker.followers().is_empty() {
            self.out
                .store_transport_cmd(DataTransportCommand::send_to_targets(
                    old_tracker.followers().to_vec(),
                    SegmentSealed {
                        segment_key: cmd.old_segment_key,
                        committed_entry_id,
                    },
                ));
        }

        self.replication.segment_handoff(
            cmd.old_segment_key,
            cmd.old_segment_key.with_segment_id(cmd.new_segment_id),
        );
        self.retire_old_segment(cmd.old_segment_key);

        // The successor may already exist: on a roll the coordinator co-dispatches a
        // `PlaceSegment` for the new active segment alongside this committed result,
        // and it can win the race (creating the successor empty). Carry the tail onto
        // the existing tracker rather than a fresh one that `insert_active` would
        // silently refuse — which would drop the records (data loss) and strand the
        // counter.
        if self
            .segments
            .get(&cmd.old_segment_key.with_segment_id(cmd.new_segment_id))
            .is_none()
        {
            let new_tracker = SegmentTracker::new_with_start_entry_id(
                cmd.old_segment_key
                    .with_segment_id(cmd.new_segment_id)
                    .file_path(&self.config.data_dir, new_start_entry_id),
                SegmentRole::Leader,
                cmd.new_replica_set,
                shard_group_id,
                new_start_entry_id,
            );
            self.segments.insert_active(
                cmd.old_segment_key.with_segment_id(cmd.new_segment_id),
                new_tracker,
            );
        }

        if let Some(successor) = self
            .segments
            .get_mut(&cmd.old_segment_key.with_segment_id(cmd.new_segment_id))
        {
            for (data, record_count) in uncommitted_entry {
                let entry_id = successor.next_staged_entry_id();
                successor.stage_entry(
                    cmd.old_segment_key.with_segment_id(cmd.new_segment_id),
                    data,
                    record_count,
                    entry_id,
                );
            }
        }
        self.buffer_byte_count += replayed_bytes;
        self.dirty_segments
            .insert(cmd.old_segment_key.with_segment_id(cmd.new_segment_id));
        self.needs_flush = true;
    }

    fn handle_segment_sealed(&mut self, cmd: SegmentSealed) {
        match cmd.committed_entry_id {
            // Determine the end offset and notify the coordinator
            None => {
                if let Some(tracker) = self.segments.get(&cmd.segment_key)
                    && tracker.role() == SegmentRole::Leader
                {
                    if !tracker.is_fully_committed() {
                        self.pending_seals.insert(cmd.segment_key);
                        return; // Defer and exit early
                    }

                    let end_entry_id = tracker
                        .last_committed_entry_id()
                        .unwrap_or_else(|| tracker.start_entry_id().saturating_sub(1));

                    self.out
                        .store_transport_cmd(DataTransportSendToCoordinator {
                            shard_group_id: tracker.shard_group_id(),
                            message: RequestSegmentRoll {
                                from: self.node_id.clone(),
                                segment_key: cmd.segment_key,
                                failed_nodes: vec![],
                                end_entry_id,
                            }
                            .into(),
                        });
                }
            }

            // Commit the specified entries
            Some(end) => {
                if let Some(tracker) = self.segments.get_mut(&cmd.segment_key) {
                    let start = tracker
                        .last_committed_entry_id()
                        .map_or(tracker.start_entry_id(), |c| c + 1u64);

                    for offset in *start..=*end {
                        tracker.commit_entry(EntryId(offset));
                    }
                }
            }
        }

        // Cleanup applies to all paths that weren't deferred
        self.retire_old_segment(cmd.segment_key);
    }

    fn check_pending_seal(&mut self, segment_key: SegmentKey) {
        if !self.pending_seals.contains(&segment_key) {
            return;
        }

        let Some(tracker) = self.segments.get(&segment_key) else {
            return;
        };

        if !tracker.is_fully_committed() {
            return;
        }

        let actual_end_offset = tracker
            .last_committed_entry_id()
            .unwrap_or_else(|| tracker.start_entry_id().saturating_sub(1));

        self.out
            .store_transport_cmd(DataTransportSendToCoordinator {
                shard_group_id: tracker.shard_group_id(),
                message: RequestSegmentRoll {
                    from: self.node_id.clone(),
                    segment_key,
                    failed_nodes: vec![],
                    end_entry_id: actual_end_offset,
                }
                .into(),
            });

        self.retire_old_segment(segment_key);
        self.pending_seals.remove(&segment_key);
    }

    // Drop the sealed segment's live tracker and checkpoint its committed
    // records so cold reads can still serve them.
    fn retire_old_segment(&mut self, segment_key: SegmentKey) {
        if let Some(tracker) = self.segments.take_active_and_seal(segment_key) {
            if tracker.role() == SegmentRole::Leader {
                let staged_bytes: usize = tracker
                    .staged_for_replay()
                    .map(|(data, _)| data.len())
                    .sum();
                self.buffer_byte_count -= staged_bytes;
            }
            self.out.store_checkpoint(tracker.checkpoint(segment_key));
        }
    }

    fn commit_segment(&mut self, segment_key: SegmentKey, entry_id: EntryId) {
        let Some(tracker) = self.segments.get_mut(&segment_key) else {
            return;
        };

        tracker.commit_entry(entry_id);

        let followers = tracker.followers().to_vec();
        if !followers.is_empty() {
            self.out
                .store_transport_cmd(DataTransportCommand::send_to_targets(
                    followers,
                    AdvanceReplicaCommit {
                        segment_key,
                        committed_entry_id: entry_id,
                    },
                ));
        }

        if tracker.role() == SegmentRole::Leader
            && tracker.size_limit_reached(self.config.segment_size_limit)
            && tracker.is_fully_committed()
        {
            self.out.store_checkpoint(tracker.checkpoint(segment_key));
            self.enqueue_seal_request(segment_key);
        }
    }

    pub(crate) fn enqueue_seal_for_aged_segments(&mut self, max_age: std::time::Duration) {
        let aged: Box<[SegmentKey]> = self
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
                message: RequestSegmentRoll {
                    from: self.node_id.clone(),
                    segment_key,
                    failed_nodes: failed_nodes.clone(),
                    end_entry_id: tracker.committed_entry_id(),
                }
                .into(),
            });
        self.pending_seal_requests
            .track(segment_key, failed_nodes, tokio::time::Instant::now());
    }

    fn enqueue_seal_request(&mut self, segment_key: SegmentKey) {
        if self.pending_seal_requests.is_tracked(&segment_key) {
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
                message: RequestSegmentRoll {
                    from: self.node_id.clone(),
                    segment_key,
                    failed_nodes: vec![],
                    end_entry_id: tracker.committed_entry_id(),
                }
                .into(),
            });
        self.pending_seal_requests
            .track(segment_key, vec![], tokio::time::Instant::now());
    }

    // Fast Path should_flush check
    fn should_flush(&self) -> bool {
        self.needs_flush || self.buffer_byte_count >= self.config.batch_max_bytes
    }

    fn has_staged_data(&self) -> bool {
        self.consumer_offsets.has_pending_offsets()
            || self
                .dirty_segments
                .iter()
                .any(|k| self.segments.get(k).is_some_and(|t| t.has_staged()))
    }

    fn flush_batch(&mut self) {
        let Ok(encoded_offsets) = self.consumer_offsets.encode_offsets() else {
            self.needs_flush = !self.dirty_segments.is_empty();
            return;
        };

        for &key in &self.dirty_segments {
            if let Some(tracker) = self.segments.get_mut(&key) {
                tracker.stage_to_wal(self.wal.buf());
            }
        }

        for payload in encoded_offsets {
            let _ = WalRecord::consumer_offset(payload.into()).encode_to(self.wal.buf());
        }

        if !self.has_staged_data() {
            self.needs_flush = false;
            return;
        }
        let _ = WalRecord::batch_end().encode_to(self.wal.buf());

        let lsn = match self.wal.flush_batch() {
            Ok(lsn) => lsn,
            Err(e) => {
                self.rollback(e);
                return;
            }
        };

        for c in self.consumer_offsets.flush_batch(&self.node_id, lsn) {
            self.out.store_transport_cmd(c);
        }

        let mut segment_batches: Vec<PendingReplicationBatch> = Vec::new();

        for key in std::mem::take(&mut self.dirty_segments) {
            let Some(tracker) = self.segments.get_mut(&key) else {
                continue;
            };
            for entry in tracker.publish_staged(lsn) {
                match tracker.role() {
                    SegmentRole::Leader => {
                        if tracker.followers().is_empty() {
                            // No followers → committed the moment it's durable.
                            tracker.commit_entry(entry.entry_id);
                            if let Some(reply) = self.replication.pop_pending_reply(&key) {
                                let _ = reply.send(ProduceAck::Ok {
                                    entry_id: entry.entry_id,
                                });
                            }
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
                                ReplicaEntriesAppended {
                                    segment_key: key,
                                    entry_id: entry.entry_id,
                                    from: self.node_id.clone(),
                                },
                            ));
                    }
                }
            }

            if tracker.role() == SegmentRole::Leader
                && tracker.size_limit_reached(self.config.segment_size_limit)
                && tracker.is_fully_committed()
            {
                self.out.store_checkpoint(tracker.checkpoint(key));
                self.enqueue_seal_request(key);
            }

            self.check_pending_seal(key);
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

        self.buffer_byte_count = 0;
        self.needs_flush = false;
        self.submit_checkpoint_if_cache_pressure();
        self.reclaim_checkpointed_wal();
    }

    fn rollback(&mut self, e: std::io::Error) {
        tracing::error!("WAL flush failed: {e}");
        self.wal.clear_buf();

        for key in std::mem::take(&mut self.dirty_segments) {
            if let Some(tracker) = self.segments.get_mut(&key) {
                tracker.abort_staged();
            }
        }

        self.replication.fail_all(e.to_string());
        self.consumer_offsets.fail_all(&e.to_string());
        self.needs_flush = false;
    }

    // Calculates global maximum safe boundary(LSN) that is safe to delete.
    fn compute_safe_deletion_lsn(&self) -> u64 {
        let Some(reclaimable_lsn) = self.wal.reclaimable_lsn() else {
            return 0;
        };

        let segment_watermark = self.segments.reclamation_watermark(reclaimable_lsn);
        let offset_watermark = self.consumer_offsets.reclamation_watermark(reclaimable_lsn);

        segment_watermark.min(offset_watermark)
    }

    fn hot_cache_bytes(&self) -> u64 {
        self.segments.values().map(|t| t.hot_cache_bytes()).sum()
    }

    fn submit_checkpoint_if_cache_pressure(&mut self) {
        let Some(threshold) = self.config.cache_pressure_threshold_bytes() else {
            return;
        };
        if threshold == 0 || self.hot_cache_bytes() >= threshold {
            self.submit_checkpoint_for_pressure();
        }
    }

    fn submit_checkpoint_for_pressure(&mut self) {
        let mut candidates: Vec<(SegmentKey, u64)> = self
            .segments
            .iter()
            .filter(|(key, _)| !self.pressure_checkpoints_in_flight.contains(key))
            .map(|(key, tracker)| (*key, tracker.checkpointable_bytes()))
            .collect();

        candidates.sort_by_key(|b| std::cmp::Reverse(b.1));

        if let Some((key, bytes)) = candidates.first()
            && *bytes > 0
            && let Some(tracker) = self.segments.get(key)
        {
            self.out.store_checkpoint(tracker.checkpoint(*key));
            self.pressure_checkpoints_in_flight.insert(*key);
        }
    }

    fn enqueue_timed_out_seal_retries(&mut self) {
        let due = self.pending_seal_requests.take_due(
            tokio::time::Instant::now(),
            self.config.seal_request_timeout,
        );
        for (segment_key, failed_nodes) in due {
            let Some(tracker) = self.segments.get(&segment_key) else {
                continue;
            };
            if tracker.role() != SegmentRole::Leader {
                continue;
            }
            self.out
                .store_transport_cmd(DataTransportSendToCoordinator {
                    shard_group_id: tracker.shard_group_id(),
                    message: RequestSegmentRoll {
                        from: self.node_id.clone(),
                        segment_key,
                        failed_nodes,
                        end_entry_id: tracker.committed_entry_id(),
                    }
                    .into(),
                });
        }
    }

    fn is_follower_of(&self, replicas: &Replicas) -> bool {
        replicas.contains(&self.node_id) && replicas.leader() != Some(&self.node_id)
    }

    fn is_leader_of(&self, replicas: &Replicas) -> bool {
        replicas.contains(&self.node_id) && replicas.leader() == Some(&self.node_id)
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
            self.buffer_byte_count,
            actual_byte_count,
            "buffer_byte_count ({}) != actual leader staged bytes ({actual_byte_count}); \
             segments(id,role,staged_bytes)={:?}",
            self.buffer_byte_count,
            self.segments
                .iter()
                .map(|(k, t)| {
                    let staged: usize = t.staged_entries().iter().map(|e| e.byte_len()).sum();
                    (k.segment_id.0, t.role(), staged)
                })
                .collect::<Vec<_>>()
        );

        self.segments.assert_invariants();
    }
}

#[cfg(test)]
mod tests {
    use crate::control_plane::Replicas;
    use crate::data_plane::consumer_offset_management::ledger::{
        ConsumerOffsetKey, ConsumerOffsetPosition, ConsumerOffsetUpdate, EpochSeal, OffsetLedger,
    };
    use std::path::PathBuf;

    use super::*;
    use crate::control_plane::membership::ShardGroupId;
    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
    use crate::data_plane::checkpoint::CheckpointTask;
    use crate::data_plane::cold_read::DEFAULT_POOL_SIZE;
    use crate::data_plane::recovery::segment_scan::RecoveredSegments;
    use crate::data_plane::wal::WalWriter;
    use tokio::sync::oneshot;

    use bytes::Bytes;

    impl<T: WalStorage> DataPlane<T> {
        fn has_buffered_data(&self) -> bool {
            self.buffer_byte_count > 0
        }

        fn process_peer(&mut self, message: impl Into<DataPlanePeerMessage>) {
            self.handle_command(receive_peer_message(message.into()));
        }
    }

    fn test_key() -> SegmentKey {
        SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0))
    }

    fn receive_peer_message(message: DataPlanePeerMessage) -> DataPlaneCommand {
        let from = match &message {
            DataPlanePeerMessage::ReplicateSegmentEntries(command) => {
                command.replicas.leader().cloned().unwrap()
            }
            DataPlanePeerMessage::SegmentPlaced(event) => event.from.clone(),
            DataPlanePeerMessage::ReplicaEntriesAppended(event) => event.from.clone(),
            DataPlanePeerMessage::ReplicateConsumerOffset(command) => {
                command.replica_set.leader().cloned().unwrap()
            }
            DataPlanePeerMessage::ConsumerOffsetReplicated(event) => event.from.clone(),
            DataPlanePeerMessage::InstallConsumerOffsetSnapshot(command) => {
                command.replica_set.leader().cloned().unwrap()
            }
            DataPlanePeerMessage::RequestConsumerOffsetSnapshot(request) => {
                request.requester.clone()
            }
            DataPlanePeerMessage::ConsumerOffsetSnapshotInstalled(event) => event.from.clone(),
            DataPlanePeerMessage::RequestSegmentRoll(request) => request.from.clone(),
            DataPlanePeerMessage::RequestCatchUpEntries(request) => request.from.clone(),
            DataPlanePeerMessage::SegmentCaughtUp(event) => event.from.clone(),
            DataPlanePeerMessage::RequestDurableSegmentEnd(request) => request.coordinator.clone(),
            DataPlanePeerMessage::DurableSegmentEndReported(event) => event.from.clone(),
            DataPlanePeerMessage::PlaceSegment(_)
            | DataPlanePeerMessage::AdvanceReplicaCommit(_)
            | DataPlanePeerMessage::SegmentRollCommitted(_)
            | DataPlanePeerMessage::SegmentSealed(_)
            | DataPlanePeerMessage::AssignSegmentCatchUp(_)
            | DataPlanePeerMessage::CatchUpEntries(_)
            | DataPlanePeerMessage::CatchUpEntriesSent(_)
            | DataPlanePeerMessage::DeleteSegments(_)
            | DataPlanePeerMessage::ConsumerGroupEpochSealed(_) => NodeId::new("test-peer"),
        };
        receive_peer_message_from(from, message)
    }

    fn receive_peer_message_from(
        from: NodeId,
        message: impl Into<DataPlanePeerMessage>,
    ) -> DataPlaneCommand {
        ReceivePeerMessage {
            from,
            message: Box::new(message.into()),
        }
        .into()
    }

    fn consumer_offset_key() -> ConsumerOffsetKey {
        ConsumerOffsetKey {
            topic_id: TopicId(1),
            range_id: RangeId(0),
            group_id: "group".into(),
        }
    }

    #[test]
    fn future_consumer_offset_waits_for_epoch_seal() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.process(assign_segment(test_key(), vec![test_node_id()]));
        let position = ConsumerOffsetPosition {
            entry_id: EntryId(7),
            batch_offset: 2,
            absolute_offset: 12,
        };
        let (reply, mut response) = oneshot::channel();
        dp.process(CommitConsumerOffset {
            update: ConsumerOffsetUpdate {
                key: consumer_offset_key(),
                generation: 2.into(),
                position,
            },
            reply,
        });
        assert!(matches!(
            response.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        dp.process_peer(DataPlanePeerMessage::ConsumerGroupEpochSealed(EpochSeal {
            generation: 2.into(),
            key: ConsumerOffsetKey {
                topic_id: TopicId(1),
                range_id: RangeId(0),
                group_id: "group".into(),
            },
        }));
        assert!(matches!(
            response.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        dp.flush_batch();
        assert_eq!(
            response.try_recv().unwrap(),
            ConsumerOffsetCommitAck::Committed
        );
        assert_eq!(
            dp.consumer_offsets.offset(&consumer_offset_key()),
            Some(position)
        );
        assert_eq!(
            dp.consumer_offsets
                .uncheckpointed_offset_lsns()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(dp.compute_safe_deletion_lsn(), 0);
        assert!(dp.out.checkpoint_tasks.is_empty());

        let mut scanner =
            crate::data_plane::recovery::wal_scan::WalScanner::open(dir.path()).unwrap();
        let batch = scanner.next_batch().unwrap().unwrap();
        assert_eq!(batch.records.len(), 2);
        assert!(
            batch.records.iter().all(
                |record| record.record_type == super::super::wal::WalRecordType::ConsumerOffset
            )
        );
        drop(scanner);

        // A later rotation makes the file reclaimable. Only then do we clone
        // and snapshot the cache, covering both offset batches at once.
        dp.wal.set_max_file_size(1);
        let next_position = ConsumerOffsetPosition {
            entry_id: EntryId(8),
            batch_offset: 0,
            absolute_offset: 13,
        };
        let (next_reply, mut next_response) = oneshot::channel();
        dp.process(CommitConsumerOffset {
            update: ConsumerOffsetUpdate {
                key: consumer_offset_key(),
                generation: 2.into(),
                position: next_position,
            },
            reply: next_reply,
        });
        dp.flush_batch();
        assert_eq!(
            next_response.try_recv().unwrap(),
            ConsumerOffsetCommitAck::Committed
        );
        dp.segments
            .get_mut(&test_key())
            .unwrap()
            .advance_checkpoint(2, 0, 0);
        dp.reclaim_checkpointed_wal();
        assert!(matches!(
            dp.out.checkpoint_tasks.as_slice(),
            [CheckpointTask::ConsumerOffsets(job)] if job.checkpointed_lsn == 2
        ));

        dp.handle_command(OffsetCheckpointComplete {
            checkpointed_lsn: 2,
        });
        assert!(dp.consumer_offsets.uncheckpointed_offset_lsns().is_empty());
        assert_eq!(dp.consumer_offsets.offset_checkpoint_lsn(), 2);
        assert_eq!(dp.wal.reclaimable_lsn(), None);
    }

    #[test]
    fn stale_consumer_offset_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.process(assign_segment(test_key(), vec![test_node_id()]));
        dp.process_peer(DataPlanePeerMessage::ConsumerGroupEpochSealed(EpochSeal {
            generation: 4.into(),
            key: ConsumerOffsetKey {
                topic_id: TopicId(1),
                range_id: RangeId(0),
                group_id: "group".into(),
            },
        }));
        let (reply, mut response) = oneshot::channel();
        dp.process(CommitConsumerOffset {
            update: ConsumerOffsetUpdate {
                key: consumer_offset_key(),
                generation: 3.into(),
                position: ConsumerOffsetPosition {
                    entry_id: EntryId(1),
                    batch_offset: 0,
                    absolute_offset: 1,
                },
            },
            reply,
        });
        assert_eq!(
            response.try_recv().unwrap(),
            ConsumerOffsetCommitAck::StaleEpoch(StaleEpoch(4.into()))
        );
    }

    #[test]
    fn sealed_range_retains_offset_coordinator() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.process(assign_segment(test_key(), vec![test_node_id()]));
        dp.process_peer(DataPlanePeerMessage::SegmentSealed(SegmentSealed {
            segment_key: test_key(),
            committed_entry_id: None,
        }));
        assert!(!dp.segments.contains_key(&test_key()));

        let position = ConsumerOffsetPosition {
            entry_id: EntryId(3),
            batch_offset: 0,
            absolute_offset: 3,
        };
        let (reply, mut response) = oneshot::channel();
        dp.process(CommitConsumerOffset {
            update: ConsumerOffsetUpdate {
                key: consumer_offset_key(),
                generation: 0.into(),
                position,
            },
            reply,
        });
        dp.flush_batch();

        assert_eq!(
            response.try_recv().unwrap(),
            ConsumerOffsetCommitAck::Committed
        );
        assert_eq!(
            dp.consumer_offsets.offset(&consumer_offset_key()),
            Some(position)
        );
    }

    #[test]
    fn consumer_offset_leader_waits_for_every_replica_ack() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower_1 = NodeId::new("follower-1");
        let follower_2 = NodeId::new("follower-2");
        dp.process(assign_segment(
            test_key(),
            vec![test_node_id(), follower_1.clone(), follower_2.clone()],
        ));
        for follower in [&follower_1, &follower_2] {
            dp.process_peer(DataPlanePeerMessage::ConsumerOffsetSnapshotInstalled(
                ConsumerOffsetSnapshotInstalled {
                    segment_key: test_key(),
                    from: follower.clone(),
                    leader: test_node_id(),
                },
            ));
        }
        dp.process_peer(DataPlanePeerMessage::ConsumerGroupEpochSealed(EpochSeal {
            generation: 1.into(),
            key: consumer_offset_key(),
        }));
        dp.flush_batch();
        dp.out.transport_cmds.clear();

        let position = ConsumerOffsetPosition {
            entry_id: EntryId(4),
            batch_offset: 0,
            absolute_offset: 4,
        };
        let (reply, mut response) = oneshot::channel();
        dp.process(CommitConsumerOffset {
            update: ConsumerOffsetUpdate {
                key: consumer_offset_key(),
                generation: 1.into(),
                position,
            },
            reply,
        });
        dp.flush_batch();
        assert!(matches!(
            response.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));

        let replicated = dp
            .out
            .transport_cmds
            .iter()
            .find_map(|command| match command {
                DataTransportCommand::SendToTargets(send) => match &send.message {
                    DataPlanePeerMessage::ReplicateConsumerOffset(commit) => Some(commit.clone()),
                    DataPlanePeerMessage::PlaceSegment(_)
                    | DataPlanePeerMessage::SegmentPlaced(_)
                    | DataPlanePeerMessage::ReplicateSegmentEntries(_)
                    | DataPlanePeerMessage::ReplicaEntriesAppended(_)
                    | DataPlanePeerMessage::ConsumerOffsetReplicated(_)
                    | DataPlanePeerMessage::InstallConsumerOffsetSnapshot(_)
                    | DataPlanePeerMessage::RequestConsumerOffsetSnapshot(_)
                    | DataPlanePeerMessage::ConsumerOffsetSnapshotInstalled(_)
                    | DataPlanePeerMessage::AdvanceReplicaCommit(_)
                    | DataPlanePeerMessage::RequestSegmentRoll(_)
                    | DataPlanePeerMessage::SegmentRollCommitted(_)
                    | DataPlanePeerMessage::SegmentSealed(_)
                    | DataPlanePeerMessage::AssignSegmentCatchUp(_)
                    | DataPlanePeerMessage::RequestCatchUpEntries(_)
                    | DataPlanePeerMessage::CatchUpEntries(_)
                    | DataPlanePeerMessage::CatchUpEntriesSent(_)
                    | DataPlanePeerMessage::SegmentCaughtUp(_)
                    | DataPlanePeerMessage::RequestDurableSegmentEnd(_)
                    | DataPlanePeerMessage::DurableSegmentEndReported(_)
                    | DataPlanePeerMessage::DeleteSegments(_)
                    | DataPlanePeerMessage::ConsumerGroupEpochSealed(_) => None,
                },
                DataTransportCommand::SendToCoordinator(_)
                | DataTransportCommand::DisconnectPeer(_) => None,
            })
            .expect("leader must replicate the offset commit");

        for follower in [follower_1, follower_2] {
            dp.process_peer(DataPlanePeerMessage::ConsumerOffsetReplicated(
                ConsumerOffsetReplicated {
                    seq: replicated.seq,
                    update: replicated.update.clone(),
                    from: follower,
                    result: ConsumerOffsetReplicationResult::Committed,
                },
            ));
        }
        assert_eq!(
            response.try_recv().unwrap(),
            ConsumerOffsetCommitAck::Committed
        );
    }

    #[test]
    fn consumer_offset_follower_acks_only_after_wal_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader");
        let position = ConsumerOffsetPosition {
            entry_id: EntryId(5),
            batch_offset: 1,
            absolute_offset: 6,
        };
        dp.process_peer(DataPlanePeerMessage::ReplicateConsumerOffset(
            ReplicateConsumerOffset {
                seq: 9,
                segment_key: test_key(),
                replica_set: Replicas::new(vec![leader, test_node_id()]),
                update: ConsumerOffsetUpdate {
                    key: consumer_offset_key(),
                    generation: 0.into(),
                    position,
                },
            },
        ));
        assert!(dp.out.transport_cmds.is_empty());

        dp.flush_batch();
        assert_eq!(
            dp.consumer_offsets.offset(&consumer_offset_key()),
            Some(position)
        );
        assert!(dp.out.transport_cmds.iter().any(|command| matches!(
            command,
            DataTransportCommand::SendToTargets(send)
                if matches!(
                    &send.message,
                    DataPlanePeerMessage::ConsumerOffsetReplicated(ConsumerOffsetReplicated {
                        seq: 9,
                        result: ConsumerOffsetReplicationResult::Committed,
                        ..
                    })
                )
        )));
    }

    #[test]
    fn consumer_offset_bootstrap_acks_only_after_wal_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader");
        let position = ConsumerOffsetPosition {
            entry_id: EntryId(8),
            batch_offset: 2,
            absolute_offset: 11,
        };

        dp.process_peer(DataPlanePeerMessage::InstallConsumerOffsetSnapshot(
            InstallConsumerOffsetSnapshot {
                segment_key: test_key(),
                replica_set: Replicas::new(vec![leader.clone(), test_node_id()]),
                entries: vec![
                    crate::data_plane::consumer_offset_management::ledger::ConsumerOffsetSnapshot {
                        key: consumer_offset_key(),
                        generation: 3.into(),
                        position: Some(position),
                    },
                ]
                .into_boxed_slice(),
            },
        ));

        assert!(dp.out.transport_cmds.iter().all(|cmd| !matches!(
            cmd,
            DataTransportCommand::SendToTargets(send)
                if matches!(send.message, DataPlanePeerMessage::ConsumerOffsetSnapshotInstalled(_))
        )));

        dp.flush_batch();

        assert_eq!(
            dp.consumer_offsets.offset(&consumer_offset_key()),
            Some(position)
        );
        assert!(dp.out.transport_cmds.iter().any(|cmd| matches!(
            cmd,
            DataTransportCommand::SendToTargets(send)
                if send.targets.as_ref() == [leader.clone()]
                    && matches!(send.message, DataPlanePeerMessage::ConsumerOffsetSnapshotInstalled(_))
        )));
    }

    #[test]
    fn consumer_offset_bootstrap_releases_a_parked_newer_epoch_commit() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader");
        let position = ConsumerOffsetPosition {
            entry_id: EntryId(9),
            batch_offset: 0,
            absolute_offset: 12,
        };
        let update = ConsumerOffsetUpdate {
            key: consumer_offset_key(),
            generation: 2.into(),
            position,
        };
        dp.process_peer(DataPlanePeerMessage::ReplicateConsumerOffset(
            ReplicateConsumerOffset {
                seq: 10,
                segment_key: test_key(),
                replica_set: Replicas::new(vec![leader.clone(), test_node_id()]),
                update: update.clone(),
            },
        ));
        assert!(!dp.consumer_offsets.has_pending_offsets());
        assert!(dp.out.transport_cmds.iter().any(|cmd| matches!(
            cmd,
            DataTransportCommand::SendToTargets(send)
                if send.targets.as_ref() == [leader.clone()]
                    && matches!(send.message, DataPlanePeerMessage::RequestConsumerOffsetSnapshot(_))
        )));

        dp.process_peer(DataPlanePeerMessage::InstallConsumerOffsetSnapshot(
            InstallConsumerOffsetSnapshot {
                segment_key: test_key(),
                replica_set: Replicas::new(vec![leader.clone(), test_node_id()]),
                entries: vec![
                    crate::data_plane::consumer_offset_management::ledger::ConsumerOffsetSnapshot {
                        key: consumer_offset_key(),
                        generation: 2.into(),
                        position: None,
                    },
                ]
                .into_boxed_slice(),
            },
        ));
        dp.flush_batch();

        assert_eq!(
            dp.consumer_offsets.offset(&consumer_offset_key()),
            Some(position)
        );
        assert!(dp.out.transport_cmds.iter().any(|cmd| matches!(
            cmd,
            DataTransportCommand::SendToTargets(send)
                if matches!(
                    &send.message,
                    DataPlanePeerMessage::ConsumerOffsetReplicated(ack)
                        if ack.seq == 10 && ack.update == update
                )
        )));
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
            hot_cache_budget_bytes: 0,
            hot_cache_pressure_watermark: 0.9,
            seal_request_timeout: std::time::Duration::from_secs(5),
            orphan_gc_interval: std::time::Duration::from_secs(300),
            data_dir: dir,
        }
    }

    /// A trivial empty-dir recovery: nothing on disk → an empty inventory. The
    /// same shape production builds `DataPlane` with, just with no segments.
    fn empty_inventory() -> LocalInventory {
        LocalInventory::from_recovered(&RecoveredSegments::default())
    }

    fn make_data_plane(dir: &tempfile::TempDir) -> DataPlane<WalWriter> {
        make_data_plane_with(dir, empty_inventory())
    }

    fn make_data_plane_with(
        dir: &tempfile::TempDir,
        recovered: LocalInventory,
    ) -> DataPlane<WalWriter> {
        let wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        let out = DataPlaneOutputs::test();
        // Tests don't exercise the cold-read path; an unbounded sink channel
        // whose receiver we leak keeps any dispatched request from panicking.
        let (cold_read_tx, _cold_read_rx) = flume::unbounded();
        std::mem::forget(_cold_read_rx);
        let (self_tx, _self_rx) = flume::unbounded::<DataPlaneMessage>();
        std::mem::forget(_self_rx);
        DataPlane::new(
            test_node_id(),
            test_config(dir.path().to_path_buf()),
            wal,
            cold_read_tx,
            DataPlaneSender(self_tx),
            out,
            RecoveryOutput {
                inventory: recovered,
                data_dir: dir.path().to_path_buf(),
                offsets: OffsetLedger::default(),
            },
        )
    }

    /// A `LocalInventory` reporting `key` verified through `local_end` — as if
    /// recovery had scanned a local copy of that prefix off disk.
    fn inventory_with(key: SegmentKey, local_end: u64) -> LocalInventory {
        let mut recovered = RecoveredSegments::default();
        for id in 0..=local_end {
            recovered.advance(key, EntryId(id));
        }
        LocalInventory::from_recovered(&recovered)
    }

    /// Write a segment file in the checkpoint framing (one `(Data, BatchEnd)` batch
    /// per entry, bare payloads) so a catch-up's partial path can scan it.
    fn write_seg_file(path: &std::path::Path, entries: &[(&[u8], u32)]) {
        use crate::data_plane::wal::WalRecord;
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut buf = Vec::new();
        for &(data, record_count) in entries {
            WalRecord::data(Bytes::copy_from_slice(data), record_count)
                .encode_to(&mut buf)
                .unwrap();
            WalRecord::batch_end().encode_to(&mut buf).unwrap();
        }
        std::fs::write(path, buf).unwrap();
    }

    fn process_and_flush(dp: &mut DataPlane<WalWriter>, cmd: DataPlaneCommand) {
        dp.handle_command(cmd);
        if dp.should_flush() {
            dp.flush_batch();
        }
    }

    fn assign_segment(key: SegmentKey, replica_set: Vec<NodeId>) -> DataPlaneCommand {
        receive_peer_message(
            PlaceSegment {
                segment_key: key,
                shard_group_id: ShardGroupId(1),
                replica_set: Replicas::new(replica_set),
                start_entry_id: EntryId(0),
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

    fn seal_response(
        old: SegmentKey,
        new_segment_id: SegmentId,
        new_replica_set: Vec<NodeId>,
    ) -> DataPlaneCommand {
        receive_peer_message(
            SegmentRollCommitted {
                old_segment_key: old,
                new_segment_id,
                new_replica_set: Replicas::new(new_replica_set),
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
        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));
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

        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        let (cmd, rx) = produce(test_key());
        dp.handle_command(cmd);

        dp.handle_command(DataPlaneCommand::DataPlaneTimeoutCallback(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));

        let ack = rx.blocking_recv().unwrap();
        assert!(matches!(ack, ProduceAck::Ok { .. }));
    }

    #[test]
    fn produce_tracks_size_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        let (tx, _) = oneshot::channel();
        dp.handle_command(Produce {
            segment_key: test_key(),
            data: Bytes::from("hello world!").into(),
            record_count: 2,
            reply: tx,
        });

        let tracker = dp.segments.get(&test_key()).unwrap();
        assert_eq!(tracker.size_bytes(), 12);
        assert_eq!(tracker.next_entry_id(), EntryId(0));
    }

    #[test]
    fn segment_assignment_creates_tracker() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));
        assert!(dp.segments.contains_key(&test_key()));
    }

    #[test]
    fn batch_deadline_flushes_buffered_data() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        let (cmd, _) = produce(test_key());
        dp.handle_command(cmd);

        assert!(dp.has_buffered_data());

        process_and_flush(
            &mut dp,
            DataPlaneCommand::DataPlaneTimeoutCallback(
                DataPlaneTimeoutCallback::BatchFlushDeadline,
            ),
        );

        assert!(!dp.has_buffered_data());

        let cache = dp.segments.get(&test_key()).unwrap().cache();
        assert_eq!(cache.load_read_cursor(), 1);
    }

    #[test]
    fn produce_schedules_flush_timer() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

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

        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        let (cmd, _) = produce(test_key());
        dp.handle_command(cmd);

        dp.flush_batch();

        dp.handle_command(DataPlaneCommand::DataPlaneTimeoutCallback(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));
        assert!(!dp.has_buffered_data());
    }

    #[test]
    fn batch_flush_deadline_flushes_inline() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![dp.node_id.clone()]));

        let (cmd, _) = produce(test_key());
        dp.handle_command(cmd);

        dp.handle_command(DataPlaneCommand::DataPlaneTimeoutCallback(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));

        assert!(!dp.has_buffered_data());
    }

    #[test]
    fn volume_trigger_flushes_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![dp.node_id.clone()]));

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
        dp.handle_command(assign_segment(test_key(), vec![dp.node_id.clone()]));

        dp.handle_command(Produce {
            segment_key: test_key(),
            data: Bytes::from(vec![0u8; TEST_BATCH_MAX_BYTES]).into(),
            record_count: 1,
            reply: oneshot::channel().0,
        });
        assert!(dp.buffer_byte_count >= TEST_BATCH_MAX_BYTES);
        dp.flush_batch();

        dp.handle_command(DataPlaneCommand::DataPlaneTimeoutCallback(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));
        assert!(!dp.has_buffered_data());
    }

    #[test]
    fn cache_pressure_checkpoint_disabled_when_budget_zero() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![dp.node_id.clone()]));

        dp.handle_command(Produce {
            segment_key: test_key(),
            data: Bytes::from(vec![0u8; 16]).into(),
            record_count: 1,
            reply: oneshot::channel().0,
        });
        dp.flush_batch();

        assert_eq!(dp.hot_cache_bytes(), 16);
        assert!(dp.out.checkpoint_tasks.is_empty());
    }

    #[test]
    fn cache_pressure_submits_largest_checkpointable_segment() {
        use crate::data_plane::checkpoint::CheckpointTask;

        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.config.hot_cache_budget_bytes = 100;
        dp.config.hot_cache_pressure_watermark = 0.5;

        let small = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));
        let large = SegmentKey::new(TopicId(2), RangeId(0), SegmentId(0));
        let replica_set = vec![NodeId::new(dp.node_id.to_string())];

        dp.handle_command(assign_segment(small, replica_set.clone()));
        dp.handle_command(assign_segment(large, replica_set));

        dp.handle_command(Produce {
            segment_key: small,
            data: Bytes::from(vec![0u8; 10]).into(),
            record_count: 1,
            reply: oneshot::channel().0,
        });
        dp.handle_command(Produce {
            segment_key: large,
            data: Bytes::from(vec![0u8; 45]).into(),
            record_count: 1,
            reply: oneshot::channel().0,
        });
        dp.flush_batch();

        assert_eq!(dp.hot_cache_bytes(), 55);
        assert_eq!(dp.out.checkpoint_tasks.len(), 1);
        let CheckpointTask::Checkpoint(job) = &dp.out.checkpoint_tasks[0] else {
            panic!("expected checkpoint task");
        };
        assert_eq!(job.segment_key, large);
    }

    #[test]
    fn cache_pressure_counts_uncommitted_hot_bytes_but_checkpoints_only_committed() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.config.hot_cache_budget_bytes = 10;
        dp.config.hot_cache_pressure_watermark = 0.5;
        dp.handle_command(assign_segment(
            test_key(),
            vec![test_node_id(), NodeId::new("follower")],
        ));

        dp.handle_command(Produce {
            segment_key: test_key(),
            data: Bytes::from(vec![0u8; 8]).into(),
            record_count: 1,
            reply: oneshot::channel().0,
        });
        dp.flush_batch();

        assert_eq!(dp.hot_cache_bytes(), 8);
        assert_eq!(
            dp.segments.get(&test_key()).unwrap().checkpointable_bytes(),
            0
        );
        assert!(dp.out.checkpoint_tasks.is_empty());
    }

    #[test]
    fn cache_pressure_does_not_duplicate_in_flight_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.config.hot_cache_budget_bytes = 10;
        dp.config.hot_cache_pressure_watermark = 0.5;
        dp.handle_command(assign_segment(
            test_key(),
            vec![NodeId::new(dp.node_id.to_string())],
        ));

        dp.handle_command(Produce {
            segment_key: test_key(),
            data: Bytes::from(vec![0u8; 8]).into(),
            record_count: 1,
            reply: oneshot::channel().0,
        });
        dp.flush_batch();
        assert_eq!(dp.out.checkpoint_tasks.len(), 1);

        dp.submit_checkpoint_if_cache_pressure();
        assert_eq!(dp.out.checkpoint_tasks.len(), 1);

        dp.handle_command(DataPlaneCommand::SegmentCheckpointComplete(
            SegmentCheckpointComplete {
                segment_key: test_key(),
                checkpointed_lsn: 1,
                new_frontier: 1,
                checkpointed_bytes: 8,
            },
        ));
        assert!(dp.pressure_checkpoints_in_flight.is_empty());
    }

    /// A seal that arrives while the active segment still holds a staged-but-
    /// unflushed produce must carry that produce into the successor. The old
    /// tracker is retired (removed from the leader-staged sum), so dropping its
    /// staged entry would strand `buffer_byte_count` above the real staged bytes
    /// and trip `assert_invariants`. Regression for the `produce_then_fetch_hot`
    /// flake (worker panic at the `buffer_byte_count` invariant).
    #[test]
    fn seal_carries_unflushed_staged_into_successor() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        // Produce — staged into the active segment, no flush yet.
        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);
        assert!(dp.has_buffered_data());

        // Seal arrives before the batch flush. `handle_command` runs
        // `assert_invariants` afterwards — pre-fix this panics on the stranded
        // counter.
        dp.handle_command(seal_response(
            test_key(),
            SegmentId(1),
            vec![test_node_id()],
        ));

        // The staged produce moved to the successor; the counter still matches.
        let successor = test_key().with_segment_id(SegmentId(1));
        assert!(dp.segments.get(&successor).unwrap().has_staged());
        assert!(dp.has_buffered_data());
    }

    /// On a roll the coordinator co-dispatches a `PlaceSegment` for the new
    /// active segment alongside `SegmentRollCommitted`. If the assignment wins the race
    /// (creating the successor empty first), the seal handoff must still carry the
    /// old segment's staged tail onto that existing successor — not build a fresh
    /// tracker that `insert_active` refuses, dropping the records (data loss) and
    /// stranding `buffer_byte_count`. Regression for the age-seal stress flake.
    #[test]
    fn seal_handoff_carries_into_a_successor_already_created_by_assignment() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        // Produce — staged into the active segment, unflushed.
        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);
        assert!(dp.has_buffered_data());

        // The successor's PlaceSegment wins the race: seg1 is created empty.
        let successor = test_key().with_segment_id(SegmentId(1));
        dp.handle_command(assign_segment(successor, vec![test_node_id()]));

        // The committed roll handoff arrives second. Pre-fix `insert_active` refuses
        // (seg1 already active) → the staged produce is dropped and `buffer_byte_count`
        // strands, tripping `assert_invariants` after the command.
        dp.handle_command(seal_response(
            test_key(),
            SegmentId(1),
            vec![test_node_id()],
        ));

        // The staged produce was carried onto the pre-existing successor, not lost.
        assert!(dp.segments.get(&successor).unwrap().has_staged());
        assert!(dp.has_buffered_data());
    }

    #[test]
    fn checkpoint_complete_advances_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let key1 = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));
        let key2 = SegmentKey::new(TopicId(2), RangeId(0), SegmentId(0));

        dp.handle_command(assign_segment(key1, vec![test_node_id()]));
        dp.handle_command(assign_segment(key2, vec![test_node_id()]));

        let wal_file_count =
            || -> usize { std::fs::read_dir(dir.path().join("wal")).unwrap().count() };
        let initial_count = wal_file_count();

        dp.handle_command(DataPlaneCommand::SegmentCheckpointComplete(
            SegmentCheckpointComplete {
                segment_key: key1,
                checkpointed_lsn: 10,
                new_frontier: 0,
                checkpointed_bytes: 0,
            },
        ));
        assert_eq!(wal_file_count(), initial_count);

        dp.handle_command(DataPlaneCommand::SegmentCheckpointComplete(
            SegmentCheckpointComplete {
                segment_key: key2,
                checkpointed_lsn: 5,
                new_frontier: 0,
                checkpointed_bytes: 0,
            },
        ));
    }

    #[test]
    fn duplicate_segment_assignment_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));
        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        assert_eq!(dp.segments.len(), 1);
    }

    #[test]
    fn multiple_produces_accumulate_in_tracker() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

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
            assert_eq!(t.next_entry_id(), EntryId(0));
        }

        dp.flush_batch();

        let t = dp.segments.get(&test_key()).unwrap();
        assert!(t.staged_entries().is_empty());
        assert_eq!(t.cache_write_cursor(), 3);
        assert_eq!(t.next_entry_id(), EntryId(3));
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
            DataPlaneCommand::DataPlaneTimeoutCallback(
                DataPlaneTimeoutCallback::BatchFlushDeadline,
            ),
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

        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);

        process_and_flush(
            &mut dp,
            DataPlaneCommand::DataPlaneTimeoutCallback(
                DataPlaneTimeoutCallback::BatchFlushDeadline,
            ),
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
            DataPlaneCommand::DataPlaneTimeoutCallback(
                DataPlaneTimeoutCallback::BatchFlushDeadline,
            ),
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
            DataPlaneCommand::DataPlaneTimeoutCallback(
                DataPlaneTimeoutCallback::BatchFlushDeadline,
            ),
        );
        dp.out.transport_cmds.clear();

        dp.commit_segment(test_key(), EntryId(0));

        let cache = dp.segments.get(&test_key()).unwrap().cache();
        assert_eq!(cache.load_read_cursor(), 1);

        assert!(!dp.out.transport_cmds.is_empty());
    }

    #[test]
    fn follower_rejects_produce() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader-node");

        dp.handle_command(receive_peer_message(
            ReplicateSegmentEntries {
                segment_key: test_key(),
                replicas: Replicas::new(vec![leader, test_node_id()]),
                data: b"setup".to_vec().into(),
                record_count: 1,
                entry_id: EntryId(0),
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

        dp.handle_command(receive_peer_message(
            ReplicateSegmentEntries {
                segment_key: test_key(),
                replicas: Replicas::new(vec![leader.clone(), test_node_id()]),
                data: b"data".to_vec().into(),
                record_count: 1,
                entry_id: EntryId(0),
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
    fn segment_replication_rejects_a_non_leader_transport_peer() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader-node");

        dp.handle_command(receive_peer_message_from(
            NodeId::new("other-peer"),
            ReplicateSegmentEntries {
                segment_key: test_key(),
                replicas: Replicas::new(vec![leader, test_node_id()]),
                data: b"data".to_vec().into(),
                record_count: 1,
                entry_id: EntryId(0),
            },
        ));

        assert!(!dp.segments.contains_key(&test_key()));
        assert!(!dp.needs_flush);
    }

    #[test]
    fn replica_append_rejected_if_not_in_replica_set() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(receive_peer_message(
            ReplicateSegmentEntries {
                segment_key: test_key(),
                replicas: Replicas::new(vec![
                    NodeId::new("other-leader"),
                    NodeId::new("other-follower"),
                ]),
                data: b"data".to_vec().into(),
                record_count: 1,
                entry_id: EntryId(0),
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

        dp.handle_command(receive_peer_message(
            ReplicateSegmentEntries {
                segment_key: test_key(),
                replicas: Replicas::new(vec![leader.clone(), test_node_id()]),
                data: b"data".to_vec().into(),
                record_count: 1,
                entry_id: EntryId(0),
            }
            .into(),
        ));
        dp.flush_batch();

        dp.handle_command(receive_peer_message(
            AdvanceReplicaCommit {
                segment_key: test_key(),
                committed_entry_id: EntryId(0),
            }
            .into(),
        ));

        let tracker = dp.segments.get(&test_key()).unwrap();
        assert_eq!(tracker.committed_entry_id(), EntryId(0));
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
            DataPlaneCommand::DataPlaneTimeoutCallback(
                DataPlaneTimeoutCallback::BatchFlushDeadline,
            ),
        );

        let seq = dp.out.repl_schedules[0].0;
        dp.out.repl_schedules.clear();
        dp.out.transport_cmds.clear();

        dp.handle_command(DataPlaneCommand::DataPlaneTimeoutCallback(
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
            DataPlaneCommand::DataPlaneTimeoutCallback(
                DataPlaneTimeoutCallback::BatchFlushDeadline,
            ),
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
            DataPlaneCommand::DataPlaneTimeoutCallback(
                DataPlaneTimeoutCallback::BatchFlushDeadline,
            ),
        );

        let seq = dp.out.repl_schedules[0].0;
        dp.handle_replication_timeout(test_key(), seq);

        let failed = dp.pending_seal_requests.failed_nodes(&test_key()).unwrap();
        assert!(failed.contains(&follower));
    }

    #[test]
    fn needs_flush_set_by_replica_append() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader-node");

        assert!(!dp.needs_flush);

        dp.handle_command(receive_peer_message(
            ReplicateSegmentEntries {
                segment_key: test_key(),
                replicas: Replicas::new(vec![leader, test_node_id()]),
                data: b"data".to_vec().into(),
                record_count: 1,
                entry_id: EntryId(0),
            }
            .into(),
        ));

        assert!(dp.needs_flush);
    }

    #[test]
    fn segment_roll_committed_without_uncommitted_data_skips_wal_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower");
        let rs = vec![test_node_id(), follower.clone()];

        process_and_flush(&mut dp, assign_segment(test_key(), rs.clone()));

        let (cmd, _) = produce(test_key());
        dp.handle_command(cmd);
        dp.handle_command(DataPlaneCommand::DataPlaneTimeoutCallback(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));
        dp.segments
            .get_mut(&test_key())
            .unwrap()
            .commit_entry(EntryId(0));

        dp.handle_command(receive_peer_message(
            SegmentRollCommitted {
                old_segment_key: test_key(),
                new_segment_id: SegmentId(1),
                new_replica_set: Replicas::new(vec![test_node_id(), NodeId::new("new-follower")]),
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
        assert!(dp.pending_seal_requests.is_tracked(&test_key()));
    }

    #[test]
    fn enqueue_seal_request_deduplicates() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        dp.enqueue_seal_request(test_key());
        dp.enqueue_seal_request(test_key());

        // Count only seal requests — `handle_place_segment` also emits a
        // SegmentPlaced via SendToCoordinator, which is not a seal.
        assert_eq!(
            dp.out
                .transport_cmds
                .iter()
                .filter(|c| matches!(
                    c,
                    DataTransportCommand::SendToCoordinator(s)
                        if matches!(s.message, DataPlanePeerMessage::RequestSegmentRoll(_))
                ))
                .count(),
            1
        );
    }

    #[test]
    fn enqueue_seal_request_skips_follower() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader-node");

        dp.handle_command(receive_peer_message(
            ReplicateSegmentEntries {
                segment_key: test_key(),
                replicas: Replicas::new(vec![leader, test_node_id()]),
                data: b"data".to_vec().into(),
                record_count: 1,
                entry_id: EntryId(0),
            }
            .into(),
        ));

        dp.enqueue_seal_request(test_key());

        assert!(dp.out.transport_cmds.is_empty());
        assert!(!dp.pending_seal_requests.is_tracked(&test_key()));
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
    // PlaceSegment routes through `handle_place_segment`, seal
    // routes through `handle_seal_response`, etc. The unit-level tests for
    // the index itself live in `states/segment_store.rs`.

    use crate::data_plane::states::segment_store::SegmentReadState;

    #[test]
    fn segment_assignment_indexes_for_resolver() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        let resolved = dp
            .segments
            .resolve(TopicId(1), RangeId(0), EntryId(0))
            .expect("active segment should resolve");
        assert!(matches!(resolved, SegmentReadState::Active(k) if k == test_key()));
    }

    /// Verifies the leader-side seal path: after `SegmentRollCommitted`, the old
    /// segment migrates to the sealed table with `end_entry_id` set from
    /// the tracker's `committed_entry_id`, and the new segment becomes the
    /// active index entry for the range.
    #[test]
    fn segment_roll_committed_routes_old_offsets_through_sealed_table() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        // Single-replica produce auto-commits on flush, so entry 0 is
        // committed at the time we issue the seal. After seal: end=0 on the
        // sealed entry, new segment starts at offset 1.
        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));
        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);
        process_and_flush(
            &mut dp,
            DataPlaneCommand::DataPlaneTimeoutCallback(
                DataPlaneTimeoutCallback::BatchFlushDeadline,
            ),
        );

        dp.handle_command(receive_peer_message(
            SegmentRollCommitted {
                old_segment_key: test_key(),
                new_segment_id: SegmentId(1),
                new_replica_set: Replicas::new(vec![test_node_id()]),
            }
            .into(),
        ));

        let s0 = test_key();
        let s1 = s0.with_segment_id(SegmentId(1));

        let lookup = dp
            .segments
            .resolve(TopicId(1), RangeId(0), EntryId(0))
            .unwrap();
        match lookup {
            SegmentReadState::Sealed {
                key, end_entry_id, ..
            } => {
                assert_eq!(key, s0);
                assert_eq!(end_entry_id, EntryId(0));
            }
            other => panic!("expected Sealed, got {other:?}"),
        }

        let active = dp
            .segments
            .resolve(TopicId(1), RangeId(0), EntryId(1))
            .unwrap();
        assert!(matches!(active, SegmentReadState::Active(k) if k == s1));
    }

    /// Seal before the first commit: the uncommitted record is replayed into the
    /// successor at the same id, the old (empty) segment is dropped from the
    /// resolver, and a fetch from offset 0 resolves to the active successor.
    #[test]
    fn seal_before_first_commit_keeps_offset_zero_readable() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let follower = NodeId::new("follower-1");

        // Assign with a follower so the produce does NOT auto-commit: entry 0
        // is published (write cursor 1) but uncommitted (read cursor 0).
        dp.handle_command(assign_segment(test_key(), vec![test_node_id(), follower]));
        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);
        process_and_flush(
            &mut dp,
            DataPlaneCommand::DataPlaneTimeoutCallback(
                DataPlaneTimeoutCallback::BatchFlushDeadline,
            ),
        );
        assert_eq!(
            dp.segments
                .get(&test_key())
                .unwrap()
                .cache()
                .load_read_cursor(),
            0,
            "precondition: nothing committed before the seal",
        );

        // Replication-timeout failover seals the never-committed segment.
        dp.handle_command(receive_peer_message(
            SegmentRollCommitted {
                old_segment_key: test_key(),
                new_segment_id: SegmentId(1),
                new_replica_set: Replicas::new(vec![test_node_id()]),
            }
            .into(),
        ));

        let s1 = test_key().with_segment_id(SegmentId(1));

        // Offset 0 resolves to the active successor (which reuses start 0), not
        // an empty sealed segment — the old segment owned no committed offsets.
        let resolved = dp
            .segments
            .resolve(TopicId(1), RangeId(0), EntryId(0))
            .unwrap();
        assert!(
            matches!(resolved, SegmentReadState::Active(k) if k == s1),
            "offset 0 must resolve to the active successor, got {resolved:?}",
        );
        assert_eq!(dp.segments.get(&s1).unwrap().start_entry_id(), EntryId(0));

        // The replayed record commits on flush (single replica) and is readable
        // at offset 0 — proving no record was stranded by the seal.
        dp.flush_batch();
        assert_eq!(
            dp.segments.get(&s1).unwrap().cache().load_read_cursor(),
            1,
            "replayed record must commit at offset 0 on the successor",
        );
    }

    /// End-to-end cold read through `DataPlane`: a sealed segment's data lives on
    /// disk; a `Fetch` resolves to it and is served by a *real* `ColdReadPool`
    /// over a *real* sparse index — exercising the dispatch wiring, the on-disk
    /// `WalRecord` format agreement with `checkpoint.rs`, and `record_count`
    /// round-tripping through the file. Bypasses the coordinator seal round-trip
    /// (driven directly) so it's deterministic.
    #[test]
    fn cold_fetch_serves_sealed_segment_from_disk() {
        use crate::connections::protocol::RangeProgressSignal;
        use crate::data_plane::cold_read::ColdReadPool;
        use crate::data_plane::messages::query::{DataPlaneQuery, Fetch};
        use crate::data_plane::sparse_index::{SparseEntry, SparseIndex};
        use crate::data_plane::wal::WalRecord;
        use std::io::Write as _;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();

        // Real sparse index + cold-read pool, shared the way production wires them.
        let sparse: Arc<dyn SparseIndex> =
            Arc::new(rocksdb::DB::open_default(dir.path().join("sparse")).unwrap());
        let cold_read_tx = ColdReadPool::spawn(DEFAULT_POOL_SIZE, Arc::clone(&sparse));

        let wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        let (self_tx, _self_rx) = flume::unbounded::<DataPlaneMessage>();
        std::mem::forget(_self_rx);
        let mut dp = DataPlane::new(
            test_node_id(),
            test_config(dir.path().to_path_buf()),
            wal,
            cold_read_tx,
            DataPlaneSender(self_tx),
            DataPlaneOutputs::test(),
            RecoveryOutput {
                inventory: empty_inventory(),
                data_dir: dir.path().to_path_buf(),
                offsets: OffsetLedger::default(),
            },
        );

        // Assign + produce 3 records (single replica → commit inline on flush).
        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));
        let records: [(&[u8], u32); 3] = [(b"alpha", 2), (b"bravo", 5), (b"charlie", 1)];
        for (payload, record_count) in records {
            let (tx, _rx) = oneshot::channel();
            dp.handle_command(Produce {
                segment_key: test_key(),
                data: Bytes::copy_from_slice(payload).into(),
                record_count,
                reply: tx,
            });
        }
        dp.flush_batch();

        // Write the segment file + sparse index exactly as the checkpoint worker
        // would (Data + BatchEnd per entry), then seal so the resolver routes
        // offset 0 to the sealed table → cold path.
        let seg_path = test_key().file_path(dir.path(), EntryId(0));
        if let Some(parent) = seg_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut buf = Vec::new();
        let mut index_entries = Vec::new();
        for (i, (payload, record_count)) in records.iter().enumerate() {
            index_entries.push(SparseEntry::new(
                test_key(),
                (i as u64).into(),
                (buf.len() as u64).to_be_bytes(),
            ));
            WalRecord::data(Bytes::copy_from_slice(payload), *record_count)
                .encode_to(&mut buf)
                .unwrap();
            WalRecord::batch_end().encode_to(&mut buf).unwrap();
        }
        std::fs::File::create(&seg_path)
            .unwrap()
            .write_all(&buf)
            .unwrap();
        sparse.put_batch(index_entries).unwrap();

        dp.handle_command(receive_peer_message(
            SegmentSealed {
                segment_key: test_key(),
                committed_entry_id: None,
            }
            .into(),
        ));

        // Fetch offset 0 → must resolve Sealed and be served cold from disk.
        let (reply, reply_rx) = oneshot::channel();
        dp.process(DataPlaneMessage::Query(DataPlaneQuery::Fetch(Fetch {
            topic_id: TopicId(1),
            range_id: RangeId(0),
            entry_id: EntryId(0),
            max_bytes: 1 << 20,
            progress_signal: RangeProgressSignal::Active,
            reply,
        })));

        let result = reply_rx
            .blocking_recv()
            .expect("cold-read pool dropped the reply");
        let FetchResult::Records {
            entries,
            next_entry_id,
            ..
        } = result
        else {
            panic!("expected Records from cold read, got {result:?}");
        };
        let got: Vec<(u64, u32, Vec<u8>)> = entries
            .iter()
            .map(|e| (*e.entry_id, e.record_count, e.data.to_vec()))
            .collect();
        assert_eq!(
            got,
            vec![
                (0, 2, b"alpha".to_vec()),
                (1, 5, b"bravo".to_vec()),
                (2, 1, b"charlie".to_vec()),
            ],
            "cold read must restore payloads AND record_counts from disk"
        );
        assert_eq!(next_entry_id, EntryId(3));
    }

    // ── Catch-up source side (commit 20) ──────────────────────────────────

    /// Build a data plane that captures cold-read dispatches and already holds a
    /// sealed segment `test_key()` spanning `[0, 10]` — a stand-in for a healthy
    /// catch-up source resolving its own committed bounds.
    fn source_with_sealed_segment(
        dir: &tempfile::TempDir,
    ) -> (DataPlane<WalWriter>, flume::Receiver<ColdReadRequest>) {
        let wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        let (cold_read_tx, cold_read_rx) = flume::unbounded::<ColdReadRequest>();
        let (self_tx, _self_rx) = flume::unbounded::<DataPlaneMessage>();
        std::mem::forget(_self_rx);
        let mut dp = DataPlane::new(
            test_node_id(),
            test_config(dir.path().to_path_buf()),
            wal,
            cold_read_tx,
            DataPlaneSender(self_tx),
            DataPlaneOutputs::test(),
            RecoveryOutput {
                inventory: empty_inventory(),
                data_dir: dir.path().to_path_buf(),
                offsets: OffsetLedger::default(),
            },
        );
        let mut tracker = SegmentTracker::new_with_start_entry_id(
            dir.path().to_path_buf(),
            SegmentRole::Leader,
            Replicas::new(vec![test_node_id()]),
            ShardGroupId(1),
            EntryId(0),
        );
        tracker.commit_entry(EntryId(10));
        dp.segments.insert_active(test_key(), tracker);
        dp.segments.take_active_and_seal(test_key());
        (dp, cold_read_rx)
    }

    /// The source resolves the read window from its OWN committed bounds (not
    /// values relayed by the requester) plus the requester's `local_end`, then
    /// hands the read to the pool routed back here.
    #[test]
    fn catch_up_request_dispatches_read_from_its_own_sealed_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let (mut dp, cold_read_rx) = source_with_sealed_segment(&dir);

        let requester = NodeId::new("replacement");
        dp.handle_command(receive_peer_message(
            RequestCatchUpEntries {
                segment_key: test_key(),
                from: requester.clone(),
                local_end: Some(EntryId(4)),
            }
            .into(),
        ));

        let req = cold_read_rx.try_recv().expect("dispatched cold read");
        assert_eq!(req.start_entry_offset, EntryId(5)); // requester's local_end + 1
        assert_eq!(req.end_entry_id, EntryId(10)); // the source's own sealed end
        assert!(matches!(
            req.reply,
            ColdReadReply::CatchUp(cu) if cu.requester == requester && cu.sealed_end == EntryId(10)
        ));
    }

    /// A requester already at (or past) the source's committed end gets an
    /// immediate `CatchUpEntriesSent` and no read is dispatched — the source short-
    /// circuits on its OWN `sealed_end`, not on anything the requester relayed.
    #[test]
    fn catch_up_request_already_caught_up_sends_done_without_reading() {
        let dir = tempfile::tempdir().unwrap();
        let (mut dp, cold_read_rx) = source_with_sealed_segment(&dir);

        let requester = NodeId::new("replacement");
        dp.handle_command(receive_peer_message(
            RequestCatchUpEntries {
                segment_key: test_key(),
                from: requester.clone(),
                local_end: Some(EntryId(10)), // == the source's sealed end
            }
            .into(),
        ));

        // Nothing handed to the pool...
        assert!(cold_read_rx.try_recv().is_err());
        // ...and a CatchUpEntriesSent went straight back to the requester.
        assert_eq!(dp.out.transport_cmds.len(), 1);
        let DataTransportCommand::SendToTargets(s) = &dp.out.transport_cmds[0] else {
            panic!("expected SendToTargets");
        };
        assert_eq!(s.targets[0], requester);
        assert!(matches!(
            &s.message,
            DataPlanePeerMessage::CatchUpEntriesSent(d) if d.segment_key == test_key()
        ));
    }

    /// A batch whose `next_offset` is past the sealed end emits one `CatchUpEntries`
    /// (entries intact, with `record_count`) followed by `CatchUpEntriesSent` — no re-arm.
    #[test]
    fn catch_up_read_complete_emits_chunk_then_done() {
        use crate::data_plane::states::segment::cache::CachedEntry;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let requester = NodeId::new("replacement");

        let entries = vec![
            Arc::new(CachedEntry {
                data: Bytes::copy_from_slice(b"a").into(),
                record_count: 1,
                entry_id: EntryId(0),
                lsn: 0,
            }),
            Arc::new(CachedEntry {
                data: Bytes::copy_from_slice(b"bb").into(),
                record_count: 2,
                entry_id: EntryId(1),
                lsn: 0,
            }),
            Arc::new(CachedEntry {
                data: Bytes::copy_from_slice(b"ccc").into(),
                record_count: 3,
                entry_id: EntryId(2),
                lsn: 0,
            }),
        ];
        dp.handle_command(DataPlaneCommand::CatchUpReadComplete(CatchUpReadComplete {
            requester: requester.clone(),
            segment_key: test_key(),
            start_offset: EntryId(0),
            sealed_end: EntryId(2),
            entries,
            next_offset: EntryId(3),
        }));

        assert_eq!(dp.out.transport_cmds.len(), 2);

        let DataTransportCommand::SendToTargets(chunk) = &dp.out.transport_cmds[0] else {
            panic!("expected chunk SendToTargets");
        };
        assert_eq!(chunk.targets[0], requester);
        let DataPlanePeerMessage::CatchUpEntries(c) = &chunk.message else {
            panic!("expected CatchUpEntries");
        };
        assert_eq!(c.segment_key, test_key());
        let got: Vec<(u64, u32, Vec<u8>)> = c
            .entries
            .iter()
            .map(|e| (*e.entry_id, e.record_count, e.data.to_vec()))
            .collect();
        assert_eq!(
            got,
            vec![
                (0, 1, b"a".to_vec()),
                (1, 2, b"bb".to_vec()),
                (2, 3, b"ccc".to_vec()),
            ]
        );

        let DataTransportCommand::SendToTargets(done) = &dp.out.transport_cmds[1] else {
            panic!("expected done SendToTargets");
        };
        assert!(matches!(
            &done.message,
            DataPlanePeerMessage::CatchUpEntriesSent(_)
        ));
    }

    /// A batch that ends below the sealed end emits its chunk and re-arms the next
    /// read against the pool, resuming at `next_offset` — no `CatchUpEntriesSent` yet.
    #[test]
    fn catch_up_read_complete_re_arms_until_sealed_end() {
        use crate::data_plane::states::segment::cache::CachedEntry;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let wal = WalWriter::new(dir.path().to_path_buf()).unwrap();
        let (cold_read_tx, cold_read_rx) = flume::unbounded::<ColdReadRequest>();
        let (self_tx, _self_rx) = flume::unbounded::<DataPlaneMessage>();
        std::mem::forget(_self_rx);
        let mut dp = DataPlane::new(
            test_node_id(),
            test_config(dir.path().to_path_buf()),
            wal,
            cold_read_tx,
            DataPlaneSender(self_tx),
            DataPlaneOutputs::test(),
            RecoveryOutput {
                inventory: empty_inventory(),
                data_dir: dir.path().to_path_buf(),
                offsets: OffsetLedger::default(),
            },
        );
        let requester = NodeId::new("replacement");

        dp.handle_command(DataPlaneCommand::CatchUpReadComplete(CatchUpReadComplete {
            requester: requester.clone(),
            segment_key: test_key(),
            start_offset: EntryId(0),
            sealed_end: EntryId(9),
            entries: vec![Arc::new(CachedEntry {
                data: Bytes::copy_from_slice(b"x").into(),
                record_count: 1,
                entry_id: EntryId(4),
                lsn: 0,
            })],
            next_offset: EntryId(5),
        }));

        // One chunk, no Done yet.
        assert_eq!(dp.out.transport_cmds.len(), 1);
        let DataTransportCommand::SendToTargets(chunk) = &dp.out.transport_cmds[0] else {
            panic!("expected chunk SendToTargets");
        };
        assert!(matches!(
            &chunk.message,
            DataPlanePeerMessage::CatchUpEntries(_)
        ));

        // The next read was re-armed against the pool, resuming at next_offset.
        let req = cold_read_rx.try_recv().expect("re-armed cold read");
        assert_eq!(req.start_entry_offset, EntryId(5));
        assert_eq!(req.end_entry_id, EntryId(9));
        assert!(matches!(
            req.reply,
            ColdReadReply::CatchUp(cu) if cu.requester == requester && cu.sealed_end == EntryId(9)
        ));
    }

    // ── Replacement side: receive → verify → register (commit 21) ──────────

    /// End-to-end loopback: an assignment triggers a request, streamed chunks are
    /// written, and `Done` verifies the file to `sealed_end`, indexes the anchors,
    /// and registers the segment cold-readable.
    #[test]
    fn catch_up_receive_writes_verifies_and_registers() {
        use crate::data_plane::checkpoint::CheckpointTask;

        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let source = NodeId::new("source");

        // Coordinator assigns this node the sealed segment [0, 2].
        dp.handle_command(receive_peer_message(
            AssignSegmentCatchUp {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: EntryId(0),
                sealed_end_entry_id: EntryId(2),
                replica_set: Replicas::new(vec![test_node_id(), source.clone()]),
            }
            .into(),
        ));

        // → a request-all (`local_end: None`) to the source.
        assert_eq!(dp.out.transport_cmds.len(), 1);
        let DataTransportCommand::SendToTargets(s) = &dp.out.transport_cmds[0] else {
            panic!("expected SendToTargets");
        };
        assert_eq!(s.targets[0], source);
        assert!(matches!(
            &s.message,
            DataPlanePeerMessage::RequestCatchUpEntries(r)
                if r.segment_key == test_key() && r.local_end.is_none()
        ));

        // Source streams the three entries, then signals end of stream.
        dp.handle_command(receive_peer_message(
            CatchUpEntries {
                segment_key: test_key(),
                entries: vec![
                    CatchUpEntry {
                        entry_id: EntryId(0),
                        data: b"a".to_vec().into(),
                        record_count: 1,
                    },
                    CatchUpEntry {
                        entry_id: EntryId(1),
                        data: b"bb".to_vec().into(),
                        record_count: 2,
                    },
                    CatchUpEntry {
                        entry_id: EntryId(2),
                        data: b"ccc".to_vec().into(),
                        record_count: 3,
                    },
                ]
                .into(),
            }
            .into(),
        ));
        dp.handle_command(receive_peer_message(
            CatchUpEntriesSent {
                segment_key: test_key(),
            }
            .into(),
        ));

        // Registered cold-readable at its sealed bounds...
        assert_eq!(
            dp.segments.sealed_bounds(&test_key()),
            Some((EntryId(0), EntryId(2)))
        );
        // ...the file verifies through the sealed end...
        let scan = scan_segment_file(&test_key().file_path(dir.path(), EntryId(0))).unwrap();
        assert_eq!(scan.last_entry_id, Some(2.into()));
        // ...and the suffix anchors were handed to the checkpoint worker (entry 0
        // at byte 0 is always an anchor).
        assert!(
            dp.out.checkpoint_tasks.iter().any(|t| {
                matches!(t, CheckpointTask::PutAnchors(anchors) if !anchors.is_empty())
            })
        );
        // ...and a SegmentCaughtUp confirmed completion back to the coordinator.
        assert!(dp.out.transport_cmds.iter().any(|c| matches!(
            c,
            DataTransportCommand::SendToCoordinator(coord)
                if matches!(&coord.message, DataPlanePeerMessage::SegmentCaughtUp(a) if a.segment_key == test_key())
        )));
    }

    /// A stream that stops short of `sealed_end` fails verification, so the
    /// segment is NOT registered — the coordinator re-drives with another source.
    #[test]
    fn catch_up_receive_short_of_sealed_end_does_not_register() {
        use crate::data_plane::checkpoint::CheckpointTask;

        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(receive_peer_message(
            AssignSegmentCatchUp {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: EntryId(0),
                sealed_end_entry_id: EntryId(5), // need through 5...
                replica_set: Replicas::new(vec![test_node_id(), NodeId::new("source")]),
            }
            .into(),
        ));
        // ...but only 0..=2 arrive.
        dp.handle_command(receive_peer_message(
            CatchUpEntries {
                segment_key: test_key(),
                entries: vec![
                    CatchUpEntry {
                        entry_id: EntryId(0),
                        data: b"a".to_vec().into(),
                        record_count: 1,
                    },
                    CatchUpEntry {
                        entry_id: EntryId(1),
                        data: b"b".to_vec().into(),
                        record_count: 1,
                    },
                    CatchUpEntry {
                        entry_id: EntryId(2),
                        data: b"c".to_vec().into(),
                        record_count: 1,
                    },
                ]
                .into(),
            }
            .into(),
        ));
        dp.handle_command(receive_peer_message(
            CatchUpEntriesSent {
                segment_key: test_key(),
            }
            .into(),
        ));

        // Verification failed (have 2, need 5) → not registered, no anchors handed off.
        assert_eq!(dp.segments.sealed_bounds(&test_key()), None);
        assert!(
            !dp.out
                .checkpoint_tasks
                .iter()
                .any(|t| matches!(t, CheckpointTask::PutAnchors(_)))
        );
        // ...and NO SegmentCaughtUp — an unverified receive must not confirm completion.
        assert!(!dp.out.transport_cmds.iter().any(|c| matches!(
            c,
            DataTransportCommand::SendToCoordinator(s)
                if matches!(&s.message, DataPlanePeerMessage::SegmentCaughtUp(_))
        )));
    }

    /// Full match (the catch-up "lottery"): the recovered inventory already holds
    /// the segment through the sealed end. The announcement registers it
    /// cold-readable and transfers nothing.
    #[test]
    fn catch_up_full_match_registers_without_transfer() {
        let dir = tempfile::tempdir().unwrap();
        // Recovered from disk through entry 5 — past the sealed end of 2.
        let mut dp = make_data_plane_with(&dir, inventory_with(test_key(), 5));

        dp.handle_command(receive_peer_message(
            AssignSegmentCatchUp {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: EntryId(0),
                sealed_end_entry_id: EntryId(2),
                replica_set: Replicas::new(vec![test_node_id(), NodeId::new("peer")]),
            }
            .into(),
        ));

        // Zero transfer — no RequestCatchUpEntries dispatched, only a SegmentCaughtUp back to
        // the coordinator so its re-drive stops re-announcing the assignment.
        assert_eq!(dp.out.transport_cmds.len(), 1);
        let DataTransportCommand::SendToCoordinator(s) = &dp.out.transport_cmds[0] else {
            panic!("expected SendToCoordinator");
        };
        assert!(matches!(
            &s.message,
            DataPlanePeerMessage::SegmentCaughtUp(a) if a.segment_key == test_key()
        ));
        // ...and the segment is now registered cold-readable at the sealed bounds.
        assert_eq!(
            dp.segments.sealed_bounds(&test_key()),
            Some((EntryId(0), EntryId(2)))
        );
    }

    #[test]
    fn orphan_gc_deletes_strays_and_keeps_reused() {
        use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
        let dir = tempfile::tempdir().unwrap();

        let stray = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));
        let reused = SegmentKey::new(TopicId(2), RangeId(0), SegmentId(0));

        // Recovery found both on disk (start_offset 0, verified through 0).
        let mut recovered = RecoveredSegments::default();
        recovered.advance(stray, EntryId(0));
        recovered.advance(reused, EntryId(0));
        let mut dp = make_data_plane_with(&dir, LocalInventory::from_recovered(&recovered));

        let stray_path = stray.file_path(dir.path(), EntryId(0));
        let reused_path = reused.file_path(dir.path(), EntryId(0));
        write_seg_file(&stray_path, &[(b"x", 1)]);
        write_seg_file(&reused_path, &[(b"y", 1)]);

        // The owning group reassigned `reused` here and its catch-up registered it (the
        // lottery won) — now a live sealed segment. `stray` was never assigned here.
        dp.segments
            .insert_sealed_from_catch_up(reused, EntryId(0), EntryId(0));

        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::channel(1);
        dp.handle_orphan_gc_check(OrphanGcCheck { reply: reply_tx });

        // Stray: not a data replica → file reclaimed, dropped from the inventory.
        assert!(!stray_path.exists(), "stray segment file must be deleted");
        assert_eq!(
            dp.recovered.get(&stray),
            None,
            "stray pruned from inventory"
        );

        // Reused: registered → file kept, and dropped from the inventory (no longer an orphan).
        assert!(reused_path.exists(), "reused segment file must be kept");
        assert!(
            dp.segments.sealed_bounds(&reused).is_some(),
            "reused stays registered"
        );
        assert_eq!(
            dp.recovered.get(&reused),
            None,
            "reused pruned from inventory"
        );

        // recovered is drained → the handler tells the ticker to stop (self-terminating).
        assert!(
            matches!(reply_rx.try_recv(), Ok(OrphanGcSignal::Stop)),
            "ticker told to stop once every orphan is resolved"
        );
    }

    #[test]
    fn orphan_gc_skips_a_segment_with_an_in_flight_catch_up() {
        let dir = tempfile::tempdir().unwrap();
        // A partial local copy + matching cursor → a real in-flight (delta) catch-up.
        let path = test_key().file_path(dir.path(), EntryId(0));
        write_seg_file(&path, &[(b"a", 1), (b"b", 1)]);
        let mut dp = make_data_plane_with(&dir, inventory_with(test_key(), 1));

        dp.handle_command(receive_peer_message(
            AssignSegmentCatchUp {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: EntryId(0),
                sealed_end_entry_id: EntryId(4),
                replica_set: Replicas::new(vec![test_node_id(), NodeId::new("source")]),
            }
            .into(),
        ));
        assert!(
            dp.pending_catch_ups.contains_key(&test_key()),
            "catch-up must be in flight"
        );

        let (reply_tx, mut reply_rx) = tokio::sync::mpsc::channel(1);
        dp.handle_orphan_gc_check(OrphanGcCheck { reply: reply_tx });

        // The sweep must not delete out from under an in-flight catch-up — that catch-up
        // is the group reassigning the segment here; let it register.
        assert!(
            path.exists(),
            "file must survive while its catch-up is in flight"
        );
        assert_eq!(
            dp.recovered.get(&test_key()),
            Some(EntryId(1)),
            "kept in inventory"
        );

        // an orphan still pending → the handler tells the ticker to keep going.
        assert!(
            matches!(reply_rx.try_recv(), Ok(OrphanGcSignal::KeepTicking)),
            "ticker told to keep going while an orphan is pending"
        );
    }

    /// Retention (D7): a `DeleteSegment` for a sealed segment this node holds reclaims
    /// the file, drops it from the store, and queues a sparse-index delete.
    #[test]
    fn retention_delete_reclaims_a_sealed_segment() {
        use crate::data_plane::checkpoint::CheckpointTask;
        let dir = tempfile::tempdir().unwrap();
        let key = test_key();
        let path = key.file_path(dir.path(), EntryId(0));
        write_seg_file(&path, &[(b"a", 1)]);
        let mut dp = make_data_plane(&dir);
        dp.segments
            .insert_sealed_from_catch_up(key, EntryId(0), EntryId(0));
        assert!(dp.segments.sealed_bounds(&key).is_some());

        dp.handle_command(receive_peer_message(
            DeleteSegments {
                segment_keys: Box::new([key]),
            }
            .into(),
        ));

        assert!(!path.exists(), "segment file must be reclaimed");
        assert!(
            dp.segments.sealed_bounds(&key).is_none(),
            "segment dropped from the store"
        );
        assert!(
            dp.out
                .checkpoint_tasks
                .iter()
                .any(|t| matches!(t, CheckpointTask::DeleteSegmentIndex(k) if *k == key)),
            "sparse-index delete queued for the worker"
        );
    }

    /// Idempotent: a `DeleteSegment` for a segment this node doesn't hold (already
    /// gone, or never had it) is a no-op — no panic, no index-delete task.
    #[test]
    fn retention_delete_of_unheld_segment_is_a_noop() {
        use crate::data_plane::checkpoint::CheckpointTask;
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(receive_peer_message(
            DeleteSegments {
                segment_keys: Box::new([test_key()]),
            }
            .into(),
        ));

        assert!(
            !dp.out
                .checkpoint_tasks
                .iter()
                .any(|t| matches!(t, CheckpointTask::DeleteSegmentIndex(_))),
            "no index delete for a segment not held here"
        );
    }

    /// Partial match: a local prefix on disk (entries 0..=1) means the receiver
    /// asks only for the delta after its local end, not a full copy.
    #[test]
    fn catch_up_partial_requests_only_the_delta() {
        let dir = tempfile::tempdir().unwrap();
        // A partial local copy on disk plus the matching inventory cursor.
        write_seg_file(
            &test_key().file_path(dir.path(), EntryId(0)),
            &[(b"a", 1), (b"b", 1)],
        );
        let mut dp = make_data_plane_with(&dir, inventory_with(test_key(), 1));

        dp.handle_command(receive_peer_message(
            AssignSegmentCatchUp {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: EntryId(0),
                sealed_end_entry_id: EntryId(4),
                replica_set: Replicas::new(vec![test_node_id(), NodeId::new("source")]),
            }
            .into(),
        ));

        assert_eq!(dp.out.transport_cmds.len(), 1);
        let DataTransportCommand::SendToTargets(s) = &dp.out.transport_cmds[0] else {
            panic!("expected SendToTargets");
        };

        assert!(matches!(
            &s.message,
            DataPlanePeerMessage::RequestCatchUpEntries(r) if r.local_end == Some(EntryId(1))
        ));
    }

    /// Source selection is the receiver's call: it fetches from a non-leader peer
    /// (not `replica_set[0]`) so repair reads stay off the former write leader.
    #[test]
    fn catch_up_picks_a_non_leader_peer_as_source() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir); // empty inventory → needs a full copy
        let leader = NodeId::new("leader");
        let follower = NodeId::new("follower");

        dp.handle_command(receive_peer_message(
            AssignSegmentCatchUp {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: EntryId(0),
                sealed_end_entry_id: EntryId(2),
                // self (test-node) is a follower here; `leader` is replica_set[0].
                replica_set: Replicas::new(vec![leader.clone(), test_node_id(), follower.clone()]),
            }
            .into(),
        ));

        let DataTransportCommand::SendToTargets(s) = &dp.out.transport_cmds[0] else {
            panic!("expected SendToTargets");
        };
        assert_eq!(
            s.targets[0], follower,
            "should fetch from the non-leader peer, not {leader:?}"
        );
    }

    // ── Re-drive rescues a stalled in-flight receive ───────────────────────

    fn catch_up_assignment_to(source: &NodeId, sealed_end: u64) -> DataPlaneCommand {
        receive_peer_message(
            AssignSegmentCatchUp {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: EntryId(0),
                sealed_end_entry_id: EntryId(sealed_end),
                replica_set: Replicas::new(vec![test_node_id(), source.clone()]),
            }
            .into(),
        )
    }

    fn catch_up_chunk_of(ids: &[u64]) -> DataPlaneCommand {
        receive_peer_message(
            CatchUpEntries {
                segment_key: test_key(),
                entries: ids
                    .iter()
                    .map(|&entry_id| CatchUpEntry {
                        entry_id: EntryId(entry_id),
                        data: b"x".to_vec().into(),
                        record_count: 1,
                    })
                    .collect(),
            }
            .into(),
        )
    }

    /// A receive that makes no progress is re-requested (resumed) once it has gone
    /// `CATCH_UP_IDLE_REDRIVES` re-drives without an append — not before.
    #[test]
    fn catch_up_redrive_resumes_a_stalled_receive() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let source = NodeId::new("source");

        dp.handle_command(catch_up_assignment_to(&source, 5)); // initial request
        dp.out.transport_cmds.clear();

        // Within the grace window the re-drive is silent.
        for _ in 0..CATCH_UP_IDLE_REDRIVES - 1 {
            dp.handle_command(catch_up_assignment_to(&source, 5));
            assert!(
                dp.out.transport_cmds.is_empty(),
                "no re-request within grace"
            );
        }
        // At the budget: a resume request to the source, from local_end = None
        // (nothing written yet).
        dp.handle_command(catch_up_assignment_to(&source, 5));
        assert_eq!(dp.out.transport_cmds.len(), 1, "stalled → resume request");
        let DataTransportCommand::SendToTargets(s) = &dp.out.transport_cmds[0] else {
            panic!("expected SendToTargets");
        };
        assert_eq!(s.targets[0], source);
        assert!(matches!(
            &s.message,
            DataPlanePeerMessage::RequestCatchUpEntries(r)
                if r.segment_key == test_key() && r.local_end.is_none()
        ));
    }

    /// Progress (an appended chunk) resets the stall counter, so a steadily
    /// advancing receive is never re-requested.
    #[test]
    fn catch_up_redrive_is_a_noop_while_progressing() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let source = NodeId::new("source");

        dp.handle_command(catch_up_assignment_to(&source, 5));
        // Bring the counter to the brink...
        for _ in 0..CATCH_UP_IDLE_REDRIVES - 1 {
            dp.handle_command(catch_up_assignment_to(&source, 5));
        }
        // ...a chunk resets it...
        dp.handle_command(catch_up_chunk_of(&[0]));
        dp.out.transport_cmds.clear();
        // ...so the same run of re-drives again still doesn't trip a resume.
        for _ in 0..CATCH_UP_IDLE_REDRIVES - 1 {
            dp.handle_command(catch_up_assignment_to(&source, 5));
        }
        assert!(
            dp.out.transport_cmds.is_empty(),
            "progress reset the stall counter; no resume"
        );
    }

    /// A resume can re-stream entries already written; the chunk handler appends
    /// only the strictly-next id, so duplicates never inflate the verified prefix.
    #[test]
    fn catch_up_chunk_skips_a_resume_overlap() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let source = NodeId::new("source");

        dp.handle_command(catch_up_assignment_to(&source, 4));
        dp.handle_command(catch_up_chunk_of(&[0, 1, 2]));
        // Overlap: 1,2 are duplicates (skipped), 3,4 are new (appended).
        dp.handle_command(catch_up_chunk_of(&[1, 2, 3, 4]));
        dp.handle_command(receive_peer_message(
            CatchUpEntriesSent {
                segment_key: test_key(),
            }
            .into(),
        ));

        // Verified exactly through the sealed end — no duplicate inflation.
        assert_eq!(
            dp.segments.sealed_bounds(&test_key()),
            Some((EntryId(0), EntryId(4)))
        );
        let scan = scan_segment_file(&test_key().file_path(dir.path(), EntryId(0))).unwrap();
        assert_eq!(scan.last_entry_id, Some(EntryId(4)));
    }

    // --- Leader-crash boundary recovery: durable extent + query handler -----

    /// A follower publishes (fsyncs) entries on flush but isn't told of the
    /// commit until a `AdvanceReplicaCommit` — so `durable_end` (the fsync'd extent)
    /// runs ahead of `committed_entry_id`. Boundary recovery reads the former.
    #[test]
    fn durable_end_reports_published_extent_not_commit_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader-node");

        for entry_id in 0..3 {
            dp.handle_command(receive_peer_message(
                ReplicateSegmentEntries {
                    segment_key: test_key(),
                    replicas: Replicas::new(vec![leader.clone(), test_node_id()]),
                    data: b"x".to_vec().into(),
                    record_count: 1,
                    entry_id: EntryId(entry_id),
                }
                .into(),
            ));
        }
        dp.flush_batch(); // publishes 0,1,2 past the WAL fsync; no AdvanceReplicaCommit yet

        assert_eq!(
            dp.segments.get(&test_key()).unwrap().committed_entry_id(),
            EntryId(0),
            "follower hasn't been told of any commit"
        );
        assert_eq!(
            dp.durable_end(&test_key()),
            Some(EntryId(2)),
            "durable extent is the highest fsync'd entry, ahead of the commit cursor"
        );
    }

    /// With no live tracker, `durable_end` falls back to the sealed store, then
    /// the recovered inventory, then `None`.
    #[test]
    fn durable_end_falls_back_to_sealed_then_recovered() {
        // Sealed store: a sealed segment with bounds (0, 10).
        let sealed_dir = tempfile::tempdir().unwrap();
        let (sealed_dp, _rx) = source_with_sealed_segment(&sealed_dir);
        assert_eq!(sealed_dp.durable_end(&test_key()), Some(EntryId(10)));

        // Recovered inventory only (restarted node, not yet in the live store).
        let recovered_dir = tempfile::tempdir().unwrap();
        let recovered_dp = make_data_plane_with(&recovered_dir, inventory_with(test_key(), 7));
        assert_eq!(recovered_dp.durable_end(&test_key()), Some(EntryId(7)));

        // Hold nothing → None.
        let empty_dir = tempfile::tempdir().unwrap();
        let empty_dp = make_data_plane(&empty_dir);
        assert_eq!(empty_dp.durable_end(&test_key()), None);
    }

    /// A `RequestDurableSegmentEnd` is answered with our durable extent, addressed back to
    /// the coordinator that owns this gather.
    #[test]
    fn seal_boundary_query_replies_with_durable_end() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane_with(&dir, inventory_with(test_key(), 4));

        dp.handle_command(receive_peer_message(
            RequestDurableSegmentEnd {
                segment_key: test_key(),
                coordinator: NodeId::new("coordinator"),
            }
            .into(),
        ));

        assert_eq!(dp.out.transport_cmds.len(), 1);
        let DataTransportCommand::SendToTargets(s) = &dp.out.transport_cmds[0] else {
            panic!("boundary report must route to the coordinator");
        };
        assert_eq!(s.targets.as_ref(), &[NodeId::new("coordinator")]);
        let DataPlanePeerMessage::DurableSegmentEndReported(report) = &s.message else {
            panic!("expected a DurableSegmentEndReported");
        };
        assert_eq!(report.segment_key, test_key());
        assert_eq!(report.from, test_node_id());
        assert_eq!(report.durable_end, Some(EntryId(4)));
    }
}
