use super::SegmentKey;
use super::actor::DataPlaneSender;
use super::cold_read::{CatchUpReadReply, ColdReadReply, ColdReadRequest};
use super::messages::DataPlaneMessage;
use super::messages::command::DataPlaneInterNodeCommand;
use super::messages::command::*;
use super::messages::pending::DataPlaneOutputs;
use super::messages::query::{DataPlaneQuery, Fetch, FetchResult, ListOffsets, ListOffsetsResult};
use super::recovery::inventory::LocalInventory;
use super::recovery::orphan::OrphanCandidate;
use super::recovery::segment_scan::scan_segment_file;
use super::segment_writer::SegmentAppender;
use super::sparse_index::SparseEntry;
use super::states::replication::PendingReplicationBatch;
use super::states::replication::ReplicationState;
use super::states::seal_request::PendingSealRequests;
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
use crate::data_plane::states::segment_store::SegmentReadState;
use crate::data_plane::timer::{BatchFlushTimer, ReplicationTimer};
use crate::data_plane::transport::command::DataTransportSendToCoordinator;
use crate::schedulers::ticker_message::TimerCommand;
#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;
use std::collections::HashMap;

/// Same rational as the Raft transport's 4MiB cap, Per-`CatchUpChunk` read cap.
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
struct PendingCatchUp {
    appender: SegmentAppender,
    shard_group_id: ShardGroupId,
    start_offset: u64,
    sealed_end: u64,
    /// Highest entry id written so far; `None` until the first append. A resume
    /// re-requests `(last_written, sealed_end]`.
    last_written: Option<u64>,
    /// Re-drives since the last append — the stall detector. Reset on append.
    idle_rounds: u32,
    anchors: Vec<SparseEntry>,
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
    pending_seal_requests: PendingSealRequests,
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
}

impl<W: WalStorage> DataPlane<W> {
    pub(crate) fn new(
        node_id: NodeId,
        config: DataNodeConfig,
        wal: W,
        cold_read_handoff_sender: flume::Sender<ColdReadRequest>,
        self_tx: DataPlaneSender,
        out: DataPlaneOutputs,
        recovered: LocalInventory,
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
            pending_seal_requests: PendingSealRequests::default(),
            pending_catch_ups: HashMap::new(),
            cold_read_handoff_sender,
            self_tx,
            out,
            recovered,
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
            DataPlaneCommand::CheckpointComplete(complete) => {
                self.handle_checkpoint_complete(complete);
            }
            DataPlaneCommand::DataPlaneTimeoutCallback(callback) => {
                self.handle_timeout(callback);
            }
            DataPlaneCommand::DataPlaneInterNodeCommand(inter) => {
                self.process_inter_node(inter);
            }
            DataPlaneCommand::CatchUpReadComplete(cmd) => {
                self.handle_catch_up_read_complete(cmd);
            }
            DataPlaneCommand::OrphanGcCheck(check) => self.handle_orphan_gc_check(check),
        }

        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    fn handle_query(&self, q: impl Into<DataPlaneQuery>) {
        match q.into() {
            DataPlaneQuery::Fetch(f) => self.handle_fetch(f),
            DataPlaneQuery::ListOffsets(lo) => self.handle_list_offsets(lo),
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
        tracker.stage_entry(cmd.segment_key, cmd.data, cmd.record_count);
        self.dirty_segments.push(cmd.segment_key);

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

        // The active tail can have `committed_entry_id = 0` and no entries
        // yet committed; distinguish "no data committed at all" by also
        // checking against the cache read cursor.
        if cmd.entry_id >= read_cursor {
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

        while let Some(entry_arc) = cache.read_committed(next_entry_id) {
            let payload_len = entry_arc.data.len();
            // Always include at least one entry (even if max_bytes is small).
            if !entries.is_empty() && bytes_read + payload_len > max_bytes {
                break;
            }
            next_entry_id = entry_arc.entry_id + 1;
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
        start_entry_id: u64,
        end_entry_id: u64,
    ) {
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
            self.segments.resolve(cmd.topic_id, cmd.range_id, u64::MAX)
        else {
            let _ = cmd.reply.send(ListOffsetsResult::SegmentNotLocal);
            return;
        };

        let Some(tracker) = self.segments.get(&key) else {
            let _ = cmd.reply.send(ListOffsetsResult::SegmentNotLocal);
            return;
        };
        let _ = cmd.reply.send(ListOffsetsResult::Offsets {
            start_entry_id: tracker.start_entry_id(),
            committed_entry_id: tracker.committed_entry_id(),
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
        for (key, start_offset) in self.recovered.orphan_candidates().collect::<Vec<_>>() {
            if self.segments.sealed_bounds(&key).is_some() {
                self.recovered.remove(&key); // reused (lottery won) → live, no longer an orphan
            } else if self.pending_catch_ups.contains_key(&key) {
                continue; // catch-up in flight → it will register; don't delete under it
            } else if deletes < ORPHAN_GC_BATCH
                && OrphanCandidate::new(key, start_offset)
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

    fn process_inter_node(&mut self, cmd: DataPlaneInterNodeCommand) {
        use DataPlaneInterNodeCommand as C;
        match cmd {
            C::SegmentAssignment(cmd) => self.handle_segment_assignment(cmd),
            C::ReplicaAppend(cmd) => self.process_replica_append(cmd),
            C::ReplicaAck(cmd) => self.handle_replica_ack(cmd),
            C::CommitAdvance(cmd) => self.handle_commit_advance(cmd),
            C::SealResponse(cmd) => self.handle_seal_response(cmd),
            C::SegmentSealed(cmd) => self.retire_old_segment(cmd.segment_key),

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

            // Assignment confirmation from a data-leader — forward to the local
            // MultiRaftActor (we are this shard's coordinator), which marks the
            // segment confirmed so the heartbeat sweep stops re-driving it.
            C::SegmentAssignmentAck(cmd) => {
                self.out
                    .store_coordinator_cmd(MultiRaftActorCommand::AssignmentAck(cmd));
            }

            // Catch-ups: re-replicate a sealed segment to a newly assigned replica.
            C::CatchUpAssignment(cmd) => self.handle_catch_up_assignment(cmd),
            C::CatchUpRequest(cmd) => self.handle_catch_up_request(cmd),
            C::CatchUpChunk(cmd) => self.handle_catch_up_chunk(cmd),
            C::CatchUpStreamEnd(cmd) => self.handle_catch_up_stream_end(cmd),
            // Pass-through to the local MultiRaftActor (we coordinate this group):
            // a replica's confirmation that it finished catching up.
            C::CatchUpAck(cmd) => {
                self.out
                    .store_coordinator_cmd(MultiRaftActorCommand::CatchUpAck(cmd));
            }

            C::SealBoundaryQuery(cmd) => self.handle_seal_boundary_query(cmd),
            // Pass-through to the local MultiRaftActor
            C::SealBoundaryReport(cmd) => {
                self.out
                    .store_coordinator_cmd(MultiRaftActorCommand::SealBoundaryReport(cmd));
            }
        }
    }

    fn handle_seal_boundary_query(&mut self, cmd: SealBoundaryQuery) {
        let durable_end = self.durable_end(&cmd.segment_key);
        self.out
            .store_transport_cmd(DataTransportSendToCoordinator {
                shard_group_id: cmd.shard_group_id,
                message: SealBoundaryReport {
                    segment_key: cmd.segment_key,
                    from: self.node_id.clone(),
                    durable_end,
                }
                .into(),
            });
    }

    // ── Replacement-side catch-up: receive → verify → register ────────────

    /// The coordinator announced our (new) membership of this sealed segment.
    /// Reconcile against local state — declarative and idempotent:
    /// - already hold it through `sealed_end` → register if needed, transfer nothing;
    /// - already reconciling → no-op while progressing, resume if stalled;
    /// - else fetch `(local, sealed_end]` from a peer — a delta or a full copy.
    fn handle_catch_up_assignment(&mut self, cmd: CatchUpAssignment) {
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
                    CatchUpRequest {
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
                CatchUpRequest {
                    segment_key: cmd.segment_key,
                    from: self.node_id.clone(),
                    local_end: local,
                },
            ));
    }

    /// Highest verified entry id we already hold for this sealed segment — from the
    /// live sealed store (a survivor or a prior catch-up) or the recovered inventory
    /// (a restarted node's on-disk data). `None` if we hold nothing.
    fn local_sealed_progress(&self, key: &SegmentKey) -> Option<u64> {
        let registered = self.segments.sealed_bounds(key).map(|(_, end)| end);
        registered.max(self.recovered.get(key))
    }

    /// Highest durable (fsync'd) entry id this node holds for `key`, across every
    /// place a copy can live: a live tracker (the active segment a survivor is
    /// still following), the sealed store, or the recovered inventory. `None` if
    /// we hold nothing. Reported in a `SealBoundaryReport` so the coordinator can seal
    /// a leader-crashed segment at the `min` of survivors' durable extents.
    fn durable_end(&self, key: &SegmentKey) -> Option<u64> {
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
        local: Option<u64>,
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
    fn handle_catch_up_chunk(&mut self, cmd: CatchUpChunk) {
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
            let next = pending.last_written.map_or(pending.start_offset, |w| w + 1);
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
    fn handle_catch_up_stream_end(&mut self, cmd: CatchUpStreamEnd) {
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
    /// Routed via `SendToCoordinator`, mirroring `SegmentAssignmentAck`.
    fn send_catch_up_ack(&mut self, shard_group_id: ShardGroupId, segment_key: SegmentKey) {
        self.out
            .store_transport_cmd(DataTransportSendToCoordinator {
                shard_group_id,
                message: CatchUpAck {
                    segment_key,
                    shard_group_id,
                    from: self.node_id.clone(),
                }
                .into(),
            });
    }

    /// Source side: a peer wants `cmd.segment_key` brought up to its local end.
    fn handle_catch_up_request(&mut self, cmd: CatchUpRequest) {
        let Some((start_offset, sealed_end)) = self.segments.sealed_bounds(&cmd.segment_key) else {
            tracing::warn!(
                "catch-up source has no sealed segment {:?}; dropping request",
                cmd.segment_key
            );
            return;
        };
        let start = cmd.local_end.map_or(start_offset, |held| held + 1);
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
        start_offset: u64, // Segment base — names the file (`{segment_id}-{start_offset}.seg`)
        sealed_end: u64,
        read_from: u64,
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
    /// as a `CatchUpChunk` to the requester, then re-arm the next read until the
    /// sealed end is reached, at which point send `CatchUpStreamEnd`.
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
                    CatchUpChunk {
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
                CatchUpStreamEnd { segment_key },
            ));
    }

    fn handle_replica_ack(&mut self, cmd: ReplicaAck) {
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
    }

    fn handle_segment_assignment(&mut self, cmd: SegmentAssignment) {
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

        self.out
            .store_transport_cmd(DataTransportSendToCoordinator {
                shard_group_id: cmd.shard_group_id,
                message: SegmentAssignmentAck {
                    segment_key: cmd.segment_key,
                    shard_group_id: cmd.shard_group_id,
                    from: self.node_id.clone(),
                }
                .into(),
            });
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
                    cmd.segment_key
                        .file_path(&self.config.data_dir, cmd.entry_id),
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
        self.pending_seal_requests.clear(&cmd.old_segment_key);
        let Some(old_tracker) = self.segments.get(&cmd.old_segment_key) else {
            return;
        };

        let new_segment_key = cmd.old_segment_key.with_segment_id(cmd.new_segment_id);
        let shard_group_id = old_tracker.shard_group_id();
        let new_start_entry_id = old_tracker.successor_start_entry_id();

        // Collect the tail to carry into the successor (owned) so the `old_tracker`
        // borrow ends before we mutate the successor. Re-staged records reach cache
        // via the normal flush path and are re-WAL'd with new headers; on recovery,
        // metadata's seal boundary clips any duplicate. `uncommitted` (published)
        // records were cleared from `buffer_byte_count` at flush, so re-staging
        // re-adds their bytes; `staged` records are still counted, so they don't.
        let uncommitted: Vec<_> = old_tracker.uncommitted_entries().collect();
        let staged: Vec<_> = old_tracker.staged_for_replay().collect();
        let followers = old_tracker.followers().to_vec();
        let replayed_bytes: usize = uncommitted.iter().map(|(data, _)| data.len()).sum();

        if !followers.is_empty() {
            self.out
                .store_transport_cmd(DataTransportCommand::send_to_targets(
                    followers,
                    SegmentSealed {
                        segment_key: cmd.old_segment_key,
                    },
                ));
        }

        self.replication
            .segment_handoff(cmd.old_segment_key, new_segment_key);
        self.retire_old_segment(cmd.old_segment_key);

        // The successor may already exist: on a roll the coordinator co-dispatches a
        // `SegmentAssignment` for the new active segment alongside this `SealResponse`,
        // and it can win the race (creating the successor empty). Carry the tail onto
        // the existing tracker rather than a fresh one that `insert_active` would
        // silently refuse — which would drop the records (data loss) and strand the
        // counter.
        if self.segments.get(&new_segment_key).is_none() {
            let new_tracker = SegmentTracker::new_with_start_entry_id(
                new_segment_key.file_path(&self.config.data_dir, new_start_entry_id),
                SegmentRole::Leader,
                cmd.new_replica_set,
                shard_group_id,
                new_start_entry_id,
            );
            self.segments.insert_active(new_segment_key, new_tracker);
        }
        if let Some(successor) = self.segments.get_mut(&new_segment_key) {
            for (data, record_count) in uncommitted.into_iter().chain(staged) {
                successor.stage_entry(new_segment_key, data, record_count);
            }
        }
        self.buffer_byte_count += replayed_bytes;
        self.dirty_segments.push(new_segment_key);
        self.needs_flush = true;
    }

    // Drop the sealed segment's live tracker and checkpoint its committed
    // records so cold reads can still serve them.
    fn retire_old_segment(&mut self, segment_key: SegmentKey) {
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
                message: SealRequest {
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
                message: SealRequest {
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
                self.out.store_checkpoint(tracker.checkpoint(key));
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
            self.out.store_checkpoint(tracker.checkpoint(*key));
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
                    message: SealRequest {
                        from: self.node_id.clone(),
                        segment_key,
                        failed_nodes,
                        end_entry_id: tracker.committed_entry_id(),
                    }
                    .into(),
                });
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
    use std::path::PathBuf;

    use super::*;
    use crate::control_plane::membership::ShardGroupId;
    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
    use crate::data_plane::cold_read::DEFAULT_POOL_SIZE;
    use crate::data_plane::recovery::segment_scan::RecoveredSegments;
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
            recovered,
        )
    }

    /// A `LocalInventory` reporting `key` verified through `local_end` — as if
    /// recovery had scanned a local copy of that prefix off disk.
    fn inventory_with(key: SegmentKey, local_end: u64) -> LocalInventory {
        let mut recovered = RecoveredSegments::default();
        for id in 0..=local_end {
            recovered.advance(key, id);
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
        DataPlaneCommand::DataPlaneInterNodeCommand(
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

    fn seal_response(
        old: SegmentKey,
        new_segment_id: SegmentId,
        new_replica_set: Vec<NodeId>,
    ) -> DataPlaneCommand {
        DataPlaneCommand::DataPlaneInterNodeCommand(
            SealResponse {
                old_segment_key: old,
                new_segment_id,
                new_replica_set,
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

        dp.handle_command(DataPlaneCommand::DataPlaneTimeoutCallback(
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

        dp.handle_command(DataPlaneCommand::DataPlaneTimeoutCallback(
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

        dp.handle_command(DataPlaneCommand::DataPlaneTimeoutCallback(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));
        assert!(!dp.has_buffered_data());
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
        dp.handle_command(assign_segment(test_key(), vec![]));

        // Produce — staged into the active segment, no flush yet.
        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);
        assert!(dp.has_buffered_data());

        // Seal arrives before the batch flush. `handle_command` runs
        // `assert_invariants` afterwards — pre-fix this panics on the stranded
        // counter.
        dp.handle_command(seal_response(test_key(), SegmentId(1), vec![]));

        // The staged produce moved to the successor; the counter still matches.
        let successor = test_key().with_segment_id(SegmentId(1));
        assert!(dp.segments.get(&successor).unwrap().has_staged());
        assert!(dp.has_buffered_data());
    }

    /// On a roll the coordinator co-dispatches a `SegmentAssignment` for the new
    /// active segment alongside the `SealResponse`. If the assignment wins the race
    /// (creating the successor empty first), the seal handoff must still carry the
    /// old segment's staged tail onto that existing successor — not build a fresh
    /// tracker that `insert_active` refuses, dropping the records (data loss) and
    /// stranding `buffer_byte_count`. Regression for the age-seal stress flake.
    #[test]
    fn seal_handoff_carries_into_a_successor_already_created_by_assignment() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![]));

        // Produce — staged into the active segment, unflushed.
        let (cmd, _rx) = produce(test_key());
        dp.handle_command(cmd);
        assert!(dp.has_buffered_data());

        // The successor's SegmentAssignment wins the race: seg1 is created empty.
        let successor = test_key().with_segment_id(SegmentId(1));
        dp.handle_command(assign_segment(successor, vec![]));

        // The SealResponse handoff arrives second. Pre-fix `insert_active` refuses
        // (seg1 already active) → the staged produce is dropped and `buffer_byte_count`
        // strands, tripping `assert_invariants` after the command.
        dp.handle_command(seal_response(test_key(), SegmentId(1), vec![]));

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

        dp.handle_command(assign_segment(test_key(), vec![]));

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

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
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

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
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

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
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

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
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

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
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

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
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
        dp.handle_command(DataPlaneCommand::DataPlaneTimeoutCallback(
            DataPlaneTimeoutCallback::BatchFlushDeadline,
        ));
        dp.segments.get_mut(&test_key()).unwrap().commit_entry(0);

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
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
        assert!(dp.pending_seal_requests.is_tracked(&test_key()));
    }

    #[test]
    fn enqueue_seal_request_deduplicates() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        dp.handle_command(assign_segment(test_key(), vec![test_node_id()]));

        dp.enqueue_seal_request(test_key());
        dp.enqueue_seal_request(test_key());

        // Count only seal requests — `handle_segment_assignment` also emits a
        // SegmentAssignmentAck via SendToCoordinator, which is not a seal.
        assert_eq!(
            dp.out
                .transport_cmds
                .iter()
                .filter(|c| matches!(
                    c,
                    DataTransportCommand::SendToCoordinator(s)
                        if matches!(s.message, DataPlaneInterNodeCommand::SealRequest(_))
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

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
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
            DataPlaneCommand::DataPlaneTimeoutCallback(
                DataPlaneTimeoutCallback::BatchFlushDeadline,
            ),
        );

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
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
            SegmentReadState::Sealed {
                key, end_entry_id, ..
            } => {
                assert_eq!(key, s0);
                assert_eq!(end_entry_id, 0);
            }
            other => panic!("expected Sealed, got {other:?}"),
        }

        let active = dp.segments.resolve(TopicId(1), RangeId(0), 1).unwrap();
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
        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            SealResponse {
                old_segment_key: test_key(),
                new_segment_id: SegmentId(1),
                new_replica_set: vec![test_node_id()],
            }
            .into(),
        ));

        let s1 = test_key().with_segment_id(SegmentId(1));

        // Offset 0 resolves to the active successor (which reuses start 0), not
        // an empty sealed segment — the old segment owned no committed offsets.
        let resolved = dp.segments.resolve(TopicId(1), RangeId(0), 0).unwrap();
        assert!(
            matches!(resolved, SegmentReadState::Active(k) if k == s1),
            "offset 0 must resolve to the active successor, got {resolved:?}",
        );
        assert_eq!(dp.segments.get(&s1).unwrap().start_entry_id(), 0);

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
            empty_inventory(),
        );

        // Assign + produce 3 records (single replica → commit inline on flush).
        dp.handle_command(assign_segment(test_key(), vec![]));
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
        let seg_path = test_key().file_path(dir.path(), 0);
        if let Some(parent) = seg_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut buf = Vec::new();
        let mut index_entries = Vec::new();
        for (i, (payload, record_count)) in records.iter().enumerate() {
            index_entries.push(SparseEntry::new(
                test_key(),
                i as u64,
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

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            SegmentSealed {
                segment_key: test_key(),
            }
            .into(),
        ));

        // Fetch offset 0 → must resolve Sealed and be served cold from disk.
        let (reply, reply_rx) = oneshot::channel();
        dp.process(DataPlaneMessage::Query(DataPlaneQuery::Fetch(Fetch {
            topic_id: TopicId(1),
            range_id: RangeId(0),
            entry_id: 0,
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
            .map(|e| (e.entry_id, e.record_count, e.data.to_vec()))
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
        assert_eq!(next_entry_id, 3);
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
            empty_inventory(),
        );
        let mut tracker = SegmentTracker::new_with_start_entry_id(
            dir.path().to_path_buf(),
            SegmentRole::Leader,
            vec![],
            ShardGroupId(1),
            0,
        );
        tracker.commit_entry(10);
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
        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpRequest {
                segment_key: test_key(),
                from: requester.clone(),
                local_end: Some(4),
            }
            .into(),
        ));

        let req = cold_read_rx.try_recv().expect("dispatched cold read");
        assert_eq!(req.start_entry_offset, 5); // requester's local_end + 1
        assert_eq!(req.end_entry_id, 10); // the source's own sealed end
        assert!(matches!(
            req.reply,
            ColdReadReply::CatchUp(cu) if cu.requester == requester && cu.sealed_end == 10
        ));
    }

    /// A requester already at (or past) the source's committed end gets an
    /// immediate `CatchUpStreamEnd` and no read is dispatched — the source short-
    /// circuits on its OWN `sealed_end`, not on anything the requester relayed.
    #[test]
    fn catch_up_request_already_caught_up_sends_done_without_reading() {
        let dir = tempfile::tempdir().unwrap();
        let (mut dp, cold_read_rx) = source_with_sealed_segment(&dir);

        let requester = NodeId::new("replacement");
        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpRequest {
                segment_key: test_key(),
                from: requester.clone(),
                local_end: Some(10), // == the source's sealed end
            }
            .into(),
        ));

        // Nothing handed to the pool...
        assert!(cold_read_rx.try_recv().is_err());
        // ...and a CatchUpStreamEnd went straight back to the requester.
        assert_eq!(dp.out.transport_cmds.len(), 1);
        let DataTransportCommand::SendToTargets(s) = &dp.out.transport_cmds[0] else {
            panic!("expected SendToTargets");
        };
        assert_eq!(s.targets[0], requester);
        assert!(matches!(
            &s.message,
            DataPlaneInterNodeCommand::CatchUpStreamEnd(d) if d.segment_key == test_key()
        ));
    }

    /// A batch whose `next_offset` is past the sealed end emits one `CatchUpChunk`
    /// (entries intact, with `record_count`) followed by `CatchUpStreamEnd` — no re-arm.
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
                entry_id: 0,
                lsn: 0,
            }),
            Arc::new(CachedEntry {
                data: Bytes::copy_from_slice(b"bb").into(),
                record_count: 2,
                entry_id: 1,
                lsn: 0,
            }),
            Arc::new(CachedEntry {
                data: Bytes::copy_from_slice(b"ccc").into(),
                record_count: 3,
                entry_id: 2,
                lsn: 0,
            }),
        ];
        dp.handle_command(DataPlaneCommand::CatchUpReadComplete(CatchUpReadComplete {
            requester: requester.clone(),
            segment_key: test_key(),
            start_offset: 0,
            sealed_end: 2,
            entries,
            next_offset: 3,
        }));

        assert_eq!(dp.out.transport_cmds.len(), 2);

        let DataTransportCommand::SendToTargets(chunk) = &dp.out.transport_cmds[0] else {
            panic!("expected chunk SendToTargets");
        };
        assert_eq!(chunk.targets[0], requester);
        let DataPlaneInterNodeCommand::CatchUpChunk(c) = &chunk.message else {
            panic!("expected CatchUpChunk");
        };
        assert_eq!(c.segment_key, test_key());
        let got: Vec<(u64, u32, Vec<u8>)> = c
            .entries
            .iter()
            .map(|e| (e.entry_id, e.record_count, e.data.to_vec()))
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
            DataPlaneInterNodeCommand::CatchUpStreamEnd(_)
        ));
    }

    /// A batch that ends below the sealed end emits its chunk and re-arms the next
    /// read against the pool, resuming at `next_offset` — no `CatchUpStreamEnd` yet.
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
            empty_inventory(),
        );
        let requester = NodeId::new("replacement");

        dp.handle_command(DataPlaneCommand::CatchUpReadComplete(CatchUpReadComplete {
            requester: requester.clone(),
            segment_key: test_key(),
            start_offset: 0,
            sealed_end: 9,
            entries: vec![Arc::new(CachedEntry {
                data: Bytes::copy_from_slice(b"x").into(),
                record_count: 1,
                entry_id: 4,
                lsn: 0,
            })],
            next_offset: 5,
        }));

        // One chunk, no Done yet.
        assert_eq!(dp.out.transport_cmds.len(), 1);
        let DataTransportCommand::SendToTargets(chunk) = &dp.out.transport_cmds[0] else {
            panic!("expected chunk SendToTargets");
        };
        assert!(matches!(
            &chunk.message,
            DataPlaneInterNodeCommand::CatchUpChunk(_)
        ));

        // The next read was re-armed against the pool, resuming at next_offset.
        let req = cold_read_rx.try_recv().expect("re-armed cold read");
        assert_eq!(req.start_entry_offset, 5);
        assert_eq!(req.end_entry_id, 9);
        assert!(matches!(
            req.reply,
            ColdReadReply::CatchUp(cu) if cu.requester == requester && cu.sealed_end == 9
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
        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpAssignment {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: 0,
                sealed_end_entry_id: 2,
                replica_set: vec![test_node_id(), source.clone()],
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
            DataPlaneInterNodeCommand::CatchUpRequest(r)
                if r.segment_key == test_key() && r.local_end.is_none()
        ));

        // Source streams the three entries, then signals end of stream.
        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpChunk {
                segment_key: test_key(),
                entries: vec![
                    CatchUpEntry {
                        entry_id: 0,
                        data: b"a".to_vec().into(),
                        record_count: 1,
                    },
                    CatchUpEntry {
                        entry_id: 1,
                        data: b"bb".to_vec().into(),
                        record_count: 2,
                    },
                    CatchUpEntry {
                        entry_id: 2,
                        data: b"ccc".to_vec().into(),
                        record_count: 3,
                    },
                ]
                .into(),
            }
            .into(),
        ));
        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpStreamEnd {
                segment_key: test_key(),
            }
            .into(),
        ));

        // Registered cold-readable at its sealed bounds...
        assert_eq!(dp.segments.sealed_bounds(&test_key()), Some((0, 2)));
        // ...the file verifies through the sealed end...
        let scan = scan_segment_file(&test_key().file_path(dir.path(), 0)).unwrap();
        assert_eq!(scan.last_entry_id, Some(2));
        // ...and the suffix anchors were handed to the checkpoint worker (entry 0
        // at byte 0 is always an anchor).
        assert!(
            dp.out.checkpoint_tasks.iter().any(|t| {
                matches!(t, CheckpointTask::PutAnchors(anchors) if !anchors.is_empty())
            })
        );
        // ...and a CatchUpAck confirmed completion back to the coordinator.
        assert!(dp.out.transport_cmds.iter().any(|c| matches!(
            c,
            DataTransportCommand::SendToCoordinator(coord)
                if matches!(&coord.message, DataPlaneInterNodeCommand::CatchUpAck(a) if a.segment_key == test_key())
        )));
    }

    /// A stream that stops short of `sealed_end` fails verification, so the
    /// segment is NOT registered — the coordinator re-drives with another source.
    #[test]
    fn catch_up_receive_short_of_sealed_end_does_not_register() {
        use crate::data_plane::checkpoint::CheckpointTask;

        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpAssignment {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: 0,
                sealed_end_entry_id: 5, // need through 5...
                replica_set: vec![test_node_id(), NodeId::new("source")],
            }
            .into(),
        ));
        // ...but only 0..=2 arrive.
        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpChunk {
                segment_key: test_key(),
                entries: vec![
                    CatchUpEntry {
                        entry_id: 0,
                        data: b"a".to_vec().into(),
                        record_count: 1,
                    },
                    CatchUpEntry {
                        entry_id: 1,
                        data: b"b".to_vec().into(),
                        record_count: 1,
                    },
                    CatchUpEntry {
                        entry_id: 2,
                        data: b"c".to_vec().into(),
                        record_count: 1,
                    },
                ]
                .into(),
            }
            .into(),
        ));
        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpStreamEnd {
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
        // ...and NO CatchUpAck — an unverified receive must not confirm completion.
        assert!(!dp.out.transport_cmds.iter().any(|c| matches!(
            c,
            DataTransportCommand::SendToCoordinator(s)
                if matches!(&s.message, DataPlaneInterNodeCommand::CatchUpAck(_))
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

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpAssignment {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: 0,
                sealed_end_entry_id: 2,
                replica_set: vec![test_node_id(), NodeId::new("peer")],
            }
            .into(),
        ));

        // Zero transfer — no CatchUpRequest dispatched, only a CatchUpAck back to
        // the coordinator so its re-drive stops re-announcing the assignment.
        assert_eq!(dp.out.transport_cmds.len(), 1);
        let DataTransportCommand::SendToCoordinator(s) = &dp.out.transport_cmds[0] else {
            panic!("expected SendToCoordinator");
        };
        assert!(matches!(
            &s.message,
            DataPlaneInterNodeCommand::CatchUpAck(a) if a.segment_key == test_key()
        ));
        // ...and the segment is now registered cold-readable at the sealed bounds.
        assert_eq!(dp.segments.sealed_bounds(&test_key()), Some((0, 2)));
    }

    #[test]
    fn orphan_gc_deletes_strays_and_keeps_reused() {
        use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
        let dir = tempfile::tempdir().unwrap();

        let stray = SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0));
        let reused = SegmentKey::new(TopicId(2), RangeId(0), SegmentId(0));

        // Recovery found both on disk (start_offset 0, verified through 0).
        let mut recovered = RecoveredSegments::default();
        recovered.advance(stray, 0);
        recovered.advance(reused, 0);
        let mut dp = make_data_plane_with(&dir, LocalInventory::from_recovered(&recovered));

        let stray_path = stray.file_path(dir.path(), 0);
        let reused_path = reused.file_path(dir.path(), 0);
        write_seg_file(&stray_path, &[(b"x", 1)]);
        write_seg_file(&reused_path, &[(b"y", 1)]);

        // The owning group reassigned `reused` here and its catch-up registered it (the
        // lottery won) — now a live sealed segment. `stray` was never assigned here.
        dp.segments.insert_sealed_from_catch_up(reused, 0, 0);

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
        let path = test_key().file_path(dir.path(), 0);
        write_seg_file(&path, &[(b"a", 1), (b"b", 1)]);
        let mut dp = make_data_plane_with(&dir, inventory_with(test_key(), 1));

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpAssignment {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: 0,
                sealed_end_entry_id: 4,
                replica_set: vec![test_node_id(), NodeId::new("source")],
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
        assert_eq!(dp.recovered.get(&test_key()), Some(1), "kept in inventory");

        // an orphan still pending → the handler tells the ticker to keep going.
        assert!(
            matches!(reply_rx.try_recv(), Ok(OrphanGcSignal::KeepTicking)),
            "ticker told to keep going while an orphan is pending"
        );
    }

    /// Partial match: a local prefix on disk (entries 0..=1) means the receiver
    /// asks only for the delta after its local end, not a full copy.
    #[test]
    fn catch_up_partial_requests_only_the_delta() {
        let dir = tempfile::tempdir().unwrap();
        // A partial local copy on disk plus the matching inventory cursor.
        write_seg_file(
            &test_key().file_path(dir.path(), 0),
            &[(b"a", 1), (b"b", 1)],
        );
        let mut dp = make_data_plane_with(&dir, inventory_with(test_key(), 1));

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpAssignment {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: 0,
                sealed_end_entry_id: 4,
                replica_set: vec![test_node_id(), NodeId::new("source")],
            }
            .into(),
        ));

        assert_eq!(dp.out.transport_cmds.len(), 1);
        let DataTransportCommand::SendToTargets(s) = &dp.out.transport_cmds[0] else {
            panic!("expected SendToTargets");
        };
        assert!(matches!(
            &s.message,
            DataPlaneInterNodeCommand::CatchUpRequest(r) if r.local_end == Some(1)
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

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpAssignment {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: 0,
                sealed_end_entry_id: 2,
                // self (test-node) is a follower here; `leader` is replica_set[0].
                replica_set: vec![leader.clone(), test_node_id(), follower.clone()],
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
        DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpAssignment {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(1),
                start_entry_id: 0,
                sealed_end_entry_id: sealed_end,
                replica_set: vec![test_node_id(), source.clone()],
            }
            .into(),
        )
    }

    fn catch_up_chunk_of(ids: &[u64]) -> DataPlaneCommand {
        DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpChunk {
                segment_key: test_key(),
                entries: ids
                    .iter()
                    .map(|&entry_id| CatchUpEntry {
                        entry_id,
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
            DataPlaneInterNodeCommand::CatchUpRequest(r)
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
        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            CatchUpStreamEnd {
                segment_key: test_key(),
            }
            .into(),
        ));

        // Verified exactly through the sealed end — no duplicate inflation.
        assert_eq!(dp.segments.sealed_bounds(&test_key()), Some((0, 4)));
        let scan = scan_segment_file(&test_key().file_path(dir.path(), 0)).unwrap();
        assert_eq!(scan.last_entry_id, Some(4));
    }

    // --- Leader-crash boundary recovery: durable extent + query handler -----

    /// A follower publishes (fsyncs) entries on flush but isn't told of the
    /// commit until a `CommitAdvance` — so `durable_end` (the fsync'd extent)
    /// runs ahead of `committed_entry_id`. Boundary recovery reads the former.
    #[test]
    fn durable_end_reports_published_extent_not_commit_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane(&dir);
        let leader = NodeId::new("leader-node");

        for entry_id in 0..3 {
            dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
                ReplicaAppend {
                    segment_key: test_key(),
                    replica_set: vec![leader.clone(), test_node_id()],
                    data: b"x".to_vec().into(),
                    record_count: 1,
                    entry_id,
                }
                .into(),
            ));
        }
        dp.flush_batch(); // publishes 0,1,2 past the WAL fsync; no CommitAdvance yet

        assert_eq!(
            dp.segments.get(&test_key()).unwrap().committed_entry_id(),
            0,
            "follower hasn't been told of any commit"
        );
        assert_eq!(
            dp.durable_end(&test_key()),
            Some(2),
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
        assert_eq!(sealed_dp.durable_end(&test_key()), Some(10));

        // Recovered inventory only (restarted node, not yet in the live store).
        let recovered_dir = tempfile::tempdir().unwrap();
        let recovered_dp = make_data_plane_with(&recovered_dir, inventory_with(test_key(), 7));
        assert_eq!(recovered_dp.durable_end(&test_key()), Some(7));

        // Hold nothing → None.
        let empty_dir = tempfile::tempdir().unwrap();
        let empty_dp = make_data_plane(&empty_dir);
        assert_eq!(empty_dp.durable_end(&test_key()), None);
    }

    /// A `SealBoundaryQuery` is answered with our durable extent, addressed back to
    /// the coordinator of the query's shard group.
    #[test]
    fn seal_boundary_query_replies_with_durable_end() {
        let dir = tempfile::tempdir().unwrap();
        let mut dp = make_data_plane_with(&dir, inventory_with(test_key(), 4));

        dp.handle_command(DataPlaneCommand::DataPlaneInterNodeCommand(
            SealBoundaryQuery {
                segment_key: test_key(),
                shard_group_id: ShardGroupId(9),
            }
            .into(),
        ));

        assert_eq!(dp.out.transport_cmds.len(), 1);
        let DataTransportCommand::SendToCoordinator(s) = &dp.out.transport_cmds[0] else {
            panic!("boundary report must route to the coordinator");
        };
        assert_eq!(s.shard_group_id, ShardGroupId(9));
        let DataPlaneInterNodeCommand::SealBoundaryReport(report) = &s.message else {
            panic!("expected a SealBoundaryReport");
        };
        assert_eq!(report.segment_key, test_key());
        assert_eq!(report.from, test_node_id());
        assert_eq!(report.durable_end, Some(4));
    }
}
