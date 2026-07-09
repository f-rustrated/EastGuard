use super::RangeCursor;

use crate::client::consumer::cursor::StartPolicy;
use crate::client::consumer::group::{ConsumerGroup, ConsumerPosition};
use crate::client::consumer::range_fetcher::{RangeFetchActor, RangeFetchActorCommand};
use crate::client::consumer::{
    ConsumerContext, ConsumerRecord, MergeSiblingState, PendingCursorStore,
};
use crate::client::{ClientError, CommitMode, ConsumerConfig};
use crate::connections::protocol::{RangeDetail, RangeTransition};
use crate::control_plane::metadata::{EntryId, RangeId, RangeState};

#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

pub(crate) enum TopicFetchManagerCommand {
    Pause {
        range_id: RangeId,
        reply: oneshot::Sender<Result<(), ClientError>>,
    },
    Resume {
        range_id: RangeId,
        reply: oneshot::Sender<Result<(), ClientError>>,
    },
    Seek {
        range_id: RangeId,
        absolute_offset: u64,
        reply: oneshot::Sender<Result<(), ClientError>>,
    },
}

pub(crate) struct RangeDrained {
    pub cursor: RangeCursor,
    pub transition: RangeTransition,
}

pub(crate) struct TopicFetchManagerState {
    pending_cursors: PendingCursorStore,
    senders: HashMap<RangeId, flume::Sender<RangeFetchActorCommand>>,

    /// The optional shared consumer group context for dynamic partition rebalancing.
    consumer_group: Option<Arc<ConsumerGroup>>,
    config: ConsumerConfig,
    record_tx: flume::Sender<Result<ConsumerRecord, ClientError>>,
}

impl TopicFetchManagerState {
    pub(crate) fn new(
        pending_cursors: PendingCursorStore,
        consumer_group: Option<Arc<ConsumerGroup>>,
        config: ConsumerConfig,
        record_tx: flume::Sender<Result<ConsumerRecord, ClientError>>,
    ) -> Self {
        Self {
            pending_cursors,
            senders: HashMap::new(),
            consumer_group,
            config,
            record_tx,
        }
    }
    fn auto_commit_interval_ms(&self) -> u64 {
        self.config.auto_commit_interval_ms
    }

    pub(crate) fn should_exit(&self) -> bool {
        self.senders.is_empty() && self.pending_cursors.is_empty() && self.consumer_group.is_none()
    }

    pub(crate) fn should_auto_commit(&self) -> bool {
        self.consumer_group.is_some() && self.config.commit_mode == CommitMode::Auto
    }

    fn provision_initial_tasks(&mut self, ctx: &Arc<ConsumerContext>) {
        // If a consumer_group is present, partition ownership is dynamic and determined entirely by the
        // group rebalancer. We return early to prevent the consumer from starting fetch tasks before
        // partitions are assigned to it.
        if self.consumer_group.is_some() {
            return;
        }
        for cursor in self.pending_cursors.iter().cloned().collect::<Vec<_>>() {
            self.spawn_and_register(cursor, ctx.clone());
        }
    }

    fn handle_cursor_drained(&mut self, event: RangeDrained, ctx: &Arc<ConsumerContext>) {
        let range_id = event.cursor.range_id;
        // If not owned, it was already revoked — late drain event, safe to ignore.
        if self.senders.remove(&range_id).is_none() {
            return;
        }
        let sibling_state = self.merge_sibling_state(&event.cursor, &event.transition);
        let added = self.pending_cursors.apply_drained_cursor(
            event.cursor,
            event.transition,
            sibling_state,
        );

        for new_cursor in added {
            if self
                .consumer_group
                .as_ref()
                .is_some_and(|group| !group.is_responsible_for(new_cursor.range_id))
            {
                continue;
            }
            self.spawn_and_register(new_cursor, ctx.clone());
        }
    }

    fn merge_sibling_state(
        &self,
        drained: &RangeCursor,
        transition: &RangeTransition,
    ) -> MergeSiblingState {
        let RangeTransition::Merged { merged_from, .. } = transition else {
            return MergeSiblingState::Untracked;
        };
        let sibling = drained.merge_sibling(*merged_from);
        if self.senders.contains_key(&sibling) || self.pending_cursors.contains(sibling) {
            MergeSiblingState::Tracked
        } else {
            MergeSiblingState::Untracked
        }
    }

    fn handle_command(&mut self, command: TopicFetchManagerCommand) {
        match command {
            TopicFetchManagerCommand::Pause { range_id, reply } => {
                self.forward_actor_command("pause", range_id, reply, |reply| {
                    RangeFetchActorCommand::Pause { reply }
                });
            }
            TopicFetchManagerCommand::Resume { range_id, reply } => {
                self.forward_actor_command("resume", range_id, reply, |reply| {
                    RangeFetchActorCommand::Resume { reply }
                });
            }
            TopicFetchManagerCommand::Seek {
                range_id,
                absolute_offset,
                reply,
            } => {
                self.forward_actor_command("seek", range_id, reply, |reply| {
                    RangeFetchActorCommand::Seek {
                        absolute_offset,
                        reply,
                    }
                });
            }
        }
    }

    fn forward_actor_command(
        &self,
        operation: &'static str,
        range_id: RangeId,
        reply: oneshot::Sender<Result<(), ClientError>>,
        callback_build: impl FnOnce(oneshot::Sender<()>) -> RangeFetchActorCommand,
    ) {
        let Some(tx) = self.senders.get(&range_id) else {
            let reason = "no active fetch actor for range";
            let _ = reply.send(Err(ClientError::on_control(operation, range_id, reason)));
            return;
        };

        let (actor_ack_tx, actor_ack_rx) = oneshot::channel();

        if tx.try_send(callback_build(actor_ack_tx)).is_err() {
            let reason = "fetch actor command queue is full or closed";
            let _ = reply.send(Err(ClientError::on_control(operation, range_id, reason)));
            return;
        }

        tokio::spawn(async move {
            let result = actor_ack_rx.await.map_err(|_| {
                let reason = "fetch actor stopped before acknowledging command";
                ClientError::on_control(operation, range_id, reason)
            });
            let _ = reply.send(result);
        });
    }

    async fn handle_rebalance(&mut self, ctx: &Arc<ConsumerContext>) {
        let Some(group) = &self.consumer_group else {
            return;
        };

        // Perform rebalance inside ConsumerGroup
        let (to_drop, to_start) =
            group.rebalance(&ctx.all_ranges(), self.senders.keys().cloned().collect());

        // Abort revoked tasks immediately and commit their offsets in the background
        // to prevent blocking the main cursor manager loop.
        if !to_drop.is_empty() {
            for range in &to_drop {
                // Revoke ownership: send Stop, remove cursor and stop channel.
                if let Some(stop_tx) = self.senders.remove(range) {
                    let _ = stop_tx.send(RangeFetchActorCommand::Stop);
                }
                self.pending_cursors.remove(*range);
            }

            let group = group.clone();
            tokio::spawn(async move {
                let _ = group.revoke_ranges(&to_drop).await;
            });
        }

        if to_start.is_empty() {
            return;
        }

        // Fetch committed offsets with a 5-second timeout, falling back to empty offsets on timeout/error.
        let saved_offsets = tokio::time::timeout(
            Duration::from_secs(5),
            group
                .client
                .clone()
                .fetch_all_saved_offsets(&group.group_id, &ctx.topic),
        )
        .await
        .map(|res| res.unwrap_or_default())
        .unwrap_or_default();

        let metadata = ctx.metadata.load();

        for range in to_start {
            let (resolved_entry_id, skip_below_offset, next_absolute_offset) = if let Some(pos) =
                saved_offsets.get(&range)
            {
                // Prefer explicitly saved offsets. The committed offset is a
                // record-level checkpoint. Because one entry can contain multiple
                // records, the next entry we fetch begins at the physical batch containing
                // the committed offset, and we skip any records that have already been processed.
                (
                    pos.entry_id,
                    Some(pos.batch_offset),
                    pos.absolute_offset.saturating_add(1),
                )
            } else {
                let resolved = match self.resolve_start_entry_id(range, ctx).await {
                    Ok(resolved) => resolved,
                    Err(error) => {
                        tracing::warn!(?range, ?error, "failed to resolve consumer start entry id");
                        continue;
                    }
                };
                (resolved, None, 0)
            };

            if self.should_skip_start(range, &metadata.ranges, &saved_offsets, resolved_entry_id) {
                continue;
            }

            if let Some(r_meta) = metadata.ranges.iter().find(|r| r.range_id == range) {
                self.spawn_and_register(
                    RangeCursor::new(
                        range,
                        resolved_entry_id,
                        r_meta.keyspace_start.clone(),
                        r_meta.keyspace_end.clone(),
                    )
                    .with_skip_batch_offsets_below(skip_below_offset)
                    .with_skip_absolute_offsets_below(None)
                    .with_next_absolute_offset(next_absolute_offset),
                    ctx.clone(),
                );
            }
        }
    }

    /// Transfers a pending cursor into a live fetch actor.
    fn spawn_and_register(&mut self, cursor: RangeCursor, ctx: Arc<ConsumerContext>) {
        if self.senders.contains_key(&cursor.range_id) {
            return;
        }
        self.pending_cursors.remove(cursor.range_id);
        let (stop_tx, stop_rx) = flume::bounded(8);
        let range_id = cursor.range_id;
        let actor = RangeFetchActor::new(cursor, ctx, self.record_tx.clone());
        tokio::spawn(actor.run(stop_rx));
        self.senders.insert(range_id, stop_tx);
    }

    /// Resolves the starting entry ID for a range cursor when it is dynamically started.
    ///
    /// In EastGuard:
    /// - **Entry ID** refers to the physical index of a record in the range's log.
    /// - **Offset** refers to the logical consumer-committed checkpoint (which is the last processed entry ID).
    async fn resolve_start_entry_id(
        &self,
        range: RangeId,
        ctx: &Arc<ConsumerContext>,
    ) -> Result<EntryId, ClientError> {
        // 1. Fetch the latest boundary from the replica if the start policy is Latest.
        if self.config.start_policy == StartPolicy::Latest {
            let (_, tail_entry_id) = ctx.client.fetch_range_entry_ids(&ctx.topic, range).await?;
            return Ok(tail_entry_id);
        }

        // 2. Fallback to the local cursor's next entry ID.
        if let Some(cursor) = self.pending_cursors.get(range) {
            return Ok(cursor.next_entry_id);
        }

        // 3. Default start entry ID is 0
        Ok(EntryId::default())
    }

    async fn abort_all(&mut self) {
        for (_, stop_tx) in self.senders.drain() {
            let _ = stop_tx.send(RangeFetchActorCommand::Stop);
        }
        if let Some(group) = &self.consumer_group {
            let _ = tokio::time::timeout(Duration::from_secs(1), group.commit()).await;
        }
    }

    /// Returns true if this range should NOT be started. Consolidates:
    /// - Active descendant check (children already being fetched)
    /// - Undrained parent check (parent not yet fully consumed)
    /// - Sealed-and-consumed check (range already fully consumed)
    fn should_skip_start(
        &self,
        range_id: RangeId,
        ranges: &[RangeDetail],
        saved_offsets: &HashMap<RangeId, ConsumerPosition>,
        resolved_entry_id: EntryId,
    ) -> bool {
        self.has_active_descendant(range_id, ranges)
            || self.has_undrained_parent(range_id, ranges, saved_offsets)
            || ranges
                .iter()
                .find(|r| r.range_id == range_id)
                .is_some_and(|r| {
                    r.state == RangeState::Sealed && resolved_entry_id > r.end_entry_id()
                })
    }

    /// Returns true if any descendant (split child or merge target) is already
    /// represented either by a live actor or by a pending cursor.
    fn has_active_descendant(&self, range_id: RangeId, ranges: &[RangeDetail]) -> bool {
        let Some(r_meta) = ranges.iter().find(|r| r.range_id == range_id) else {
            return false;
        };

        if let Some(children) = r_meta.split_into {
            if self.senders.contains_key(&children.0) || self.senders.contains_key(&children.1) {
                return true;
            }
            if self.pending_cursors.contains(children.0)
                || self.pending_cursors.contains(children.1)
            {
                return true;
            }
            if self.has_active_descendant(children.0, ranges)
                || self.has_active_descendant(children.1, ranges)
            {
                return true;
            }
        }

        if let Some(child) = r_meta.merged_into {
            if self.senders.contains_key(&child) {
                return true;
            }
            if self.pending_cursors.contains(child) {
                return true;
            }
            if self.has_active_descendant(child, ranges) {
                return true;
            }
        }

        false
    }

    /// Returns true if any ancestor (split parent or merge source) of the
    /// given range still needs to be drained before this range can start.
    fn has_undrained_parent(
        &self,
        range_id: RangeId,
        ranges: &[RangeDetail],
        saved_offsets: &HashMap<RangeId, ConsumerPosition>,
    ) -> bool {
        for p in ranges.iter() {
            let is_parent = p
                .split_into
                .is_some_and(|children| children.0 == range_id || children.1 == range_id)
                || p.merged_into.is_some_and(|child| child == range_id);

            if !is_parent {
                continue;
            }

            // Parent still has an active fetch actor — not yet drained.
            if self.senders.contains_key(&p.range_id) {
                return true;
            }

            // Parent is still active (not sealed) — can't have drained.
            if p.state == RangeState::Active {
                return true;
            }

            // Parent is sealed but consumer hasn't consumed past its end.
            if p.state == RangeState::Sealed {
                let parent_resolved = saved_offsets
                    .get(&p.range_id)
                    .map(|pos| pos.entry_id)
                    .unwrap_or_default();

                if parent_resolved < p.end_entry_id() {
                    return true;
                }
            }

            // Walk further up the lineage tree.
            if self.has_undrained_parent(p.range_id, ranges, saved_offsets) {
                return true;
            }
        }

        false
    }
}

pub(crate) async fn run_topic_fetch_manager(
    mut state: TopicFetchManagerState,
    drain_event_rx: flume::Receiver<RangeDrained>,
    command_rx: flume::Receiver<TopicFetchManagerCommand>,
    weak_ctx: std::sync::Weak<ConsumerContext>,
) {
    if let Some(ctx) = weak_ctx.upgrade() {
        state.provision_initial_tasks(&ctx);
    }

    let mut rebalance_interval = tokio::time::interval(Duration::from_secs(1));
    let mut commit_interval =
        tokio::time::interval(Duration::from_millis(state.auto_commit_interval_ms()));

    // Waiting exactly 2 tickets(~2secs) before the first rebalance.
    let mut startup_grace_ticks = if state.consumer_group.is_some() { 2 } else { 0 };

    loop {
        if state.record_tx.is_disconnected() {
            tracing::info!("Consumer Rx Dropped!");
            break;
        }

        let Some(ctx) = weak_ctx.upgrade() else {
            break;
        };

        tokio::select! {
            // CursorDrained Event arrived! meaning that fetch actor figured that the range is sealed
            res = drain_event_rx.recv_async() => {
                let Ok(event) = res else { break };
                state.handle_cursor_drained(event, &ctx);

                if state.should_exit() {
                     break;
                }
            }

            res = command_rx.recv_async() => {
                let Ok(command) = res else { break };
                state.handle_command(command);
            }

            _ = commit_interval.tick(), if state.should_auto_commit() => {
                let group = state.consumer_group.as_ref().unwrap().clone();
                tokio::spawn(async move {
                    let _ = group.commit().await;
                });
            }

            _ = rebalance_interval.tick(), if state.consumer_group.is_some() => {
                if startup_grace_ticks > 0 {
                    startup_grace_ticks -= 1;
                } else {
                    if state.pending_cursors.is_empty() {
                        let _ = ctx.refresh_metadata().await;
                    }
                    state.handle_rebalance(&ctx).await;
                }
            }
        }

        #[cfg(any(test, debug_assertions))]
        state.assert_invariants();
    }

    state.abort_all().await;
}

#[cfg(any(test, debug_assertions))]
impl TAssertInvariant for TopicFetchManagerState {
    fn assert_invariants(&self) {
        // Invariant 1: active cursors are owned by actors; inactive/pending
        // cursors are owned by the manager. A range must not be in both sets.
        for range_id in self.senders.keys() {
            assert!(
                !self.pending_cursors.contains(*range_id),
                "Invariant violated: Range {:?} has both an active fetch actor and a pending cursor",
                range_id
            );
        }

        // Invariant 2: a grouped consumer only fetches ranges it currently owns.
        if let Some(group) = &self.consumer_group {
            let owned_ranges = group.owned_ranges.load();
            for range_id in self.senders.keys() {
                assert!(
                    owned_ranges.contains(range_id),
                    "Invariant violated: grouped consumer has an active fetch actor for unowned range {:?}",
                    range_id
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::consumer::cursor::PendingCursorStore;
    use crate::connections::protocol::SegmentDetail;
    use crate::control_plane::metadata::SegmentId;
    use std::collections::HashMap;

    fn manager_state() -> TopicFetchManagerState {
        let (record_tx, _record_rx) = flume::unbounded();
        TopicFetchManagerState::new(
            PendingCursorStore::new(Vec::new()),
            None,
            ConsumerConfig::new(StartPolicy::Earliest),
            record_tx,
        )
    }

    fn split_ranges() -> Box<[RangeDetail]> {
        Box::new([
            RangeDetail {
                range_id: RangeId(0),
                keyspace_start: Vec::new(),
                keyspace_end: vec![255],
                state: RangeState::Sealed,
                active_segment: None,
                sealed_segments: Box::new([SegmentDetail {
                    segment_id: SegmentId(0),
                    start_entry_id: EntryId(0),
                    end_entry_id: Some(EntryId(2)),
                    replica_set: Vec::new(),
                }]),
                split_into: Some((RangeId(1), RangeId(2))),
                merged_into: None,
                merged_from: None,
            },
            RangeDetail {
                range_id: RangeId(1),
                keyspace_start: Vec::new(),
                keyspace_end: vec![128],
                state: RangeState::Active,
                active_segment: None,
                sealed_segments: Box::new([]),
                split_into: None,
                merged_into: None,
                merged_from: None,
            },
            RangeDetail {
                range_id: RangeId(2),
                keyspace_start: vec![128],
                keyspace_end: vec![255],
                state: RangeState::Active,
                active_segment: None,
                sealed_segments: Box::new([]),
                split_into: None,
                merged_into: None,
                merged_from: None,
            },
        ])
    }

    #[test]
    fn saved_offset_at_parent_end_unblocks_split_child() {
        let state = manager_state();
        let ranges = split_ranges();
        let mut offsets = HashMap::new();
        offsets.insert(
            RangeId(0),
            ConsumerPosition {
                entry_id: EntryId(2),
                batch_offset: 0,
                absolute_offset: 2,
            },
        );

        assert!(!state.has_undrained_parent(RangeId(1), &ranges, &offsets));
    }

    #[test]
    fn saved_offset_before_parent_end_blocks_split_child() {
        let state = manager_state();
        let ranges = split_ranges();
        let mut offsets = HashMap::new();
        offsets.insert(
            RangeId(0),
            ConsumerPosition {
                entry_id: EntryId(1),
                batch_offset: 0,
                absolute_offset: 1,
            },
        );

        assert!(state.has_undrained_parent(RangeId(1), &ranges, &offsets));
    }
}
