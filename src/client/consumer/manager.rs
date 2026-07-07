use super::RangeCursor;

use crate::client::consumer::cursor::StartPolicy;
use crate::client::consumer::fetch::{FetchActor, FetchActorCommand};
use crate::client::consumer::group::ConsumerGroup;
use crate::client::consumer::{ConsumerContext, ConsumerRecord, RangeCursorSet};
use crate::client::{ClientError, ConsumerConfig};
use crate::connections::protocol::{RangeDetail, RangeTransition};
use crate::control_plane::metadata::{RangeId, RangeState};

#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub(crate) struct CursorDrained {
    pub range_id: RangeId,
    pub transition: RangeTransition,
}

struct CursorManagerState {
    cursors: RangeCursorSet,
    senders: HashMap<RangeId, flume::Sender<FetchActorCommand>>,

    /// The optional shared consumer group context for dynamic partition rebalancing.
    consumer_group: Option<Arc<ConsumerGroup>>,
    /// The policy (Earliest/Latest) used to start consumption on newly assigned ranges.
    start_policy: StartPolicy,
}

impl CursorManagerState {
    fn new(
        cursors: RangeCursorSet,
        consumer_group: Option<Arc<ConsumerGroup>>,
        start_policy: StartPolicy,
    ) -> Self {
        Self {
            cursors,
            senders: HashMap::new(),
            consumer_group,
            start_policy,
        }
    }

    fn should_exit(&self) -> bool {
        self.cursors.is_empty() && self.consumer_group.is_none()
    }

    fn provision_initial_tasks(
        &mut self,
        ctx: &Arc<ConsumerContext>,
        record_tx: &flume::Sender<Result<ConsumerRecord, ClientError>>,
    ) {
        // If a consumer_group is present, partition ownership is dynamic and determined entirely by the
        // group rebalancer. We return early to prevent the consumer from starting fetch tasks before
        // partitions are assigned to it.
        if self.consumer_group.is_some() {
            return;
        }
        for cursor in self.cursors.iter().cloned().collect::<Vec<_>>() {
            self.spawn_and_register(cursor, ctx.clone(), record_tx.clone());
        }
    }

    fn handle_cursor_drained(
        &mut self,
        event: CursorDrained,
        ctx: &Arc<ConsumerContext>,
        record_tx: &flume::Sender<Result<ConsumerRecord, ClientError>>,
    ) {
        // If not owned, it was already revoked — late drain event, safe to ignore.
        if self.senders.remove(&event.range_id).is_none() {
            return;
        }
        let added = self.cursors.apply_drained(event.range_id, event.transition);

        for new_cursor in added {
            self.spawn_and_register(new_cursor, ctx.clone(), record_tx.clone());
        }
    }

    async fn handle_rebalance(
        &mut self,
        ctx: &Arc<ConsumerContext>,
        record_tx: &flume::Sender<Result<ConsumerRecord, ClientError>>,
    ) {
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
                    let _ = stop_tx.send(FetchActorCommand::Stop);
                }
                self.cursors.remove(*range);
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
            let resolved_entry_id = self
                .resolve_start_entry_id(range, ctx, saved_offsets.get(&range).copied())
                .await;

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
                    ),
                    ctx.clone(),
                    record_tx.clone(),
                );
            }
        }
    }

    /// Spawn a fetch actor, register its stop channel, and update/add the cursor
    /// in the cursor set.
    fn spawn_and_register(
        &mut self,
        cursor: RangeCursor,
        ctx: Arc<ConsumerContext>,
        record_tx: flume::Sender<Result<ConsumerRecord, ClientError>>,
    ) {
        if self.senders.contains_key(&cursor.range_id) {
            return;
        }
        let (stop_tx, stop_rx) = flume::bounded(1);
        let actor = FetchActor::new(cursor.range_id, cursor.next_entry_id, ctx, record_tx);
        tokio::spawn(actor.run(stop_rx));
        self.senders.insert(cursor.range_id, stop_tx);
        self.cursors.add_or_update(cursor);
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
        committed_offset: Option<u64>,
    ) -> u64 {
        // 1. Prefer explicitly saved offsets. If a consumer committed offset `N`,
        // the next record to fetch starts at entry ID `N + 1`.
        if let Some(offset) = committed_offset {
            return offset + 1;
        }

        // 2. Fetch the latest boundary from the replica if the start policy is Latest.
        // Note: fetch_range_entry_ids returns (start_entry_id, committed_entry_id).
        if matches!(self.start_policy, StartPolicy::Latest)
            && let Ok((_, committed_entry_id)) =
                ctx.client.fetch_range_entry_ids(&ctx.topic, range).await
        {
            return committed_entry_id;
        }

        // 3. Fallback to the local cursor's next entry ID.
        if let Some(cursor) = self.cursors.get(range) {
            return cursor.next_entry_id;
        }

        // 4. Default start entry ID is 0
        0
    }

    async fn abort_all(&mut self) {
        for (_, stop_tx) in self.senders.drain() {
            let _ = stop_tx.send(FetchActorCommand::Stop);
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
        saved_offsets: &HashMap<RangeId, u64>,
        resolved_entry_id: u64,
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

    /// Returns true if any descendant (split child or merge target) of the
    /// given range is currently owned or has a live cursor.
    fn has_active_descendant(&self, range_id: RangeId, ranges: &[RangeDetail]) -> bool {
        let Some(r_meta) = ranges.iter().find(|r| r.range_id == range_id) else {
            return false;
        };

        if let Some(children) = r_meta.split_into {
            if self.senders.contains_key(&children.0) || self.senders.contains_key(&children.1) {
                return true;
            }
            if self.cursors.contains(children.0) || self.cursors.contains(children.1) {
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
            if self.cursors.contains(child) {
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
        saved_offsets: &HashMap<RangeId, u64>,
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
                let parent_resolved = saved_offsets.get(&p.range_id).map(|o| o + 1).unwrap_or(0);

                if parent_resolved <= p.end_entry_id() {
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

pub(crate) async fn run_cursor_manager(
    cursors: RangeCursorSet,
    rx: flume::Receiver<CursorDrained>,
    weak_ctx: std::sync::Weak<ConsumerContext>,
    record_tx: flume::Sender<Result<ConsumerRecord, ClientError>>,
    consumer_group: Option<Arc<ConsumerGroup>>,
    config: ConsumerConfig,
) {
    let mut state = CursorManagerState::new(cursors, consumer_group, config.start_policy);
    if state.should_exit() {
        return;
    }

    if let Some(ctx) = weak_ctx.upgrade() {
        state.provision_initial_tasks(&ctx, &record_tx);
    }

    let mut rebalance_interval = tokio::time::interval(Duration::from_secs(1));
    let mut commit_interval =
        tokio::time::interval(Duration::from_millis(config.auto_commit_interval_ms));

    // Waiting exactly 2 tickets(~2secs) before the first rebalance.
    let mut startup_grace_ticks = if state.consumer_group.is_some() { 2 } else { 0 };

    loop {
        if record_tx.is_disconnected() {
            break;
        }

        let Some(ctx) = weak_ctx.upgrade() else {
            break;
        };

        tokio::select! {
            res = rx.recv_async() => {
                let Ok(event) = res else { break };
                state.handle_cursor_drained(event, &ctx, &record_tx);

                if state.should_exit() {
                     break;
                }
            }

            _ = commit_interval.tick(), if state.consumer_group.is_some() => {
                let group = state.consumer_group.as_ref().unwrap().clone();
                tokio::spawn(async move {
                    let _ = group.commit().await;
                });
            }

            _ = rebalance_interval.tick(), if state.consumer_group.is_some() => {
                if startup_grace_ticks > 0 {
                    startup_grace_ticks -= 1;
                } else {
                    if state.cursors.is_empty() {
                        let _ = ctx.refresh_metadata().await;
                    }
                    state.handle_rebalance(&ctx, &record_tx).await;
                }
            }
        }

        #[cfg(any(test, debug_assertions))]
        state.assert_invariants();
    }

    state.abort_all().await;
}

#[cfg(any(test, debug_assertions))]
impl TAssertInvariant for CursorManagerState {
    fn assert_invariants(&self) {
        for range_id in self.senders.keys() {
            assert!(
                self.cursors.contains(*range_id),
                "Invariant violated: Range {:?} has an active fetch actor in senders but is missing from self.cursors",
                range_id
            );
        }
    }
}
