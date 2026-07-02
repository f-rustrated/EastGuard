use super::RangeCursorSet;
use crate::client::consumer::bootstrap::{KeyInterest, StartPolicy};
use crate::client::consumer::fetch::run_fetch_actor;
use crate::client::consumer::group::ConsumerGroup;
use crate::client::consumer::{ConsumerContext, ConsumerRecord};
use crate::client::{ClientError, ConsumerConfig};
use crate::connections::protocol::RangeTransition;
use crate::control_plane::metadata::RangeId;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Interval;
use uuid::Uuid;

pub(crate) struct CursorDrained {
    pub range_id: RangeId,
    pub transition: RangeTransition,
}

struct CursorManagerState {
    /// Active partition range cursors and their next entry IDs to fetch.
    cursors: RangeCursorSet,
    /// Active background fetch actors (running tasks) keyed by their partition RangeId.
    active_tasks: HashMap<RangeId, JoinHandle<()>>,
    /// The last observed heartbeat sequence number for each active group peer.
    last_seen_sequences: HashMap<Uuid, u64>,
    /// Consecutive rebalance ticks where a peer's heartbeat sequence number has not advanced.
    /// Peers are declared dead and pruned when this count reaches 3.
    stale_ticks: HashMap<Uuid, u8>,
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
        if let Some(ref group) = consumer_group {
            group.owned_ranges.store(Arc::new(Some(HashSet::new())));
        }
        Self {
            cursors,
            active_tasks: HashMap::new(),
            last_seen_sequences: HashMap::new(),
            stale_ticks: HashMap::new(),
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
        // If a consumer_group  is present, partition ownership is dynamic and determined entirely by the
        // group rebalancer. We return early to prevent the consumer from starting fetch tasks before
        // partitions are assigned to it.
        if self.consumer_group.is_some() {
            return;
        }
        for cursor in self.cursors.cursors().to_vec() {
            self.spawn_fetch_actor(
                cursor.range_id,
                cursor.next_entry_id,
                ctx.clone(),
                record_tx.clone(),
            );
        }
    }

    fn handle_cursor_drained(
        &mut self,
        event: CursorDrained,
        ctx: &Arc<ConsumerContext>,
        record_tx: &flume::Sender<Result<ConsumerRecord, ClientError>>,
    ) {
        self.active_tasks.remove(&event.range_id);
        let added = self.cursors.apply_drained(event.range_id, event.transition);

        for new_cursor in added.iter() {
            self.spawn_fetch_actor(
                new_cursor.range_id,
                new_cursor.next_entry_id,
                ctx.clone(),
                record_tx.clone(),
            );
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
        let mut dead_peers = Vec::new();

        // Cleanup dead peers
        let peers_copy: Vec<(Uuid, u64)> = group
            .active_peers
            .iter()
            .map(|kv| (*kv.key(), *kv.value()))
            .collect();

        for (peer_id, current_seq) in peers_copy {
            let last_seq = self
                .last_seen_sequences
                .entry(peer_id)
                .or_insert(current_seq);
            let ticks = self.stale_ticks.entry(peer_id).or_insert(0);

            if current_seq == *last_seq {
                *ticks += 1;
                if *ticks >= 3 {
                    dead_peers.push(peer_id);
                }
            } else {
                *last_seq = current_seq;
                *ticks = 0;
            }
        }

        for dead_id in dead_peers {
            group.active_peers.remove(&dead_id);
            self.last_seen_sequences.remove(&dead_id);
            self.stale_ticks.remove(&dead_id);
        }

        // Perform rebalance inside ConsumerGroup
        let (to_drop, to_start) = group.rebalance(
            &ctx.all_ranges(),
            self.active_tasks.keys().cloned().collect(),
        );

        // Commit offsets for revoked tasks BEFORE dropping them
        if !to_drop.is_empty() {
            let _ = group.commit_ranges(&to_drop).await;
        }

        for range in to_drop {
            if let Some(handle) = self.active_tasks.remove(&range) {
                handle.abort();
            }
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

        for range in to_start {
            let resolved_entry_id = self
                .resolve_start_entry_id(range, ctx, saved_offsets.get(&range).copied())
                .await;

            self.spawn_fetch_actor(range, resolved_entry_id, ctx.clone(), record_tx.clone());
        }
    }

    fn spawn_fetch_actor(
        &mut self,
        range_id: RangeId,
        entry_id: u64,
        ctx: Arc<ConsumerContext>,
        record_tx: flume::Sender<Result<ConsumerRecord, ClientError>>,
    ) {
        let handle = tokio::spawn(run_fetch_actor(
            range_id,
            entry_id,
            ctx.clone(),
            record_tx.clone(),
        ));
        self.active_tasks.insert(range_id, handle);
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
        if let Some(cursor) = self.cursors.cursors().iter().find(|c| c.range_id == range) {
            return cursor.next_entry_id;
        }

        // 4. Default start entry ID is 0
        0
    }

    fn abort_all(&mut self) {
        for (_, handle) in self.active_tasks.drain() {
            handle.abort();
        }
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
                    if state.active_tasks.is_empty() {
                        let _ = ctx.refresh_metadata().await;
                    }
                    state.handle_rebalance(&ctx, &record_tx).await;
                }
            }
        }
    }

    state.abort_all();
}
