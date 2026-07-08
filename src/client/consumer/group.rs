use crate::client::consumer::cursor::{KeyInterest, StartPolicy};
use crate::client::{
    Client, ClientError, Consumer, ConsumerConfig, ConsumerRecord, PartitionStrategy, Producer,
    ProducerConfig, StoragePolicy,
};
use crate::control_plane::metadata::{EntryId, RangeId};
use arc_swap::ArcSwap;
use borsh::{BorshDeserialize, BorshSerialize};
use dashmap::DashMap;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub const SYSTEM_TOPIC_OFFSETS: &str = "__eastguard_offsets";
pub const SYSTEM_TOPIC_ASSIGNMENTS: &str = "__eastguard_assignments";
const VIRTUAL_NODES: u32 = 20;

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct HeartbeatPayload {
    pub consumer_id: [u8; 16],
    pub sequence_number: u64, // Repurposed from timestamp to be a deterministic counter
}
impl HeartbeatPayload {
    fn stop_signal(consumer_id: &[u8; 16]) -> Self {
        HeartbeatPayload {
            consumer_id: *consumer_id,
            sequence_number: 0, // indicates leaving
        }
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerPosition {
    // TODO absolute_offset: u64
    pub entry_id: EntryId,
    pub batch_offset: u64,
}

impl Default for ConsumerPosition {
    fn default() -> Self {
        Self {
            batch_offset: 0,
            entry_id: EntryId(0),
        }
    }
}

impl Ord for ConsumerPosition {
    fn cmp(&self, other: &Self) -> Ordering {
        // Idiomatic Rust: Pack fields into a tuple in the exact
        // priority order you want them evaluated.
        (self.entry_id, self.batch_offset).cmp(&(other.entry_id, other.batch_offset))
    }
}

impl PartialOrd for ConsumerPosition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct OffsetCommitPayload {
    pub range_id: RangeId,
    pub position: ConsumerPosition,
}

#[derive(Debug, Clone, Copy)]
pub struct OffsetTracker {
    pub uncommitted: ConsumerPosition,
    pub committed: Option<ConsumerPosition>,
}

impl OffsetTracker {
    pub fn new(position: ConsumerPosition) -> Self {
        Self {
            uncommitted: position,
            committed: None,
        }
    }

    pub fn needs_commit(&self) -> bool {
        self.committed
            .is_none_or(|committed| self.uncommitted > committed)
    }
}

pub struct ConsumerGroup {
    pub group_id: String,
    pub topic: String,
    pub consumer_id: Uuid,
    pub client: Arc<Client>,
    pub offset_producer: Producer,
    pub active_peers: Arc<DashMap<Uuid, u64>>,
    /// Active ranges owned by this group member. Updated immediately during rebalance
    /// to discard records fetched from revoked ranges while their final offsets are
    /// being committed and before their fetch tasks are aborted.
    pub owned_ranges: ArcSwap<HashSet<RangeId>>,
    pub offsets: DashMap<RangeId, OffsetTracker>,

    /// The last observed heartbeat sequence number for each active group peer.
    last_seen_sequences: DashMap<Uuid, u64>,
    /// Consecutive rebalance ticks where a peer's heartbeat sequence number has not advanced.
    /// Peers are declared dead and pruned when this count reaches 3.
    stale_ticks: DashMap<Uuid, u8>,

    _hb_stop_tx: flume::Sender<()>,
}

impl ConsumerGroup {
    pub fn new(client: Arc<Client>, group_id: String, topic: String) -> Result<Self, ClientError> {
        let consumer_id = Uuid::new_v4();
        let active_peers = Arc::new(DashMap::new());

        let (hb_stop_tx, hb_stop_rx) = flume::bounded(0);

        let routing_key = format!("hb:{}", group_id.clone());
        tokio::spawn(group_heartbeat_sender(
            consumer_id,
            client.clone(),
            routing_key.clone(),
            hb_stop_rx.clone(),
        ));

        let offset_producer = Producer::new(
            client.clone(),
            SYSTEM_TOPIC_OFFSETS.to_string(),
            ProducerConfig::default(),
        );

        tokio::spawn(heartbeat_receiver(
            client.clone(),
            active_peers.clone(),
            routing_key,
            hb_stop_rx,
        ));

        Ok(Self {
            group_id,
            topic,
            consumer_id,
            client,
            offset_producer,
            active_peers,
            owned_ranges: ArcSwap::from_pointee(HashSet::new()),
            offsets: DashMap::new(),
            _hb_stop_tx: hb_stop_tx,
            last_seen_sequences: DashMap::new(),
            stale_ticks: DashMap::new(),
        })
    }

    pub fn routing_key(&self) -> String {
        format!("{}:{}", self.group_id, self.topic)
    }

    pub async fn bootstrap(&self) -> Result<(), ClientError> {
        let policy = StoragePolicy {
            retention_ms: None,
            replication_factor: 1, // Single replica for system topics locally for now, or match cluster default
            partition_strategy: PartitionStrategy::AutoSplit,
        };

        if self
            .client
            .resolve_topic(SYSTEM_TOPIC_ASSIGNMENTS)
            .await
            .is_err()
        {
            let _ = self
                .client
                .create_topic(SYSTEM_TOPIC_ASSIGNMENTS, policy.clone())
                .await;
        }
        if self
            .client
            .resolve_topic(SYSTEM_TOPIC_OFFSETS)
            .await
            .is_err()
        {
            let _ = self.client.create_topic(SYSTEM_TOPIC_OFFSETS, policy).await;
        }

        Ok(())
    }

    /// Returns true if this member owns the range. Filters out stale records in transit
    /// during the revocation commit transition window of a rebalance.
    pub(crate) fn is_responsible_for(&self, range_id: RangeId) -> bool {
        self.owned_ranges.load().contains(&range_id)
    }

    pub(crate) fn record_offset(&self, range_id: RangeId, processed: ConsumerPosition) {
        self.offsets
            .entry(range_id)
            .and_modify(|tracker| {
                if processed > tracker.uncommitted {
                    tracker.uncommitted = processed;
                }
            })
            .or_insert_with(|| OffsetTracker::new(processed));
    }

    pub async fn commit(&self) -> Result<(), ClientError> {
        // Collect commits synchronously to release DashMap locks before awaiting network calls.
        let uncommitted: Vec<(RangeId, ConsumerPosition)> = self
            .offsets
            .iter()
            .filter(|entry| entry.needs_commit())
            .map(|entry| (*entry.key(), entry.value().uncommitted))
            .collect();

        tracing::info!(?uncommitted);
        if uncommitted.is_empty() {
            return Ok(());
        }

        self.write_offset_commits(uncommitted).await
    }

    fn remove_dead_peers(&self) {
        let mut dead_peers = Vec::new();

        // Identify stale peers directly from the iterator
        for entry in self.active_peers.iter() {
            let peer_id = *entry.key();
            let current_seq = *entry.value();

            let mut last_seq = self
                .last_seen_sequences
                .entry(peer_id)
                .or_insert(current_seq);
            let mut ticks = self.stale_ticks.entry(peer_id).or_insert(0);

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

        // Remove newly dead peers from the active set
        for dead_id in dead_peers {
            self.active_peers.remove(&dead_id);
        }

        // Garbage collect tracking maps:
        // This single pass cleans up the peers we just removed, as well as
        // any peers that disconnected or were removed externally since the last tick.
        self.last_seen_sequences
            .retain(|k, _| self.active_peers.contains_key(k));
        self.stale_ticks
            .retain(|k, _| self.active_peers.contains_key(k));
    }

    pub async fn revoke_ranges(&self, ranges: &[RangeId]) -> Result<(), ClientError> {
        // Collect commits synchronously to release DashMap locks before awaiting network calls.
        let mut commits = Vec::new();
        for &range in ranges {
            if let Some(entry) = self.offsets.get(&range)
                && entry.needs_commit()
            {
                commits.push((range, entry.uncommitted));
            }
        }

        if !commits.is_empty() {
            self.write_offset_commits(commits).await?;
        }

        // Remove revoked ranges from local offsets map
        for &range in ranges {
            self.offsets.remove(&range);
        }
        Ok(())
    }

    async fn write_offset_commits(
        &self,
        commits: Vec<(RangeId, ConsumerPosition)>,
    ) -> Result<(), ClientError> {
        let futures = commits.into_iter().map(|(range_id, position)| async move {
            let data = match borsh::to_vec(&OffsetCommitPayload { range_id, position }) {
                Ok(d) => d,
                Err(e) => {
                    tracing::error!(?range_id, error = ?e, "Failed to serialize offset payload");
                    return; // Skip this range, retry on next tick
                }
            };

            //  Send over network
            if let Err(e) = self
                .offset_producer
                .send(self.routing_key().as_bytes(), data)
                .await
            {
                tracing::warn!(?range_id, error = ?e, "Failed to send offset commit to broker");
                return; // Skip this range, retry on next tick
            }

            // Update DashMap safely
            if let Some(mut entry) = self.offsets.get_mut(&range_id) {
                // Guard against out-of-order network responses dragging the pointer backward
                let is_newer = entry.committed.is_none_or(|committed| position > committed);
                if is_newer {
                    entry.committed = Some(position);
                }
            }
        });

        futures::future::join_all(futures).await;
        Ok(())
    }

    pub(crate) fn assigned_ranges(&self, available_ranges: &[RangeId]) -> Vec<RangeId> {
        let mut peers: Vec<Uuid> = Vec::new();
        peers.push(self.consumer_id); // Always include self

        // Because we don't have a clock in this function, we rely on the manager's background
        // tick to eventually clear out stale peers if they haven't advanced their sequence number.
        // For the Hash Ring calculation itself, we just use whoever is currently in the DashMap.
        for peer in self.active_peers.iter() {
            if *peer.key() != self.consumer_id {
                peers.push(*peer.key());
            }
        }

        let mut ring: BTreeMap<u32, Uuid> = BTreeMap::new();
        for peer in peers {
            for vnode in 0..VIRTUAL_NODES {
                let key = format!("{}-{}", peer, vnode);
                let hash =
                    murmur3::murmur3_32(&mut std::io::Cursor::new(key.as_bytes()), 0).unwrap_or(0);
                ring.insert(hash, peer);
            }
        }

        let mut ranges = Vec::new();

        for &range in available_ranges {
            let range_bytes = range.0.to_le_bytes();
            let range_hash =
                murmur3::murmur3_32(&mut std::io::Cursor::new(&range_bytes), 0).unwrap_or(0);

            let assigned_peer = ring
                .range(range_hash..)
                .next()
                .unwrap_or_else(|| ring.iter().next().unwrap())
                .1;

            if assigned_peer == &self.consumer_id {
                ranges.push(range);
            }
        }

        ranges
    }

    pub fn rebalance(
        &self,
        ranges: &[RangeId],
        active_ranges: HashSet<RangeId>,
    ) -> (Vec<RangeId>, Vec<RangeId>) {
        self.remove_dead_peers();

        let current_set = self.owned_ranges.load();
        let latest_set: HashSet<RangeId> = self.assigned_ranges(ranges).into_iter().collect();
        let to_drop: Vec<RangeId> = current_set
            .iter()
            .filter(|r| !latest_set.contains(r))
            .copied()
            .collect();

        // Start tasks for any range in the target assignments that does not currently have an active task.
        // This covers both:
        //   1. Newly assigned ranges (which won't be in active_ranges)
        //   2. Previously owned ranges whose fetch tasks crashed or stopped running (reconciliation)
        let to_start: Vec<RangeId> = latest_set
            .iter()
            .filter(|r| !active_ranges.contains(r))
            .copied()
            .collect();

        if **current_set != latest_set {
            self.owned_ranges.store(Arc::new(latest_set));
        }

        (to_drop, to_start)
    }
}

async fn group_heartbeat_sender(
    consumer_id: Uuid,
    hb_client: Arc<Client>,
    routing_key: String,
    stop_rx: flume::Receiver<()>,
) {
    let producer = Producer::new(
        hb_client,
        SYSTEM_TOPIC_ASSIGNMENTS.to_string(),
        ProducerConfig::default(),
    );

    let mut seq = 0;
    loop {
        seq += 1;
        let payload = HeartbeatPayload {
            consumer_id: *consumer_id.as_bytes(),
            sequence_number: seq,
        };

        if let Ok(data) = borsh::to_vec(&payload) {
            let _ = producer.send(routing_key.as_bytes(), data).await;
        }

        tokio::select! {
            _ = stop_rx.recv_async() => { // Matches any Result (Ok or Err!)
                // Gracefully publish a leave tombstone signal before exiting
                if let Ok(data) = borsh::to_vec(&HeartbeatPayload::stop_signal(consumer_id.as_bytes())) {
                    let _ = producer.send(routing_key.as_bytes(), data).await;
                }
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

async fn heartbeat_receiver(
    client: Arc<Client>,
    active_peers: Arc<DashMap<Uuid, u64>>,
    heartbeat_key: String,
    stop_rx: flume::Receiver<()>,
) {
    let mut end_key = heartbeat_key.as_bytes().to_vec();
    end_key.push(0u8);

    let consumer_res = Consumer::new(
        client,
        SYSTEM_TOPIC_ASSIGNMENTS.to_string(),
        KeyInterest::KeySpan {
            start: heartbeat_key.as_bytes().to_vec(),
            end: end_key,
        },
        ConsumerConfig::new(StartPolicy::Latest),
    )
    .await;

    let Ok(consumer) = consumer_res else {
        return;
    };

    loop {
        tokio::select! {
            _ = stop_rx.recv_async() => {
                break;
            }
            res = consumer.next_record() => {

                let Ok(Some(record)) = res else {
                    break;
                };
                if record.key_match(heartbeat_key.as_bytes().iter())
                    && let Ok(payload) = HeartbeatPayload::try_from_slice(&record.value)
                    && let Ok(cid) = Uuid::from_slice(&payload.consumer_id)
                {
                    if payload.sequence_number == 0 {
                        // Peer is leaving gracefully; remove immediately
                        active_peers.remove(&cid);
                    } else {
                        // Store the highest sequence number seen
                        if let Some(mut current) = active_peers.get_mut(&cid) {
                            if payload.sequence_number > *current {
                                *current = payload.sequence_number;
                            }
                        } else {
                            active_peers.insert(cid, payload.sequence_number);
                        }
                    }
                }
            }
        }
    }
}
