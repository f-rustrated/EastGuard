use crate::client::{
    Client, ClientError, Consumer, ConsumerConfig, ConsumerRecord, KeyInterest, PartitionStrategy,
    Producer, ProducerConfig, StartPolicy, StoragePolicy,
};
use crate::control_plane::metadata::RangeId;
use arc_swap::ArcSwap;
use borsh::{BorshDeserialize, BorshSerialize};
use dashmap::DashMap;
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

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct OffsetCommitPayload {
    pub range_id: RangeId,
    pub offset: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct OffsetTracker {
    pub processed: u64,
    pub committed: Option<u64>,
}

impl OffsetTracker {
    pub fn new(processed: u64) -> Self {
        Self {
            processed,
            committed: None,
        }
    }

    pub fn needs_commit(&self) -> bool {
        match self.committed {
            Some(committed) => self.processed > committed,
            None => true,
        }
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

    // Hold the handles so we can kill them on drop
    hb_sender_handle: tokio::task::AbortHandle,
    hb_receiver_handle: tokio::task::AbortHandle,
}

impl ConsumerGroup {
    pub fn new(client: Arc<Client>, group_id: String, topic: String) -> Result<Self, ClientError> {
        let consumer_id = Uuid::new_v4();
        let active_peers = Arc::new(DashMap::new());

        let hb_sender_handle = tokio::spawn(group_heartbeat_sender(
            consumer_id,
            client.clone(),
            format!("hb:{}", group_id.clone()),
        ))
        .abort_handle();
        let offset_producer = Producer::new(
            client.clone(),
            SYSTEM_TOPIC_OFFSETS.to_string(),
            ProducerConfig::default(),
        );

        let hb_receiver_handle = tokio::spawn(heartbeat_receiver(
            client.clone(),
            active_peers.clone(),
            format!("hb:{}", group_id),
        ))
        .abort_handle();

        Ok(Self {
            group_id,
            topic,
            consumer_id,
            client,
            offset_producer,
            active_peers,
            owned_ranges: ArcSwap::from_pointee(HashSet::new()),
            offsets: DashMap::new(),
            hb_sender_handle,
            hb_receiver_handle,
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

        // Attempt to create system topics. Ignore errors if they already exist.
        let _ = self
            .client
            .create_topic(SYSTEM_TOPIC_ASSIGNMENTS, policy.clone())
            .await;
        let _ = self.client.create_topic(SYSTEM_TOPIC_OFFSETS, policy).await;

        Ok(())
    }

    /// Returns true if this member owns the range. Filters out stale records in transit
    /// during the revocation commit transition window of a rebalance.
    pub(crate) fn is_responsible_for(&self, range_id: RangeId) -> bool {
        self.owned_ranges.load().contains(&range_id)
    }

    pub(crate) fn record_offset(&self, range_id: RangeId, offset: u64) {
        self.offsets
            .entry(range_id)
            .and_modify(|tracker| tracker.processed = offset)
            .or_insert_with(|| OffsetTracker::new(offset));
    }

    pub async fn commit(&self) -> Result<(), ClientError> {
        // Collect commits synchronously to release DashMap locks before awaiting network calls.
        let commits: Vec<(RangeId, u64)> = self
            .offsets
            .iter()
            .filter(|entry| entry.needs_commit())
            .map(|entry| (*entry.key(), entry.value().processed))
            .collect();

        self.write_offset_commits(commits).await
    }

    pub async fn revoke_ranges(&self, ranges: &[RangeId]) -> Result<(), ClientError> {
        // Collect commits synchronously to release DashMap locks before awaiting network calls.
        let mut commits = Vec::new();
        for &range in ranges {
            if let Some(entry) = self.offsets.get(&range)
                && entry.needs_commit()
            {
                commits.push((range, entry.processed));
            }
        }

        self.write_offset_commits(commits).await?;

        // Remove revoked ranges from local offsets map
        for &range in ranges {
            self.offsets.remove(&range);
        }
        Ok(())
    }

    async fn write_offset_commits(&self, commits: Vec<(RangeId, u64)>) -> Result<(), ClientError> {
        for (range_id, offset) in commits {
            let payload = OffsetCommitPayload { range_id, offset };
            if let Ok(data) = borsh::to_vec(&payload)
                && self
                    .offset_producer
                    .send(self.routing_key().as_bytes(), data)
                    .await
                    .is_ok()
                && let Some(mut entry) = self.offsets.get_mut(&range_id)
            {
                entry.committed = Some(offset);
            }
        }
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

async fn group_heartbeat_sender(consumer_id: Uuid, hb_client: Arc<Client>, routing_key: String) {
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
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn heartbeat_receiver(
    client: Arc<Client>,
    active_peers: Arc<DashMap<Uuid, u64>>,
    heartbeat_key: String,
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

    while let Ok(Some(record)) = consumer.next_record().await {
        if record.key_match(heartbeat_key.as_bytes().iter())
            && let Ok(payload) = HeartbeatPayload::try_from_slice(&record.value)
            && let Ok(cid) = Uuid::from_slice(&payload.consumer_id)
        {
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

impl Drop for ConsumerGroup {
    fn drop(&mut self) {
        self.hb_sender_handle.abort();
        self.hb_receiver_handle.abort();
    }
}
