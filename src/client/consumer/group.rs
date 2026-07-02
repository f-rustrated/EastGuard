use crate::client::{
    Client, ClientError, Consumer, ConsumerRecord, KeyInterest, PartitionStrategy, Producer,
    ProducerConfig, StartPolicy, StoragePolicy,
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

pub struct ConsumerGroup {
    pub group_id: String,
    pub topic: String,
    pub consumer_id: Uuid,
    pub client: Arc<Client>,
    pub offset_producer: Producer,
    pub active_peers: DashMap<Uuid, u64>,
    pub owned_ranges: ArcSwap<Option<HashSet<RangeId>>>,
    pub offsets: DashMap<RangeId, u64>,

    // Hold the handles so we can kill them on drop
    hb_sender_handle: tokio::task::AbortHandle,
    hb_receiver_handle: tokio::task::AbortHandle,
}

impl ConsumerGroup {
    pub fn new(client: Arc<Client>, group_id: String, topic: String) -> Result<Self, ClientError> {
        let consumer_id = Uuid::new_v4();
        let active_peers = DashMap::new();

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

        let owned_ranges = ArcSwap::from_pointee(None);
        let offsets = DashMap::new();

        Ok(Self {
            group_id,
            topic,
            consumer_id,
            client,
            offset_producer,
            active_peers,
            owned_ranges,
            offsets,
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

    pub(crate) fn is_responsible_for(&self, range_id: RangeId) -> bool {
        let guard = self.owned_ranges.load();
        if let Some(owned) = &**guard {
            owned.contains(&range_id)
        } else {
            true
        }
    }

    pub(crate) fn record_offset(&self, range_id: RangeId, offset: u64) {
        self.offsets.insert(range_id, offset);
    }

    pub async fn commit(&self) -> Result<(), ClientError> {
        for entry in self.offsets.iter() {
            if let Some(owned) = &**self.owned_ranges.load()
                && !owned.contains(entry.key())
            {
                continue;
            }

            let payload = OffsetCommitPayload {
                range_id: *entry.key(),
                offset: *entry.value(),
            };
            if let Ok(data) = borsh::to_vec(&payload) {
                let _ = self
                    .offset_producer
                    .send(self.routing_key().as_bytes(), data)
                    .await;
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

    pub fn rebalance(&self, active_ranges: &[RangeId]) -> (Vec<RangeId>, Vec<RangeId>) {
        let latest_assignments = self.assigned_ranges(active_ranges);
        let current_owned = self.owned_ranges.load();
        let current_set = current_owned.as_ref().as_ref().cloned().unwrap_or_default();
        let latest_set: HashSet<RangeId> = latest_assignments.into_iter().collect();
        let to_drop: Vec<RangeId> = current_set
            .iter()
            .filter(|r| !latest_set.contains(r))
            .copied()
            .collect();
        let to_start: Vec<RangeId> = latest_set
            .iter()
            .filter(|r| !current_set.contains(r))
            .copied()
            .collect();

        if !to_drop.is_empty() || !to_start.is_empty() {
            self.owned_ranges.store(Arc::new(Some(latest_set)));
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
    active_peers: DashMap<Uuid, u64>,
    heartbeat_key: String,
) {
    let consumer_res = Consumer::new(
        client,
        SYSTEM_TOPIC_ASSIGNMENTS.to_string(),
        KeyInterest::AllKeys,
        StartPolicy::Latest,
        None,
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
