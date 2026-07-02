use crate::client::{
    Client, ClientError, Consumer, ConsumerRecord, KeyInterest, Producer, ProducerConfig,
    StartPolicy,
};
use crate::control_plane::metadata::RangeId;
use borsh::{BorshDeserialize, BorshSerialize};
use dashmap::DashMap;
use std::collections::BTreeMap;
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
    pub range_id: u64,
    pub offset: u64,
}

pub struct ConsumerGroup {
    pub group_id: String,
    pub consumer_id: Uuid,
    pub client: Arc<Client>,
    pub active_peers: Arc<DashMap<Uuid, u64>>,
    // FIX: Hold the handles so we can kill them on drop!
    hb_sender_task: tokio::task::JoinHandle<()>,
    hb_receiver_task: tokio::task::JoinHandle<()>,
}

impl ConsumerGroup {
    pub fn new(client: Arc<Client>, group_id: String) -> Result<Self, ClientError> {
        let consumer_id = Uuid::new_v4();
        let active_peers = Arc::new(DashMap::new());

        let hb_sender_task = tokio::spawn(group_hb_sender(
            consumer_id,
            client.clone(),
            group_id.clone(),
        ));

        let hb_receiver_task = tokio::spawn(hb_receiver(
            client.clone(),
            active_peers.clone(),
            group_id.clone(),
        ));

        Ok(Self {
            group_id,
            consumer_id,
            client,
            active_peers,
            hb_sender_task,
            hb_receiver_task,
        })
    }

    pub fn assigned_ranges(&self, available_ranges: &[RangeId]) -> Vec<RangeId> {
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

        let mut my_ranges = Vec::new();

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
                my_ranges.push(range);
            }
        }

        my_ranges
    }

    pub fn fetch_saved_offset<'a>(
        &'a self,
        topic: &'a str,
        range_id: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<u64>> + Send + 'a>> {
        Box::pin(async move {
            let offsets_consumer = Consumer::new(
                self.client.clone(),
                SYSTEM_TOPIC_OFFSETS.to_string(),
                KeyInterest::AllKeys,
                StartPolicy::Earliest,
            )
            .await
            .ok()?;

            let routing_key = format!("{}:{}", self.group_id, topic);
            let mut latest_offset = None;

            while let Ok(Ok(Some(record))) =
                tokio::time::timeout(Duration::from_millis(2000), offsets_consumer.next_record())
                    .await
            {
                if record.key == routing_key.as_bytes()
                    && let Ok(payload) = OffsetCommitPayload::try_from_slice(&record.value)
                    && payload.range_id == range_id
                {
                    latest_offset = Some(payload.offset);
                }
            }
            latest_offset
        })
    }
}

async fn group_hb_sender(consumer_id: Uuid, hb_client: Arc<Client>, hb_group: String) {
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
            let _ = producer.send(hb_group.as_bytes(), data).await;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn hb_receiver(rx_client: Arc<Client>, rx_peers: Arc<DashMap<Uuid, u64>>, rx_group: String) {
    let consumer_res = Consumer::new(
        rx_client,
        SYSTEM_TOPIC_ASSIGNMENTS.to_string(),
        KeyInterest::AllKeys,
        StartPolicy::Latest,
    )
    .await;

    if let Ok(consumer) = consumer_res {
        while let Ok(Some(record)) = consumer.next_record().await {
            if record.key == rx_group.as_bytes()
                && let Ok(payload) = HeartbeatPayload::try_from_slice(&record.value)
                && let Ok(cid) = Uuid::from_slice(&payload.consumer_id)
            {
                // Store the highest sequence number seen
                if let Some(mut current) = rx_peers.get_mut(&cid) {
                    if payload.sequence_number > *current {
                        *current = payload.sequence_number;
                    }
                } else {
                    rx_peers.insert(cid, payload.sequence_number);
                }
            }
        }
    }
}

impl Drop for ConsumerGroup {
    fn drop(&mut self) {
        self.hb_sender_task.abort();
        self.hb_receiver_task.abort();
    }
}
