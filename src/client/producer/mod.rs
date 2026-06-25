mod buffers;
mod config;

pub use config::{BufferConfig, ProducerConfig};

use std::sync::Arc;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::client::error::ClientError;
use crate::client::{Client, ClientRecord, CompressionCodec};
use crate::control_plane::metadata::RangeId;
use buffers::{PendingRecord, ProducerBuffers, PushResult};

/// A thread-safe, internally synchronized producer for a single topic.
/// Cheap to clone and share across tasks.
#[derive(Clone)]
pub struct Producer {
    inner: Arc<Inner>,
}

struct Inner {
    client: Arc<Client>,
    topic: String,
    buffers: ProducerBuffers,
    codec: CompressionCodec,
    // Idempotency seam: globally unique session ID and monotonic sequence counter
    producer_id: Uuid,
    next_sequence: std::sync::atomic::AtomicU32,
}

impl Producer {
    /// Create a new producer for the specified topic.
    pub fn new(client: Arc<Client>, topic: String, config: ProducerConfig) -> Self {
        // Generate a globally unique UUID for the producer session ID (idempotency seam)
        let producer_id = Uuid::new_v4();

        Self {
            inner: Arc::new(Inner {
                client,
                topic,
                buffers: ProducerBuffers::new(config.buffer.clone()),
                codec: config.codec,
                producer_id,
                next_sequence: std::sync::atomic::AtomicU32::new(0),
            }),
        }
    }

    /// Produce a single record. Returns the committed entry ID once the batch flushes.
    pub async fn send(&self, key: &[u8], value: Vec<u8>) -> Result<u64, ClientError> {
        let topic = &self.inner.topic;
        let client = &self.inner.client;

        // Ensure the topic's routing metadata is cached
        let routing = match client.cache.get(topic) {
            Some(r) => r,
            None => {
                client.resolve_topic(topic).await?;
                client
                    .cache
                    .get(topic)
                    .ok_or(ClientError::UnexpectedResponse)?
            }
        };

        // Locate the target range ID for the key
        let range_id = routing
            .range_id(key)
            .map(RangeId)
            .ok_or(ClientError::TopicNotFound)?;

        // Idempotency seam: allocate sequence number
        let sequence_number = self
            .inner
            .next_sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let (tx, rx) = oneshot::channel();

        let pending = PendingRecord {
            record: ClientRecord::new(key, value),
            producer_id: self.inner.producer_id,
            sequence_number,
            tx,
        };

        let push_res = self.inner.buffers.push(range_id, pending);

        match push_res {
            PushResult::Flush(records_to_flush) => {
                self.flush_records(records_to_flush).await;
            }
            PushResult::SpawnLinger(linger, seq) => {
                let producer = self.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(linger).await;
                    producer.flush_range(range_id, seq).await;
                });
            }
            PushResult::Buffered => {}
        }

        rx.await.map_err(|_| ClientError::UnexpectedResponse)?
    }

    /// Flush the buffered records for a specific range if they exist.
    async fn flush_range(&self, range_id: RangeId, task_batch_seq: u64) {
        if let Some(records_to_flush) = self.inner.buffers.take(range_id, task_batch_seq) {
            self.flush_records(records_to_flush).await;
        }
    }

    /// Serialize, compress, and publish a batch of records.
    async fn flush_records(&self, pending_records: Vec<PendingRecord>) {
        if pending_records.is_empty() {
            return;
        }

        let mut records = Vec::with_capacity(pending_records.len());
        let mut txs = Vec::with_capacity(pending_records.len());
        for pending in pending_records {
            records.push(pending.record);
            txs.push(pending.tx);
        }
        let record_count = records.len() as u32;

        // Use the first record's key as the routing key since all belong to the same range
        let routing_key = &records[0].key;

        // Encode and compress the payload
        let encode_res = self.inner.codec.encode_payload(&records);
        let res = match encode_res {
            Ok(data) => {
                self.inner
                    .client
                    .produce(&self.inner.topic, routing_key, data, record_count)
                    .await
            }
            Err(_) => Err(ClientError::UnexpectedResponse),
        };

        // Deliver the result to all awaiting senders in this batch
        for tx in txs {
            let _ = tx.send(res.clone());
        }
    }
}
