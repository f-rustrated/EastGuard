pub(crate) mod buffers;
mod config;
pub(crate) mod record;
use crate::client::error::ClientError;
use crate::client::{Client, CompressionCodec};
use crate::control_plane::metadata::{EntryId, RangeId};
use crate::impl_new_struct_wrapper;
use buffers::{PendingRecord, ProducerBuffers, PushResult};
pub use config::{BufferConfig, ProducerConfig};

use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::Instant;
use uuid::Uuid;

/// A thread-safe, internally synchronized producer for a single topic.
/// Cheap to clone and share across tasks.
#[derive(Clone)]
pub struct Producer(Arc<Inner>);

impl_new_struct_wrapper!(Producer, Arc<Inner>);

pub struct Inner {
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

        Self::new_with_producer_id(client, topic, config, producer_id)
    }
    pub fn new_with_producer_id(
        client: Arc<Client>,
        topic: String,
        config: ProducerConfig,
        producer_id: Uuid,
    ) -> Self {
        // Generate a globally unique UUID for the producer session ID (idempotency seam)

        Self(Arc::new(Inner {
            client,
            topic,
            buffers: ProducerBuffers::new(config.buffer.clone()),
            codec: config.codec,
            producer_id,
            next_sequence: std::sync::atomic::AtomicU32::new(0),
        }))
    }

    /// Produce a single record. Returns the committed entry ID once the batch flushes.
    pub async fn send(&self, key: &[u8], value: Vec<u8>) -> Result<EntryId, ClientError> {
        let topic = &self.topic;
        let client = &self.client;

        let routing = client.resolve_topic_if_missing(topic).await?;
        let range_id = routing.range_id(key).ok_or(ClientError::TopicNotFound)?;

        // Idempotency seam: allocate sequence number
        let sequence_number = self
            .next_sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let (tx, rx) = oneshot::channel();

        let pending = PendingRecord {
            key: key.to_vec(),
            value,
            producer_id: self.producer_id,
            sequence_number,
            tx,
        };

        let push_res = self.buffers.push(range_id, pending);

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
        if let Some(records_to_flush) = self.buffers.take(range_id, task_batch_seq) {
            self.flush_records(records_to_flush).await;
        }
    }

    /// Flush all currently buffered records across all partitions and wait for their completion.
    pub async fn flush(&self) {
        let batches = self.buffers.take_all();
        if batches.is_empty() {
            return;
        }
        let futures = batches
            .into_iter()
            .map(|(_, records)| self.flush_records(records));
        futures::future::join_all(futures).await;
    }

    /// Serialize, compress, and publish a batch of records.
    async fn flush_records(&self, mut records_to_publish: Vec<PendingRecord>) {
        let deadline = Instant::now() + self.client.retry.deadline;
        let mut backoff = self.client.retry.initial_backoff;
        let timeout_error = ClientError::Timeout {
            waited: self.client.retry.deadline,
            last_error: Some("topic routing remained stale".to_string()),
        };

        loop {
            if records_to_publish.is_empty() {
                return;
            }

            let resolve_remaining = deadline.saturating_duration_since(Instant::now());
            if resolve_remaining.is_zero() {
                for pending in records_to_publish {
                    let _ = pending.tx.send(Err(timeout_error.clone()));
                }
                return;
            }

            let resolve_result = tokio::time::timeout(
                resolve_remaining,
                self.client.resolve_topic_if_missing(&self.topic),
            )
            .await;
            let routing = match resolve_result {
                Ok(Ok(routing)) => routing,
                Ok(Err(error)) => {
                    for pending in records_to_publish {
                        let _ = pending.tx.send(Err(error.clone()));
                    }
                    return;
                }
                Err(_) => {
                    for pending in records_to_publish {
                        let _ = pending.tx.send(Err(timeout_error.clone()));
                    }
                    return;
                }
            };

            // Group in range-ID order while preserving record order within each range.
            let mut recs_per_range: BTreeMap<RangeId, Vec<PendingRecord>> = BTreeMap::new();

            for pending in records_to_publish {
                let Some(range_id) = routing.range_id(&pending.key) else {
                    let _ = pending.tx.send(Err(ClientError::UnexpectedResponse));
                    continue;
                };
                recs_per_range.entry(range_id).or_default().push(pending);
            }

            let mut next_retry_records = Vec::new();

            for (range_id, range_recs) in recs_per_range {
                let routing_key = &range_recs[0].key;

                // Encode and compress the payload
                let Ok(payload) = self.codec.encode_payload(&range_recs) else {
                    tracing::error!("Compression error while flushing records");
                    for pending in range_recs {
                        let _ = pending.tx.send(Err(ClientError::UnexpectedResponse));
                    }
                    continue;
                };

                let produce_result = match tokio::time::timeout(
                    deadline.saturating_duration_since(Instant::now()),
                    self.client.produce_to_range(
                        &self.topic,
                        range_id,
                        routing_key,
                        payload,
                        range_recs.len() as u32,
                    ),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(timeout_error.clone()),
                };

                match produce_result {
                    Ok(entry_id) => {
                        // Success! Deliver to all awaiting senders of this sub-batch
                        for pending in range_recs {
                            let _ = pending.tx.send(Ok(entry_id));
                        }
                    }
                    Err(ClientError::StaleRange) => {
                        // Invalidate routing cache and collect records to retry in the next loop iteration
                        self.client.cache.invalidate(&self.topic);
                        next_retry_records.extend(range_recs);
                    }
                    Err(err) => {
                        // Terminal error. Fail this sub-batch.
                        for pending in range_recs {
                            let _ = pending.tx.send(Err(err.clone()));
                        }
                    }
                }
            }

            if !next_retry_records.is_empty() {
                let backoff_remaining = deadline.saturating_duration_since(Instant::now());
                tokio::time::sleep(backoff.min(backoff_remaining)).await;
                backoff = (backoff * 2).min(self.client.retry.max_backoff);
            }
            records_to_publish = next_retry_records;
        }
    }
}
