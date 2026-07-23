pub(crate) mod buffers;
mod config;
pub(crate) mod record;
mod session;
use crate::client::error::ClientError;
use crate::client::routing::TopicRouting;
use crate::client::{Client, CompressionCodec};
use crate::control_plane::metadata::{EntryId, RangeId};
use crate::data_plane::ProduceError;
use crate::impl_new_struct_wrapper;
use buffers::{PendingRecord, ProducerBuffers, PushResult};
pub use config::{BufferConfig, ProducerConfig};
use session::{ClientProducerSession, ClientProducerSessionManager};

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
    session_manager: ClientProducerSessionManager,
    next_record_order: std::sync::atomic::AtomicU64,
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
        Self(Arc::new(Inner {
            client,
            topic,
            buffers: ProducerBuffers::new(config.buffer.clone()),
            codec: config.codec,
            session_manager: ClientProducerSessionManager::new(producer_id),
            next_record_order: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    /// Produce a single record. Returns the committed entry ID once the batch flushes.
    pub async fn send(&self, key: &[u8], value: Vec<u8>) -> Result<EntryId, ClientError> {
        let order = self
            .next_record_order
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let topic = &self.topic;
        let client = &self.client;

        let routing = client.resolve_topic_if_missing(topic).await?;
        let range_id = routing.range_id(key).ok_or(ClientError::TopicNotFound)?;

        let (tx, rx) = oneshot::channel();

        let pending = PendingRecord {
            order,
            key: key.to_vec(),
            value,
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
        records_to_publish.sort_unstable_by_key(|record| record.order);
        let deadline = Instant::now() + self.client.retry.deadline;
        let mut backoff = self.client.retry.initial_backoff;

        loop {
            if records_to_publish.is_empty() {
                return;
            }

            let routing = match self.resolve_routing(deadline).await {
                Ok(routing) => routing,
                Err(error) => {
                    PendingRecord::complete_all(records_to_publish, Err(error));
                    return;
                }
            };

            let session = match self.session_manager.ensure(&self.client, &self.topic).await {
                Ok(session) => session,
                Err(error) => {
                    PendingRecord::complete_all(records_to_publish, Err(error));
                    return;
                }
            };

            self.session_manager
                .observe_topology(session, routing.active_range_ids());

            let mut next_retry_records = Vec::new();
            for (range_id, records) in Self::group_by_range(records_to_publish, &routing) {
                if let Some(records) = self
                    .publish_range_attempt(session, range_id, records, deadline)
                    .await
                {
                    next_retry_records.extend(records);
                }
            }

            if next_retry_records.is_empty() {
                return;
            }

            let backoff_remaining = deadline.saturating_duration_since(Instant::now());
            tokio::time::sleep(backoff.min(backoff_remaining)).await;
            backoff = (backoff * 2).min(self.client.retry.max_backoff);
            records_to_publish = next_retry_records;
        }
    }

    async fn resolve_routing(&self, deadline: Instant) -> Result<Arc<TopicRouting>, ClientError> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(self.flush_timeout());
        }

        match tokio::time::timeout(remaining, self.client.resolve_topic_if_missing(&self.topic))
            .await
        {
            Ok(result) => result,
            Err(_) => Err(self.flush_timeout()),
        }
    }

    fn group_by_range(
        records: Vec<PendingRecord>,
        routing: &TopicRouting,
    ) -> BTreeMap<RangeId, Vec<PendingRecord>> {
        let mut grouped: BTreeMap<RangeId, Vec<PendingRecord>> = BTreeMap::new();
        for pending in records {
            match routing.range_id(&pending.key) {
                Some(range_id) => grouped.entry(range_id).or_default().push(pending),
                None => {
                    let _ = pending.tx.send(Err(ClientError::UnexpectedResponse));
                }
            }
        }
        grouped
    }

    /// Own one range batch for one network attempt.
    ///
    /// Returns the records only when the caller must retry them. Every other
    /// outcome completes their senders before returning `None`.
    async fn publish_range_attempt(
        &self,
        session: ClientProducerSession,
        range_id: RangeId,
        records: Vec<PendingRecord>,
        deadline: Instant,
    ) -> Option<Vec<PendingRecord>> {
        let sequence = self.session_manager.sequence_for(session, range_id);
        // Contiguous per-range sequences require serialization through durability.
        let mut sequence = sequence.lock().await;

        let payload = match self.codec.encode_payload(&records) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::error!(?error, "failed to encode producer batch");
                PendingRecord::complete_all(records, Err(ClientError::UnexpectedResponse));
                return None;
            }
        };
        let digest = crc32fast::hash(&payload);
        let result = match tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            self.client.produce_to_range(
                &self.topic,
                range_id,
                &records[0].key,
                payload,
                records.len() as u32,
                Some(session.append_identity(*sequence, digest)),
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(self.flush_timeout()),
        };

        match result {
            Ok(entry_id) => {
                *sequence += 1;
                PendingRecord::complete_all(records, Ok(entry_id));
                None
            }
            Err(ClientError::StaleRange) => {
                self.client.cache.invalidate(&self.topic);
                Some(records)
            }
            Err(ClientError::ProduceRejected(
                ProduceError::SessionNotInstalled | ProduceError::RequestInFlight,
            )) => Some(records),
            Err(ClientError::ProduceRejected(ProduceError::SessionExpired)) => {
                self.session_manager.mark_expired(session).await;
                Some(records)
            }
            Err(error) => {
                PendingRecord::complete_all(records, Err(error));
                None
            }
        }
    }

    fn flush_timeout(&self) -> ClientError {
        ClientError::Timeout {
            waited: self.client.retry.deadline,
            last_error: Some("producer flush deadline elapsed".to_string()),
        }
    }
}
