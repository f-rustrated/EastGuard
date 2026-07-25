pub(crate) mod buffers;
mod config;
pub(crate) mod record;
mod session;
use crate::client::error::ClientError;
use crate::client::routing::TopicRouting;
use crate::client::{Client, CompressionCodec};
use crate::control_plane::metadata::{EntryId, RangeId};
use crate::data_plane::ProduceError;
use buffers::{PendingRecord, ProducerBuffers, PushResult};
pub use config::{BufferConfig, ProducerConfig};
use session::{ClientProducerSession, ClientProducerSessionManager};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{RwLock, oneshot};
use tokio::time::Instant;
use uuid::Uuid;

/// A thread-safe, internally synchronized producer for a single topic.
/// Cheap to clone and share across tasks.
///
/// Every clone refers to the same buffers and shutdown state. Closing any clone
/// closes the producer for all clones.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use east_guard::client::{Client, Producer, ProducerConfig};
/// use std::{net::SocketAddr, sync::Arc};
///
/// let seed: SocketAddr = "127.0.0.1:9091".parse()?;
/// let client = Arc::new(Client::connect([seed])?);
/// let producer = Producer::new(client, "orders".to_string(), ProducerConfig::default())?;
///
/// let sender = producer.clone();
/// let sent = tokio::spawn(async move {
///     sender.send(b"customer-42", b"created".to_vec()).await
/// });
/// let committed_entry = sent.await??;
///
/// producer.close().await;
/// # let _ = committed_entry;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Producer {
    inner: Arc<Inner>,
}

/// # Synchronization & Deadlock Prevention Model
///
/// The producer relies on two distinct `RwLock` gates to coordinate async application
/// callers, background linger tasks, and explicit `flush()` / `close()` barriers:
///
/// 1. **[`send_gate`] (Application Invocation Barrier)**:
///    - Every [`send()`] call holds [`send_gate`].read() across its entire execution
///      (routing, pushing to buffer, and awaiting `oneshot` completion).
///    - [`flush()`] and [`close()`] acquire [`send_gate`].write() to wait for all pre-existing
///      `send()` callers to finish before returning.
///
/// 2. **[`flush_gate`] (Batch Ownership & Network Publication Barrier)**:
///    - Any task publishing a batch over the network (immediate push or background linger task)
///      holds [`flush_gate`].read() during payload encoding and broker RPCs.
///    - [`flush()`] and [`close()`] acquire `flush_gate.write() after draining buffers to guarantee
///      [`flush()`] does not return while a background linger task is still in-flight.
///
/// ### Why [`send_gate`]
/// Imagine if [`flush()`] simply drained ProducerBuffers without a send gate:
///
/// ```text
///     Task A (send caller)                      Task B (flush caller)
///  --------------------                      ---------------------
///  1. Calls producer.send(k, v)
///  2. Awaits resolve_topic...
///     (Record is NOT in buffer yet!)
///                                            3. Calls producer.flush()
///                                            4. Drains ProducerBuffers (finds 0 records)
///                                            5. flush() RETURNS OK!
///  6. Pushes record into buffer!
/// ```
///
/// The **Bug**: Task B’s flush().await returned, assuring the application that all previously initiated records were flushed—when in reality, Task A’s record was still
/// sitting in memory un-flushed.
/// [`send_gate`]'s read guard allow concurrent send execution while by the time flush is really made it forces write lock acqusition for serialization.
///
///
/// [`send_gate`]: Inner::send_gate
/// [`flush_gate`]: Inner::flush_gate
/// [`send()`]: Producer::send
/// [`flush()`]: Producer::flush
/// [`close()`]: Producer::close
struct Inner {
    client: Arc<Client>,
    topic: String,
    buffers: ProducerBuffers,
    codec: CompressionCodec,
    session_manager: ClientProducerSessionManager,
    send_gate: RwLock<()>,
    flush_gate: Arc<RwLock<()>>,
    should_reject: AtomicBool,
    next_record_order: AtomicU64,
}

impl Inner {
    fn invalidate_cache(&self) {
        self.client.cache.invalidate(&self.topic);
    }

    fn retry_deadline(&self) -> std::time::Duration {
        self.client.retry.deadline
    }

    fn initial_backoff(&self) -> std::time::Duration {
        self.client.retry.initial_backoff
    }

    fn max_backoff(&self) -> std::time::Duration {
        self.client.retry.max_backoff
    }

    fn flush_timeout(&self) -> ClientError {
        ClientError::Timeout {
            waited: self.client.retry.deadline,
            last_error: Some("producer flush deadline elapsed".to_string()),
        }
    }

    async fn resolve_routing_and_session(
        &self,
        deadline: Instant,
    ) -> Result<(Arc<TopicRouting>, ClientProducerSession), ClientError> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(self.flush_timeout());
        }

        let routing = match tokio::time::timeout(
            remaining,
            self.client.resolve_topic_if_missing(&self.topic),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => return Err(self.flush_timeout()),
        };

        let session = self
            .session_manager
            .ensure(&self.client, &self.topic)
            .await?;

        self.session_manager
            .observe_topology(session, routing.active_range_ids());

        Ok((routing, session))
    }

    // ! Dedicated spawning is required because otherwise when the client cancels the operation,
    // ! It would accidantely cancel all other records in that shared batch.
    fn flush_in_background(
        self: &Arc<Self>,
        records: Vec<PendingRecord>,
        flush: tokio::sync::OwnedRwLockReadGuard<()>,
    ) {
        let inner = self.clone();
        tokio::spawn(async move {
            let _flush = flush;
            inner.flush_records(records).await;
        });
    }

    /// Flush the buffered records for a specific range if they exist.
    async fn flush_range(self: &Arc<Self>, range_id: RangeId, task_batch_seq: u64) {
        let _flush = self.flush_gate.read().await;
        if let Some(records_to_flush) = self.buffers.take(range_id, task_batch_seq) {
            self.flush_records(records_to_flush).await;
        }
    }

    async fn flush_buffers(self: &Arc<Self>) {
        let batches = self.buffers.take_all();
        let futures = batches
            .into_iter()
            .map(|(_, records)| self.flush_records(records));
        futures::future::join_all(futures).await;
    }

    /// Serialize, compress, and publish a batch of records.
    async fn flush_records(self: &Arc<Self>, mut records_to_publish: Vec<PendingRecord>) {
        records_to_publish.sort_unstable_by_key(|record| record.order);
        let deadline = Instant::now() + self.retry_deadline();
        let mut backoff = self.initial_backoff();

        loop {
            if records_to_publish.is_empty() {
                return;
            }

            let (routing, session) = match self.resolve_routing_and_session(deadline).await {
                Ok(res) => res,
                Err(error) => {
                    PendingRecord::complete_all(records_to_publish, Err(error));
                    return;
                }
            };

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
            backoff = (backoff * 2).min(self.max_backoff());
            records_to_publish = next_retry_records;
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
                self.invalidate_cache();
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
    async fn flush(self: &Arc<Self>) {
        let inner = self.clone();
        let handle = tokio::spawn(async move {
            let _exclusive = inner.send_gate.write().await;
            let _flush = inner.flush_gate.write().await;
            inner.flush_buffers().await;
        });
        let _ = handle.await;
    }

    async fn close(self: &Arc<Self>) {
        self.should_reject.store(true, Ordering::Release);
        self.flush().await;
    }

    async fn send(self: &Arc<Self>, key: &[u8], value: Vec<u8>) -> Result<EntryId, ClientError> {
        let send_guard = self.send_gate.read().await;
        if self.should_reject.load(Ordering::Acquire) {
            return Err(ClientError::ProducerClosed);
        }

        let order = self.next_record_order.fetch_add(1, Ordering::Relaxed);
        let routing = self.client.resolve_topic_if_missing(&self.topic).await?;
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
                let flush = self.flush_gate.clone().read_owned().await;
                self.flush_in_background(records_to_flush, flush);
            }
            PushResult::SpawnLinger(linger, seq) => {
                let inner = self.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(linger).await;
                    inner.flush_range(range_id, seq).await;
                });
            }
            PushResult::Buffered => {}
        }
        drop(send_guard);

        rx.await.map_err(|_| ClientError::UnexpectedResponse)?
    }
}

impl Producer {
    /// Create a producer, returning a structured error for invalid configuration.
    pub fn new(
        client: Arc<Client>,
        topic: String,
        config: ProducerConfig,
    ) -> Result<Self, ClientError> {
        // Generate a globally unique UUID for the producer session ID (idempotency seam)
        let producer_id = Uuid::new_v4();

        if topic.is_empty() {
            return Err(ClientError::invalid_configuration(
                "producer.topic",
                "must not be empty",
            ));
        }
        config.validate()?;

        Ok(Self {
            inner: Arc::new(Inner {
                client,
                topic,
                buffers: ProducerBuffers::new(config.buffer.clone()),
                codec: config.codec,
                session_manager: ClientProducerSessionManager::new(producer_id),
                send_gate: RwLock::new(()),
                flush_gate: Arc::new(RwLock::new(())),
                should_reject: AtomicBool::new(false),
                next_record_order: AtomicU64::new(0),
            }),
        })
    }

    /// Produce a single record. Returns the committed entry ID once the batch flushes.
    ///
    /// Canceling before the record enters a batch prevents publication. Once buffered,
    /// publication is shared work and may still complete; canceling only discards this
    /// caller's acknowledgement.
    pub async fn send(&self, key: &[u8], value: Vec<u8>) -> Result<EntryId, ClientError> {
        self.inner.send(key, value).await
    }

    pub async fn flush(&self) {
        self.inner.flush().await;
    }

    /// Close every clone and wait for accepted records to finish.
    ///
    /// New sends fail with [`ClientError::ProducerClosed`]. Canceling this future keeps
    /// the producer closed, while the producer-owned drain continues in the background.
    pub async fn close(&self) {
        self.inner.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn producer() -> Producer {
        let seed = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9));
        let client = Arc::new(Client::connect(vec![seed]).unwrap());
        Producer::new(client, "topic".to_string(), ProducerConfig::default()).unwrap()
    }

    #[tokio::test]
    async fn close_empty_producer_rejects_future_sends() {
        let producer = producer();

        producer.close().await;
        assert!(matches!(
            producer.send(b"key", b"value".to_vec()).await,
            Err(ClientError::ProducerClosed)
        ));
    }
}
