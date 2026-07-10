use crate::client::producer::config::BufferConfig;
use crate::control_plane::metadata::RangeId;
use crate::{client::error::ClientError, control_plane::metadata::EntryId};

use dashmap::DashMap;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use uuid::Uuid;

pub(crate) type Records = Box<[(Vec<u8>, Vec<u8>)]>;

/// A thread-safe, encapsulated manager for partition-level range buffers.
pub struct ProducerBuffers {
    inner: DashMap<RangeId, RangeBuffer>,
    config: BufferConfig,
}

pub struct RangeBuffer {
    pub records: Vec<PendingRecord>,
    pub current_bytes: usize,
    pub spawned_linger: bool,
    pub first_added_at: Option<Instant>,
    pub current_batch_seq: u64,
}
impl RangeBuffer {
    fn push(&mut self, pending: PendingRecord) {
        // Estimate serialized size: key_len (4B) + value_len (4B) + content bytes
        let record_bytes = 8 + pending.key.len() + pending.value.len();
        self.current_bytes += record_bytes;
        self.records.push(pending);
        if self.first_added_at.is_none() {
            self.first_added_at = Some(Instant::now());
        }
    }

    fn needs_flush(&self, config: &BufferConfig) -> bool {
        self.current_bytes >= config.max_batch_bytes
            || self.records.len() >= config.max_batch_records
    }

    fn flush(&mut self) -> Vec<PendingRecord> {
        self.current_batch_seq = self.current_batch_seq.wrapping_add(1);
        let records_to_flush = std::mem::take(&mut self.records);
        self.current_bytes = 0;
        self.spawned_linger = false;
        self.first_added_at = None;
        records_to_flush
    }
}

pub struct PendingRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub producer_id: Uuid,
    pub sequence_number: u32,
    pub tx: oneshot::Sender<Result<EntryId, ClientError>>,
}
impl PendingRecord {
    /// Serialize a slice of records into a byte buffer.
    pub fn serialize_batch(records: &[PendingRecord]) -> Vec<u8> {
        let mut buf = Vec::new();
        for r in records {
            buf.extend_from_slice(&(r.key.len() as u32).to_be_bytes());
            buf.extend_from_slice(&r.key);
            buf.extend_from_slice(&(r.value.len() as u32).to_be_bytes());
            buf.extend_from_slice(&r.value);
        }
        buf
    }

    /// Deserialize a byte buffer into a vector of records.
    pub fn deserialize_batch(mut buf: &[u8], count: u32) -> Result<Records, std::io::Error> {
        let mut records = Vec::with_capacity(count as usize);
        for _ in 0..count {
            if buf.len() < 4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Unexpected EOF reading key length",
                ));
            }
            let key_len = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;
            buf = &buf[4..];

            if buf.len() < key_len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Unexpected EOF reading key data",
                ));
            }
            let key = buf[..key_len].to_vec();
            buf = &buf[key_len..];

            if buf.len() < 4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Unexpected EOF reading value length",
                ));
            }
            let val_len = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;
            buf = &buf[4..];

            if buf.len() < val_len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Unexpected EOF reading value data",
                ));
            }
            let value = buf[..val_len].to_vec();
            buf = &buf[val_len..];

            records.push((key, value));
        }
        Ok(records.into_boxed_slice())
    }
}

/// The action to be taken after pushing a record to the buffer.
pub enum PushResult {
    /// The record was added, and the caller should spawn a linger task to flush after this duration with the given batch sequence.
    SpawnLinger(Duration, u64),
    /// The record was added, and it is buffered (linger is already active).
    Buffered,
    /// The buffer has reached size or record count limits and must be flushed immediately.
    Flush(Vec<PendingRecord>),
}

impl ProducerBuffers {
    /// Create a new set of producer buffers configured with the given options.
    pub fn new(config: BufferConfig) -> Self {
        Self {
            inner: DashMap::new(),
            config,
        }
    }

    /// Push a pending record into the range buffer for a specific partition.
    /// Returns the action the caller must take based on the stored buffer limits.
    pub fn push(&self, range_id: RangeId, pending: PendingRecord) -> PushResult {
        let mut buf = self.inner.entry(range_id).or_insert_with(|| RangeBuffer {
            records: Vec::new(),
            current_bytes: 0,
            spawned_linger: false,
            first_added_at: None,
            current_batch_seq: 0,
        });

        buf.push(pending);

        if buf.needs_flush(&self.config) {
            PushResult::Flush(buf.flush())
        } else if !buf.spawned_linger {
            buf.spawned_linger = true;
            PushResult::SpawnLinger(self.config.linger, buf.current_batch_seq)
        } else {
            PushResult::Buffered
        }
    }

    /// Take and clear all records from the buffer for a specific partition, if the task sequence matches the current batch.
    pub fn take(&self, range_id: RangeId, task_batch_seq: u64) -> Option<Vec<PendingRecord>> {
        let mut buf = self.inner.get_mut(&range_id)?;
        if buf.current_batch_seq != task_batch_seq {
            return None;
        }
        if buf.records.is_empty() {
            return None;
        }
        Some(buf.flush())
    }

    /// Take and clear all buffered records unconditionally across all partitions.
    /// Increment the batch sequence to invalidate any pending linger tasks.
    pub fn take_all(&self) -> Vec<(RangeId, Vec<PendingRecord>)> {
        let mut all_records = Vec::with_capacity(self.inner.len());
        for mut entry in self.inner.iter_mut() {
            if !entry.records.is_empty() {
                all_records.push((*entry.key(), entry.flush()));
            }
        }
        all_records
    }
}
