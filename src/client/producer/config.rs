use std::time::Duration;
use crate::client::CompressionCodec;

/// Configuration options for the Producer.
#[derive(Clone, Debug)]
pub struct ProducerConfig {
    /// Maximum time to buffer records before flushing a batch.
    pub linger: Duration,
    /// Maximum serialized bytes in a batch before triggering a flush.
    pub max_batch_bytes: usize,
    /// Maximum number of records in a batch before triggering a flush.
    pub max_batch_records: usize,
    /// Compression codec applied to the batch payload.
    pub codec: CompressionCodec,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            linger: Duration::from_millis(10),
            max_batch_bytes: 1024 * 1024, // 1 MB
            max_batch_records: 1000,
            codec: CompressionCodec::None,
        }
    }
}
