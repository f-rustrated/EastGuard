use crate::client::CompressionCodec;
use std::time::Duration;

fn parse_duration_ms(s: &str) -> Result<Duration, String> {
    let ms: u64 = s.parse().map_err(|e| format!("invalid duration: {}", e))?;
    Ok(Duration::from_millis(ms))
}

/// Configuration specific to buffer management.
#[derive(Clone, Debug, clap::Args)]
pub struct BufferConfig {
    /// Maximum time to buffer records before flushing a batch (in milliseconds).
    #[arg(long, env = "PRODUCER_LINGER_MS", default_value = "10", value_parser = parse_duration_ms)]
    pub linger: Duration,

    /// Maximum serialized bytes in a batch before triggering a flush.
    #[arg(long, env = "PRODUCER_MAX_BATCH_BYTES", default_value = "1048576")]
    pub max_batch_bytes: usize,

    /// Maximum number of records in a batch before triggering a flush.
    #[arg(long, env = "PRODUCER_MAX_BATCH_RECORDS", default_value = "1000")]
    pub max_batch_records: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            linger: Duration::from_millis(10),
            max_batch_bytes: 1024 * 1024, // 1 MB
            max_batch_records: 1000,
        }
    }
}

/// Configuration options for the Producer.
#[derive(Clone, Debug, clap::Args)]
pub struct ProducerConfig {
    /// Buffer-specific configuration.
    #[command(flatten)]
    pub buffer: BufferConfig,

    /// Compression codec applied to the batch payload.
    #[arg(long, env = "PRODUCER_CODEC", value_enum, default_value_t = CompressionCodec::None)]
    pub codec: CompressionCodec,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            buffer: BufferConfig::default(),
            codec: CompressionCodec::None,
        }
    }
}
