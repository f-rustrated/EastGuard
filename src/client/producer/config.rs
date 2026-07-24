use crate::client::{ClientError, CompressionCodec};
use std::time::Duration;

fn parse_duration_ms(s: &str) -> Result<Duration, String> {
    let ms: u64 = s.parse().map_err(|e| format!("invalid duration: {}", e))?;
    Ok(Duration::from_millis(ms))
}

fn parse_nonzero_usize(s: &str) -> Result<usize, String> {
    let val: usize = s.parse().map_err(|e| format!("invalid number: {}", e))?;
    if val == 0 {
        return Err("must be greater than zero".to_string());
    }
    Ok(val)
}

/// Configuration specific to buffer management.
#[derive(Clone, Debug, clap::Args)]
pub struct BufferConfig {
    /// Maximum time to buffer records before flushing a batch (in milliseconds).
    #[arg(long, env = "PRODUCER_LINGER_MS", default_value = "10", value_parser = parse_duration_ms)]
    pub linger: Duration,

    /// Maximum serialized bytes in a batch before triggering a flush.
    #[arg(long, env = "PRODUCER_MAX_BATCH_BYTES", default_value = "1048576", value_parser = parse_nonzero_usize)]
    pub max_batch_bytes: usize,

    /// Maximum number of records in a batch before triggering a flush.
    #[arg(long, env = "PRODUCER_MAX_BATCH_RECORDS", default_value = "1000", value_parser = parse_nonzero_usize)]
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

impl BufferConfig {
    pub(crate) fn validate(&self) -> Result<(), ClientError> {
        if self.max_batch_bytes == 0 {
            return Err(ClientError::invalid_configuration(
                "producer.buffer.max_batch_bytes",
                "must be greater than zero",
            ));
        }
        if self.max_batch_records == 0 {
            return Err(ClientError::invalid_configuration(
                "producer.buffer.max_batch_records",
                "must be greater than zero",
            ));
        }
        Ok(())
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

impl ProducerConfig {
    pub(crate) fn validate(&self) -> Result<(), ClientError> {
        self.buffer.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct CommandWrapper {
        #[command(flatten)]
        buffer: BufferConfig,
    }

    #[test]
    fn rejects_zero_batch_limits() {
        let zero_bytes = BufferConfig {
            max_batch_bytes: 0,
            ..BufferConfig::default()
        };
        assert!(zero_bytes.validate().is_err());

        let zero_records = BufferConfig {
            max_batch_records: 0,
            ..BufferConfig::default()
        };
        assert!(zero_records.validate().is_err());
    }

    #[test]
    fn clap_rejects_zero_batch_limits() {
        assert!(CommandWrapper::try_parse_from(["app", "--max-batch-bytes", "0"]).is_err());
        assert!(CommandWrapper::try_parse_from(["app", "--max-batch-records", "0"]).is_err());
        assert!(
            CommandWrapper::try_parse_from([
                "app",
                "--max-batch-bytes",
                "1024",
                "--max-batch-records",
                "100"
            ])
            .is_ok()
        );
    }
}
