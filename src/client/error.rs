use std::net::SocketAddr;
use std::time::Duration;

use crate::client::RangeId;
use crate::control_plane::metadata::consumer_group::GenerationId;

/// Errors a caller decides on. Redirect-following, reconnect, and retry-within-deadline
/// are handled internally.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ClientError {
    /// Could not reach `addr`. Internal to the pool — the retry loop catches it and
    /// re-resolves; a persistent failure surfaces as `Timeout`, not this.
    #[error("connection to {addr} unavailable: {reason}")]
    Connection { addr: SocketAddr, reason: String },

    /// Topic absent — the answer after retrying through propagation lag for the whole
    /// deadline (a just-created topic is retried, not reported here at once).
    #[error("topic not found")]
    TopicNotFound,

    /// Cluster stayed unavailable (no leader, unreachable) for the whole deadline.
    /// Retriable with a fresh call.
    #[error("operation did not complete within {waited:?} (last error: {last_error:?})")]
    Timeout {
        waited: Duration,
        last_error: Option<String>,
    },

    /// Constructed with no seed addresses.
    #[error("no seed addresses configured")]
    NoSeeds,

    /// Response didn't match the request — a wire/version mismatch, not routing.
    #[error("unexpected response for request")]
    UnexpectedResponse,

    /// Expected range has split or metadata is stale.
    #[error("stale range routing")]
    StaleRange,

    #[error(
        "consumer group generation {request_generation:?} is stale; data layer sealed at {sealed_generation:?}"
    )]
    StaleConsumerGroupEpoch {
        request_generation: GenerationId,
        sealed_generation: GenerationId,
    },

    #[error("consumer checkpoint lookup failed for ranges {ranges:?}: {reason}")]
    ConsumerCheckpointLookupFailed { ranges: Box<[u64]>, reason: String },

    /// A consumer range-control command could not be delivered or acknowledged.
    #[error("consumer {operation} failed for range {range_id}: {reason}")]
    ConsumerControl {
        operation: &'static str,
        range_id: u64,
        reason: String,
    },

    #[error("consumer commit failed: {reason}")]
    ConsumerCommit { reason: String },
}

impl ClientError {
    pub(crate) fn on_ack(range_id: RangeId, reason: &'static str) -> Self {
        Self::on_control("ack", range_id, reason)
    }

    pub(crate) fn on_control(
        operation: &'static str,
        range_id: RangeId,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        tracing::warn!(operation, range_id = *range_id, reason);
        ClientError::ConsumerControl {
            operation,
            range_id: *range_id,
            reason,
        }
    }

    pub(crate) fn on_commit(reason: impl Into<String>) -> Self {
        let reason = reason.into();
        tracing::warn!(reason, "consumer commit failed");
        ClientError::ConsumerCommit { reason }
    }

    pub(crate) fn on_checkpoint_lookup(ranges: &[RangeId], reason: impl Into<String>) -> Self {
        let reason = reason.into();
        let ranges = ranges.iter().map(|range| **range).collect();
        tracing::warn!(?ranges, reason, "consumer offset recovery failed");
        ClientError::ConsumerCheckpointLookupFailed { ranges, reason }
    }
}
