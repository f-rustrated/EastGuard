use std::net::SocketAddr;
use std::time::Duration;

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
}
