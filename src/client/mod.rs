//! Client SDK — the routing & connection foundation (phase C1).
//!
//! A smart client: it owns a pooled, multiplexed connection per node, a per-topic
//! routing cache built from `DescribeTopic`, and one uniform redirect-follow loop
//! that keeps the cache honest. The server never proxies — it redirects, and this
//! library follows. Producer (C2) and consumer (C3) layer on top of this core.
//!
//! `#![allow(dead_code)]` — C1 is the foundation; its tests and the later C2/C3 phases
//! are the first non-test consumers (mirrors `consumer`).

#![allow(dead_code, unused_imports)]

mod admin;
mod error;
mod nodes;
mod pool;
mod produce;
mod redirect;
mod routing;
pub use crate::connections::protocol::{TopicDetail, TopicSummary};
pub use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
pub use error::ClientError;
use pool::ConnectionPool;
pub use redirect::RetryPolicy;
use routing::RoutingCache;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A connection to an EastGuard cluster. Cheap to construct (connections are lazy);
/// share one across tasks behind an `Arc` — every method takes `&self` and the pool
/// and cache are internally synchronized.
pub struct Client {
    pool: ConnectionPool,
    cache: RoutingCache,
    seeds: Box<[SocketAddr]>,
    seed_cursor: AtomicUsize,
    retry: RetryPolicy,
}

impl Client {
    /// Build a client from one or more seed addresses with the default [`RetryPolicy`].
    /// Seeds are the fallback the redirect loop re-resolves against; they need not be leaders or even alive.
    pub fn connect(seeds: impl Into<Vec<SocketAddr>>) -> Result<Self, ClientError> {
        Self::connect_with(seeds, RetryPolicy::default())
    }

    /// Build a client with an explicit retry policy
    pub fn connect_with(
        seeds: impl Into<Vec<SocketAddr>>,
        retry: RetryPolicy,
    ) -> Result<Self, ClientError> {
        let seeds: Box<[SocketAddr]> = seeds.into().into_boxed_slice();
        if seeds.is_empty() {
            return Err(ClientError::NoSeeds);
        }
        Ok(Self {
            pool: ConnectionPool::new(),
            cache: RoutingCache::new(),
            seeds,
            seed_cursor: AtomicUsize::new(0),
            retry,
        })
    }

    /// Round-robin the next seed to re-resolve against. Infallible — `seeds` is
    /// non-empty by construction.
    pub(crate) fn next_seed(&self) -> SocketAddr {
        let i = self.seed_cursor.fetch_add(1, Ordering::Relaxed) % self.seeds.len();
        self.seeds[i]
    }
}

#[cfg(test)]
impl Client {
    /// Test seam: poison the cached write leader for `topic` to a wrong (but live)
    /// node, to exercise stale-cache self-correction.
    pub(crate) fn poison_cache(&self, topic: &str, wrong: SocketAddr) {
        self.cache.poison_write_leaders(topic, wrong);
    }

    /// Test seam: read the currently-cached write leader for a routing key.
    pub(crate) fn cached_write_leader(&self, topic: &str, key: &[u8]) -> Option<SocketAddr> {
        self.cache.write_leader(topic, key)
    }
}
