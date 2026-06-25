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
mod codec;
mod consumer;
mod error;
mod nodes;
mod pool;
mod produce;
mod producer;

mod redirect;
mod routing;
use crate::client::nodes::KnownNodes;
pub use crate::connections::protocol::{TopicDetail, TopicSummary};
pub use crate::consumer::{KeyInterest, StartPolicy};
pub use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
pub use codec::CompressionCodec;
pub use consumer::{Consumer, ConsumerRecord};
pub use error::ClientError;
use pool::ConnectionPool;
pub use producer::{BufferConfig, Producer, ProducerConfig};

pub use redirect::RetryPolicy;
use routing::RoutingCache;
use std::net::SocketAddr;

/// A connection to an EastGuard cluster. Cheap to construct (connections are lazy);
/// share one across tasks behind an `Arc` — every method takes `&self` and the pool
/// and cache are internally synchronized.
pub struct Client {
    pool: ConnectionPool,
    cache: RoutingCache,
    known_nodes: KnownNodes,
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
        let seeds = seeds.into();
        if seeds.is_empty() {
            return Err(ClientError::NoSeeds);
        }
        Ok(Self {
            pool: ConnectionPool::new(),
            cache: RoutingCache::new(),
            known_nodes: KnownNodes::new(seeds),
            retry,
        })
    }

    /// Round-robin the next known node to (re-)resolve against. Infallible — the pool
    /// is non-empty by construction and only ever grows.
    pub(crate) fn next_known_node(&self) -> SocketAddr {
        self.known_nodes.pick()
    }

    /// Record the cluster nodes a describe response revealed (active-segment replica
    /// addresses), growing the contact pool beyond the bootstrap seeds.
    pub(crate) fn remember_nodes(&self, detail: &TopicDetail) {
        for range in &detail.ranges {
            if let Some(seg) = &range.active_segment {
                for replica in &seg.replica_set {
                    self.known_nodes.remember(replica.client_addr);
                }
            }
        }
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

    /// Test seam: how many nodes the contact pool currently knows about.
    pub(crate) fn known_node_count(&self) -> usize {
        self.known_nodes.len()
    }
}
