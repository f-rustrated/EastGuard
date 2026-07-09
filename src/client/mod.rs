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
pub use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
pub use crate::control_plane::metadata::{EntryId, RangeId};
pub use codec::CompressionCodec;
pub use consumer::{
    CommitMode, Consumer, ConsumerConfig, ConsumerRecord, DeliverySemantic, KeyInterest,
    StartPolicy,
};
pub use error::ClientError;
use pool::ConnectionPool;
pub use producer::{BufferConfig, Producer, ProducerConfig};

use crate::client::consumer::group::SYSTEM_TOPIC_OFFSETS;
use crate::client::consumer::group::{ConsumerPosition, OffsetCommitPayload};
use crate::connections::protocol::{ClientResponse, DataPlaneResponse, RangeOffsetRequest};
use borsh::BorshDeserialize;

pub use redirect::RetryPolicy;
use routing::RoutingCache;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Instant;

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

    /// Returns the range's surviving start and the next entry id to request at
    /// the observed tail. The second value is not the last committed entry id.
    pub async fn fetch_range_entry_ids(
        &self,
        topic: &str,
        range_id: RangeId,
    ) -> Result<(EntryId, EntryId), ClientError> {
        let deadline = Instant::now() + self.retry.deadline;
        let mut backoff = self.retry.initial_backoff;
        let mut last_error = None;

        loop {
            if Instant::now() >= deadline {
                return Err(ClientError::Timeout {
                    waited: self.retry.deadline,
                    last_error,
                });
            }

            let detail = self.resolve_topic(topic).await?;
            let addr = detail
                .ranges
                .iter()
                .find(|r| r.range_id == range_id)
                .and_then(|r| {
                    r.active_segment
                        .as_ref()
                        .or_else(|| r.sealed_segments.last())
                })
                .and_then(|seg| seg.pick_replica())
                .unwrap_or_else(|| self.next_known_node());

            let req = RangeOffsetRequest {
                topic_name: topic.to_string(),
                range_id,
            };

            let served = self.call(addr, req).await?;

            match served.response {
                ClientResponse::DataPlane(DataPlaneResponse::RangeOffset {
                    start_entry_id,
                    next_entry_id,
                }) => return Ok((start_entry_id, next_entry_id)),
                ClientResponse::DataPlane(DataPlaneResponse::SegmentNotLocal) => {
                    last_error = Some("range offsets segment not local".to_string());
                }
                _ => return Err(ClientError::UnexpectedResponse),
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::time::sleep(backoff.min(remaining)).await;
            backoff = (backoff * 2).min(self.retry.max_backoff);
        }
    }

    pub async fn fetch_all_saved_offsets(
        self: Arc<Self>,
        group_id: &str,
        topic: &str,
    ) -> Result<HashMap<RangeId, ConsumerPosition>, ClientError> {
        let mut map = HashMap::new();
        let offsets_consumer = Consumer::new(
            self,
            SYSTEM_TOPIC_OFFSETS.to_string(),
            KeyInterest::AllKeys,
            ConsumerConfig::new(StartPolicy::Earliest),
        )
        .await?;

        let g_bytes = group_id.as_bytes();
        let t_bytes = topic.as_bytes();
        let routing_key = g_bytes.iter().chain(b":".iter()).chain(t_bytes.iter());

        let mut timeout_ms = 2000;
        while let Ok(Ok(Some(record))) = tokio::time::timeout(
            std::time::Duration::from_millis(timeout_ms),
            offsets_consumer.next_record(),
        )
        .await
        {
            timeout_ms = 100;
            let is_key_match = record.key_match(routing_key.clone());
            if is_key_match && let Ok(payload) = OffsetCommitPayload::try_from_slice(&record.value)
            {
                map.insert(payload.range_id, payload.position);
            }
        }
        Ok(map)
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
