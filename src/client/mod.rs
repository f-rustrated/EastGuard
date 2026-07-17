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

mod codec;
mod consumer;
mod error;
mod nodes;
mod pool;
mod producer;

mod redirect;
mod routing;
use crate::client::nodes::KnownNodes;
use crate::client::routing::TopicRouting;

pub use crate::connections::protocol::{TopicDetail, TopicSummary};
#[cfg(test)]
use crate::control_plane::NodeAddressInfo;
use crate::control_plane::metadata::consumer_group::GenerationId;
pub use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
pub use crate::control_plane::metadata::{EntryId, RangeId};
use crate::control_plane::metadata::{SyncConsumerGroupRequest, TopicId};
use crate::data_plane::consumer_offset_management::ledger::{
    ConsumerOffsetKey, ConsumerOffsetPosition,
};
pub use codec::CompressionCodec;
pub use consumer::{
    CommitMode, Consumer, ConsumerConfig, ConsumerRecord, DeliverySemantic, KeyInterest,
    StartPolicy,
};
pub use error::ClientError;
use pool::ConnectionPool;
pub use producer::{BufferConfig, Producer, ProducerConfig};
use uuid::Uuid;

use crate::connections::protocol::{
    ClientDataPlaneRequest, ClientRequest, ClientResponse, CommitConsumerOffsetRequest,
    ConsumerGroupAssignmentResponse, ConsumerGroupSyncAction, ControlPlaneRequest,
    ControlPlaneResponse, DataPlaneResponse, FetchConsumerOffsetRequest, ProduceRequest,
    RangeOffsetRequest,
};

use futures::{StreamExt, stream::FuturesUnordered};
pub use redirect::RetryPolicy;
use redirect::{Redirect, RetryState, Served};
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
                    self.known_nodes.remember(replica.client_addr());
                }
            }
        }
    }

    pub(crate) fn get_routing(&self, topic: &str) -> Option<Arc<TopicRouting>> {
        self.cache.get(topic)
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
                .and_then(|seg| seg.pick_read_replica())
                .map(|addr| addr.client_addr())
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

    pub(crate) async fn commit_consumer_offset(
        &self,
        topic_name: &str,
        request: impl Into<CommitConsumerOffsetRequest>,
    ) -> Result<(), ClientError> {
        let req = request.into();

        // resolve routing
        let routing = self.resolve_topic_if_missing(topic_name).await?;
        let leader = routing
            .write_leader_for_range(req.key.range_id)
            .ok_or(ClientError::StaleRange)?;
        let request_generation = req.generation;

        let served = self.call(leader.client_addr(), req).await?;
        if served.redirected {
            self.cache.invalidate(topic_name);
        }
        match served.response {
            ClientResponse::DataPlane(DataPlaneResponse::ConsumerOffsetCommitted) => Ok(()),
            ClientResponse::DataPlane(DataPlaneResponse::StaleConsumerGroupEpoch(stale)) => {
                Err(ClientError::StaleConsumerGroupEpoch {
                    request_generation,
                    sealed_generation: stale,
                })
            }
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    pub(crate) async fn fetch_consumer_offsets(
        &self,
        topic_name: &str,
        consumer_offset_keys: Box<[ConsumerOffsetKey]>,
    ) -> Result<HashMap<RangeId, ConsumerOffsetPosition>, ClientError> {
        let routing = self.resolve_topic_if_missing(topic_name).await?;

        let futures = consumer_offset_keys.into_vec().into_iter().map(|offset| {
            let routing = routing.clone();
            async move {
                let range_id = offset.range_id;
                let leader = routing
                    .write_leader_for_range(range_id)
                    .ok_or(ClientError::StaleRange)?;
                let served = self
                    .call(leader.client_addr(), FetchConsumerOffsetRequest(offset))
                    .await?;
                if served.redirected {
                    self.cache.invalidate(topic_name);
                }
                match served.response {
                    ClientResponse::DataPlane(DataPlaneResponse::ConsumerOffset(position)) => {
                        Ok(position.map(|p| (range_id, p)))
                    }
                    _ => Err(ClientError::UnexpectedResponse),
                }
            }
        });
        let results = futures::future::try_join_all(futures).await?;
        Ok(results.into_iter().flatten().collect())
    }

    /// Create a topic. `Ok(true)` if newly created, `Ok(false)` if it already existed.
    pub async fn create_topic(
        &self,
        name: &str,
        storage_policy: StoragePolicy,
    ) -> Result<bool, ClientError> {
        let request = ControlPlaneRequest::CreateTopic {
            name: name.to_string(),
            storage_policy,
        };
        let served = self.call(self.next_known_node(), request).await?;
        match served.response {
            ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated) => Ok(true),
            ClientResponse::ControlPlane(ControlPlaneResponse::AlreadyExists) => Ok(false),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Delete a topic. A missing topic surfaces as `ClientError::TopicNotFound`.
    pub async fn delete_topic(&self, name: &str) -> Result<(), ClientError> {
        let request = ControlPlaneRequest::DeleteTopic {
            name: name.to_string(),
        };
        let served = self.call(self.next_known_node(), request).await?;
        // The redirect loop already turned a `TopicNotFound` response into an error.
        self.cache.invalidate(name);
        match served.response {
            ClientResponse::ControlPlane(ControlPlaneResponse::TopicDeleted) => Ok(()),
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Return a stable routing snapshot, resolving the topic when the cache is empty.
    pub(crate) async fn resolve_topic_if_missing(
        &self,
        topic: &str,
    ) -> Result<Arc<TopicRouting>, ClientError> {
        loop {
            if let Some(routing) = self.get_routing(topic) {
                return Ok(routing);
            }
            self.resolve_topic(topic).await?;
        }
    }

    pub async fn resolve_topic(&self, name: &str) -> Result<TopicDetail, ClientError> {
        let request = ControlPlaneRequest::DescribeTopic {
            name: name.to_string(),
        };
        let served = self.call(self.next_known_node(), request).await?;
        match served.response {
            ClientResponse::ControlPlane(ControlPlaneResponse::TopicDetail(detail)) => {
                self.remember_nodes(&detail);
                self.cache.insert(&detail);
                Ok(detail)
            }
            err => {
                tracing::error!("{err:?}{}{}", file!(), line!());
                Err(ClientError::UnexpectedResponse)
            }
        }
    }

    pub(crate) async fn produce(
        &self,
        topic: &str,
        routing_key: &[u8],
        data: Vec<u8>,
        record_count: u32,
    ) -> Result<EntryId, ClientError> {
        let routing = self.resolve_topic_if_missing(topic).await?;
        let range_id = routing
            .range_id(routing_key)
            .ok_or(ClientError::TopicNotFound)?;

        self.produce_to_range(topic, range_id, routing_key, data, record_count)
            .await
    }

    /// Produce one entry under `routing_key`, returning the committed `entry_id`.
    /// Routes to the cached write leader; a redirect self-corrects and drops the
    /// stale entry so the next call re-resolves.
    pub(crate) async fn produce_to_range(
        &self,
        topic: &str,
        range_id: RangeId,
        routing_key: &[u8],
        data: Vec<u8>,
        record_count: u32,
    ) -> Result<EntryId, ClientError> {
        // Describe once to seed the cache (gives the first hop).
        let routing = self.resolve_topic_if_missing(topic).await?;

        // Start at the cached leader; fall back to a seed if the key has no cached
        // active range (the server will redirect).
        let start = routing
            .write_leader(routing_key)
            .map(|addr| addr.client_addr())
            .unwrap_or_else(|| self.next_known_node());

        let request = ProduceRequest {
            topic_name: topic.to_string(),
            range_id,
            routing_key: routing_key.to_vec(),
            data,
            record_count,
        };

        let served = self.call(start, request).await?;
        // A redirect or stale range means the cached route is stale; drop it so
        // the next produce re-describes.
        if served.redirected
            || matches!(
                &served.response,
                ClientResponse::DataPlane(DataPlaneResponse::StaleRange)
            )
        {
            self.cache.invalidate(topic);
        }
        match served.response {
            ClientResponse::DataPlane(DataPlaneResponse::Produced { entry_id }) => Ok(entry_id),
            ClientResponse::DataPlane(DataPlaneResponse::StaleRange) => {
                Err(ClientError::StaleRange)
            }
            _ => Err(ClientError::UnexpectedResponse),
        }
    }

    /// Drive `request` to completion from `start`, following redirects until the
    /// deadline. Returns the serving response or a terminal error.
    pub(crate) async fn call(
        &self,
        start: SocketAddr,
        request: impl Into<ClientRequest>,
    ) -> Result<Served, ClientError> {
        let mut state = RetryState::new(start, &self.retry);
        let mut last_error: Option<String> = None;

        let request = request.into();
        loop {
            // Budget spent: not-found if that's all we saw, else timeout.
            if Instant::now() >= state.deadline {
                if state.saw_not_found {
                    return Err(ClientError::TopicNotFound);
                }
                return Err(ClientError::Timeout {
                    waited: self.retry.deadline,
                    last_error,
                });
            }

            // Cap the attempt at the remaining budget: a request on an established
            // connection awaits a reply with no inner deadline, so a node that never
            // answers (parked reply, half-open socket) can't outlast the call.
            let attempt =
                tokio::time::timeout(state.budget(), self.pool.send(state.addr, request.clone()));

            let attempt_res = attempt.await;

            if attempt_res.is_err() {
                last_error = Some("request attempt timed out".to_string());
                continue;
            }

            let res = attempt_res.unwrap();

            match res {
                Ok(response) => {
                    let is_direct_to_node = matches!(
                        request,
                        ClientRequest::DataPlane(ClientDataPlaneRequest::FetchById(_))
                            | ClientRequest::DataPlane(ClientDataPlaneRequest::ListOffsets(_))
                    );

                    let redirect = if is_direct_to_node {
                        match &response {
                            ClientResponse::DataPlane(DataPlaneResponse::SegmentNotLocal) => {
                                Redirect::Done
                            }
                            ClientResponse::DataPlane(DataPlaneResponse::ShardNotLocal {
                                hint_node: None,
                            }) => Redirect::Done,
                            _ => Self::redirect_target(&response),
                        }
                    } else {
                        Self::redirect_target(&response)
                    };

                    match redirect {
                        Redirect::Done => {
                            return Ok(Served {
                                response,
                                redirected: state.redirected,
                            });
                        }
                        Redirect::Follow(next) => {
                            // The hint is a node the server just pointed us at — remember it
                            // so future re-resolves can reach it, not just the bootstrap set.
                            self.known_nodes.remember(next);
                            if state.try_follow(next) {
                                continue;
                            }
                        }
                        Redirect::NotFound => state.mark_not_found(),
                        Redirect::Reresolve => state.mark_redirected(),
                    }
                }
                // Pool dropped the dead connection; re-resolve like any transient.
                Err(ClientError::Connection { addr, reason }) => {
                    last_error = Some(format!("Connection to {} failed: {}", addr, reason));
                    state.mark_redirected()
                }
                Err(error) => return Err(error),
            }

            // Transient: back off (never past the deadline), then try a fresh seed.
            let remaining = state.budget();
            let sleep_for = state.prepare_reresolve(self.next_known_node(), self.retry.max_backoff);
            tokio::time::sleep(sleep_for.min(remaining)).await;
        }
    }

    /// Map a response to its action. All variants listed, so a new wire variant
    /// won't compile until handled.
    pub(crate) fn redirect_target(response: &ClientResponse) -> Redirect {
        match response {
            ClientResponse::ControlPlane(cp) => match cp {
                ControlPlaneResponse::TopicMetadataRedirect { owner } => {
                    Redirect::Follow(owner.client_addr())
                }
                ControlPlaneResponse::NotRaftLeader { leader_addr } => match leader_addr {
                    Some(addr) => Redirect::Follow(addr.client_addr()),
                    None => Redirect::Reresolve,
                },
                ControlPlaneResponse::TopicNotFound => Redirect::NotFound,
                ControlPlaneResponse::InternalError(_) => Redirect::Reresolve,
                ControlPlaneResponse::TopicCreated
                | ControlPlaneResponse::AlreadyExists
                | ControlPlaneResponse::TopicDeleted
                | ControlPlaneResponse::ConsumerGroupAssignment(_)
                | ControlPlaneResponse::ConsumerGroupLeft
                | ControlPlaneResponse::TopicList { .. }
                | ControlPlaneResponse::TopicDetail(_) => Redirect::Done,
            },
            ClientResponse::DataPlane(dp) => match dp {
                DataPlaneResponse::NotWriteLeader { leader_addr } => match leader_addr {
                    Some(addr) => Redirect::Follow(addr.client_addr()),
                    None => Redirect::Reresolve,
                },
                DataPlaneResponse::ShardNotLocal { hint_node } => match hint_node {
                    Some(addr) => Redirect::Follow(addr.client_addr()),
                    None => Redirect::Reresolve,
                },
                DataPlaneResponse::TopicNotFound => Redirect::NotFound,
                DataPlaneResponse::StaleRange => Redirect::Done,
                DataPlaneResponse::InternalError(_) => Redirect::Reresolve,
                DataPlaneResponse::SegmentNotLocal
                | DataPlaneResponse::Produced { .. }
                | DataPlaneResponse::Fetched { .. }
                | DataPlaneResponse::EntryIdOutOfRange
                | DataPlaneResponse::KeyspaceBoundNarrowed
                | DataPlaneResponse::RangeOffset { .. } => Redirect::Done,
                DataPlaneResponse::ConsumerOffsetCommitted
                | DataPlaneResponse::StaleConsumerGroupEpoch(_)
                | DataPlaneResponse::ConsumerOffset(_) => Redirect::Done,
            },
            ClientResponse::Admin(_) => Redirect::Done,
            // The server's writer-loop sentinel; a client never legitimately reads it.
            ClientResponse::Stop => Redirect::Reresolve,
        }
    }
}

#[cfg(test)]
impl Client {
    /// Test seam: poison the cached write leader for `topic` to a wrong (but live)
    /// node, to exercise stale-cache self-correction.
    pub(crate) fn poison_cache(&self, topic: &str, wrong: NodeAddressInfo) {
        self.cache.poison_write_leaders(topic, wrong);
    }

    /// Test seam: read the currently-cached write leader for a routing key.
    pub(crate) fn cached_write_leader(&self, topic: &str, key: &[u8]) -> Option<NodeAddressInfo> {
        self.cache.write_leader(topic, key)
    }

    /// Test seam: how many nodes the contact pool currently knows about.
    pub(crate) fn known_node_count(&self) -> usize {
        self.known_nodes.len()
    }
}
