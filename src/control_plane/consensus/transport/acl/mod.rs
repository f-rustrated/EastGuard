mod message;

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

use crate::control_plane::NodeAddressInfo;
use crate::control_plane::NodeId;
use crate::control_plane::consensus::raft::states::security::AclRecord;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::AclResource;
use crate::net::TransportTcpStream;
use crate::security::NodeTransportSecurity;
use message::*;

use super::inbound::ClusterMessageReader;
use super::protocol::{AclSnapshotRequest, InitialClusterMessage, encode_frame};

const ACL_SNAPSHOT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
const MAX_IN_FLIGHT_FETCHES: usize = 16;
const MAX_QUEUED_FETCHES: usize = 128;
const MAX_WAITERS_PER_FETCH: usize = 256;
const ACL_SNAPSHOT_MAILBOX_CAPACITY: usize = 256;

/// Bounded async boundary for remote ACL cache refreshes.
///
/// A cache miss never opens a socket from a client-request task directly. This
/// actor limits concurrent dials and makes all callers waiting for the same
/// record share one read and one response.
pub(crate) struct AclSnapshotActor;

impl AclSnapshotActor {
    pub(crate) fn spawn(security: NodeTransportSecurity) -> AclSnapshotSender {
        let (sender, mailbox) = mpsc::channel(ACL_SNAPSHOT_MAILBOX_CAPACITY);
        tokio::spawn(Self::run(security, mailbox));
        AclSnapshotSender(sender)
    }

    async fn run(
        security: NodeTransportSecurity,
        mut mailbox: mpsc::Receiver<AclSnapshotFetchRequest>,
    ) {
        let (completed_tx, mut completed_rx) =
            mpsc::channel::<AclSnapshotCompleted>(MAX_IN_FLIGHT_FETCHES);
        let mut state = AclSnapshotState::new(security);
        let mut requests = Vec::with_capacity(64);

        loop {
            tokio::select! {
                count = mailbox.recv_many(&mut requests, 64) => {
                    if count == 0 {
                        break;
                    }
                    for request in requests.drain(..) {
                        state.request_snapshot(request);
                    }
                }
                Some(completed) = completed_rx.recv() => {
                    state.complete(completed);
                }
            }

            for fetch in state.take_pending() {
                let completed_tx = completed_tx.clone();
                let sec = state.security.clone();
                tokio::spawn(async move {
                    let key = fetch.key.clone();
                    let snapshot = AclSnapshotActor::fetch_snapshot(sec, fetch).await;
                    let _ = completed_tx
                        .send(AclSnapshotCompleted { key, snapshot })
                        .await;
                });
            }
        }
    }

    async fn fetch_snapshot(
        security: NodeTransportSecurity,
        fetch: AclSnapshotFetch,
    ) -> Option<AclRecord> {
        let owner_id = fetch.owner.node_id.clone();
        match tokio::time::timeout(
            ACL_SNAPSHOT_TIMEOUT,
            Self::fetch_snapshot_inner(security, fetch),
        )
        .await
        {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(error)) => {
                tracing::debug!(owner = %owner_id, "ACL snapshot fetch failed: {error}");
                None
            }
            Err(_) => {
                tracing::debug!(owner = %owner_id, "ACL snapshot fetch timed out");
                None
            }
        }
    }

    async fn fetch_snapshot_inner(
        security: NodeTransportSecurity,
        fetch: AclSnapshotFetch,
    ) -> Result<Option<AclRecord>> {
        let stream =
            TransportTcpStream::connect_node(fetch.owner.cluster_addr(), &security).await?;
        let transport_identity = stream.peer_identity();
        let (read_half, mut write_half) = stream.into_split();
        write_half
            .write_all(&encode_frame(&InitialClusterMessage::AclSnapshot(
                AclSnapshotRequest {
                    requester_node_id: fetch.node_id,
                    shard_group_id: fetch.key.shard_group_id,
                    resource: fetch.key.resource,
                },
            ))?)
            .await
            .context("write initial ACL snapshot request")?;

        let mut reader = ClusterMessageReader::new(read_half, transport_identity);
        Ok(reader.read_acl_snapshot_response().await?.snapshot)
    }
}

/// Scheduling state for bounded, coalesced snapshot reads.
struct AclSnapshotState {
    security: NodeTransportSecurity,
    active: HashSet<AclSnapshotKey>,
    queued: VecDeque<AclSnapshotFetch>,
    waiters: HashMap<AclSnapshotKey, Vec<oneshot::Sender<Option<AclRecord>>>>,
    pending_events: Vec<AclSnapshotFetch>,
}

impl AclSnapshotState {
    fn new(security: NodeTransportSecurity) -> Self {
        Self {
            security,
            active: HashSet::with_capacity(MAX_IN_FLIGHT_FETCHES),
            queued: VecDeque::with_capacity(MAX_QUEUED_FETCHES),
            waiters: HashMap::with_capacity(MAX_WAITERS_PER_FETCH),
            pending_events: Vec::new(),
        }
    }

    fn request_snapshot(&mut self, request: AclSnapshotFetchRequest) {
        let AclSnapshotFetchRequest { fetch, reply } = request;

        if let Some(waiting) = self.waiters.get_mut(&fetch.key) {
            if waiting.len() == MAX_WAITERS_PER_FETCH {
                tracing::debug!(?fetch.key, "ACL snapshot refresh has too many waiting callers");
                let _ = reply.send(None);
                return;
            }
            waiting.push(reply);
            return;
        }

        if self.active.len() < MAX_IN_FLIGHT_FETCHES {
            self.active.insert(fetch.key.clone());
            self.waiters.insert(fetch.key.clone(), vec![reply]);
            self.pending_events.push(fetch);
            return;
        }
        if self.queued.len() < MAX_QUEUED_FETCHES {
            self.waiters.insert(fetch.key.clone(), vec![reply]);
            self.queued.push_back(fetch);
            return;
        }
        tracing::debug!(?fetch.key, "ACL snapshot refresh queue is full");
        let _ = reply.send(None);
    }

    fn complete(&mut self, completed: AclSnapshotCompleted) {
        debug_assert!(self.active.remove(&completed.key));
        if let Some(waiting) = self.waiters.remove(&completed.key) {
            for reply in waiting {
                let _ = reply.send(completed.snapshot.clone());
            }
        } else {
            tracing::debug!("ACL snapshot completed without waiting callers");
        }
        if let Some(next) = self.queued.pop_front() {
            self.active.insert(next.key.clone());
            self.pending_events.push(next);
        }
    }

    fn take_pending(&mut self) -> Vec<AclSnapshotFetch> {
        std::mem::take(&mut self.pending_events)
    }
}

/// Sends bounded ACL snapshot refresh requests to [`AclSnapshotActor`].
#[derive(Clone)]
pub(crate) struct AclSnapshotSender(mpsc::Sender<AclSnapshotFetchRequest>);

impl AclSnapshotSender {
    /// Reads one committed ACL record from a shard host.
    ///
    /// A saturated or stopped refresh actor fails closed. The actor logs the
    /// reason and coalesces concurrent reads for the same shard resource.
    pub(crate) async fn fetch(
        &self,
        node_id: NodeId,
        owner: NodeAddressInfo,
        shard_group_id: ShardGroupId,
        resource: AclResource,
    ) -> Option<AclRecord> {
        let (reply, recv) = oneshot::channel();
        match self.0.try_send(AclSnapshotFetchRequest {
            fetch: AclSnapshotFetch::new(node_id, shard_group_id, resource, owner),
            reply,
        }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::debug!("ACL snapshot refresh actor mailbox is full");
                return None;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!("ACL snapshot refresh actor is stopped");
                return None;
            }
        }
        match tokio::time::timeout(ACL_SNAPSHOT_TIMEOUT, recv).await {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(_)) => {
                tracing::debug!("ACL snapshot refresh actor stopped before replying");
                None
            }
            Err(_) => {
                tracing::debug!("ACL snapshot refresh did not complete before its deadline");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::NodeAddress;
    use crate::control_plane::metadata::TopicId;
    use std::net::SocketAddr;
    use tokio::sync::oneshot;

    fn fetch(topic_id: u64) -> AclSnapshotFetch {
        AclSnapshotFetch {
            node_id: NodeId::new("requester"),
            key: AclSnapshotKey {
                shard_group_id: ShardGroupId(42),
                resource: AclResource::TopicData(TopicId(topic_id)),
            },
            owner: NodeAddressInfo::new(
                NodeId::new("owner"),
                NodeAddress::test(
                    "127.0.0.1:9000".parse::<SocketAddr>().unwrap(),
                    "127.0.0.1:9001".parse::<SocketAddr>().unwrap(),
                ),
            ),
        }
    }

    fn request(
        topic_id: u64,
    ) -> (
        AclSnapshotFetchRequest,
        oneshot::Receiver<Option<AclRecord>>,
    ) {
        let (reply, receiver) = oneshot::channel();
        (
            AclSnapshotFetchRequest {
                fetch: fetch(topic_id),
                reply,
            },
            receiver,
        )
    }

    #[test]
    fn joins_identical_fetches_without_starting_another_dial() {
        let mut state = AclSnapshotState::new(NodeTransportSecurity::TrustedDevelopment);
        let (first_request, mut first_reply) = request(7);
        let (second_request, mut second_reply) = request(7);

        state.request_snapshot(first_request);
        state.request_snapshot(second_request);

        assert_eq!(state.take_pending().len(), 1);
        state.complete(AclSnapshotCompleted {
            key: fetch(7).key,
            snapshot: None,
        });
        assert_eq!(first_reply.try_recv(), Ok(None));
        assert_eq!(second_reply.try_recv(), Ok(None));
    }

    #[test]
    fn completion_starts_the_next_queued_fetch() {
        let mut state = AclSnapshotState::new(NodeTransportSecurity::TrustedDevelopment);
        let first = fetch(0);
        let (first_request, _first_reply) = request(0);
        state.request_snapshot(first_request);
        for topic_id in 1..MAX_IN_FLIGHT_FETCHES as u64 {
            let (request, _reply) = request(topic_id);
            state.request_snapshot(request);
        }
        let (queued_request, _queued_reply) = request(99);
        state.request_snapshot(queued_request);
        assert_eq!(state.take_pending().len(), MAX_IN_FLIGHT_FETCHES);

        state.complete(AclSnapshotCompleted {
            key: first.key,
            snapshot: None,
        });

        let started = state.take_pending();
        assert_eq!(started.len(), 1);
        let [started] = started.as_slice() else {
            panic!("expected one fetch event");
        };
        assert_eq!(started.key, fetch(99).key);
    }

    #[test]
    fn rejects_a_new_fetch_when_the_queue_is_full() {
        let mut state = AclSnapshotState::new(NodeTransportSecurity::TrustedDevelopment);
        for topic_id in 0..MAX_IN_FLIGHT_FETCHES as u64 + MAX_QUEUED_FETCHES as u64 {
            let (request, _reply) = request(topic_id);
            state.request_snapshot(request);
        }

        let (request, mut reply) = request(999);
        state.request_snapshot(request);
        assert_eq!(reply.try_recv(), Ok(None));
    }
}
