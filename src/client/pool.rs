//! Connection pool — one multiplexed TCP connection per node, keyed by address.
//! A `NodeConnection` carries many requests in flight on one socket; each send takes
//! a request id and the read loop demuxes responses back by that id (see
//! `connections::reader`). Dials lazily; drops a connection when its read loop dies.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};

use crate::client::error::ClientError;
use crate::connections::protocol::{ClientRequest, ClientResponse};
use crate::connections::reader::ClientStreamReader;
use crate::connections::writer::ClientRawWriter;
use crate::net::TcpStream;

/// Responses arrive out of order.
/// So we shouldn't assume "pipeline"-ed data arrival with the use of VecDeque.
type Inflight = Arc<DashMap<u64, oneshot::Sender<ClientResponse>>>;

/// Dial cap, so an unreachable node fails fast and the redirect loop bounces instead
/// of blocking forever on a crashed host.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// One multiplexed connection to a single node.
pub(crate) struct NodeConnection {
    addr: SocketAddr,
    outbound: mpsc::Sender<(u64, ClientRequest)>,
    inflight: Inflight,
    next_id: AtomicU64,
    alive: Arc<AtomicBool>,
}

impl NodeConnection {
    /// Dial `addr` and spawn the writer + reader loops. Fails only if the dial fails;
    /// a later peer death surfaces on the next `send`.
    async fn connect(addr: SocketAddr) -> Result<Self, ClientError> {
        let dial = ClientError::Connection {
            addr,
            reason: "connect timed out".into(),
        };
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| dial)?
            .map_err(|e| ClientError::Connection {
                addr,
                reason: e.to_string(),
            })?;

        let (read_half, write_half) = stream.into_split();

        let (outbound, rx) = mpsc::channel(128);
        let inflight: Inflight = Arc::new(DashMap::new());
        let alive = Arc::new(AtomicBool::new(true));

        tokio::spawn(Self::write_loop(ClientRawWriter::new(write_half), rx));
        tokio::spawn(Self::read_loop(
            ClientStreamReader::new(read_half),
            inflight.clone(),
            alive.clone(),
        ));

        Ok(Self {
            addr,
            outbound,
            inflight,
            next_id: AtomicU64::new(0),
            alive,
        })
    }

    /// Drains the outbound queue onto the wire. Ends when the connection is dropped
    /// (all senders gone) or a write fails — either way the reader observes the close.
    async fn write_loop(mut writer: ClientRawWriter, mut rx: mpsc::Receiver<(u64, ClientRequest)>) {
        while let Some((id, req)) = rx.recv().await {
            if writer.write(id, &req).await.is_err() {
                tracing::error!("client {:?} write loop closes...", req);
                break;
            }
        }
    }

    /// Demultiplexes responses to waiters by request id until the socket closes,
    /// then marks the connection dead and drops every outstanding waiter (so their
    /// callers see `Connection`).
    async fn read_loop(mut reader: ClientStreamReader, inflight: Inflight, alive: Arc<AtomicBool>) {
        while let Ok((id, response)) = reader.read_request::<ClientResponse>().await {
            if let Some((_, waiter)) = inflight.remove(&id) {
                let _ = waiter.send(response);
            }
        }
        // Teardown: Mark dead first, then clear the map to drop all pending waiters.
        // Because we aren't using a rigid Mutex, we rely on memory ordering.
        alive.store(false, Ordering::Release);
        inflight.clear();
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    fn dead(&self) -> ClientError {
        ClientError::Connection {
            addr: self.addr,
            reason: "connection closed".into(),
        }
    }

    /// Send one request, await its matching response. Concurrent callers share the
    /// socket — responses are correlated by request id, not arrival order.
    async fn send(&self, request: ClientRequest) -> Result<ClientResponse, ClientError> {
        // 1. Initial liveness check
        if !self.alive.load(Ordering::Acquire) {
            return Err(self.dead());
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (waiter_tx, waiter_rx) = oneshot::channel();

        // 2. Insert into concurrent map
        self.inflight.insert(id, waiter_tx);

        // 3. Double-check liveness AFTER insert.
        // This prevents the race condition where read_loop crashes and clears the map
        // right before we inserted our waiter, which would leave us hanging forever.
        if !self.alive.load(Ordering::Acquire) {
            self.inflight.remove(&id);
            return Err(self.dead());
        }

        if self.outbound.send((id, request)).await.is_err() {
            self.inflight.remove(&id);
            return Err(self.dead());
        }

        // Resolves on the matching response, or errors when teardown drops the waiter.
        waiter_rx.await.map_err(|_| self.dead())
    }
}

/// Lazy, wait-free pool of multiplexed connections keyed by node address.
pub(crate) struct ConnectionPool {
    connections: ArcSwap<HashMap<SocketAddr, Arc<NodeConnection>>>,
}

impl ConnectionPool {
    pub(crate) fn new() -> Self {
        Self {
            connections: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    fn live(&self, addr: SocketAddr) -> Option<Arc<NodeConnection>> {
        self.connections
            .load()
            .get(&addr)
            .filter(|c| c.is_alive())
            .cloned()
    }

    /// Reuse a live connection or dial a new one. The dial happens outside the lock,
    /// so a concurrent dial to the same node is possible — the loser is simply dropped.
    async fn get(&self, addr: SocketAddr) -> Result<Arc<NodeConnection>, ClientError> {
        if let Some(conn) = self.live(addr) {
            return Ok(conn);
        }

        let new_conn = Arc::new(NodeConnection::connect(addr).await?);

        self.connections.rcu(|current| {
            if current.get(&addr).is_some_and(|c| c.is_alive()) {
                return current.clone();
            }
            let mut next = (**current).clone();
            next.insert(addr, new_conn.clone());
            Arc::new(next)
        });

        // We bypass `self.live()` here to avoid the `.is_alive()` filter.
        // If the connection died between insertion and this read, we return it anyway.
        // The caller (`send`) will observe it's dead, fail gracefully, and evict it.
        let winner = self.connections.load().get(&addr).cloned();
        Ok(winner.unwrap_or(new_conn))
    }

    /// Send a request to `addr`, opening the connection if needed. On failure the dead
    /// entry is dropped so the next attempt redials.
    pub(crate) async fn send(
        &self,
        addr: SocketAddr,
        request: ClientRequest,
    ) -> Result<ClientResponse, ClientError> {
        let conn = self.get(addr).await?;
        let result = conn.send(request).await;
        if result.is_err() {
            self.evict_dead(addr);
        }
        result
    }

    fn evict_dead(&self, addr: SocketAddr) {
        self.connections.rcu(|current| {
            if current.get(&addr).is_some_and(|c| !c.is_alive()) {
                let mut next = (**current).clone();
                next.remove(&addr);
                Arc::new(next)
            } else {
                current.clone()
            }
        });
    }
}
