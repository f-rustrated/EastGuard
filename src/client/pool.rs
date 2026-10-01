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

        tokio::spawn(Self::write_loop(addr, ClientRawWriter::new(write_half), rx));
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
    async fn write_loop(
        addr: SocketAddr,
        mut writer: ClientRawWriter,
        mut rx: mpsc::Receiver<(u64, ClientRequest)>,
    ) {
        while let Some((id, req)) = rx.recv().await {
            if let Err(error) = writer.write(id, &req).await {
                tracing::error!(
                    request_kind = req.kind(),
                    request_id = id,
                    destination = %addr,
                    %error,
                    "client write loop closed"
                );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connections::MAX_FRAME_SIZE;
    use crate::connections::protocol::ProduceRequest;
    use crate::control_plane::metadata::RangeId;
    use crate::net::TcpListener;
    use std::sync::Mutex;
    use tracing::instrument::WithSubscriber;

    #[derive(Clone, Default)]
    struct LogCapture(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for LogCapture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn failed_writes_log_context_without_request_payloads() -> turmoil::Result {
        let mut sim = turmoil::Builder::new().rng_seed(7).build();
        sim.client("writer", async {
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let destination = SocketAddr::new(turmoil::lookup("writer"), 9000);
            let (client, server) = tokio::join!(TcpStream::connect(destination), listener.accept());
            let _server = server?;
            let (_, write_half) = client?.into_split();
            let (tx, rx) = mpsc::channel(1);
            let mut payload = vec![0; MAX_FRAME_SIZE];
            payload[..16].copy_from_slice(b"private-payload!");
            tx.send((
                73,
                ProduceRequest {
                    topic_name: "private-topic".into(),
                    range_id: RangeId(0),
                    routing_key: b"private-key".to_vec(),
                    data: payload.into(),
                    record_count: 1,
                    producer_identity: None,
                }
                .into(),
            ))
            .await?;
            let captured = LogCapture::default();
            let writer = captured.clone();
            let subscriber = tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish();
            NodeConnection::write_loop(destination, ClientRawWriter::new(write_half), rx)
                .with_subscriber(subscriber)
                .await;
            let log = String::from_utf8(captured.0.lock().unwrap().clone())?;
            assert!(log.contains("request_kind=\"Produce\""));
            assert!(log.contains("request_id=73"));
            assert!(log.contains(&format!("destination={destination}")));
            assert!(log.contains("error=Message exceeds the 4 MiB frame limit"));
            assert!(!log.contains("private-"));
            assert!(
                !log.contains("112, 114, 105, 118"),
                "payload bytes leaked: {log}"
            );
            Ok(())
        });
        sim.run()
    }
}
