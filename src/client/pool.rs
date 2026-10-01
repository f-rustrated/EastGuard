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
use crate::net::TransportTcpStream;

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
    async fn connect(
        addr: SocketAddr,
        tls: Option<Arc<rustls::ClientConfig>>,
    ) -> Result<Self, ClientError> {
        let dial = ClientError::Connection {
            addr,
            reason: "connect timed out".into(),
        };
        let stream = tokio::time::timeout(
            CONNECT_TIMEOUT,
            TransportTcpStream::connect_client(addr, tls),
        )
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
    pub(super) tls: Option<Arc<rustls::ClientConfig>>,
}

impl ConnectionPool {
    pub(crate) fn new() -> Self {
        Self {
            connections: ArcSwap::from_pointee(HashMap::new()),
            tls: None,
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

        let new_conn = Arc::new(NodeConnection::connect(addr, self.tls.clone()).await?);

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
    use crate::net::{TcpListener, TcpStream};
    use std::sync::Mutex;
    use tracing::instrument::WithSubscriber;

    #[test]
    fn sdk_redirects_and_reconnects_use_mutual_tls() -> turmoil::Result {
        use crate::client::{Client, RetryPolicy};
        use crate::connections::protocol::{AdminRequest, ClientSuccess, ServerError};
        use crate::control_plane::{NodeAddress, NodeAddressInfo, NodeId};
        use crate::security::{NodeTransportSecurity, client_certificate_principal};
        use rcgen::{CertificateParams, KeyPair, SanType, string::Ia5String};
        use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

        let mut sim = turmoil::Builder::new().rng_seed(27).build();
        sim.client("sdk", async {
            let ip = turmoil::lookup("sdk");
            let certificate = |uri: &str, ip_san| {
                let mut params = CertificateParams::default();
                params.subject_alt_names = vec![
                    SanType::URI(Ia5String::try_from(uri).unwrap()),
                    SanType::IpAddress(ip_san),
                ];
                let key = KeyPair::generate().unwrap();
                (
                    params.self_signed(&key).unwrap().der().clone(),
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
                )
            };
            let (client_cert, client_key) = certificate("urn:eastguard:client:reader", ip);
            let (server_cert, server_key) = certificate("urn:eastguard:node:broker", ip);
            let server = NodeTransportSecurity::test_secure(
                vec![server_cert.clone()],
                server_key,
                std::slice::from_ref(&client_cert),
            )?;
            let mut roots = rustls::RootCertStore::empty();
            roots.add(server_cert)?;
            let tls = Arc::new(
                rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                    .with_root_certificates(roots)
                    .with_client_auth_cert(vec![client_cert.clone()], client_key)?,
            );
            let first = SocketAddr::new(ip, 9000);
            let second = SocketAddr::new(ip, 9001);
            let mut tasks = Vec::new();
            for address in [first, second] {
                let listener = TcpListener::bind(("0.0.0.0", address.port())).await?;
                let server = server.clone();
                tasks.push(tokio::spawn(async move {
                    for _ in 0..2 {
                        let (stream, _) = listener.accept().await?;
                        let stream = TransportTcpStream::accept(
                            stream,
                            &server,
                            client_certificate_principal,
                            Duration::from_secs(1),
                        )
                        .await?;
                        assert_eq!(stream.peer_principal().unwrap().as_ref(), "reader");
                        let (read, write) = stream.into_split();
                        let (id, _): (_, ClientRequest) =
                            ClientStreamReader::new(read).read_request().await?;
                        let response = if address == first {
                            ClientResponse::Err(ServerError::TopicMetadataRedirect {
                                owner: NodeAddressInfo::new(
                                    NodeId::new("broker::2"),
                                    NodeAddress::test(second, second),
                                ),
                            })
                        } else {
                            ClientResponse::Ok(ClientSuccess::ClusterInfo {
                                nodes: Box::new([]),
                            })
                        };
                        ClientRawWriter::new(write).write(id, &response).await?;
                    }
                    Ok::<_, anyhow::Error>(())
                }));
            }
            let client = Client::connect_secure(vec![first], RetryPolicy::default(), tls.clone())?;
            for _ in 0..2 {
                let served = client
                    .call(first, ClientRequest::Admin(AdminRequest::DescribeCluster))
                    .await?;
                assert!(served.redirected);
                assert!(matches!(
                    served.response,
                    ClientResponse::Ok(ClientSuccess::ClusterInfo { .. })
                ));
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            for task in tasks {
                task.await??;
            }

            // A trusted certificate for another IP must still fail. No fallback
            // connection or application bytes may reach the redirected destination.
            let (wrong_cert, wrong_key) =
                certificate("urn:eastguard:node:wrong", "127.0.0.1".parse().unwrap());
            let wrong_server = NodeTransportSecurity::test_secure(
                vec![wrong_cert.clone()],
                wrong_key,
                &[client_cert],
            )?;
            let mut wrong_tls = (*tls).clone();
            let mut wrong_roots = rustls::RootCertStore::empty();
            wrong_roots.add(wrong_cert)?;
            wrong_tls.dangerous().set_certificate_verifier(
                rustls::client::WebPkiServerVerifier::builder(Arc::new(wrong_roots)).build()?,
            );
            let listener = TcpListener::bind(("0.0.0.0", first.port())).await?;
            let accept = tokio::spawn(async move {
                let (stream, _) = listener.accept().await?;
                assert!(
                    TransportTcpStream::accept(
                        stream,
                        &wrong_server,
                        client_certificate_principal,
                        Duration::from_secs(1)
                    )
                    .await
                    .is_err()
                );
                Ok::<_, anyhow::Error>(())
            });
            assert!(
                TransportTcpStream::connect_client(first, Some(Arc::new(wrong_tls)))
                    .await
                    .is_err()
            );
            accept.await??;
            Ok(())
        });
        sim.run()
    }

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
