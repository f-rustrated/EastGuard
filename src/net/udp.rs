#![allow(dead_code)]
use super::inner;
use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};
use std::fmt;
use std::future::Future;
use std::io::{self, IoSliceMut};
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
type Readiness = Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;

pub struct UdpSocket(inner::UdpSocket);

impl Deref for UdpSocket {
    type Target = inner::UdpSocket;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for UdpSocket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl UdpSocket {
    pub async fn bind<A: inner::ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        let socket = inner::UdpSocket::bind(addr).await?;
        Ok(Self(socket))
    }
}

pub struct QuinnUdpSocket {
    socket: Arc<UdpSocket>,
    readable: Mutex<Option<Readiness>>,
}

impl QuinnUdpSocket {
    pub fn new(socket: UdpSocket) -> Self {
        Self {
            socket: Arc::new(socket),
            readable: Mutex::new(None),
        }
    }
}

impl fmt::Debug for QuinnUdpSocket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnUdpSocket")
            .field("local_addr", &self.socket.local_addr())
            .finish()
    }
}

impl AsyncUdpSocket for QuinnUdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(QuinnUdpPoller {
            socket: self.socket.clone(),
            writable: Mutex::new(None),
        })
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        debug_assert!(transmit.segment_size.is_none());
        self.socket
            .try_send_to(transmit.contents, transmit.destination)
            .map(|_| ())
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() || meta.is_empty() {
            return Poll::Ready(Ok(0));
        }

        loop {
            match self.socket.try_recv_from(&mut bufs[0]) {
                Ok((len, addr)) => {
                    meta[0] = RecvMeta {
                        addr,
                        len,
                        stride: len,
                        ecn: None,
                        dst_ip: None,
                    };
                    self.readable
                        .lock()
                        .expect("readable mutex poisoned")
                        .take();
                    return Poll::Ready(Ok(1));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Poll::Ready(Err(error)),
            }

            let mut readable = self.readable.lock().expect("readable mutex poisoned");
            let socket = self.socket.clone();
            let readiness = readable.get_or_insert_with(|| {
                Box::pin(async move { socket.readable().await }) as Readiness
            });

            match readiness.as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    readable.take();
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.socket.local_addr()
    }
}

struct QuinnUdpPoller {
    socket: Arc<UdpSocket>,
    writable: Mutex<Option<Readiness>>,
}

impl fmt::Debug for QuinnUdpPoller {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("QuinnUdpPoller").finish()
    }
}

impl UdpPoller for QuinnUdpPoller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut writable = self.writable.lock().expect("writable mutex poisoned");
        let socket = self.socket.clone();
        let readiness =
            writable.get_or_insert_with(|| Box::pin(async move { socket.writable().await }));

        let result = readiness.as_mut().poll(cx);
        if result.is_ready() {
            writable.take();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use bytes::Bytes;
    use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
    use rcgen::{CertifiedKey, generate_simple_self_signed};
    use rustls::RootCertStore;
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::server::WebPkiClientVerifier;
    use turmoil::Builder;

    use super::*;

    fn configs() -> (quinn::ServerConfig, quinn::ClientConfig) {
        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(["server".to_string()]).unwrap();
        let certificate = cert.der().clone();
        let private_key: PrivateKeyDer<'static> =
            PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into();

        let mut roots = RootCertStore::empty();
        roots.add(certificate.clone()).unwrap();
        let roots = Arc::new(roots);

        let client_verifier = WebPkiClientVerifier::builder(roots.clone())
            .build()
            .unwrap();
        let server_crypto =
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(vec![certificate.clone()], private_key.clone_key())
                .unwrap();
        let client_crypto =
            rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_root_certificates((*roots).clone())
                .with_client_auth_cert(vec![certificate], private_key)
                .unwrap();

        (
            quinn::ServerConfig::with_crypto(Arc::new(
                QuicServerConfig::try_from(server_crypto).unwrap(),
            )),
            quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto).unwrap())),
        )
    }

    #[test]
    fn quinn_exchanges_datagram_under_turmoil() -> turmoil::Result {
        let (server_config, client_config) = configs();
        let mut sim = Builder::new()
            .simulation_duration(Duration::from_secs(10))
            .rng_seed(1)
            .build();

        sim.host("server", move || {
            let server_config = server_config.clone();
            async move {
                let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 4433)))
                    .await
                    .unwrap();
                let endpoint = quinn::Endpoint::new_with_abstract_socket(
                    quinn::EndpointConfig::default(),
                    Some(server_config),
                    Arc::new(QuinnUdpSocket::new(socket)),
                    Arc::new(quinn::TokioRuntime),
                )
                .unwrap();

                let connection = endpoint.accept().await.unwrap().await.unwrap();
                assert_eq!(connection.read_datagram().await.unwrap(), b"hello"[..]);
                Ok(())
            }
        });

        sim.client("client", async move {
            let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 4434)))
                .await
                .unwrap();
            let mut endpoint = quinn::Endpoint::new_with_abstract_socket(
                quinn::EndpointConfig::default(),
                None,
                Arc::new(QuinnUdpSocket::new(socket)),
                Arc::new(quinn::TokioRuntime),
            )
            .unwrap();
            endpoint.set_default_client_config(client_config);

            let server = SocketAddr::new(turmoil::lookup("server"), 4433);
            let connection = endpoint.connect(server, "server").unwrap().await.unwrap();
            connection
                .send_datagram(Bytes::from_static(b"hello"))
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(())
        });

        sim.run()
    }
}
