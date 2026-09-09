use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{Context as _, Result};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};

use super::inner;
use crate::security::{CertificatePrincipal, NodeTransportSecurity, node_certificate_principal};

macro_rules! tcp_wrapper {
    ($name:ident) => {
        pub struct $name(pub(super) inner::$name);

        impl Deref for $name {
            type Target = inner::$name;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}

tcp_wrapper!(TcpListener);
tcp_wrapper!(TcpStream);
tcp_wrapper!(OwnedReadHalf);
tcp_wrapper!(OwnedWriteHalf);

/// Mutually authenticated node connection used by the Raft and data transports.
///
/// TLS authenticates the certificate chain before this stream exposes the peer's
/// Node Certificate Principal. The transport identity exchange binds the
/// claimed node ID to that certificate's namespace.
pub struct AuthenticatedTcpStream {
    peer_principal: CertificatePrincipal,
    stream: TlsStream<TcpStream>,
}

impl AuthenticatedTcpStream {
    pub async fn connect<A: inner::ToSocketAddrs>(
        addr: A,
        config: Arc<rustls::ClientConfig>,
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        // NodeServerCertVerifier authenticates the certificate chain and node
        // principal. It intentionally does not use DNS-name matching.
        let server_name = ServerName::try_from("unused.eastguard")?;
        let stream = TlsConnector::from(config)
            .connect(server_name, stream)
            .await?;
        Self::from_tls_stream(stream.into(), node_certificate_principal)
    }

    fn from_tls_stream(
        stream: TlsStream<TcpStream>,
        read_principal: fn(&rustls::pki_types::CertificateDer<'_>) -> Result<CertificatePrincipal>,
    ) -> Result<Self> {
        let certificate = stream
            .get_ref()
            .1
            .peer_certificates()
            .and_then(|certificates| certificates.first())
            .context("authenticated TLS peer supplied no certificate")?;
        let peer_principal = read_principal(certificate)?;
        Ok(Self {
            peer_principal,
            stream,
        })
    }

    pub fn peer_principal(&self) -> &CertificatePrincipal {
        &self.peer_principal
    }
}

/// TCP transport connection. Secure mode carries an authenticated certificate
/// principal; trusted-development mode preserves the existing plaintext path.
pub enum TransportTcpStream {
    TrustedDevelopment(TcpStream),
    Secure(Box<AuthenticatedTcpStream>),
}

pub enum TransportReadHalf {
    TrustedDevelopment(OwnedReadHalf),
    Secure(tokio::io::ReadHalf<TlsStream<TcpStream>>),
}

pub enum TransportWriteHalf {
    TrustedDevelopment(OwnedWriteHalf),
    Secure(tokio::io::WriteHalf<TlsStream<TcpStream>>),
}

impl From<OwnedReadHalf> for TransportReadHalf {
    fn from(value: OwnedReadHalf) -> Self {
        Self::TrustedDevelopment(value)
    }
}

impl From<OwnedWriteHalf> for TransportWriteHalf {
    fn from(value: OwnedWriteHalf) -> Self {
        Self::TrustedDevelopment(value)
    }
}

impl TransportTcpStream {
    pub async fn accept(
        stream: TcpStream,
        security: &NodeTransportSecurity,
        read_principal: fn(&rustls::pki_types::CertificateDer<'_>) -> Result<CertificatePrincipal>,
        handshake_timeout: Duration,
    ) -> Result<Self> {
        match security {
            NodeTransportSecurity::Secure(security) => {
                let stream = tokio::time::timeout(
                    handshake_timeout,
                    TlsAcceptor::from(security.server_config()).accept(stream),
                )
                .await
                .context("TLS handshake timed out")??;
                AuthenticatedTcpStream::from_tls_stream(stream.into(), read_principal)
                    .map(Box::new)
                    .map(Self::Secure)
            }
            NodeTransportSecurity::TrustedDevelopment => Ok(Self::TrustedDevelopment(stream)),
        }
    }

    pub async fn connect_node<A: inner::ToSocketAddrs>(
        addr: A,
        security: &NodeTransportSecurity,
    ) -> Result<Self> {
        match security {
            NodeTransportSecurity::Secure(security) => {
                AuthenticatedTcpStream::connect(addr, security.client_config())
                    .await
                    .map(Box::new)
                    .map(Self::Secure)
            }
            NodeTransportSecurity::TrustedDevelopment => TcpStream::connect(addr)
                .await
                .map(Self::TrustedDevelopment)
                .map_err(Into::into),
        }
    }

    pub fn peer_principal(&self) -> Option<CertificatePrincipal> {
        match self {
            Self::Secure(stream) => Some(stream.peer_principal().clone()),
            Self::TrustedDevelopment(_) => None,
        }
    }

    pub fn into_split(self) -> (TransportReadHalf, TransportWriteHalf) {
        match self {
            Self::TrustedDevelopment(stream) => {
                let (read, write) = TcpStream::into_split(stream);
                (
                    TransportReadHalf::TrustedDevelopment(read),
                    TransportWriteHalf::TrustedDevelopment(write),
                )
            }
            Self::Secure(stream) => {
                let (read, write) = tokio::io::split(stream.stream);
                (
                    TransportReadHalf::Secure(read),
                    TransportWriteHalf::Secure(write),
                )
            }
        }
    }
}

impl AsyncRead for TransportTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::TrustedDevelopment(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Secure(stream) => Pin::new(&mut stream.stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TransportTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            Self::TrustedDevelopment(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Secure(stream) => Pin::new(&mut stream.stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::TrustedDevelopment(stream) => Pin::new(stream).poll_flush(cx),
            Self::Secure(stream) => Pin::new(&mut stream.stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::TrustedDevelopment(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Secure(stream) => Pin::new(&mut stream.stream).poll_shutdown(cx),
        }
    }
}

impl AsyncRead for TransportReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::TrustedDevelopment(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Secure(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TransportWriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            Self::TrustedDevelopment(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Secure(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::TrustedDevelopment(stream) => Pin::new(stream).poll_flush(cx),
            Self::Secure(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Self::TrustedDevelopment(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Secure(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl AsyncRead for OwnedReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for OwnedWriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl TcpListener {
    pub async fn bind<A: inner::ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        let listener = inner::TcpListener::bind(addr).await?;
        Ok(Self(listener))
    }

    pub async fn accept(&self) -> std::io::Result<(TcpStream, std::net::SocketAddr)> {
        let (stream, addr) = self.0.accept().await?;
        Ok((TcpStream(stream), addr))
    }
}

impl TcpStream {
    #[allow(dead_code)]
    pub async fn connect<A: inner::ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        let stream = inner::TcpStream::connect(addr).await?;
        Ok(Self(stream))
    }

    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let (read_half, write_half) = self.0.into_split();
        (OwnedReadHalf(read_half), OwnedWriteHalf(write_half))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::client_certificate_principal;
    use rcgen::string::Ia5String;
    use rcgen::{CertificateParams, KeyPair, SanType};
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::{ClientConfig, RootCertStore};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use turmoil::Builder;

    fn certificate(
        principal_kind: &str,
        principal: &str,
        dns_name: Option<&str>,
    ) -> (
        rustls::pki_types::CertificateDer<'static>,
        PrivateKeyDer<'static>,
    ) {
        let mut params = CertificateParams::default();
        params.subject_alt_names.push(SanType::URI(
            Ia5String::try_from(format!("urn:eastguard:{principal_kind}:{principal}")).unwrap(),
        ));
        if let Some(dns_name) = dns_name {
            params
                .subject_alt_names
                .push(SanType::DnsName(Ia5String::try_from(dns_name).unwrap()));
        }
        let key = KeyPair::generate().unwrap();
        let certificate = params.self_signed(&key).unwrap().der().clone();
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der()));
        (certificate, key)
    }

    fn tls_configs() -> (NodeTransportSecurity, Arc<ClientConfig>) {
        let (server_certificate, server_key) =
            certificate("node", "broker-server", Some("unused.eastguard"));
        let (client_certificate, client_key) = certificate("node", "broker-client", None);

        let server = NodeTransportSecurity::test_secure(
            vec![server_certificate.clone()],
            server_key,
            std::slice::from_ref(&client_certificate),
        )
        .unwrap();

        let mut server_roots = RootCertStore::empty();
        server_roots.add(server_certificate).unwrap();
        let client = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(server_roots)
            .with_client_auth_cert(vec![client_certificate], client_key)
            .unwrap();
        (server, Arc::new(client))
    }

    #[test]
    fn mutual_tls_authenticates_both_nodes_under_turmoil() -> turmoil::Result {
        let (server_config, client_config) = tls_configs();
        let mut sim = Builder::new().build();

        sim.host("server", move || {
            let server_config = server_config.clone();
            async move {
                let listener = TcpListener::bind("0.0.0.0:9000").await?;
                let (stream, _) = listener.accept().await?;
                let stream = TransportTcpStream::accept(
                    stream,
                    &server_config,
                    node_certificate_principal,
                    Duration::from_secs(1),
                )
                .await?;
                assert_eq!(stream.peer_principal().unwrap().as_ref(), "broker-client");
                let (mut reader, mut writer) = stream.into_split();
                let mut message = [0; 4];
                reader.read_exact(&mut message).await?;
                assert_eq!(&message, b"ping");
                writer.write_all(b"pong").await?;
                writer.flush().await?;
                Ok(())
            }
        });

        sim.client("client", async move {
            let stream =
                AuthenticatedTcpStream::connect((turmoil::lookup("server"), 9000), client_config)
                    .await
                    .unwrap();
            assert_eq!(stream.peer_principal().as_ref(), "broker-server");
            let mut stream = TransportTcpStream::Secure(Box::new(stream));
            stream.write_all(b"ping").await?;
            stream.flush().await?;
            let mut message = [0; 4];
            stream.read_exact(&mut message).await?;
            assert_eq!(&message, b"pong");
            Ok(())
        });

        sim.run()
    }

    #[test]
    fn mutual_tls_exposes_client_principal_under_turmoil() -> turmoil::Result {
        let (server_certificate, server_key) =
            certificate("node", "broker-server", Some("unused.eastguard"));
        let (client_certificate, client_key) = certificate("client", "producer-a", None);

        let server_config = NodeTransportSecurity::test_secure(
            vec![server_certificate.clone()],
            server_key,
            std::slice::from_ref(&client_certificate),
        )?;

        let mut server_roots = RootCertStore::empty();
        server_roots.add(server_certificate).unwrap();
        let client_config = Arc::new(
            ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_root_certificates(server_roots)
                .with_client_auth_cert(vec![client_certificate], client_key)
                .unwrap(),
        );

        let mut sim = Builder::new().build();
        sim.host("server", move || {
            let server_config = server_config.clone();
            async move {
                let listener = TcpListener::bind("0.0.0.0:9000").await?;
                let (stream, _) = listener.accept().await?;
                let stream = TransportTcpStream::accept(
                    stream,
                    &server_config,
                    client_certificate_principal,
                    Duration::from_secs(1),
                )
                .await?;
                assert_eq!(stream.peer_principal().unwrap().as_ref(), "producer-a");
                Ok(())
            }
        });
        sim.client("client", async move {
            let stream =
                AuthenticatedTcpStream::connect((turmoil::lookup("server"), 9000), client_config)
                    .await
                    .unwrap();
            assert_eq!(stream.peer_principal().as_ref(), "broker-server");
            Ok(())
        });

        sim.run()
    }

    #[test]
    fn accept_uses_the_callers_tls_timeout() -> turmoil::Result {
        let (security, _) = tls_configs();
        for timeout_ms in [25, 75] {
            let handshake_timeout = Duration::from_millis(timeout_ms);
            let mut sim = Builder::new().rng_seed(7).build();
            sim.host("server", {
                let security = security.clone();
                move || {
                    let security = security.clone();
                    async move {
                        let listener = TcpListener::bind("0.0.0.0:9000").await?;
                        let (stream, _) = listener.accept().await?;
                        let started = tokio::time::Instant::now();
                        let Err(error) = TransportTcpStream::accept(
                            stream,
                            &security,
                            node_certificate_principal,
                            handshake_timeout,
                        )
                        .await
                        else {
                            panic!("a silent peer must not finish TLS");
                        };
                        assert!(
                            error
                                .downcast_ref::<tokio::time::error::Elapsed>()
                                .is_some()
                        );
                        assert!(started.elapsed() >= handshake_timeout);
                        assert!(started.elapsed() < handshake_timeout + Duration::from_millis(10));
                        Ok(())
                    }
                }
            });
            sim.client("silent", async move {
                let mut stream = TcpStream::connect((turmoil::lookup("server"), 9000)).await?;
                let mut byte = [0; 1];
                assert_eq!(
                    tokio::time::timeout(
                        handshake_timeout + Duration::from_millis(100),
                        stream.read(&mut byte),
                    )
                    .await??,
                    0,
                    "the timed-out handshake must close its socket",
                );
                Ok(())
            });
            sim.run()?;
        }
        Ok(())
    }
}
