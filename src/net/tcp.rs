use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};

use super::inner;
use crate::security::{CertificatePrincipal, NodeTransportSecurity, node_certificate_principal};

const NODE_ADMISSION_EXPORTER_LABEL: &[u8] = b"EXPORTER-EastGuard-node-admission-v1";
const NODE_ADMISSION_BINDING_BYTES: usize = 32;

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
/// Node Certificate Principal. Admission later binds that stable principal to
/// the process-specific `NodeId` carried by the transport handshake.
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

    /// Derives a value unique to this completed TLS session.
    ///
    /// Both peers derive the same bytes. Signing them binds a process-admission
    /// proof to this connection, so a captured proof cannot be replayed.
    fn admission_binding(&self) -> Result<[u8; NODE_ADMISSION_BINDING_BYTES]> {
        let output = [0; NODE_ADMISSION_BINDING_BYTES];
        match &self.stream {
            TlsStream::Client(stream) => stream.get_ref().1.export_keying_material(
                output,
                NODE_ADMISSION_EXPORTER_LABEL,
                None,
            ),
            TlsStream::Server(stream) => stream.get_ref().1.export_keying_material(
                output,
                NODE_ADMISSION_EXPORTER_LABEL,
                None,
            ),
        }
        .context("failed to derive node-admission TLS session binding")
    }

    fn into_split(
        self,
    ) -> (
        tokio::io::ReadHalf<AuthenticatedTcpStream>,
        tokio::io::WriteHalf<AuthenticatedTcpStream>,
    ) {
        tokio::io::split(self)
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
    Secure(tokio::io::ReadHalf<AuthenticatedTcpStream>),
}

pub enum TransportWriteHalf {
    TrustedDevelopment(OwnedWriteHalf),
    Secure(tokio::io::WriteHalf<AuthenticatedTcpStream>),
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
    ) -> Result<Self> {
        match security {
            NodeTransportSecurity::Secure(security) => {
                let stream = TlsAcceptor::from(security.server_config())
                    .accept(stream)
                    .await?;
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

    pub(crate) fn admission_binding(&self) -> Result<[u8; NODE_ADMISSION_BINDING_BYTES]> {
        match self {
            Self::Secure(stream) => stream.admission_binding(),
            Self::TrustedDevelopment(_) => {
                anyhow::bail!("trusted-development connections have no TLS session binding")
            }
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
                let (read, write) = AuthenticatedTcpStream::into_split(*stream);
                (
                    TransportReadHalf::Secure(read),
                    TransportWriteHalf::Secure(write),
                )
            }
        }
    }
}

macro_rules! impl_transport_io {
    ($type:ty) => {
        impl AsyncRead for $type {
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

        impl AsyncWrite for $type {
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

            fn poll_flush(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                match &mut *self {
                    Self::TrustedDevelopment(stream) => Pin::new(stream).poll_flush(cx),
                    Self::Secure(stream) => Pin::new(stream).poll_flush(cx),
                }
            }

            fn poll_shutdown(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                match &mut *self {
                    Self::TrustedDevelopment(stream) => Pin::new(stream).poll_shutdown(cx),
                    Self::Secure(stream) => Pin::new(stream).poll_shutdown(cx),
                }
            }
        }
    };
}

impl_transport_io!(TransportTcpStream);

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

impl AsyncRead for AuthenticatedTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for AuthenticatedTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
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
    use rustls::server::WebPkiClientVerifier;
    use rustls::{ClientConfig, RootCertStore, ServerConfig};
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

    fn tls_configs() -> (Arc<ServerConfig>, Arc<ClientConfig>) {
        let (server_certificate, server_key) =
            certificate("node", "broker-server", Some("unused.eastguard"));
        let (client_certificate, client_key) = certificate("node", "broker-client", None);

        let mut client_roots = RootCertStore::empty();
        client_roots.add(client_certificate.clone()).unwrap();
        let client_verifier = WebPkiClientVerifier::builder(Arc::new(client_roots))
            .build()
            .unwrap();
        let server = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(vec![server_certificate.clone()], server_key)
            .unwrap();

        let mut server_roots = RootCertStore::empty();
        server_roots.add(server_certificate).unwrap();
        let client = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(server_roots)
            .with_client_auth_cert(vec![client_certificate], client_key)
            .unwrap();
        (Arc::new(server), Arc::new(client))
    }

    #[test]
    fn mutual_tls_shares_session_binding_under_turmoil() -> turmoil::Result {
        let (server_config, client_config) = tls_configs();
        let mut sim = Builder::new().build();

        sim.host("server", move || {
            let server_config = server_config.clone();
            async move {
                let listener = TcpListener::bind("0.0.0.0:9000").await?;
                let (stream, _) = listener.accept().await?;
                let stream = TlsAcceptor::from(server_config).accept(stream).await?;
                let mut stream = AuthenticatedTcpStream::from_tls_stream(
                    stream.into(),
                    node_certificate_principal,
                )?;
                assert_eq!(stream.peer_principal().as_ref(), "broker-client");
                let session_binding = stream.admission_binding().unwrap();
                let mut message = [0; 4];
                stream.read_exact(&mut message).await?;
                assert_eq!(&message, b"ping");
                stream.write_all(b"pong").await?;
                stream.write_all(&session_binding).await?;
                Ok(())
            }
        });

        sim.client("client", async move {
            let mut stream =
                AuthenticatedTcpStream::connect((turmoil::lookup("server"), 9000), client_config)
                    .await
                    .unwrap();
            assert_eq!(stream.peer_principal().as_ref(), "broker-server");
            let session_binding = stream.admission_binding().unwrap();
            stream.write_all(b"ping").await?;
            let mut message = [0; 4];
            stream.read_exact(&mut message).await?;
            assert_eq!(&message, b"pong");
            let mut peer_session_binding = [0; NODE_ADMISSION_BINDING_BYTES];
            stream.read_exact(&mut peer_session_binding).await?;
            assert_eq!(peer_session_binding, session_binding);
            Ok(())
        });

        sim.run()
    }

    #[test]
    fn mutual_tls_exposes_client_principal_under_turmoil() -> turmoil::Result {
        let (server_certificate, server_key) =
            certificate("node", "broker-server", Some("unused.eastguard"));
        let (client_certificate, client_key) = certificate("client", "producer-a", None);

        let mut client_roots = RootCertStore::empty();
        client_roots.add(client_certificate.clone()).unwrap();
        let client_verifier = WebPkiClientVerifier::builder(Arc::new(client_roots))
            .build()
            .unwrap();
        let server_config = Arc::new(
            ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(vec![server_certificate.clone()], server_key)
                .unwrap(),
        );

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
                let stream = TlsAcceptor::from(server_config).accept(stream).await?;
                let stream = AuthenticatedTcpStream::from_tls_stream(
                    stream.into(),
                    client_certificate_principal,
                )?;
                assert_eq!(stream.peer_principal().as_ref(), "producer-a");
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
}
