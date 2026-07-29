use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{Context as _, Result};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};

use super::inner;
use crate::security::{
    NodeTransportSecurity, TransportIdentity, client_certificate_principal,
    node_certificate_principal,
};

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
    peer_principal: String,
    stream: TlsStream<TcpStream>,
}

impl AuthenticatedTcpStream {
    pub async fn accept(stream: TcpStream, config: Arc<rustls::ServerConfig>) -> Result<Self> {
        let stream = TlsAcceptor::from(config).accept(stream).await?;

        Self::from_tls_stream(stream.into(), node_certificate_principal)
    }

    pub async fn accept_client(
        stream: TcpStream,
        config: Arc<rustls::ServerConfig>,
    ) -> Result<Self> {
        let stream = TlsAcceptor::from(config).accept(stream).await?;
        Self::from_tls_stream(stream.into(), client_certificate_principal)
    }

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
        read_principal: fn(&rustls::pki_types::CertificateDer<'_>) -> Result<String>,
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

    pub fn peer_principal(&self) -> &str {
        &self.peer_principal
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
    pub async fn accept_node(stream: TcpStream, security: &NodeTransportSecurity) -> Result<Self> {
        match security {
            NodeTransportSecurity::Secure { server, .. } => {
                AuthenticatedTcpStream::accept(stream, server.clone())
                    .await
                    .map(Box::new)
                    .map(Self::Secure)
            }
            NodeTransportSecurity::TrustedDevelopment => Ok(Self::TrustedDevelopment(stream)),
        }
    }

    pub async fn accept_client(
        stream: TcpStream,
        security: &NodeTransportSecurity,
    ) -> Result<Self> {
        match security {
            NodeTransportSecurity::Secure { server, .. } => {
                AuthenticatedTcpStream::accept_client(stream, server.clone())
                    .await
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
            NodeTransportSecurity::Secure { client, .. } => {
                AuthenticatedTcpStream::connect(addr, client.clone())
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

    pub fn peer_identity(&self) -> TransportIdentity {
        match self {
            Self::Secure(stream) => {
                TransportIdentity::CertificatePrincipal(stream.peer_principal().into())
            }
            Self::TrustedDevelopment(_) => TransportIdentity::TrustedDevelopment,
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
    fn mutual_tls_exposes_peer_principals_under_turmoil() -> turmoil::Result {
        let (server_config, client_config) = tls_configs();
        let mut sim = Builder::new().build();

        sim.host("server", move || {
            let server_config = server_config.clone();
            async move {
                let listener = TcpListener::bind("0.0.0.0:9000").await?;
                let (stream, _) = listener.accept().await?;
                let mut stream = AuthenticatedTcpStream::accept(stream, server_config)
                    .await
                    .unwrap();
                assert_eq!(stream.peer_principal(), "broker-client");
                let mut message = [0; 4];
                stream.read_exact(&mut message).await?;
                assert_eq!(&message, b"ping");
                stream.write_all(b"pong").await?;
                Ok(())
            }
        });

        sim.client("client", async move {
            let mut stream =
                AuthenticatedTcpStream::connect((turmoil::lookup("server"), 9000), client_config)
                    .await
                    .unwrap();
            assert_eq!(stream.peer_principal(), "broker-server");
            stream.write_all(b"ping").await?;
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
                let stream = AuthenticatedTcpStream::accept_client(stream, server_config)
                    .await
                    .unwrap();
                assert_eq!(stream.peer_principal(), "producer-a");
                Ok(())
            }
        });
        sim.client("client", async move {
            let stream =
                AuthenticatedTcpStream::connect((turmoil::lookup("server"), 9000), client_config)
                    .await
                    .unwrap();
            assert_eq!(stream.peer_principal(), "broker-server");
            Ok(())
        });

        sim.run()
    }
}
