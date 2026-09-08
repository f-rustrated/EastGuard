use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use super::certificates::node_certificate_principal;
use crate::config::{Environment, SecurityMode};
use crate::control_plane::NodeId;
use crate::net::TransportTcpStream;
use anyhow::{Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::verify_server_cert_signed_by_trust_anchor;
use rustls::crypto::{WebPkiSupportedAlgorithms, verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::ParsedCertificate;
use rustls::server::WebPkiClientVerifier;
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, Error as RustlsError, OtherError,
    RootCertStore, ServerConfig, SignatureScheme,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Verifies certificates presented to EastGuard's outbound node connections.
///
/// The shared client config uses this verifier when Raft or data transport
/// connects to another broker. It retains certificate-chain, validity,
/// server-usage, and TLS handshake-signature verification. It does not compare
/// the certificate with a DNS name because brokers are identified by the Node
/// Certificate Principal carried in the certificate. The identity exchange
/// checks that the process-specific `NodeId` belongs to that principal.
///
///  Peer certificate
//   ├── trusted CA chain? ── no → reject
//   ├── valid lifetime and server usage? ── no → reject
//   ├── valid TLS handshake signature? ── no → reject
//   └── exactly one Node Certificate Principal? ── no → reject
struct NodeServerCertVerifier {
    roots: Arc<RootCertStore>,
    supported: WebPkiSupportedAlgorithms,
}

impl NodeServerCertVerifier {
    fn new(roots: Arc<RootCertStore>) -> Self {
        Self {
            roots,
            supported: rustls::crypto::ring::default_provider().signature_verification_algorithms,
        }
    }
}

impl fmt::Debug for NodeServerCertVerifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeServerCertVerifier")
            .finish_non_exhaustive()
    }
}

impl ServerCertVerifier for NodeServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, RustlsError> {
        let certificate = ParsedCertificate::try_from(end_entity)?;
        verify_server_cert_signed_by_trust_anchor(
            &certificate,
            &self.roots,
            intermediates,
            now,
            self.supported.all,
        )?;
        node_certificate_principal(end_entity).map_err(|error| {
            CertificateError::Other(OtherError(Arc::new(std::io::Error::other(
                error.to_string(),
            ))))
        })?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        verify_tls12_signature(message, certificate, signature, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        verify_tls13_signature(message, certificate, signature, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported.supported_schemes()
    }
}

#[derive(Clone)]
pub(crate) enum NodeTransportSecurity {
    Secure(NodeCredentials),
    TrustedDevelopment,
}

impl NodeTransportSecurity {
    pub(crate) fn load(env: &Environment) -> Result<Self> {
        match env.security_mode {
            SecurityMode::Secure => {
                let certificate_chain = env
                    .certificate_chain_path
                    .as_deref()
                    .context("certificate_chain_path is required in secure mode")?;
                let private_key_path = env
                    .private_key_path
                    .as_deref()
                    .context("private_key_path is required in secure mode")?;
                let trust_roots = env
                    .trust_root_path
                    .as_deref()
                    .context("trust_root_path is required in secure mode")?;
                let credentials =
                    Self::load_from_paths(certificate_chain, private_key_path, trust_roots)?;
                credentials.validate_node_prefix(env.node_id_prefix.as_deref())?;
                Ok(Self::Secure(credentials))
            }
            SecurityMode::TrustedDevelopment => Ok(Self::TrustedDevelopment),
        }
    }

    fn load_from_paths(
        certificate_chain_path: &Path,
        private_key_path: &Path,
        trust_root_path: &Path,
    ) -> Result<NodeCredentials> {
        let certificate_chain =
            Self::load_certificates(certificate_chain_path, "certificate chain")?;
        let private_key = Self::load_private_key(private_key_path)?;
        let trust_roots = Self::load_trust_roots(trust_root_path)?;
        Self::build_credentials(certificate_chain, private_key, trust_roots)
    }

    fn build_credentials(
        certificate_chain: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
        trust_roots: RootCertStore,
    ) -> Result<NodeCredentials> {
        let leaf = certificate_chain
            .first()
            .context("node certificate chain is empty")?;
        let node_certificate_principal = node_certificate_principal(leaf)?;
        let trust_roots = Arc::new(trust_roots);
        let client_verifier = WebPkiClientVerifier::builder(trust_roots.clone()).build()?;
        let server = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certificate_chain.clone(), private_key.clone_key())?;
        let client = ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NodeServerCertVerifier::new(trust_roots)))
            .with_client_auth_cert(certificate_chain, private_key)?;

        Ok(NodeCredentials {
            server: Arc::new(server),
            client: Arc::new(client),
            node_certificate_principal,
        })
    }

    pub(crate) fn is_secure(&self) -> bool {
        matches!(self, Self::Secure(_))
    }

    /// Exchanges bounded node IDs inside mTLS, without consulting metadata.
    /// A certificate authorizes its own `principal::suffix` namespace only.
    /// Callers must additionally compare an outbound peer with their target.
    pub(crate) async fn exchange_node_identity(
        &self,
        stream: &mut TransportTcpStream,
        node_id: &NodeId,
    ) -> Result<NodeId> {
        let Self::Secure(credentials) = self else {
            anyhow::bail!("node identity exchange requires secure transport");
        };
        credentials
            .node_certificate_principal
            .verify_node_id(node_id)?;
        let peer_principal = stream
            .peer_principal()
            .context("node connection has no certificate")?;

        // ponytail: holders of the same certificate are equally trusted;
        // use independently issued process credentials if fencing is required.
        let payload = borsh::to_vec(node_id)?;
        stream.write_u32(u32::try_from(payload.len())?).await?;
        stream.write_all(&payload).await?;
        stream.flush().await?;
        let len = stream.read_u32().await? as usize;
        anyhow::ensure!(
            len <= super::MAX_SECURITY_ID_BYTES + std::mem::size_of::<u32>(),
            "node identity frame too large"
        );
        let mut peer_payload = vec![0; len];
        stream.read_exact(&mut peer_payload).await?;
        let peer = borsh::from_slice(&peer_payload)?;
        peer_principal.verify_node_id(&peer)?;
        Ok(peer)
    }

    fn load_certificates(path: &Path, kind: &'static str) -> Result<Vec<CertificateDer<'static>>> {
        let file =
            File::open(path).context(format!("failed to open {kind} file {}", path.display()))?;

        let certificates = rustls_pemfile::certs(&mut BufReader::new(file))
            .collect::<Result<Vec<_>, _>>()
            .context(format!("failed to parse {kind} file {}", path.display()))?;

        anyhow::ensure!(
            !certificates.is_empty(),
            "{kind} file {} contains no certificates",
            path.display()
        );
        Ok(certificates)
    }

    fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
        let file = File::open(path).context(format!(
            "failed to open private key file {}",
            path.display()
        ))?;

        rustls_pemfile::private_key(&mut BufReader::new(file))
            .context(format!(
                "failed to parse private key file {}",
                path.display()
            ))?
            .context(format!(
                "private key file {} contains no supported key",
                path.display()
            ))
    }

    fn load_trust_roots(path: &Path) -> Result<RootCertStore> {
        let certificates = Self::load_certificates(path, "trust root")?;
        let mut roots = RootCertStore::empty();
        for certificate in certificates {
            roots
                .add(certificate)
                .with_context(|| format!("invalid trust root in {}", path.display()))?;
        }
        Ok(roots)
    }

    #[cfg(test)]
    pub(crate) fn test_secure(
        certificate_chain: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
        trust_certificates: &[CertificateDer<'static>],
    ) -> Result<Self> {
        let mut trust_roots = RootCertStore::empty();
        for certificate in trust_certificates {
            trust_roots.add(certificate.clone())?;
        }
        Self::build_credentials(certificate_chain, private_key, trust_roots).map(Self::Secure)
    }
}

/// TLS credentials loaded for secure mode.
#[derive(Clone)]
pub(crate) struct NodeCredentials {
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
    node_certificate_principal: CertificatePrincipal,
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, BorshSerialize, BorshDeserialize)]
pub(crate) struct CertificatePrincipal(Box<str>);

impl CertificatePrincipal {
    pub(crate) fn new(principal: impl Into<Box<str>>) -> Self {
        Self(principal.into())
    }

    pub(crate) fn has_valid_length(&self) -> bool {
        !self.0.is_empty() && self.0.len() <= super::MAX_SECURITY_ID_BYTES
    }

    pub(crate) fn verify_node_id(&self, node_id: &NodeId) -> Result<()> {
        anyhow::ensure!(
            self.has_valid_length()
                && node_id.len() <= super::MAX_SECURITY_ID_BYTES
                && node_id
                    .rsplit_once("::")
                    .is_some_and(|(principal, suffix)| {
                        principal == self.as_ref() && !suffix.is_empty()
                    }),
            "node ID does not belong to the certificate principal"
        );
        Ok(())
    }
}

impl AsRef<str> for CertificatePrincipal {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl NodeCredentials {
    fn validate_node_prefix(&self, prefix: Option<&str>) -> Result<()> {
        anyhow::ensure!(
            prefix == Some(self.node_certificate_principal.as_ref()),
            "node_id_prefix must match the node certificate principal in secure mode"
        );
        Ok(())
    }

    pub(crate) fn server_config(&self) -> Arc<ServerConfig> {
        self.server.clone()
    }

    pub(crate) fn client_config(&self) -> Arc<ClientConfig> {
        self.client.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use rcgen::string::Ia5String;
    use rcgen::{CertificateParams, KeyPair, SanType};

    fn certificate_with_uris(uris: &[&str]) -> CertificateDer<'static> {
        let mut params = CertificateParams::default();
        params.subject_alt_names = uris
            .iter()
            .map(|uri| SanType::URI(Ia5String::try_from(*uri).unwrap()))
            .collect();
        let key = KeyPair::generate().unwrap();
        params.self_signed(&key).unwrap().der().clone()
    }

    #[test]
    fn certificate_owns_only_its_exact_node_id_namespace() {
        let principal = CertificatePrincipal::new("broker-a");
        for valid in ["broker-a::1", "broker-a::2"] {
            principal.verify_node_id(&NodeId::new(valid)).unwrap();
        }
        for invalid in [
            "",
            "broker-a",
            "broker-a::",
            "broker-ab::1",
            "broker-b::1",
            "broker-a::1::2",
        ] {
            assert!(
                principal.verify_node_id(&NodeId::new(invalid)).is_err(),
                "{invalid}"
            );
        }
        assert!(
            principal
                .verify_node_id(&NodeId::new(format!(
                    "broker-a::{}",
                    "x".repeat(super::super::MAX_SECURITY_ID_BYTES)
                )))
                .is_err()
        );
    }

    #[test]
    fn secure_config_requires_certificate_bound_node_prefix() {
        let mut params = CertificateParams::default();
        params.subject_alt_names.push(SanType::URI(
            Ia5String::try_from("urn:eastguard:node:broker-a").unwrap(),
        ));
        let key = KeyPair::generate().unwrap();
        let certificate = params.self_signed(&key).unwrap();
        let security = NodeTransportSecurity::test_secure(
            vec![certificate.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            &[certificate.der().clone()],
        )
        .unwrap();
        let NodeTransportSecurity::Secure(credentials) = security else {
            unreachable!()
        };
        assert!(credentials.validate_node_prefix(None).is_err());
        assert!(credentials.validate_node_prefix(Some("broker-b")).is_err());
        credentials.validate_node_prefix(Some("broker-a")).unwrap();
    }

    #[test]
    fn secure_mode_requires_every_credential_path() {
        let env = Environment::try_parse_from(["eastguard"]).unwrap();

        let error = NodeTransportSecurity::load(&env)
            .err()
            .expect("secure mode without credential paths must fail");

        assert_eq!(
            error.to_string(),
            "certificate_chain_path is required in secure mode"
        );
    }

    #[test]
    fn trusted_development_does_not_load_credentials() {
        let env =
            Environment::try_parse_from(["eastguard", "--security-mode", "trusted-development"])
                .unwrap();

        assert!(matches!(
            NodeTransportSecurity::load(&env).unwrap(),
            NodeTransportSecurity::TrustedDevelopment
        ));
    }

    #[test]
    fn server_verifier_requires_trust_and_node_principal() {
        let trusted = certificate_with_uris(&["urn:eastguard:node:broker-a"]);
        let missing_principal = certificate_with_uris(&["urn:example:unrelated"]);
        let mut roots = RootCertStore::empty();
        roots.add(trusted.clone()).unwrap();
        roots.add(missing_principal.clone()).unwrap();
        let verifier = NodeServerCertVerifier::new(Arc::new(roots));
        let server_name = ServerName::try_from("unused.eastguard").unwrap();

        verifier
            .verify_server_cert(&trusted, &[], &server_name, &[], UnixTime::now())
            .unwrap();
        assert!(
            verifier
                .verify_server_cert(&missing_principal, &[], &server_name, &[], UnixTime::now(),)
                .is_err()
        );

        let untrusted = certificate_with_uris(&["urn:eastguard:node:broker-b"]);
        assert!(
            verifier
                .verify_server_cert(&untrusted, &[], &server_name, &[], UnixTime::now())
                .is_err()
        );
    }
}
