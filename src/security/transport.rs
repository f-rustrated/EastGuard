use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use super::admission_proof::{AdmissionProof, ProcessSigningKey};
use super::certificates::node_certificate_principal;
use crate::config::{Environment, SecurityMode};
use crate::control_plane::NodeId;
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

/// Verifies certificates presented to EastGuard's outbound node connections.
///
/// The shared client config uses this verifier when Raft or data transport
/// connects to another broker. It retains certificate-chain, validity,
/// server-usage, and TLS handshake-signature verification. It does not compare
/// the certificate with a DNS name because brokers are identified by the Node
/// Certificate Principal carried in the certificate; admission later binds
/// that stable principal to the process-specific `NodeId`.
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
                Ok(Self::Secure(Self::load_from_paths(
                    certificate_chain,
                    private_key_path,
                    trust_roots,
                )?))
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
        let node_certificate_principal = node_certificate_principal(&certificate_chain[0])?;
        let private_key = Self::load_private_key(private_key_path)?;
        let trust_roots = Arc::new(Self::load_trust_roots(trust_root_path)?);
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
            process_signing_key: Arc::new(ProcessSigningKey::generate()?),
        })
    }

    pub(crate) fn is_secure(&self) -> bool {
        matches!(self, Self::Secure(_))
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
}

/// TLS credentials and process identity loaded for secure mode.
#[derive(Clone)]
pub(crate) struct NodeCredentials {
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
    node_certificate_principal: CertificatePrincipal,
    process_signing_key: Arc<ProcessSigningKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, BorshSerialize, BorshDeserialize)]
pub(crate) struct CertificatePrincipal(Box<str>);

impl CertificatePrincipal {
    pub(crate) fn new(principal: impl Into<Box<str>>) -> Self {
        Self(principal.into())
    }
}

impl AsRef<str> for CertificatePrincipal {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl NodeCredentials {
    pub(crate) fn server_config(&self) -> Arc<ServerConfig> {
        self.server.clone()
    }

    pub(crate) fn client_config(&self) -> Arc<ClientConfig> {
        self.client.clone()
    }

    /// Creates proof that a node connection belongs to this exact process.
    ///
    /// The TLS session binding makes the proof unique to one connection. Only
    /// the process public key is stored in metadata Raft.
    pub(crate) fn create_admission_proof(
        &self,
        node_id: &NodeId,
        tls_session_binding: &[u8],
    ) -> Result<AdmissionProof> {
        self.process_signing_key.sign(
            &self.node_certificate_principal,
            node_id,
            tls_session_binding,
        )
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
