#![allow(dead_code)]
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
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
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::config::{Environment, SecurityMode};

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
    Secure {
        server: Arc<ServerConfig>,
        client: Arc<ClientConfig>,
    },
    TrustedDevelopment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TransportIdentity {
    /// Principal authenticated for this live TLS connection. This is transport
    /// evidence, not a durable authorization or ownership record.
    CertificatePrincipal(Box<str>),
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
                Self::load_from_paths(certificate_chain, private_key_path, trust_roots)
            }
            SecurityMode::TrustedDevelopment => Ok(Self::TrustedDevelopment),
        }
    }

    fn load_from_paths(
        certificate_chain_path: &Path,
        private_key_path: &Path,
        trust_root_path: &Path,
    ) -> Result<Self> {
        let certificate_chain =
            Self::load_certificates(certificate_chain_path, "certificate chain")?;
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

        Ok(Self::Secure {
            server: Arc::new(server),
            client: Arc::new(client),
        })
    }

    pub(crate) fn is_secure(&self) -> bool {
        matches!(self, Self::Secure { .. })
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

/// Reads the stable node principal from a leaf certificate's URI Subject
/// Alternative Name.
///
/// The certificate must contain exactly one URI beginning with
/// `urn:eastguard:node:`. The text after that prefix is the principal used as
/// the admission-record key. This function only parses the certificate; callers
/// must use it after rustls has authenticated the peer's certificate chain.
pub(crate) fn node_certificate_principal(certificate: &CertificateDer<'_>) -> Result<String> {
    certificate_principal(
        certificate,
        "urn:eastguard:node:",
        "node",
        "Node Certificate Principal",
    )
}

/// Reads the client principal from exactly one
/// `urn:eastguard:client:<principal>` URI Subject Alternative Name.
///
/// TLS authentication must succeed before callers use this parsed identity for
/// authorization.
pub(crate) fn client_certificate_principal(certificate: &CertificateDer<'_>) -> Result<String> {
    certificate_principal(
        certificate,
        "urn:eastguard:client:",
        "client",
        "Client Certificate Principal",
    )
}

fn certificate_principal(
    certificate: &CertificateDer<'_>,
    uri_prefix: &str,
    certificate_kind: &str,
    principal_name: &str,
) -> Result<String> {
    let (_, certificate) =
        X509Certificate::from_der(certificate.as_ref()).context("invalid X.509 certificate")?;
    let subject_alt_name = certificate
        .subject_alternative_name()
        .context("invalid X.509 subject alternative name")?
        .with_context(|| {
            format!("{certificate_kind} certificate has no subject alternative name")
        })?;

    let mut principals =
        subject_alt_name
            .value
            .general_names
            .iter()
            .filter_map(|name| match name {
                GeneralName::URI(uri) => uri.strip_prefix(uri_prefix),
                _ => None,
            });
    let principal = principals
        .next()
        .filter(|principal| !principal.is_empty())
        .with_context(|| format!("{certificate_kind} certificate has no {principal_name}"))?;
    anyhow::ensure!(
        principals.next().is_none(),
        "{certificate_kind} certificate has multiple {principal_name}s"
    );
    Ok(principal.to_string())
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
    fn reads_node_principal_from_uri_subject_alternative_name() {
        let certificate =
            certificate_with_uris(&["urn:example:unrelated", "urn:eastguard:node:broker-a"]);

        assert_eq!(
            node_certificate_principal(&certificate).unwrap(),
            "broker-a"
        );
    }

    #[test]
    fn requires_exactly_one_node_principal() {
        let missing = certificate_with_uris(&["urn:example:unrelated"]);
        let ambiguous =
            certificate_with_uris(&["urn:eastguard:node:broker-a", "urn:eastguard:node:broker-b"]);

        assert_eq!(
            node_certificate_principal(&missing)
                .unwrap_err()
                .to_string(),
            "node certificate has no Node Certificate Principal"
        );
        assert_eq!(
            node_certificate_principal(&ambiguous)
                .unwrap_err()
                .to_string(),
            "node certificate has multiple Node Certificate Principals"
        );
    }

    #[test]
    fn reads_client_principal_from_uri_subject_alternative_name() {
        let certificate =
            certificate_with_uris(&["urn:example:unrelated", "urn:eastguard:client:producer-a"]);

        assert_eq!(
            client_certificate_principal(&certificate).unwrap(),
            "producer-a"
        );
    }

    #[test]
    fn requires_exactly_one_client_principal() {
        let missing = certificate_with_uris(&["urn:example:unrelated"]);
        let ambiguous = certificate_with_uris(&[
            "urn:eastguard:client:producer-a",
            "urn:eastguard:client:producer-b",
        ]);

        assert_eq!(
            client_certificate_principal(&missing)
                .unwrap_err()
                .to_string(),
            "client certificate has no Client Certificate Principal"
        );
        assert_eq!(
            client_certificate_principal(&ambiguous)
                .unwrap_err()
                .to_string(),
            "client certificate has multiple Client Certificate Principals"
        );
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
