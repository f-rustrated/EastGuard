#![allow(dead_code)]
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};

use crate::config::{Environment, SecurityMode};

pub(crate) struct SecureTransportConfig {
    pub(crate) server: Arc<ServerConfig>,
    pub(crate) client: Arc<ClientConfig>,
}

impl SecureTransportConfig {
    pub(crate) fn load(env: &Environment) -> Result<Option<Self>> {
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
                Self::load_from_paths(certificate_chain, private_key_path, trust_roots).map(Some)
            }
            SecurityMode::TrustedDevelopment => Ok(None),
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
            .with_root_certificates((*trust_roots).clone())
            .with_client_auth_cert(certificate_chain, private_key)?;

        Ok(Self {
            server: Arc::new(server),
            client: Arc::new(client),
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn secure_mode_requires_every_credential_path() {
        let env = Environment::try_parse_from(["eastguard"]).unwrap();

        let error = SecureTransportConfig::load(&env)
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

        assert!(SecureTransportConfig::load(&env).unwrap().is_none());
    }
}
