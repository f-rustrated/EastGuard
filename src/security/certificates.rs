use anyhow::{Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use rustls::pki_types::CertificateDer;
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::control_plane::NodeId;

/// Certificate identity and its permitted node-ID namespace.
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

/// Reads the stable node principal from a leaf certificate's URI Subject
/// Alternative Name.
///
/// The certificate must contain exactly one URI beginning with
/// `urn:eastguard:node:`. The text after that prefix is the principal used as
/// the broker's node-ID prefix. This function only parses the certificate; callers
/// must use it after rustls has authenticated the peer's certificate chain.
pub(crate) fn node_certificate_principal(
    certificate: &CertificateDer<'_>,
) -> Result<CertificatePrincipal> {
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
pub(crate) fn client_certificate_principal(
    certificate: &CertificateDer<'_>,
) -> Result<CertificatePrincipal> {
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
) -> Result<CertificatePrincipal> {
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
    let principal = CertificatePrincipal::new(principal);
    anyhow::ensure!(
        principal.has_valid_length(),
        "certificate principal exceeds the security key limit"
    );
    Ok(principal)
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn reads_node_principal_from_uri_subject_alternative_name() {
        let certificate =
            certificate_with_uris(&["urn:example:unrelated", "urn:eastguard:node:broker-a"]);

        assert_eq!(
            node_certificate_principal(&certificate).unwrap().as_ref(),
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
            client_certificate_principal(&certificate).unwrap().as_ref(),
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
}
