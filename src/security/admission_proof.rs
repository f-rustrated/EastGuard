use anyhow::{Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(test)]
use ring::signature::KeyPair;
use ring::{
    rand::SystemRandom,
    signature::{ED25519, Ed25519KeyPair, UnparsedPublicKey},
};

use crate::control_plane::consensus::raft::states::security::AdmissionRecord;
use crate::{control_plane::NodeId, security::CertificatePrincipal};

const ADMISSION_PROOF_DOMAIN: &str = "eastguard-node-admission-v1";
const ED25519_SIGNATURE_BYTES: usize = 64;

/// Private signing key generated for one broker process.
///
/// Only its public key is committed in the admission record. The private key
/// remains in this process and proves that a connection belongs to the
/// currently admitted process.
pub(crate) struct ProcessSigningKey(Ed25519KeyPair);

impl ProcessSigningKey {
    pub(crate) fn generate() -> Result<Self> {
        let random = SystemRandom::new();
        let encoded = Ed25519KeyPair::generate_pkcs8(&random)
            .map_err(|_| anyhow::anyhow!("failed to generate process signing key"))?;
        let key = Ed25519KeyPair::from_pkcs8(encoded.as_ref())
            .map_err(|_| anyhow::anyhow!("failed to load generated process signing key"))?;
        Ok(Self(key))
    }

    #[cfg(test)]
    fn public_key(&self) -> Box<[u8]> {
        self.0.public_key().as_ref().into()
    }

    pub(crate) fn sign(
        &self,
        node_certificate_principal: &CertificatePrincipal,
        node_id: &NodeId,
        tls_session_binding: &[u8],
    ) -> Result<AdmissionProof> {
        let message = AdmissionProof::signing_message(
            node_certificate_principal,
            node_id,
            tls_session_binding,
        )?;
        let signature = self.0.sign(&message);
        let signature = signature
            .as_ref()
            .try_into()
            .expect("Ed25519 signatures are always 64 bytes");
        Ok(AdmissionProof {
            node_id: node_id.clone(),
            signature,
        })
    }
}

/// Proof that this TLS connection belongs to an admitted broker process.
///
/// The receiver checks the node ID and signature against the current admission
/// record. Binding the signature to this TLS session prevents replay.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct AdmissionProof {
    pub(crate) node_id: NodeId,
    signature: [u8; ED25519_SIGNATURE_BYTES],
}

impl AdmissionProof {
    pub(crate) fn verify(
        &self,
        process_public_key: &[u8],
        node_certificate_principal: &CertificatePrincipal,
        tls_session_binding: &[u8],
    ) -> bool {
        let Ok(message) = Self::signing_message(
            node_certificate_principal,
            &self.node_id,
            tls_session_binding,
        ) else {
            return false;
        };
        UnparsedPublicKey::new(&ED25519, process_public_key)
            .verify(&message, &self.signature)
            .is_ok()
    }

    pub(crate) fn verify_admission(
        &self,
        admission: &AdmissionRecord,
        node_certificate_principal: &CertificatePrincipal,
        tls_session_binding: &[u8],
    ) -> Result<NodeId> {
        anyhow::ensure!(
            admission.node_certificate_principal == *node_certificate_principal
                && admission.node_id == self.node_id
                && self.verify(
                    &admission.process_public_key,
                    node_certificate_principal,
                    tls_session_binding,
                ),
            "node admission proof does not match the current record"
        );
        Ok(self.node_id.clone())
    }

    fn signing_message(
        node_certificate_principal: &CertificatePrincipal,
        node_id: &NodeId,
        tls_session_binding: &[u8],
    ) -> Result<Vec<u8>> {
        borsh::to_vec(&(
            ADMISSION_PROOF_DOMAIN,
            node_certificate_principal.as_ref(),
            node_id,
            tls_session_binding,
        ))
        .context("failed to encode admission proof")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_process_proves_its_admitted_identity() {
        let key = ProcessSigningKey::generate().unwrap();
        let node_id = NodeId::new("broker-a::process-2");
        let binding = [7; 32];
        let principal = CertificatePrincipal::new("broker-a");
        let proof = key.sign(&principal, &node_id, &binding).unwrap();

        assert!(proof.verify(&key.public_key(), &principal, &binding));
    }

    #[test]
    fn proof_is_bound_to_the_certificate_principal_and_tls_session() {
        let key = ProcessSigningKey::generate().unwrap();
        let node_id = NodeId::new("broker-a::process-2");
        let binding = [7; 32];
        let other_binding = [8; 32];
        let broker_a = CertificatePrincipal::new("broker-a");
        let broker_b = CertificatePrincipal::new("broker-b");
        let proof = key.sign(&broker_a, &node_id, &binding).unwrap();

        assert!(!proof.verify(&key.public_key(), &broker_b, &binding));
        assert!(!proof.verify(&key.public_key(), &broker_a, &other_binding));
    }

    #[test]
    fn old_process_key_cannot_prove_the_current_admission() {
        let current_key = ProcessSigningKey::generate().unwrap();
        let old_key = ProcessSigningKey::generate().unwrap();
        let node_id = NodeId::new("broker-a::process-2");
        let binding = [7; 32];
        let principal = CertificatePrincipal::new("broker-a");
        let proof = old_key.sign(&principal, &node_id, &binding).unwrap();

        assert!(!proof.verify(&current_key.public_key(), &principal, &binding));
    }

    #[test]
    fn changing_the_claimed_identity_invalidates_the_signature() {
        let key = ProcessSigningKey::generate().unwrap();
        let node_id = NodeId::new("broker-a::process-2");
        let binding = [7; 32];
        let principal = CertificatePrincipal::new("broker-a");
        let mut proof = key.sign(&principal, &node_id, &binding).unwrap();

        proof.node_id = NodeId::new("broker-a::process-3");
        assert!(!proof.verify(&key.public_key(), &principal, &binding));
    }
}
