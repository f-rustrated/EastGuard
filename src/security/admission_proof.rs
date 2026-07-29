use anyhow::{Context, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use ring::{
    rand::{SecureRandom, SystemRandom},
    signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey},
};

use crate::control_plane::NodeId;

const ADMISSION_PROOF_DOMAIN: &str = "eastguard-node-admission-v1";
const ADMISSION_CHALLENGE_BYTES: usize = 32;
const ED25519_SIGNATURE_BYTES: usize = 64;

/// Fresh value supplied by the accepting broker for one connection.
///
/// A proof from an earlier connection cannot be replayed because it was signed
/// for a different challenge.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct AdmissionChallenge([u8; ADMISSION_CHALLENGE_BYTES]);

impl AdmissionChallenge {
    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0; ADMISSION_CHALLENGE_BYTES];
        SystemRandom::new()
            .fill(&mut bytes)
            .map_err(|_| anyhow::anyhow!("failed to generate admission challenge"))?;
        Ok(Self(bytes))
    }
}

/// Private signing key generated for one broker process.
///
/// Only its public key is committed in the admission record. The private key
/// remains in this process and proves that a connection belongs to the exact
/// process approved for the current admission epoch.
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

    pub(crate) fn public_key(&self) -> Box<[u8]> {
        self.0.public_key().as_ref().into()
    }

    pub(crate) fn sign(
        &self,
        node_certificate_principal: &str,
        node_id: &NodeId,
        epoch: u64,
        challenge: &AdmissionChallenge,
    ) -> Result<AdmissionProof> {
        let message =
            admission_proof_message(node_certificate_principal, node_id, epoch, challenge)?;
        let signature = self.0.sign(&message);
        let signature = signature
            .as_ref()
            .try_into()
            .expect("Ed25519 signatures are always 64 bytes");
        Ok(AdmissionProof {
            node_id: node_id.clone(),
            epoch,
            signature,
        })
    }
}

/// Claim sent by a broker process after it receives a fresh challenge.
///
/// The receiver still checks `node_id`, `epoch`, and the public key against the
/// current admission record. The signature makes that comparison meaningful:
/// a process holding only the reusable node certificate cannot forge the proof.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct AdmissionProof {
    pub(crate) node_id: NodeId,
    pub(crate) epoch: u64,
    signature: [u8; ED25519_SIGNATURE_BYTES],
}

impl AdmissionProof {
    pub(crate) fn verify(
        &self,
        process_public_key: &[u8],
        node_certificate_principal: &str,
        challenge: &AdmissionChallenge,
    ) -> bool {
        let Ok(message) = admission_proof_message(
            node_certificate_principal,
            &self.node_id,
            self.epoch,
            challenge,
        ) else {
            return false;
        };
        UnparsedPublicKey::new(&ED25519, process_public_key)
            .verify(&message, &self.signature)
            .is_ok()
    }
}

fn admission_proof_message(
    node_certificate_principal: &str,
    node_id: &NodeId,
    epoch: u64,
    challenge: &AdmissionChallenge,
) -> Result<Vec<u8>> {
    borsh::to_vec(&(
        ADMISSION_PROOF_DOMAIN,
        node_certificate_principal,
        node_id,
        epoch,
        challenge,
    ))
    .context("failed to encode admission proof")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_process_proves_its_admitted_identity() {
        let key = ProcessSigningKey::generate().unwrap();
        let node_id = NodeId::new("broker-a::process-2");
        let challenge = AdmissionChallenge::generate().unwrap();
        let proof = key.sign("broker-a", &node_id, 8, &challenge).unwrap();

        assert!(proof.verify(&key.public_key(), "broker-a", &challenge));
    }

    #[test]
    fn proof_is_bound_to_the_certificate_principal_and_challenge() {
        let key = ProcessSigningKey::generate().unwrap();
        let node_id = NodeId::new("broker-a::process-2");
        let challenge = AdmissionChallenge::generate().unwrap();
        let other_challenge = AdmissionChallenge::generate().unwrap();
        let proof = key.sign("broker-a", &node_id, 8, &challenge).unwrap();

        assert!(!proof.verify(&key.public_key(), "broker-b", &challenge));
        assert!(!proof.verify(&key.public_key(), "broker-a", &other_challenge));
    }

    #[test]
    fn old_process_key_cannot_prove_the_current_admission() {
        let current_key = ProcessSigningKey::generate().unwrap();
        let old_key = ProcessSigningKey::generate().unwrap();
        let node_id = NodeId::new("broker-a::process-2");
        let challenge = AdmissionChallenge::generate().unwrap();
        let proof = old_key.sign("broker-a", &node_id, 8, &challenge).unwrap();

        assert!(!proof.verify(&current_key.public_key(), "broker-a", &challenge));
    }

    #[test]
    fn changing_the_claimed_identity_invalidates_the_signature() {
        let key = ProcessSigningKey::generate().unwrap();
        let node_id = NodeId::new("broker-a::process-2");
        let challenge = AdmissionChallenge::generate().unwrap();
        let mut proof = key.sign("broker-a", &node_id, 8, &challenge).unwrap();

        proof.node_id = NodeId::new("broker-a::process-3");
        assert!(!proof.verify(&key.public_key(), "broker-a", &challenge));

        proof.node_id = node_id;
        proof.epoch = 9;
        assert!(!proof.verify(&key.public_key(), "broker-a", &challenge));
    }
}
