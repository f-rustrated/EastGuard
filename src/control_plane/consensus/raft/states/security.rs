use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::NodeId;

/// Security records replicated by one metadata shard.
///
/// The live metadata state holds this directly. Snapshots box it so security
/// indexes do not enlarge every variant of the Raft snapshot state.
#[derive(Debug, Clone, Default, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct SecurityState {
    pub(super) admissions: HashMap<String, AdmissionRecord>,
    pub(super) acls: HashMap<String, AclRecord>,
    pub(super) revocations: HashMap<(String, Box<[u8]>), RevocationRecord>,
}

/// Current process admitted for `security/node/{node_certificate_principal}`.
///
/// A restart replaces this record through its metadata shard. Admission checks
/// accept SWIM facts only when the epoch, node ID, and process key match it.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct AdmissionRecord {
    pub node_certificate_principal: String,
    pub revision: u64,
    pub epoch: u64,
    pub node_id: NodeId,
    pub process_public_key: Box<[u8]>,
}

/// Principals granted the permissions of one exact
/// `security/acl/{resource}` entry.
///
/// Authorization caches use the revision to reject stale copies. Missing
/// principals and missing records deny access.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct AclRecord {
    pub resource: String,
    pub revision: u64,
    pub principals: Box<[String]>,
}

/// Certificate blocked by `security/revocation/{issuer}/{serial}`.
///
/// Brokers cache these records and terminate or reject matching authenticated
/// connections within the cache enforcement window.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct RevocationRecord {
    pub issuer: String,
    pub serial: Box<[u8]>,
    pub revision: u64,
    pub revoked_at: u64,
}

impl SecurityState {
    /// Returns whether an exact principal is listed on an exact ACL resource.
    ///
    /// Missing records and missing principals deny by default. Resource
    /// hierarchy or wildcard matching is intentionally not inferred here.
    pub(crate) fn authorizes(&self, resource: &str, principal: &str) -> bool {
        self.acls
            .get(resource)
            .is_some_and(|acl| acl.principals.iter().any(|entry| entry == principal))
    }
}

#[cfg(any(test, debug_assertions))]
impl crate::test_traits::TAssertInvariant for SecurityState {
    fn assert_invariants(&self) {
        for (node_certificate_principal, admission) in &self.admissions {
            assert_eq!(
                node_certificate_principal, &admission.node_certificate_principal,
                "admission map key does not match Node Certificate Principal"
            );
        }
        for (resource, acl) in &self.acls {
            assert_eq!(
                resource, &acl.resource,
                "ACL map key does not match resource"
            );
        }
        for ((issuer, serial), revocation) in &self.revocations {
            assert_eq!(
                issuer, &revocation.issuer,
                "revocation map key does not match issuer"
            );
            assert_eq!(
                serial, &revocation.serial,
                "revocation map key does not match serial"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T>(value: &T)
    where
        T: BorshSerialize + BorshDeserialize + PartialEq + std::fmt::Debug,
    {
        let bytes = borsh::to_vec(value).unwrap();
        assert_eq!(&borsh::from_slice::<T>(&bytes).unwrap(), value);
    }

    #[test]
    fn security_records_round_trip() {
        round_trip(&AdmissionRecord {
            node_certificate_principal: "broker-a".to_string(),
            revision: 3,
            epoch: 2,
            node_id: NodeId::new("broker-a::process-2"),
            process_public_key: vec![1, 2, 3].into_boxed_slice(),
        });
        round_trip(&AclRecord {
            resource: "security/cluster".to_string(),
            revision: 4,
            principals: vec!["operator".to_string()].into_boxed_slice(),
        });
        round_trip(&RevocationRecord {
            issuer: "cluster-ca".to_string(),
            serial: vec![0x12, 0x34].into_boxed_slice(),
            revision: 5,
            revoked_at: 100,
        });
    }

    #[test]
    fn acl_authorization_is_exact_and_defaults_to_deny() {
        let mut security = SecurityState::default();
        security.acls.insert(
            "topic-data/42".to_string(),
            AclRecord {
                resource: "topic-data/42".to_string(),
                revision: 1,
                principals: vec!["orders-service".to_string()].into_boxed_slice(),
            },
        );

        assert!(security.authorizes("topic-data/42", "orders-service"));
        assert!(!security.authorizes("topic-data/42", "unknown-service"));
        assert!(!security.authorizes("topic-data/43", "orders-service"));
        assert!(!security.authorizes("topic-data", "orders-service"));
    }
}
