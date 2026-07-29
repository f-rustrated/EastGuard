use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::NodeId;
use crate::control_plane::metadata::AclResource;

/// Security records replicated by one metadata shard.
///
/// The live metadata state holds this directly. Snapshots box it so security
/// indexes do not enlarge every variant of the Raft snapshot state.
#[derive(Debug, Clone, Default, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct SecurityState {
    pub(super) admissions: HashMap<String, AdmissionRecord>,
    pub(super) acls: HashMap<AclResource, AclRecord>,
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
    pub resource: AclResource,
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
    /// Returns the current ACL record, or an empty revision-zero record when
    /// the resource has never been granted to any principal. Both forms deny
    /// by default; representing absence explicitly lets callers cache that
    /// denial and avoid repeatedly querying the owning shard.
    pub(crate) fn acl_snapshot(&self, resource: &AclResource) -> AclRecord {
        self.acls
            .get(resource)
            .cloned()
            .unwrap_or_else(|| AclRecord {
                resource: resource.clone(),
                revision: 0,
                principals: Box::new([]),
            })
    }

    pub(super) fn grant(&mut self, resource: AclResource, principal: String) {
        let acl = self
            .acls
            .entry(resource.clone())
            .or_insert_with(|| AclRecord {
                resource,
                revision: 0,
                principals: Box::new([]),
            });
        if acl.principals.contains(&principal) {
            return;
        }

        let mut principals = std::mem::take(&mut acl.principals).into_vec();
        principals.push(principal);
        acl.principals = principals.into_boxed_slice();
        acl.revision += 1;
    }

    pub(super) fn revoke(&mut self, resource: AclResource, principal: &str) {
        let Some(acl) = self.acls.get_mut(&resource) else {
            return;
        };
        let Some(index) = acl.principals.iter().position(|entry| entry == principal) else {
            return;
        };

        let mut principals = std::mem::take(&mut acl.principals).into_vec();
        principals.remove(index);
        acl.principals = principals.into_boxed_slice();
        acl.revision += 1;
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
    use crate::control_plane::metadata::TopicId;

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
            resource: AclResource::TopicData(TopicId(42)),
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
    fn acl_snapshot_returns_the_exact_record_or_an_empty_denial() {
        let mut security = SecurityState::default();
        let resource = AclResource::TopicData(TopicId(42));
        security.acls.insert(
            resource.clone(),
            AclRecord {
                resource: resource.clone(),
                revision: 1,
                principals: vec!["orders-service".to_string()].into_boxed_slice(),
            },
        );

        assert_eq!(
            security.acl_snapshot(&resource).principals,
            vec!["orders-service".to_string()].into_boxed_slice()
        );
        assert_eq!(
            security
                .acl_snapshot(&AclResource::TopicData(TopicId(43)))
                .revision,
            0
        );
    }

    #[test]
    fn missing_acl_snapshot_is_an_empty_revision_zero_record() {
        let resource = AclResource::TopicData(TopicId(42));

        assert_eq!(
            SecurityState::default().acl_snapshot(&resource),
            AclRecord {
                resource,
                revision: 0,
                principals: Box::new([]),
            }
        );
    }
}
