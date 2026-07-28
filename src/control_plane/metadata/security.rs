use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::NodeId;

/// Current process admitted for `security/node/{certificate_node_id}`.
///
/// A restart replaces this record through its metadata shard. Admission checks
/// accept SWIM facts only when the epoch, node ID, and process key match it.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct AdmissionRecord {
    pub certificate_node_id: String,
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
            certificate_node_id: "broker-a".to_string(),
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
}
