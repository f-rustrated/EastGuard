use anyhow::Result;
use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::WireRaftMessage;
use crate::control_plane::consensus::raft::states::security::{AclRecord, AdmissionRecord};
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::AclResource;

use crate::security::{AdmissionProof, CertificatePrincipal};

/// The first frame on a cluster TCP connection.
///
/// Secure Raft and ACL requests include a process proof bound to their TLS
/// session. Admission lookups and trusted-development requests omit it.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(crate) struct InitialClusterMessage {
    pub(crate) admission_proof: Option<AdmissionProof>,
    pub(crate) request: ClusterRequest,
}

/// Requests accepted as the first frame on a cluster connection.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(crate) enum ClusterRequest {
    AdmissionLookup(AdmissionRecordKey),
    Raft(WireRaftMessage),
    AclSnapshot(AclSnapshotRequest),
}

/// One read of a committed ACL record from a shard host.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(crate) struct AclSnapshotRequest {
    pub(crate) requester_node_id: NodeId,
    pub(crate) shard_group_id: ShardGroupId,
    pub(crate) resource: AclResource,
}

/// The response to one ACL snapshot request on its dedicated connection.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(crate) struct AclSnapshotResponse {
    pub(crate) snapshot: Option<AclRecord>,
}

/// Identifies one admission record in its metadata shard.
///
/// The admission lookup actor uses the same value for routing, cache
/// coalescing, and the limited wire request.
#[derive(Debug, Clone, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub(crate) struct AdmissionRecordKey {
    pub(crate) shard_group_id: ShardGroupId,
    pub(crate) node_certificate_principal: CertificatePrincipal,
}

/// Response to one limited admission lookup, after which the connection closes.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(crate) struct AdmissionLookupResponse {
    pub(crate) admission: Option<AdmissionRecord>,
}

pub(crate) fn encode_frame(value: &impl BorshSerialize) -> Result<Vec<u8>> {
    let bytes = borsh::to_vec(value)?;
    let len = u32::try_from(bytes.len())?;
    let mut frame = Vec::with_capacity(std::mem::size_of::<u32>() + bytes.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&bytes);
    Ok(frame)
}
