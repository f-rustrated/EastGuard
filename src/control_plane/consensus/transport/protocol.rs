use anyhow::Result;
use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::WireRaftMessage;
use crate::control_plane::consensus::raft::states::security::{AclRecord, AdmissionRecord};
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::AclResource;
use crate::impl_from_variant;

/// The first frame on a cluster TCP connection.
///
/// A Raft connection begins with its first Raft message, which already names
/// its sender. ACL and admission lookup connections contain one read request.
/// Later Raft frames are raw; lookup connections return one response and close.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(super) enum InitialClusterMessage {
    Raft(WireRaftMessage),
    AclSnapshot(AclSnapshotRequest),
    AdmissionLookup(AdmissionLookupRequest),
}

/// One read of a committed ACL record from a shard host.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(super) struct AclSnapshotRequest {
    pub(super) requester_node_id: NodeId,
    pub(super) shard_group_id: ShardGroupId,
    pub(super) resource: AclResource,
}

/// The response to one ACL snapshot request on its dedicated connection.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(super) struct AclSnapshotResponse {
    pub(super) snapshot: Option<AclRecord>,
}

/// Limited bootstrap read of one admission record from its metadata shard.
///
/// In secure mode TLS authenticates the caller's node certificate, but this
/// request intentionally does not require process admission: admission is the
/// record the caller is trying to resolve. It cannot carry Raft, ACL, or client
/// data.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(super) struct AdmissionLookupRequest {
    pub(super) shard_group_id: ShardGroupId,
    pub(super) node_certificate_principal: Box<str>,
}

/// Response to one limited admission lookup, after which the connection closes.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(super) struct AdmissionLookupResponse {
    pub(super) admission: Option<AdmissionRecord>,
}

impl_from_variant!(
    InitialClusterMessage,
    Raft(WireRaftMessage),
    AclSnapshot(AclSnapshotRequest),
    AdmissionLookup(AdmissionLookupRequest),
);

pub(super) fn encode_frame(value: &impl BorshSerialize) -> Result<Vec<u8>> {
    let bytes = borsh::to_vec(value)?;
    let len = u32::try_from(bytes.len())?;
    let mut frame = Vec::with_capacity(std::mem::size_of::<u32>() + bytes.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&bytes);
    Ok(frame)
}
