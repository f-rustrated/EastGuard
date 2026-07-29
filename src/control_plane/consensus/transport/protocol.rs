use anyhow::Result;
use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::WireRaftMessage;
use crate::control_plane::consensus::raft::states::security::AclRecord;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::AclResource;
use crate::impl_from_variant;

/// The first frame on a cluster TCP connection.
///
/// A Raft connection begins with its first Raft message, which already names
/// its sender. An ACL snapshot connection begins with its requester and read.
/// Later Raft frames are raw; an ACL connection returns one response and closes.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(super) enum InitialClusterMessage {
    Raft(WireRaftMessage),
    AclSnapshot(AclSnapshotRequest),
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

impl_from_variant!(
    InitialClusterMessage,
    Raft(WireRaftMessage),
    AclSnapshot(AclSnapshotRequest),
);

pub(super) fn encode_frame(value: &impl BorshSerialize) -> Result<Vec<u8>> {
    let bytes = borsh::to_vec(value)?;
    let len = u32::try_from(bytes.len())?;
    let mut frame = Vec::with_capacity(std::mem::size_of::<u32>() + bytes.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&bytes);
    Ok(frame)
}
