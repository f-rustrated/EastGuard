use anyhow::Result;
use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::consensus::messages::{MAX_APPEND_ENTRIES_BATCH_BYTES, WireRaftMessage};
use crate::control_plane::consensus::raft::states::security::AclRecord;
use crate::control_plane::metadata::AclResource;
use crate::impl_from_variant;

/// Leaves one full entry-batch worth of envelope headroom for Raft metadata and
/// the wire wrapper.
pub(super) const MAX_CLUSTER_FRAME_SIZE: usize = MAX_APPEND_ENTRIES_BATCH_BYTES * 2;
/// First request after the secure identity exchange, or directly in development.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(super) enum ClusterRequest {
    Raft(WireRaftMessage),
    AclSnapshot(AclSnapshotRequest),
}

impl_from_variant!(
    ClusterRequest,
    Raft(WireRaftMessage),
    AclSnapshot(AclSnapshotRequest),
);

/// One read of a committed ACL record from a shard host.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(super) struct AclSnapshotRequest {
    pub(super) resource: AclResource,
}

/// The response to one ACL snapshot request on its dedicated connection.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(super) struct AclSnapshotResponse {
    pub(super) snapshot: AclRecord,
}

pub(super) fn encode_frame(value: &impl BorshSerialize) -> Result<Vec<u8>> {
    let payload_len = borsh::object_length(value)?;
    anyhow::ensure!(
        payload_len <= MAX_CLUSTER_FRAME_SIZE,
        "cluster frame too large: {} bytes",
        payload_len
    );
    let len = u32::try_from(payload_len)?;
    let mut frame = Vec::with_capacity(std::mem::size_of::<u32>() + payload_len);
    frame.extend_from_slice(&len.to_be_bytes());
    value.serialize(&mut frame)?;
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_frame_is_rejected_before_allocating_its_payload() {
        let oversized = vec![0_u8; MAX_CLUSTER_FRAME_SIZE + 1];

        assert!(encode_frame(&oversized).is_err());
    }
}
