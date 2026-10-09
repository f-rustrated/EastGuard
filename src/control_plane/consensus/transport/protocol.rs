use anyhow::Result;
use borsh::BorshSerialize;

use crate::control_plane::consensus::messages::MAX_APPEND_ENTRIES_BATCH_BYTES;

/// Leaves one full entry-batch worth of envelope headroom for Raft metadata and
/// the wire wrapper.
pub(super) const MAX_CLUSTER_FRAME_SIZE: usize = MAX_APPEND_ENTRIES_BATCH_BYTES * 2;
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
