use bincode::{Decode, Encode};

use crate::control_plane::{
    NodeId,
    metadata::{SegmentId, error::MetadataError},
};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum SegmentMetaState {
    Active,
    Sealed,
    Reassigning { from: NodeId, to: NodeId },
    Deleting,
}

pub(crate) type ReplicaSet = Vec<NodeId>;
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct SegmentMeta {
    pub segment_id: SegmentId,
    pub state: SegmentMetaState,
    pub replica_set: ReplicaSet,
    pub size_bytes: u64,
    pub start_offset: u64,
    pub end_offset: Option<u64>,
    pub created_at: u64,
    pub sealed_at: Option<u64>,
}

impl SegmentMeta {
    pub(crate) fn new(
        segment_id: SegmentId,
        replica_set: ReplicaSet,
        start_offset: u64,
        created_at: u64,
    ) -> Self {
        SegmentMeta {
            segment_id,
            state: SegmentMetaState::Active,
            replica_set,
            size_bytes: 0,
            start_offset,
            end_offset: None,
            created_at,
            sealed_at: None,
        }
    }
    pub(crate) fn seal(
        &mut self,
        end_offset: Option<u64>,
        sealed_at: u64,
    ) -> Result<(), MetadataError> {
        if self.state != SegmentMetaState::Active {
            return Err(MetadataError::SegmentNotActive);
        }

        self.state = SegmentMetaState::Sealed;
        self.end_offset = end_offset;
        self.sealed_at = Some(sealed_at);

        Ok(())
    }
}
