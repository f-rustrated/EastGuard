use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::{
    NodeId,
    metadata::{SegmentId, error::MetadataError},
};

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum SegmentMetaState {
    Active,
    Sealed,
    Deleting,
}

pub(crate) type ReplicaSet = Vec<NodeId>;
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct SegmentMeta {
    pub segment_id: SegmentId,
    pub state: SegmentMetaState,
    pub replica_set: ReplicaSet,
    pub size_bytes: u64,
    pub start_entry_id: u64,
    pub end_entry_id: Option<u64>,
    pub created_at: u64,
    pub sealed_at: Option<u64>,
}

impl SegmentMeta {
    pub(crate) fn new(
        segment_id: SegmentId,
        replica_set: ReplicaSet,
        start_entry_id: u64,
        created_at: u64,
    ) -> Self {
        SegmentMeta {
            segment_id,
            state: SegmentMetaState::Active,
            replica_set,
            size_bytes: 0,
            start_entry_id,
            end_entry_id: None,
            created_at,
            sealed_at: None,
        }
    }
    pub(crate) fn seal(
        &mut self,
        end_entry_id: Option<u64>,
        sealed_at: u64,
    ) -> Result<(), MetadataError> {
        if self.state != SegmentMetaState::Active {
            return Err(MetadataError::SegmentNotActive);
        }

        self.state = SegmentMetaState::Sealed;
        self.end_entry_id = end_entry_id;
        self.sealed_at = Some(sealed_at);

        Ok(())
    }

    /// Re-points a **sealed** segment's replica set.
    /// Per invariant, `replica_set` is the only field that may change after seal;
    /// the state stays `Sealed`. Returns whether the set actually changed:
    /// an identical set is an idempotent no-op, tolerating duplicate death detection
    /// and no-leader re-proposals (cf. `RollSegment` idempotency).
    pub(crate) fn reassign(&mut self, new_replica_set: ReplicaSet) -> Result<bool, MetadataError> {
        if self.state != SegmentMetaState::Sealed {
            return Err(MetadataError::SegmentNotSealed);
        }
        if self.replica_set == new_replica_set {
            return Ok(false);
        }
        self.replica_set = new_replica_set;
        Ok(true)
    }

    pub(crate) fn is_sealed_and_under_replicated(&self, replication_factor: usize) -> bool {
        self.state == SegmentMetaState::Sealed
            && self.end_entry_id.is_some()
            && self.replica_set.len() < replication_factor
    }
}
