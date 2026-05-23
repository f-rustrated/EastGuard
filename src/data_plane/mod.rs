use std::path::{Path, PathBuf};

use crate::clusters::metadata::{RangeId, SegmentId};
use crate::clusters::swims::ShardGroupId;

pub(crate) mod actor;

pub(crate) mod checkpoint;
pub(crate) mod cold_read;
pub(crate) mod messages;
pub(crate) mod sparse_index;
pub(crate) mod state;
pub(crate) mod states;
pub(crate) mod timer;
pub(crate) mod transport;
pub(crate) mod wal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bincode::Encode, bincode::Decode)]
pub struct SegmentKey {
    pub shard_group_id: ShardGroupId,
    pub range_id: RangeId,
    pub segment_id: SegmentId,
}

impl SegmentKey {
    pub fn new(shard_group_id: ShardGroupId, range_id: RangeId, segment_id: SegmentId) -> Self {
        Self {
            shard_group_id,
            range_id,
            segment_id,
        }
    }

    pub fn file_path(&self, data_dir: &Path) -> PathBuf {
        data_dir
            .join(self.shard_group_id.to_string())
            .join(self.range_id.to_string())
            .join(format!("{}.seg", *self.segment_id))
    }

    pub fn with_segment_id(&self, segment_id: SegmentId) -> Self {
        Self {
            shard_group_id: self.shard_group_id,
            range_id: self.range_id,
            segment_id,
        }
    }
}
