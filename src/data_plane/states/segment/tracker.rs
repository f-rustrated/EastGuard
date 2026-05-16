use std::{path::PathBuf, sync::Arc};

use crate::data_plane::{
    checkpoint::CheckpointJob,
    record::{SegmentKey, SegmentRecordBatch},
};

use super::*;
pub(crate) struct SegmentTracker {
    cache: Arc<SegmentCache>,
    pub(crate) size_bytes: u64,
    checkpoint_lsn: u64,
    segment_file_path: PathBuf,
}

impl SegmentTracker {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            cache: Arc::new(SegmentCache::new()),
            size_bytes: 0,
            checkpoint_lsn: 0,
            segment_file_path: path,
        }
    }

    pub(crate) fn uncheckpointed(&self) -> u64 {
        self.cache.load_tail() - self.cache.load_eviction_frontier()
    }

    pub(crate) fn publish(&self, batch: SegmentRecordBatch) {
        self.cache.publish(Arc::new(batch));
        let tail = self.cache.load_tail();
        self.cache.commit(tail);
    }

    pub(crate) fn checkpoint(&self, key: SegmentKey) -> CheckpointJob {
        CheckpointJob {
            segment_key: key,
            cache: self.cache.clone(),
            segment_file_path: self.segment_file_path.clone(),
        }
    }

    pub(crate) fn advance_checkpoint(&mut self, checkpointed_lsn: u64) {
        self.checkpoint_lsn = self.checkpoint_lsn.max(checkpointed_lsn);
    }

    pub(crate) fn checkpoint_lsn(&self) -> u64 {
        self.checkpoint_lsn
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::{
        data_plane::states::segment::tracker::SegmentTracker, test_traits::TAssertInvariant,
    };

    impl TAssertInvariant for SegmentTracker {
        fn assert_invariants(&self) {
            self.cache.assert_invariants();

            let frontier = self.cache.load_eviction_frontier();
            let tail = self.cache.load_tail();
            if frontier == tail && tail > 0 {
                assert!(
                    self.checkpoint_lsn > 0,
                    "all batches evicted but checkpoint_lsn is 0 — \
                     eviction happened without reporting checkpoint"
                );
            }
        }
    }

    impl SegmentTracker {
        pub fn cache(&self) -> Arc<SegmentCache> {
            self.cache.clone()
        }
    }
}
