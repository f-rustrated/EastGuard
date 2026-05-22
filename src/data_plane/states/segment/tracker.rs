use std::{path::PathBuf, sync::Arc};

use bytes::Bytes;

use crate::clusters::NodeId;
use crate::data_plane::{SegmentKey, checkpoint::CheckpointJob, wal::WalRecord};

use super::cache::CachedBatch;

use super::record::StagingRecord;

use super::*;

const SEGMENT_SIZE_LIMIT: u64 = 1024 * 1024 * 1024; // 1GB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentRole {
    Leader,
    Follower,
}

pub(crate) struct SegmentTracker {
    cache: Arc<SegmentRingBuffer>,
    size_bytes: u64,
    checkpoint_lsn: u64,
    segment_file_path: PathBuf,
    role: SegmentRole,
    replica_set: Vec<NodeId>,
    committed_end_offset: u64,
    next_offset: u64,
    staged_records: Vec<StagingRecord>,
}

impl SegmentTracker {
    pub(crate) fn new(path: PathBuf, role: SegmentRole, replica_set: Vec<NodeId>) -> Self {
        Self {
            cache: Arc::new(SegmentRingBuffer::new()),
            size_bytes: 0,
            checkpoint_lsn: 0,
            segment_file_path: path,
            role,
            replica_set,
            committed_end_offset: 0,
            next_offset: 0,
            staged_records: Vec::new(),
        }
    }
    pub(crate) fn new_with_offset(
        path: PathBuf,
        role: SegmentRole,
        replica_set: Vec<NodeId>,
        start_offset: u64,
    ) -> Self {
        let mut segmnet = Self::new(path, role, replica_set);
        segmnet.next_offset = start_offset;
        segmnet
    }
    pub(crate) fn replica_set(&self) -> Vec<NodeId> {
        self.replica_set.clone()
    }

    pub(crate) fn leader_node(&self) -> NodeId {
        self.replica_set[0].clone()
    }

    pub(crate) fn role(&self) -> SegmentRole {
        self.role
    }

    pub(crate) fn followers(&self) -> &[NodeId] {
        if self.replica_set.len() > 1 {
            &self.replica_set[1..]
        } else {
            &[]
        }
    }
    pub(crate) fn uncheckpointed(&self) -> u64 {
        self.cache.load_write_cursor() - self.cache.load_eviction_frontier()
    }

    /// Encodes staged_records contents into the shared WAL buffer.
    /// Records stay in staged_records until publish_staged drains them.
    pub(crate) fn stage_to_wal(&mut self, wal_buf: &mut Vec<u8>) {
        for rec in &self.staged_records {
            let _ = WalRecord::data(rec.build_wal_payload()).encode_to(wal_buf);
        }
    }

    pub(crate) fn has_staged(&self) -> bool {
        !self.staged_records.is_empty()
    }

    /// Drains staged_records into a CachedBatch and publishes to cache.
    pub(crate) fn publish_staged(&mut self, lsn: u64) -> Arc<CachedBatch> {
        let start_offset = self
            .staged_records
            .first()
            .map(|r| r.logical_offset())
            .unwrap_or(0);
        let end_offset = self
            .staged_records
            .last()
            .map(|r| r.logical_offset())
            .unwrap_or(0);
        let records: Vec<Bytes> = std::mem::take(&mut self.staged_records)
            .into_iter()
            .map(|r| r.user_data)
            .collect();

        let batch = Arc::new(CachedBatch {
            records,
            start_offset,
            end_offset,
            lsn,
        });
        self.cache.publish(batch.clone());
        batch
    }

    pub(crate) fn uncommitted_records(&self) -> impl Iterator<Item = Bytes> + '_ {
        let commit = self.cache.load_read_cursor();
        let tail = self.cache.load_write_cursor();
        (commit..tail).flat_map(|pos| {
            self.cache
                .load_published(pos)
                .into_iter()
                .flat_map(|batch| batch.records.clone())
        })
    }

    pub(crate) fn checkpoint(&self, key: SegmentKey) -> CheckpointJob {
        CheckpointJob {
            segment_key: key,
            cache: self.cache.clone(),
            segment_file_path: self.segment_file_path.clone(),
        }
    }

    pub(crate) fn commit_batch(&mut self, batch_end_offset: u64) {
        let commit = self.cache.load_read_cursor();

        #[cfg(debug_assertions)]
        if let Some(batch) = self.cache.load_published(commit) {
            assert_eq!(
                batch.end_offset, batch_end_offset,
                "commit end_offset mismatch: batch has {}, caller passed {}",
                batch.end_offset, batch_end_offset
            );
        }
        self.cache.advance_read_cursor(commit + 1);
        self.committed_end_offset = batch_end_offset;
    }

    pub(crate) fn advance_checkpoint(&mut self, checkpointed_lsn: u64) {
        self.checkpoint_lsn = self.checkpoint_lsn.max(checkpointed_lsn);
    }

    pub(crate) fn checkpoint_lsn(&self) -> u64 {
        self.checkpoint_lsn
    }

    pub(crate) fn committed_end_offset(&self) -> u64 {
        self.committed_end_offset
    }

    pub(crate) fn buffer_record(&mut self, segment_key: SegmentKey, user_data: Bytes) {
        self.size_bytes += user_data.len() as u64;
        self.staged_records
            .push(StagingRecord::new(user_data, segment_key, self.next_offset));
        self.next_offset += 1;
    }

    pub(crate) fn buffer_record_in_bulk(
        &mut self,
        segment_key: SegmentKey,
        user_data: Vec<Vec<u8>>,
        start_offset: u64,
    ) {
        if start_offset < self.next_offset {
            return;
        }
        debug_assert_eq!(
            start_offset, self.next_offset,
            "offset gap: expected {}, got {start_offset}",
            self.next_offset
        );
        for data in user_data {
            self.buffer_record(segment_key, Bytes::from(data));
        }
    }

    pub(crate) fn size_limit_reached(&self) -> bool {
        self.size_bytes >= SEGMENT_SIZE_LIMIT
    }
}

#[cfg(any(test, debug_assertions))]
impl crate::test_traits::TAssertInvariant for SegmentTracker {
    fn assert_invariants(&self) {
        crate::test_traits::TAssertInvariant::assert_invariants(&*self.cache);

        let frontier = self.cache.load_eviction_frontier();
        let tail = self.cache.load_write_cursor();

        if frontier == tail && tail > 0 {
            assert!(
                self.checkpoint_lsn > 0,
                "all batches evicted but checkpoint_lsn is 0 — \
                 eviction happened without reporting checkpoint"
            );
        }

        // Cached batches must be offset-contiguous
        let read = self.cache.load_read_cursor();
        for pos in frontier..read.saturating_sub(1) {
            if let (Some(curr), Some(next)) = (
                self.cache.load_published(pos),
                self.cache.load_published(pos + 1),
            ) {
                assert_eq!(
                    curr.end_offset + 1,
                    next.start_offset,
                    "batch offset gap: batch at pos {pos} ends at {}, \
                     batch at pos {} starts at {}",
                    curr.end_offset,
                    pos + 1,
                    next.start_offset
                );
            }
        }

        // Staged records must be offset-contiguous with each other and with the last cached batch
        let buf_len = self.staged_records.len() as u64;
        for (i, rec) in self.staged_records.iter().enumerate() {
            let expected = self.next_offset - buf_len + i as u64;
            assert_eq!(
                rec.logical_offset(),
                expected,
                "staged_records[{i}] offset {} != expected {expected}",
                rec.logical_offset()
            );
        }
    }
}

#[cfg(any(test, debug_assertions))]
impl SegmentTracker {
    pub fn staged_records(&self) -> &Vec<StagingRecord> {
        &self.staged_records
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::clusters::metadata::{RangeId, SegmentId};
    use crate::clusters::swims::ShardGroupId;
    use crate::test_traits::TAssertInvariant;
    use std::sync::Arc;

    impl SegmentTracker {
        pub fn cache(&self) -> Arc<SegmentRingBuffer> {
            self.cache.clone()
        }
        pub fn next_offset(&self) -> u64 {
            self.next_offset
        }

        pub fn cache_write_cursor(&self) -> u64 {
            self.cache.load_write_cursor()
        }
        pub fn cache_read_cursor(&self) -> u64 {
            self.cache.load_read_cursor()
        }
        pub fn size_bytes(&self) -> u64 {
            self.size_bytes
        }
    }

    fn test_key() -> SegmentKey {
        SegmentKey::new(ShardGroupId(1), RangeId(0), SegmentId(0))
    }

    fn make_tracker(role: SegmentRole) -> SegmentTracker {
        SegmentTracker::new(
            PathBuf::from("/tmp/test.seg"),
            role,
            vec![NodeId::new("leader"), NodeId::new("follower")],
        )
    }

    fn make_batch(start: u64, end: u64, lsn: u64) -> CachedBatch {
        CachedBatch {
            records: vec![],
            start_offset: start,
            end_offset: end,
            lsn,
        }
    }

    #[test]
    fn buffer_record_tracks_offset_and_size_independently() {
        let mut t = make_tracker(SegmentRole::Leader);
        t.buffer_record(test_key(), Bytes::from("abc"));
        t.buffer_record(test_key(), Bytes::from("de"));

        assert_eq!(t.next_offset, 2);
        assert_eq!(t.size_bytes, 5);
        t.assert_invariants();
    }

    #[test]
    fn buffer_record_in_bulk_continues_from_tracker_offset() {
        let mut t = SegmentTracker::new_with_offset(
            PathBuf::from("/tmp/test.seg"),
            SegmentRole::Follower,
            vec![NodeId::new("leader"), NodeId::new("follower")],
            100,
        );
        t.buffer_record_in_bulk(test_key(), vec![b"a".to_vec(), b"b".to_vec()], 100);

        assert_eq!(t.next_offset, 102);
        assert_eq!(t.size_bytes, 2);
        t.assert_invariants();
    }

    #[test]
    fn buffer_record_in_bulk_skips_duplicate_offsets() {
        let mut t = SegmentTracker::new_with_offset(
            PathBuf::from("/tmp/test.seg"),
            SegmentRole::Follower,
            vec![NodeId::new("leader"), NodeId::new("follower")],
            100,
        );
        t.buffer_record_in_bulk(test_key(), vec![b"a".to_vec(), b"b".to_vec()], 100);
        assert_eq!(t.next_offset, 102);

        // Duplicate batch — should be no-op
        t.buffer_record_in_bulk(test_key(), vec![b"a".to_vec(), b"b".to_vec()], 100);
        assert_eq!(t.next_offset, 102);
        assert_eq!(t.size_bytes, 2);
        t.assert_invariants();
    }

    #[test]
    fn stage_publish_commit_lifecycle() {
        let mut t = make_tracker(SegmentRole::Leader);
        let mut wal_buf = Vec::new();

        for i in 0..3u64 {
            t.buffer_record(test_key(), Bytes::from(format!("rec-{i}")));
            t.stage_to_wal(&mut wal_buf);
            t.publish_staged(i + 1);
        }

        assert_eq!(t.cache_write_cursor(), 3);
        assert_eq!(t.cache_read_cursor(), 0);

        for i in 0..3u64 {
            t.commit_batch(i);
            assert_eq!(t.cache_read_cursor(), i + 1);
            assert_eq!(t.committed_end_offset(), i);
        }
        t.assert_invariants();
    }

    #[test]
    fn stage_then_publish_drains_buffer() {
        let mut t = make_tracker(SegmentRole::Leader);
        let mut wal_buf = Vec::new();
        t.buffer_record(test_key(), Bytes::from("a"));
        t.buffer_record(test_key(), Bytes::from("b"));
        t.stage_to_wal(&mut wal_buf);

        assert!(!wal_buf.is_empty());
        assert!(t.has_staged());
        assert_eq!(t.next_offset, 2);

        let batch = t.publish_staged(1);
        assert_eq!(batch.start_offset, 0);
        assert_eq!(batch.end_offset, 1);
        assert!(t.staged_records.is_empty());
        assert!(!t.has_staged());
        t.assert_invariants();
    }

    #[test]
    fn followers_slicing() {
        let multi = make_tracker(SegmentRole::Leader);
        assert_eq!(multi.followers(), &[NodeId::new("follower")]);

        let single = SegmentTracker::new(
            PathBuf::from("/tmp/t.seg"),
            SegmentRole::Leader,
            vec![NodeId::new("solo")],
        );
        assert!(single.followers().is_empty());
    }

    #[test]
    fn advance_checkpoint_is_monotonic() {
        let mut t = make_tracker(SegmentRole::Leader);
        t.advance_checkpoint(10);
        t.advance_checkpoint(5);
        t.advance_checkpoint(20);
        assert_eq!(t.checkpoint_lsn(), 20);
    }
}
