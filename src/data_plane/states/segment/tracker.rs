use std::{path::PathBuf, sync::Arc, time::Duration};

use crate::control_plane::NodeId;
use crate::control_plane::membership::ShardGroupId;
use crate::data_plane::{EntryPayload, SegmentKey, checkpoint::CheckpointJob, wal::WalRecord};

use super::cache::CachedEntry;

use super::record::{RoutingHeader, StagedEntry};

use super::*;

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
    shard_group_id: ShardGroupId,
    committed_entry_id: u64,
    next_entry_id: u64,
    /// The entry id this segment was created with. Stable for the segment's
    /// lifetime; used by the data-plane (range, offset) → segment resolver to
    /// pin BTreeMap keys to a per-segment identity that survives writes.
    start_entry_id: u64,
    staged_entries: Vec<StagedEntry>,
    created_at: std::time::Instant,
}

impl SegmentTracker {
    pub(crate) fn new(
        path: PathBuf,
        role: SegmentRole,
        replica_set: Vec<NodeId>,
        shard_group_id: ShardGroupId,
    ) -> Self {
        Self {
            cache: Arc::new(SegmentRingBuffer::new()),
            size_bytes: 0,
            checkpoint_lsn: 0,
            segment_file_path: path,
            role,
            replica_set,
            shard_group_id,
            committed_entry_id: 0,
            next_entry_id: 0,
            start_entry_id: 0,
            staged_entries: Vec::new(),
            created_at: std::time::Instant::now(),
        }
    }

    pub(crate) fn new_with_start_entry_id(
        path: PathBuf,
        role: SegmentRole,
        replica_set: Vec<NodeId>,
        shard_group_id: ShardGroupId,
        start_entry_id: u64,
    ) -> Self {
        let mut tracker = Self::new(path, role, replica_set, shard_group_id);
        tracker.next_entry_id = start_entry_id;
        tracker.start_entry_id = start_entry_id;
        tracker
    }

    pub(crate) fn start_entry_id(&self) -> u64 {
        self.start_entry_id
    }

    #[allow(dead_code)]
    pub(crate) fn segment_file_path(&self) -> &PathBuf {
        &self.segment_file_path
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

    pub(crate) fn stage_to_wal(&mut self, wal_buf: &mut Vec<u8>) {
        for (i, staged) in self.staged_entries.iter().enumerate() {
            let entry_id = self.next_entry_id + i as u64;
            let header = RoutingHeader::new(staged.segment_key, entry_id, staged.record_count);
            let _ = WalRecord::data(header.build_wal_payload(&staged.data), staged.record_count)
                .encode_to(wal_buf);
        }
    }

    pub(crate) fn has_staged(&self) -> bool {
        !self.staged_entries.is_empty()
    }

    pub(crate) fn publish_staged(&mut self, lsn: u64) -> Vec<Arc<CachedEntry>> {
        self.staged_entries
            .drain(..)
            .map(|s| {
                let entry_id = self.next_entry_id;
                self.next_entry_id += 1;
                let entry = Arc::new(CachedEntry {
                    data: s.data,
                    record_count: s.record_count,
                    entry_id,
                    lsn,
                });
                self.cache.publish(entry.clone());
                entry
            })
            .collect()
    }

    pub(crate) fn uncommitted_entries(&self) -> impl Iterator<Item = (EntryPayload, u32)> + '_ {
        let commit = self.cache.load_read_cursor();
        let tail = self.cache.load_write_cursor();
        (commit..tail).filter_map(|pos| {
            self.cache
                .load_published(pos)
                .map(|entry| (entry.data.clone(), entry.record_count))
        })
    }

    pub(crate) fn checkpoint(&self, key: SegmentKey) -> CheckpointJob {
        CheckpointJob {
            segment_key: key,
            cache: self.cache.clone(),
            segment_file_path: self.segment_file_path.clone(),
        }
    }

    pub(crate) fn commit_entry(&mut self, entry_id: u64) {
        let commit = self.cache.load_read_cursor();

        #[cfg(debug_assertions)]
        if let Some(entry) = self.cache.load_published(commit) {
            assert_eq!(
                entry.entry_id, entry_id,
                "commit entry_id mismatch: cached entry has {}, caller passed {}",
                entry.entry_id, entry_id
            );
        }
        self.cache.advance_read_cursor(commit + 1);
        self.committed_entry_id = entry_id;
    }

    pub(crate) fn advance_checkpoint(&mut self, checkpointed_lsn: u64) {
        self.checkpoint_lsn = self.checkpoint_lsn.max(checkpointed_lsn);
    }

    pub(crate) fn checkpoint_lsn(&self) -> u64 {
        self.checkpoint_lsn
    }

    pub(crate) fn committed_entry_id(&self) -> u64 {
        self.committed_entry_id
    }

    pub(crate) fn stage_entry(
        &mut self,
        segment_key: SegmentKey,
        data: EntryPayload,
        record_count: u32,
    ) {
        self.size_bytes += data.len() as u64;
        self.staged_entries
            .push(StagedEntry::new(data, record_count, segment_key));
    }

    pub(crate) fn stage_entry_from_replica(
        &mut self,
        segment_key: SegmentKey,
        data: EntryPayload,
        record_count: u32,
        entry_id: u64,
    ) {
        let expected = self.next_entry_id + self.staged_entries.len() as u64;
        if entry_id < expected {
            return;
        }
        debug_assert_eq!(
            entry_id, expected,
            "entry_id gap: expected {expected}, got {entry_id}",
        );
        self.size_bytes += data.len() as u64;
        self.staged_entries
            .push(StagedEntry::new(data, record_count, segment_key));
    }

    pub(crate) fn shard_group_id(&self) -> ShardGroupId {
        self.shard_group_id
    }

    pub(crate) fn age_limit_reached(&self, max_age: Duration) -> bool {
        self.created_at.elapsed() >= max_age
    }

    pub(crate) fn size_limit_reached(&self, limit: u64) -> bool {
        self.size_bytes >= limit
    }
}

#[cfg(any(test, debug_assertions))]
pub mod tests {
    #![allow(unused)]

    use super::*;
    use crate::control_plane::metadata::TopicId;
    use crate::control_plane::metadata::{RangeId, SegmentId};
    use crate::test_traits::TAssertInvariant;
    use bytes::Bytes;
    use std::sync::Arc;

    impl crate::test_traits::TAssertInvariant for SegmentTracker {
        fn assert_invariants(&self) {
            crate::test_traits::TAssertInvariant::assert_invariants(&*self.cache);

            let frontier = self.cache.load_eviction_frontier();
            let tail = self.cache.load_write_cursor();

            if frontier == tail && tail > 0 {
                assert!(
                    self.checkpoint_lsn > 0,
                    "all entries evicted but checkpoint_lsn is 0 — \
                 eviction happened without reporting checkpoint"
                );
            }

            // Cached entries must have contiguous entry_ids
            let read = self.cache.load_read_cursor();
            for pos in frontier..read.saturating_sub(1) {
                if let (Some(curr), Some(next)) = (
                    self.cache.load_published(pos),
                    self.cache.load_published(pos + 1),
                ) {
                    assert_eq!(
                        curr.entry_id + 1,
                        next.entry_id,
                        "entry_id gap: entry at pos {pos} has id {}, \
                     entry at pos {} has id {}",
                        curr.entry_id,
                        pos + 1,
                        next.entry_id
                    );
                }
            }
        }
    }

    impl SegmentTracker {
        pub fn cache(&self) -> Arc<SegmentRingBuffer> {
            self.cache.clone()
        }
        pub fn next_entry_id(&self) -> u64 {
            self.next_entry_id
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
        pub fn staged_entries(&self) -> &[StagedEntry] {
            &self.staged_entries
        }
    }

    fn test_key() -> SegmentKey {
        SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0))
    }

    fn make_tracker(role: SegmentRole) -> SegmentTracker {
        SegmentTracker::new(
            PathBuf::from("/tmp/test.seg"),
            role,
            vec![NodeId::new("leader"), NodeId::new("follower")],
            ShardGroupId(1),
        )
    }

    #[test]
    fn stage_entry_tracks_size() {
        let mut t = make_tracker(SegmentRole::Leader);
        t.stage_entry(test_key(), Bytes::from("abcde").into(), 2);

        assert_eq!(t.size_bytes, 5);
        assert!(t.has_staged());
        t.assert_invariants();
    }

    #[test]
    fn stage_from_replica_skips_duplicate() {
        let mut t = SegmentTracker::new_with_start_entry_id(
            PathBuf::from("/tmp/test.seg"),
            SegmentRole::Follower,
            vec![NodeId::new("leader"), NodeId::new("follower")],
            ShardGroupId(1),
            5,
        );
        t.stage_entry_from_replica(test_key(), Bytes::from("data").into(), 1, 5);
        assert!(t.has_staged());

        // Publish to advance next_entry_id
        let mut wal_buf = Vec::new();
        t.stage_to_wal(&mut wal_buf);
        t.publish_staged(1);

        // Duplicate entry_id (5) should be skipped since next is now 6
        t.stage_entry_from_replica(test_key(), Bytes::from("dup").into(), 1, 5);
        assert!(!t.has_staged());
        assert_eq!(t.next_entry_id, 6);
        t.assert_invariants();
    }

    #[test]
    fn stage_publish_commit_lifecycle() {
        let mut t = make_tracker(SegmentRole::Leader);
        let mut wal_buf = Vec::new();

        for i in 0..3u64 {
            t.stage_entry(test_key(), Bytes::from(format!("entry-{i}")).into(), 1);
            t.stage_to_wal(&mut wal_buf);
            t.publish_staged(i + 1);
        }

        assert_eq!(t.cache_write_cursor(), 3);
        assert_eq!(t.cache_read_cursor(), 0);

        for i in 0..3u64 {
            t.commit_entry(i);
            assert_eq!(t.cache_read_cursor(), i + 1);
            assert_eq!(t.committed_entry_id(), i);
        }
        t.assert_invariants();
    }

    #[test]
    fn stage_then_publish_drains() {
        let mut t = make_tracker(SegmentRole::Leader);
        let mut wal_buf = Vec::new();
        t.stage_entry(test_key(), Bytes::from("payload").into(), 3);
        t.stage_to_wal(&mut wal_buf);

        assert!(!wal_buf.is_empty());
        assert!(t.has_staged());
        assert_eq!(t.next_entry_id, 0);

        let entries = t.publish_staged(1);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_id, 0);
        assert_eq!(entries[0].record_count, 3);
        assert!(!t.has_staged());
        assert_eq!(t.next_entry_id, 1);
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
            ShardGroupId(1),
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

    #[test]
    fn age_limit_not_reached_immediately() {
        let t = make_tracker(SegmentRole::Leader);
        assert!(!t.age_limit_reached(Duration::from_secs(60)));
    }

    #[test]
    fn age_limit_reached_with_zero_duration() {
        let t = make_tracker(SegmentRole::Leader);
        assert!(t.age_limit_reached(Duration::ZERO));
    }
}
