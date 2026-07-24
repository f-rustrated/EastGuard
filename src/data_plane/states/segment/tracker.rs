use std::{path::PathBuf, sync::Arc, time::Duration};

use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::EntryId;
use crate::control_plane::{NodeId, Replicas};
use crate::data_plane::ProducerAppendIdentity;
use crate::data_plane::{PayloadBytes, SegmentKey, checkpoint::CheckpointJob, wal::WalRecord};

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
    replicas: Replicas,
    shard_group_id: ShardGroupId,
    committed_entry_id: EntryId,
    next_entry_id: EntryId,
    /// The entry id this segment was created with. Stable for the segment's
    /// lifetime; used by the data-plane (range, offset) → segment resolver to
    /// pin BTreeMap keys to a per-segment identity that survives writes.
    start_entry_id: EntryId,
    staged_entries: Vec<StagedEntry>,
    last_activity_at: tokio::time::Instant,
}

impl SegmentTracker {
    pub(crate) fn new(
        path: PathBuf,
        role: SegmentRole,
        replicas: Replicas,
        shard_group_id: ShardGroupId,
    ) -> Self {
        Self {
            cache: Arc::new(SegmentRingBuffer::new()),
            size_bytes: 0,
            checkpoint_lsn: 0,
            segment_file_path: path,
            role,
            replicas,
            shard_group_id,
            committed_entry_id: EntryId::MIN,
            next_entry_id: EntryId::MIN,
            start_entry_id: EntryId::MIN,
            staged_entries: Vec::new(),
            last_activity_at: tokio::time::Instant::now(),
        }
    }

    pub(crate) fn new_with_start_entry_id(
        path: PathBuf,
        role: SegmentRole,
        replica_set: Replicas,
        shard_group_id: ShardGroupId,
        start_entry_id: EntryId,
    ) -> Self {
        let mut tracker = Self::new(path, role, replica_set, shard_group_id);
        tracker.next_entry_id = start_entry_id;
        tracker.start_entry_id = start_entry_id;
        tracker
    }

    pub(crate) fn start_entry_id(&self) -> EntryId {
        self.start_entry_id
    }

    pub(crate) fn replica_set(&self) -> Replicas {
        self.replicas.clone()
    }

    pub(crate) fn leader_node(&self) -> NodeId {
        self.replicas[0].clone()
    }

    pub(crate) fn role(&self) -> SegmentRole {
        self.role
    }

    pub(crate) fn followers(&self) -> &[NodeId] {
        if self.replicas.len() > 1 {
            &self.replicas[1..]
        } else {
            &[]
        }
    }

    pub(crate) fn checkpointable_bytes(&self) -> u64 {
        self.cache.checkpointable_bytes()
    }

    pub(crate) fn hot_cache_bytes(&self) -> u64 {
        self.cache.hot_cache_bytes()
    }

    pub(crate) fn stage_to_wal(&mut self, wal_buf: &mut Vec<u8>) {
        for (i, staged) in self.staged_entries.iter().enumerate() {
            let entry_id = self.next_entry_id + i as u64;
            let header = RoutingHeader::new(staged.segment_key, entry_id, staged.record_count)
                .with_producer(staged.producer_identity);
            let _ = WalRecord::data(header.build_wal_payload(&staged.data), staged.record_count)
                .encode_to(wal_buf);
        }
    }

    pub(crate) fn has_staged(&self) -> bool {
        !self.staged_entries.is_empty()
    }

    pub(crate) fn abort_staged(&mut self) -> Box<[crate::data_plane::ProducerAppendIdentity]> {
        self.staged_entries
            .drain(..)
            .filter_map(|entry| entry.producer_identity)
            .collect()
    }

    pub(crate) fn publish_staged(&mut self, lsn: u64) -> Box<[Arc<CachedEntry>]> {
        self.staged_entries
            .drain(..)
            .map(|s| {
                let entry_id = self.next_entry_id;
                self.next_entry_id += 1u64;
                let entry = Arc::new(CachedEntry {
                    data: s.data,
                    record_count: s.record_count,
                    entry_id,
                    lsn,
                    producer_identity: s.producer_identity,
                });
                self.cache.publish(entry.clone());
                entry
            })
            .collect()
    }

    pub(crate) fn uncommitted_entries(
        &self,
    ) -> impl Iterator<Item = (PayloadBytes, u32, Option<ProducerAppendIdentity>)> + '_ {
        let commit = self.cache.load_read_cursor();
        let tail = self.cache.load_write_cursor();
        (commit..tail).filter_map(|pos| {
            self.cache.load_published(pos).map(|entry| {
                (
                    entry.data.clone(),
                    entry.record_count,
                    entry.producer_identity,
                )
            })
        })
    }

    /// Staged-but-unpublished records, for replay into a successor on seal.
    /// Unlike `uncommitted_entries` (the published cache), these never reached
    /// the WAL, so a seal must carry them forward too — and they are already
    /// counted in the data plane's `buffer_byte_count`, so the caller must not
    /// re-count them.
    pub(crate) fn staged_for_replay(
        &self,
    ) -> impl Iterator<Item = (PayloadBytes, u32, Option<ProducerAppendIdentity>)> + '_ {
        self.staged_entries
            .iter()
            .map(|s| (s.data.clone(), s.record_count, s.producer_identity))
    }

    // carrying over uncommitted data!
    pub(crate) fn replayable_bytes(&self) -> usize {
        self.uncommitted_entries()
            .chain(self.staged_for_replay())
            .map(|(data, _, _)| data.len())
            .sum()
    }

    pub(crate) fn checkpoint(&self, key: SegmentKey) -> CheckpointJob {
        CheckpointJob {
            segment_key: key,
            cache: self.cache.clone(),
            segment_file_path: self.segment_file_path.clone(),
        }
    }

    pub(crate) fn commit_entry(&mut self, entry_id: EntryId) {
        // Commit notifications can be retried across reconnect/reassignment.
        // Once the cursor passed an entry, a stale duplicate is an idempotent
        // no-op; advancing again would commit the following cached entry under
        // the wrong id.
        if entry_id < self.start_entry_id
            || self
                .last_committed_entry_id()
                .is_some_and(|committed| entry_id <= committed)
        {
            return;
        }

        let commit = self.cache.load_read_cursor();

        if self
            .cache
            .load_published(commit)
            .is_some_and(|entry| entry_id < entry.entry_id)
        {
            return;
        }

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
        self.last_activity_at = tokio::time::Instant::now();
    }

    pub(crate) fn advance_checkpoint(
        &mut self,
        checkpointed_lsn: u64,
        new_frontier: u64,
        checkpointed_bytes: u64,
    ) {
        self.checkpoint_lsn = self.checkpoint_lsn.max(checkpointed_lsn);
        self.cache
            .complete_checkpoint(new_frontier, checkpointed_bytes);
    }

    pub(crate) fn checkpoint_lsn(&self) -> u64 {
        self.checkpoint_lsn
    }

    pub(crate) fn committed_entry_id(&self) -> EntryId {
        self.committed_entry_id
    }

    fn committed_count(&self) -> u64 {
        self.cache.load_read_cursor()
    }

    /// Successor starts one past the last committed entry — or at `old_start`
    /// when nothing committed, so replayed records keep their entry ids.
    pub(crate) fn successor_start_entry_id(&self) -> EntryId {
        self.start_entry_id() + self.committed_count()
    }

    /// Last committed entry id, or `None` if nothing committed yet — the seal
    /// boundary, so a segment sealed before its first commit owns no offsets.
    pub(crate) fn last_committed_entry_id(&self) -> Option<EntryId> {
        (self.committed_count() > 0).then_some(self.committed_entry_id)
    }

    /// Highest entry id this replica has fsync'd, or `None` if it holds nothing.
    /// `next_entry_id` only advances in `publish_staged` (after `wal.flush_batch()`),
    /// so `next_entry_id - 1` is durable — and can run ahead of the notified
    /// `committed_entry_id`.
    pub(crate) fn durable_end_entry_id(&self) -> Option<EntryId> {
        (self.next_entry_id > self.start_entry_id).then(|| self.next_entry_id - 1)
    }

    pub(crate) fn is_fully_committed(&self) -> bool {
        self.last_committed_entry_id() == self.durable_end_entry_id()
    }

    pub(crate) fn stage_entry(
        &mut self,
        segment_key: SegmentKey,
        data: PayloadBytes,
        record_count: u32,
        entry_id: EntryId,
        producer_append_id: Option<ProducerAppendIdentity>,
    ) {
        let expected = self.next_staged_entry_id();
        if entry_id < expected {
            return;
        }
        debug_assert_eq!(
            entry_id, expected,
            "entry_id gap: expected {expected}, got {entry_id}",
        );
        self.size_bytes += data.len() as u64;
        self.staged_entries.push(StagedEntry::new(
            data,
            record_count,
            segment_key,
            producer_append_id,
        ));
    }

    pub(crate) fn next_staged_entry_id(&self) -> EntryId {
        self.next_entry_id + self.staged_entries.len() as u64
    }

    pub(crate) fn shard_group_id(&self) -> ShardGroupId {
        self.shard_group_id
    }

    pub(crate) fn idle_timeout_reached(&self, idle_timeout: Duration) -> bool {
        self.last_activity_at.elapsed() >= idle_timeout
    }

    pub(crate) fn size_limit_reached(&self, limit: u64) -> bool {
        self.size_bytes >= limit
    }

    pub fn cache(&self) -> Arc<SegmentRingBuffer> {
        self.cache.clone()
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
        pub fn staged_entries(&self) -> &[StagedEntry] {
            &self.staged_entries
        }

        pub fn next_entry_id(&self) -> EntryId {
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
    }

    fn test_key() -> SegmentKey {
        SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0))
    }

    fn make_tracker(role: SegmentRole) -> SegmentTracker {
        SegmentTracker::new(
            PathBuf::from("/tmp/test.seg"),
            role,
            Replicas::new(vec![NodeId::new("leader"), NodeId::new("follower")]),
            ShardGroupId(1),
        )
    }

    #[test]
    fn stage_entry_tracks_size() {
        let mut t = make_tracker(SegmentRole::Leader);
        t.stage_entry(test_key(), Bytes::from("abcde").into(), 2, EntryId(0), None);

        assert_eq!(t.size_bytes, 5);
        assert!(t.has_staged());
        t.assert_invariants();
    }

    #[test]
    fn stage_from_replica_skips_duplicate() {
        let mut t = SegmentTracker::new_with_start_entry_id(
            PathBuf::from("/tmp/test.seg"),
            SegmentRole::Follower,
            Replicas::new(vec![NodeId::new("leader"), NodeId::new("follower")]),
            ShardGroupId(1),
            EntryId(5),
        );
        t.stage_entry(test_key(), Bytes::from("data").into(), 1, EntryId(5), None);
        assert!(t.has_staged());

        // Publish to advance next_entry_id
        let mut wal_buf = Vec::new();
        t.stage_to_wal(&mut wal_buf);
        t.publish_staged(1);

        // Duplicate entry_id (5) should be skipped since next is now 6
        t.stage_entry(test_key(), Bytes::from("dup").into(), 1, EntryId(5), None);
        assert!(!t.has_staged());
        assert_eq!(t.next_entry_id, EntryId(6));
        t.assert_invariants();
    }

    #[test]
    fn stage_publish_commit_lifecycle() {
        let mut t = make_tracker(SegmentRole::Leader);
        let mut wal_buf = Vec::new();

        for i in 0..3u64 {
            t.stage_entry(
                test_key(),
                Bytes::from(format!("entry-{i}")).into(),
                1,
                EntryId(i),
                None,
            );
            t.stage_to_wal(&mut wal_buf);
            t.publish_staged(i + 1);
        }

        assert_eq!(t.cache_write_cursor(), 3);
        assert_eq!(t.cache_read_cursor(), 0);

        for i in 0..3u64 {
            t.commit_entry(EntryId(i));
            assert_eq!(t.cache_read_cursor(), i + 1);
            assert_eq!(t.committed_entry_id(), EntryId(i));
        }

        t.commit_entry(EntryId(1));
        assert_eq!(t.cache_read_cursor(), 3, "stale commit must not advance");
        assert_eq!(t.committed_entry_id(), EntryId(2));
        t.assert_invariants();
    }

    #[test]
    fn commit_before_successor_start_is_ignored() {
        let mut t = SegmentTracker::new_with_start_entry_id(
            PathBuf::new(),
            SegmentRole::Follower,
            Replicas::new(vec![NodeId::new("leader"), NodeId::new("follower")]),
            ShardGroupId(1),
            EntryId(2),
        );
        t.stage_entry(
            test_key(),
            Bytes::from("entry-2").into(),
            1,
            EntryId(2),
            None,
        );
        t.publish_staged(1);

        t.commit_entry(EntryId(1));

        assert_eq!(t.cache_read_cursor(), 0);
        assert_eq!(t.last_committed_entry_id(), None);
    }

    #[test]
    fn stage_then_publish_drains() {
        let mut t = make_tracker(SegmentRole::Leader);
        let mut wal_buf = Vec::new();
        t.stage_entry(
            test_key(),
            Bytes::from("payload").into(),
            3,
            EntryId(0),
            None,
        );
        t.stage_to_wal(&mut wal_buf);

        assert!(!wal_buf.is_empty());
        assert!(t.has_staged());
        assert_eq!(t.next_entry_id, EntryId(0));

        let entries = t.publish_staged(1);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_id, EntryId(0));
        assert_eq!(entries[0].record_count, 3);
        assert!(!t.has_staged());
        assert_eq!(t.next_entry_id, EntryId(1));
        t.assert_invariants();
    }

    #[test]
    fn followers_slicing() {
        let multi = make_tracker(SegmentRole::Leader);
        assert_eq!(multi.followers(), &[NodeId::new("follower")]);

        let single = SegmentTracker::new(
            PathBuf::from("/tmp/t.seg"),
            SegmentRole::Leader,
            Replicas::new(vec![NodeId::new("solo")]),
            ShardGroupId(1),
        );
        assert!(single.followers().is_empty());
    }

    #[test]
    fn advance_checkpoint_is_monotonic() {
        let mut t = make_tracker(SegmentRole::Leader);
        assert_eq!(t.checkpoint_lsn(), 0);
        t.advance_checkpoint(10, 0, 0);
        assert_eq!(t.checkpoint_lsn(), 10);
        t.advance_checkpoint(5, 0, 0); // shouldn't go backward
        assert_eq!(t.checkpoint_lsn(), 10);
        t.advance_checkpoint(20, 0, 0);
        assert_eq!(t.checkpoint_lsn(), 20);
    }

    #[test]
    fn idle_timeout_not_reached_immediately() {
        let t = make_tracker(SegmentRole::Leader);
        assert!(!t.idle_timeout_reached(Duration::from_secs(60)));
    }

    #[test]
    fn idle_timeout_reached_with_zero_duration() {
        let t = make_tracker(SegmentRole::Leader);
        assert!(t.idle_timeout_reached(Duration::ZERO));
    }

    #[test]
    fn committed_progress_refreshes_last_activity() {
        let mut t = make_tracker(SegmentRole::Leader);
        t.last_activity_at = tokio::time::Instant::now() - Duration::from_secs(60);
        assert!(t.idle_timeout_reached(Duration::from_secs(30)));

        t.stage_entry(test_key(), Bytes::from("data").into(), 1, EntryId(0), None);
        t.publish_staged(1);
        t.commit_entry(EntryId(0));

        assert!(!t.idle_timeout_reached(Duration::from_secs(30)));
    }
}
