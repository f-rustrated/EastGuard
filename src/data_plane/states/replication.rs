use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use tokio::sync::oneshot;

use crate::control_plane::NodeId;
use crate::data_plane::SegmentKey;
use crate::data_plane::messages::command::{ProduceAck, ReplicaAppend};
use crate::data_plane::states::segment::cache::CachedEntry;

#[derive(Default)]
pub(crate) struct ReplicationState {
    pending_replies: HashMap<SegmentKey, Vec<oneshot::Sender<ProduceAck>>>,
    in_flight: HashMap<SegmentKey, VecDeque<PendingBatch>>,
    timer_seqs: HashMap<SegmentKey, u64>,
    next_timer_seq: u64,
}

pub(crate) struct PendingBatch {
    pub(crate) replies: Vec<oneshot::Sender<ProduceAck>>,
    pub(crate) pending_acks: HashSet<NodeId>,
    pub(crate) entry_id: u64,
}

pub(crate) struct AckCommitted {
    pub entry_id: u64,
    pub replies: Vec<oneshot::Sender<ProduceAck>>,
    pub reset_timer_seq: Option<u64>,
}

pub(crate) struct PendingReplicationBatch {
    pub segment_key: SegmentKey,
    pub entry: Arc<CachedEntry>,
    pub replica_set: Vec<NodeId>,
    pub followers: Vec<NodeId>,
}

impl PendingReplicationBatch {
    pub(crate) fn into_replica_append(self) -> (Vec<NodeId>, ReplicaAppend) {
        let targets = self.followers;
        let message = ReplicaAppend {
            segment_key: self.segment_key,
            replica_set: self.replica_set,
            data: self.entry.data.clone(),
            record_count: self.entry.record_count,
            entry_id: self.entry.entry_id,
        };
        (targets, message)
    }
}
impl ReplicationState {
    pub(crate) fn enqueue_reply(
        &mut self,
        segment_key: SegmentKey,
        reply: oneshot::Sender<ProduceAck>,
    ) {
        self.pending_replies
            .entry(segment_key)
            .or_default()
            .push(reply);
    }

    /// Pop the oldest waiting reply for a segment (FIFO). The no-follower commit
    /// path publishes one entry per produce in order, so popping one reply per
    /// published entry pairs each producer with its exact committed `entry_id`.
    pub(crate) fn pop_pending_reply(
        &mut self,
        segment_key: &SegmentKey,
    ) -> Option<oneshot::Sender<ProduceAck>> {
        let replies = self.pending_replies.get_mut(segment_key)?;
        if replies.is_empty() {
            self.pending_replies.remove(segment_key);
            return None;
        }
        let reply = replies.remove(0);
        if replies.is_empty() {
            self.pending_replies.remove(segment_key);
        }
        Some(reply)
    }

    pub(crate) fn drain_all_pending_replies(
        &mut self,
    ) -> impl Iterator<Item = oneshot::Sender<ProduceAck>> + '_ {
        self.pending_replies
            .drain()
            .flat_map(|(_, replies)| replies)
    }

    pub(crate) fn drain_all_in_flight(&mut self) -> impl Iterator<Item = PendingBatch> + '_ {
        self.in_flight.drain().flat_map(|(_, batches)| batches)
    }

    pub(crate) fn fail_all(&mut self, err: String) {
        for reply in self.drain_all_pending_replies() {
            let _ = reply.send(ProduceAck::Err(err.clone()));
        }

        for batch in self.drain_all_in_flight() {
            for reply in batch.replies {
                let _ = reply.send(ProduceAck::Err(err.clone()));
            }
        }
    }

    pub(crate) fn begin_replication(
        &mut self,
        pending_repl: &PendingReplicationBatch,
    ) -> Option<u64> {
        let mut replies = Vec::new();
        if let Some(reply) = self.pop_pending_reply(&pending_repl.segment_key) {
            replies.push(reply);
        }
        let pending_acks = pending_repl.followers.iter().cloned().collect();
        let is_first = self.push_in_flight(
            pending_repl.segment_key,
            PendingBatch {
                replies,
                pending_acks,
                entry_id: pending_repl.entry.entry_id,
            },
        );
        if is_first {
            let seq = self.alloc_timer_seq();
            self.set_timer_seq(pending_repl.segment_key, seq);
            Some(seq)
        } else {
            None
        }
    }

    fn push_in_flight(&mut self, segment_key: SegmentKey, batch: PendingBatch) -> bool {
        let deque = self.in_flight.entry(segment_key).or_default();
        let is_first = deque.is_empty();
        deque.push_back(batch);
        is_first
    }

    pub(crate) fn process_ack(
        &mut self,
        segment_key: &SegmentKey,
        from: &NodeId,
    ) -> Option<AckCommitted> {
        let deque = self.in_flight.get_mut(segment_key)?;
        let front = deque.front_mut()?;
        front.pending_acks.remove(from);

        if !front.pending_acks.is_empty() {
            return None;
        }

        let batch = deque.pop_front().unwrap();
        self.timer_seqs.remove(segment_key);

        let reset_timer_seq = (!deque.is_empty()).then_some({
            let seq = self.alloc_timer_seq();
            self.set_timer_seq(*segment_key, seq);
            seq
        });

        Some(AckCommitted {
            entry_id: batch.entry_id,
            replies: batch.replies,
            reset_timer_seq,
        })
    }

    pub(crate) fn in_flight_pending_acks(&self, segment_key: &SegmentKey) -> Option<Vec<NodeId>> {
        let front = self.in_flight.get(segment_key)?.front()?;
        if front.pending_acks.is_empty() {
            return None;
        }
        Some(front.pending_acks.iter().cloned().collect())
    }

    pub(crate) fn alloc_timer_seq(&mut self) -> u64 {
        self.next_timer_seq = self.next_timer_seq.wrapping_add(1);
        self.next_timer_seq
    }

    pub(crate) fn set_timer_seq(&mut self, segment_key: SegmentKey, seq: u64) {
        self.timer_seqs.insert(segment_key, seq);
    }

    // Guards against stale timeouts: process_ack may cancel+reset the timer
    // seq, but the old callback is already in the mailbox. Matching on seq
    // lets handle_timeout silently drop the stale firing.
    pub(crate) fn is_active_timer(&self, segment_key: &SegmentKey, seq: u64) -> bool {
        self.timer_seqs.get(segment_key) == Some(&seq)
    }

    pub(crate) fn segment_handoff(&mut self, old: SegmentKey, new: SegmentKey) {
        if let Some(replies) = self.pending_replies.remove(&old) {
            self.pending_replies.entry(new).or_default().extend(replies);
        }
        // Move reply channels from old in-flight batches to pending_replies.
        // The batch structure (pending_acks) is stale — the old replica set's
        // acks will never arrive. Replies are re-associated with new PendingBatch
        // entries when the replayed records emit ReplicationReady.
        if let Some(batches) = self.in_flight.remove(&old) {
            let replies: Vec<_> = batches.into_iter().flat_map(|b| b.replies).collect();
            self.pending_replies.entry(new).or_default().extend(replies);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use bytes::Bytes;

    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
    use crate::data_plane::EntryPayload;
    use crate::data_plane::states::segment::cache::CachedEntry;

    fn key(seg: u64) -> SegmentKey {
        SegmentKey::new(TopicId(1), RangeId(0), SegmentId(seg))
    }

    fn node(id: &str) -> NodeId {
        NodeId::new(id)
    }

    fn cached_entry(entry_id: u64) -> Arc<CachedEntry> {
        Arc::new(CachedEntry {
            data: EntryPayload::from(Bytes::from("data")),
            record_count: 1,
            entry_id,
            lsn: 1,
        })
    }

    fn pending_repl(
        segment_key: SegmentKey,
        followers: Vec<NodeId>,
        entry_id: u64,
    ) -> PendingReplicationBatch {
        PendingReplicationBatch {
            segment_key,
            entry: cached_entry(entry_id),
            replica_set: vec![node("leader")],
            followers,
        }
    }

    #[test]
    fn begin_replication_first_batch_returns_timer_seq() {
        let mut state = ReplicationState::default();
        let (reply_tx, _rx) = tokio::sync::oneshot::channel();
        let sk = key(0);

        state.enqueue_reply(sk, reply_tx);
        let repl = pending_repl(sk, vec![node("f1"), node("f2")], 0);

        let seq = state.begin_replication(&repl);
        assert!(seq.is_some());
        assert!(state.is_active_timer(&sk, seq.unwrap()));
    }

    #[test]
    fn begin_replication_subsequent_batch_returns_none() {
        let mut state = ReplicationState::default();
        let sk = key(0);

        let repl1 = pending_repl(sk, vec![node("f1")], 0);
        let seq = state.begin_replication(&repl1);
        assert!(seq.is_some());

        let repl2 = pending_repl(sk, vec![node("f1")], 1);
        let seq2 = state.begin_replication(&repl2);
        assert!(seq2.is_none());
    }

    #[test]
    fn begin_replication_pops_single_reply() {
        let mut state = ReplicationState::default();
        let sk = key(0);
        let (tx1, _) = tokio::sync::oneshot::channel();
        let (tx2, _) = tokio::sync::oneshot::channel();

        state.enqueue_reply(sk, tx1);
        state.enqueue_reply(sk, tx2);

        let repl = pending_repl(sk, vec![node("f1")], 0);
        state.begin_replication(&repl);

        assert_eq!(
            state.pending_replies.get(&sk).map(|v| v.len()).unwrap_or(0),
            1
        );
    }

    #[test]
    fn process_ack_partial_returns_none() {
        let mut state = ReplicationState::default();
        let sk = key(0);
        let f1 = node("f1");
        let f2 = node("f2");

        let repl = pending_repl(sk, vec![f1.clone(), f2.clone()], 0);
        state.begin_replication(&repl);

        let result = state.process_ack(&sk, &f1);
        assert!(result.is_none());
    }

    #[test]
    fn process_ack_all_acked_returns_committed() {
        let mut state = ReplicationState::default();
        let sk = key(0);
        let f1 = node("f1");
        let f2 = node("f2");
        let (tx, rx) = tokio::sync::oneshot::channel();

        state.enqueue_reply(sk, tx);
        let repl = pending_repl(sk, vec![f1.clone(), f2.clone()], 42);
        state.begin_replication(&repl);

        assert!(state.process_ack(&sk, &f1).is_none());

        let committed = state.process_ack(&sk, &f2).unwrap();
        assert_eq!(committed.entry_id, 42);
        assert_eq!(committed.replies.len(), 1);
        assert!(committed.reset_timer_seq.is_none());

        assert!(!state.is_active_timer(&sk, 0));

        let _ = committed
            .replies
            .into_iter()
            .next()
            .unwrap()
            .send(ProduceAck::Ok { entry_id: 42 });
        assert!(matches!(
            rx.blocking_recv().unwrap(),
            ProduceAck::Ok { entry_id: 42 }
        ));
    }

    #[test]
    fn process_ack_resets_timer_when_more_in_flight() {
        let mut state = ReplicationState::default();
        let sk = key(0);
        let f1 = node("f1");

        let repl1 = pending_repl(sk, vec![f1.clone()], 0);
        let first_seq = state.begin_replication(&repl1).unwrap();

        let repl2 = pending_repl(sk, vec![f1.clone()], 1);
        assert!(state.begin_replication(&repl2).is_none());

        let committed = state.process_ack(&sk, &f1).unwrap();
        assert_eq!(committed.entry_id, 0);
        let new_seq = committed.reset_timer_seq.unwrap();
        assert_ne!(first_seq, new_seq);
        assert!(state.is_active_timer(&sk, new_seq));
        assert!(!state.is_active_timer(&sk, first_seq));
    }

    #[test]
    fn process_ack_unknown_segment_returns_none() {
        let mut state = ReplicationState::default();
        let result = state.process_ack(&key(99), &node("f1"));
        assert!(result.is_none());
    }

    #[test]
    fn stale_timer_rejected() {
        let mut state = ReplicationState::default();
        let sk = key(0);
        let f1 = node("f1");

        let repl1 = pending_repl(sk, vec![f1.clone()], 0);
        let first_seq = state.begin_replication(&repl1).unwrap();

        let repl2 = pending_repl(sk, vec![f1.clone()], 1);
        state.begin_replication(&repl2);

        state.process_ack(&sk, &f1).unwrap();

        assert!(!state.is_active_timer(&sk, first_seq));
    }
}
