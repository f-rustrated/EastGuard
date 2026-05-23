use std::collections::{HashMap, HashSet, VecDeque};

use tokio::sync::oneshot;

use crate::clusters::NodeId;
use crate::data_plane::SegmentKey;
use crate::data_plane::messages::command::ProduceAck;

const REPLICATION_SEQ_NS: u64 = 1 << 63;

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
    pub(crate) batch_end_offset: u64,
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

    pub(crate) fn drain_pending_replies(
        &mut self,
        segment_key: &SegmentKey,
    ) -> Vec<oneshot::Sender<ProduceAck>> {
        self.pending_replies.remove(segment_key).unwrap_or_default()
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

    pub(crate) fn push_in_flight(&mut self, segment_key: SegmentKey, batch: PendingBatch) -> bool {
        let deque = self.in_flight.entry(segment_key).or_default();
        let is_first = deque.is_empty();
        deque.push_back(batch);
        is_first
    }

    pub(crate) fn in_flight_front_mut(
        &mut self,
        segment_key: &SegmentKey,
    ) -> Option<&mut PendingBatch> {
        self.in_flight.get_mut(segment_key)?.front_mut()
    }

    pub(crate) fn in_flight_pop_front(&mut self, segment_key: &SegmentKey) -> Option<PendingBatch> {
        self.in_flight.get_mut(segment_key)?.pop_front()
    }

    pub(crate) fn in_flight_pending_acks(&self, segment_key: &SegmentKey) -> Option<Vec<NodeId>> {
        let front = self.in_flight.get(segment_key)?.front()?;
        if front.pending_acks.is_empty() {
            return None;
        }
        Some(front.pending_acks.iter().cloned().collect())
    }

    pub(crate) fn in_flight_is_empty(&self, segment_key: &SegmentKey) -> bool {
        self.in_flight.get(segment_key).is_none_or(|d| d.is_empty())
    }

    pub(crate) fn alloc_timer_seq(&mut self) -> u64 {
        self.next_timer_seq = self.next_timer_seq.wrapping_add(1);
        REPLICATION_SEQ_NS | self.next_timer_seq
    }

    pub(crate) fn set_timer_seq(&mut self, segment_key: SegmentKey, seq: u64) {
        self.timer_seqs.insert(segment_key, seq);
    }

    pub(crate) fn remove_timer_seq(&mut self, segment_key: &SegmentKey) -> Option<u64> {
        self.timer_seqs.remove(segment_key)
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
