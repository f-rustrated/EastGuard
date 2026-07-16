use crate::control_plane::NodeId;
use crate::data_plane::consumer_offset_management::ledger::ConsumerOffsetUpdate;
use crate::data_plane::messages::command::{
    ConsumerOffsetCommitAck, ConsumerOffsetReplicated, ConsumerOffsetReplicationResult,
};
use std::collections::{HashMap, HashSet};
use tokio::sync::oneshot;

#[derive(Default)]
pub(crate) struct OffsetReplicationState {
    seq: u64,
    pending: HashMap<u64, PendingOffsetReplication>,
}

struct PendingOffsetReplication {
    update: ConsumerOffsetUpdate,
    pending_acks: HashSet<NodeId>,
    required_acks: HashSet<NodeId>,
    reply: Option<oneshot::Sender<ConsumerOffsetCommitAck>>,
}

impl PendingOffsetReplication {
    fn need_more_acks(&self) -> bool {
        !self.required_acks.is_empty()
    }

    fn is_valid_ack(&self, c: &ConsumerOffsetReplicated) -> bool {
        self.update == c.update && self.pending_acks.contains(&c.from)
    }

    fn is_consumer_waiting(&self) -> bool {
        self.reply
            .as_ref()
            .is_some_and(|pending_reply| !pending_reply.is_closed())
    }
}

impl OffsetReplicationState {
    pub(crate) fn begin(
        &mut self,
        followers: HashSet<NodeId>,
        required: HashSet<NodeId>,
        update: ConsumerOffsetUpdate,
        reply: oneshot::Sender<ConsumerOffsetCommitAck>,
    ) -> u64 {
        self.pending
            .retain(|_, pending| pending.need_more_acks() || pending.is_consumer_waiting());

        self.seq = self.seq.wrapping_add(1);
        let reply = if required.is_empty() {
            let _ = reply.send(ConsumerOffsetCommitAck::Committed);
            None
        } else {
            Some(reply)
        };
        self.pending.insert(
            self.seq,
            PendingOffsetReplication {
                update: update.clone(),
                pending_acks: followers,
                required_acks: required,
                reply,
            },
        );
        self.seq
    }

    pub(crate) fn process_ack(&mut self, ack: ConsumerOffsetReplicated) {
        let Some(pending) = self.pending.get_mut(&ack.seq) else {
            return;
        };

        // even though seq is monotonically increasing, since it is in-memory counter it's NOT unique across restarts
        // checking and update is to prevent ghost packet
        if !pending.is_valid_ack(&ack) {
            return;
        }

        if let ConsumerOffsetReplicationResult::StaleEpoch(stale) = ack.result {
            pending.pending_acks.remove(&ack.from);
            if pending.required_acks.remove(&ack.from)
                && let Some(reply) = pending.reply.take()
            {
                let _ = reply.send(ConsumerOffsetCommitAck::StaleEpoch(stale));
            }
        } else {
            pending.pending_acks.remove(&ack.from);
            pending.required_acks.remove(&ack.from);
        }

        if !pending.need_more_acks()
            && let Some(reply) = pending.reply.take()
        {
            let _ = reply.send(ConsumerOffsetCommitAck::Committed);
        }
        if pending.pending_acks.is_empty() {
            self.pending.remove(&ack.seq);
        }
    }

    pub(crate) fn has_pending_for(&self, node_id: &NodeId) -> bool {
        self.pending
            .values()
            .any(|pending| pending.pending_acks.contains(node_id))
    }

    pub(crate) fn fail_all(&mut self, error: &str) {
        for (_, mut pending) in self.pending.drain() {
            if let Some(reply) = pending.reply.take() {
                let _ = reply.send(ConsumerOffsetCommitAck::InternalError(error.to_string()));
            }
        }
    }
}
