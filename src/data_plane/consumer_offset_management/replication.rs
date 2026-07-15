use crate::control_plane::NodeId;
use crate::data_plane::consumer_offset_management::ledger::ConsumerOffsetUpdate;
use crate::data_plane::consumer_offset_management::types::ReplicaOffsetCommit;
use crate::data_plane::messages::command::{
    ConsumerOffsetCommitAck, ReplicaOffsetAck, ReplicaOffsetAckResult,
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
    reply: oneshot::Sender<ConsumerOffsetCommitAck>,
}

impl PendingOffsetReplication {
    fn need_more_acks(&self) -> bool {
        !self.pending_acks.is_empty()
    }

    fn is_valid_ack(&self, c: &ReplicaOffsetAck) -> bool {
        self.update == c.update && self.pending_acks.contains(&c.from)
    }
}

impl OffsetReplicationState {
    pub(crate) fn begin(
        &mut self,
        replica_set: Vec<NodeId>,
        update: ConsumerOffsetUpdate,
        reply: oneshot::Sender<ConsumerOffsetCommitAck>,
    ) -> ReplicaOffsetCommit {
        self.pending.retain(|_, pending| !pending.reply.is_closed());
        self.seq = self.seq.wrapping_add(1);
        let followers = replica_set.iter().skip(1).cloned().collect();
        self.pending.insert(
            self.seq,
            PendingOffsetReplication {
                update: update.clone(),
                pending_acks: followers,
                reply,
            },
        );
        ReplicaOffsetCommit {
            seq: self.seq,
            replica_set,
            update,
        }
    }

    pub(crate) fn process_ack(&mut self, ack: ReplicaOffsetAck) {
        let Some(pending) = self.pending.get_mut(&ack.seq) else {
            return;
        };

        // even though seq is monotonically increasing, since it is in-memory counter it's NOT unique across restarts
        // checking and update is to prevent ghost packet
        if !pending.is_valid_ack(&ack) {
            return;
        }

        if let ReplicaOffsetAckResult::StaleEpoch(stale) = ack.result {
            let rejected = self.pending.remove(&ack.seq).unwrap();
            let _ = rejected
                .reply
                .send(ConsumerOffsetCommitAck::StaleEpoch(stale));
            return;
        }

        pending.pending_acks.remove(&ack.from);
        if pending.need_more_acks() {
            return;
        }
        let committed = self.pending.remove(&ack.seq).unwrap();
        let _ = committed.reply.send(ConsumerOffsetCommitAck::Committed);
    }

    pub(crate) fn fail_all(&mut self, error: &str) {
        for (_, pending) in self.pending.drain() {
            let _ = pending
                .reply
                .send(ConsumerOffsetCommitAck::InternalError(error.to_string()));
        }
    }
}
