pub(crate) mod replication;
pub(crate) mod state;
pub(crate) mod types;
use crate::control_plane::NodeId;
use crate::control_plane::Replicas;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::RangeId;
use crate::control_plane::metadata::TopicId;
use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::data_plane::SegmentKey;
use crate::data_plane::messages::ConsumerOffsetCommitAck;
use crate::data_plane::messages::ConsumerOffsetReplicated;
use replication::OffsetReplicationState;
use state::*;
use std::collections::HashMap;
use std::collections::HashSet;
use tokio::sync::oneshot;
use types::*;

#[derive(Debug, Default)]
pub(crate) struct ConsumerOffsetCoordination {
    pub(crate) placements: HashMap<(TopicId, RangeId), OffsetPlacement>,
    pending_offset_mutations: Vec<PendingOffsetMutation>,
    future_offset_commits: HashMap<ConsumerOffsetKey, Vec<FutureOffsetCommit>>,
    offset_replication: OffsetReplicationState,
}

impl ConsumerOffsetCoordination {
    pub(crate) fn future_entry(&mut self, key: ConsumerOffsetKey) -> &mut Vec<FutureOffsetCommit> {
        self.future_offset_commits.entry(key).or_default()
    }

    pub(crate) fn push_pending(&mut self, entry: PendingOffsetMutation) {
        self.pending_offset_mutations.push(entry);
    }

    pub(crate) fn has_pending_replication_for(&self, node: &NodeId) -> bool {
        self.offset_replication.has_pending_replication_for(node)
    }

    pub(crate) fn ack_replication(&mut self, ack: ConsumerOffsetReplicated) {
        self.offset_replication.process_ack(ack);
    }

    pub(crate) fn latest_generation(&self, key: &ConsumerOffsetKey) -> Option<GenerationId> {
        self.pending_offset_mutations
            .iter()
            .rev() // Look backwards through the pending queue (newest first)
            .find_map(|pending| match &pending.record {
                OffsetRecord::EpochSeal(EpochSeal {
                    key: pending_key,
                    generation,
                }) if pending_key == key => Some(*generation),
                OffsetRecord::BootstrapEntry(snapshot) if &snapshot.key == key => {
                    Some(snapshot.generation)
                }
                _ => None,
            })
    }

    pub(crate) fn encode_offsets(&mut self) -> Result<Vec<Vec<u8>>, std::io::Error> {
        match self
            .pending_offset_mutations
            .iter()
            .map(|pending| borsh::to_vec(&pending.record))
            .collect()
        {
            Ok(encoded) => Ok(encoded),
            Err(error) => {
                tracing::error!(?error, "consumer offset WAL encoding failed");
                for pending in std::mem::take(&mut self.pending_offset_mutations) {
                    if let OffsetMutationCompletion::LeaderCommit(commit) = pending.completion {
                        let _ = commit
                            .reply
                            .send(ConsumerOffsetCommitAck::InternalError(error.to_string()));
                    }
                }
                Err(error)
            }
        }
    }

    pub(crate) fn begin_replication(
        &mut self,
        followers: HashSet<NodeId>,
        required: HashSet<NodeId>,
        update: ConsumerOffsetUpdate,
        reply: oneshot::Sender<ConsumerOffsetCommitAck>,
    ) -> u64 {
        self.offset_replication
            .begin(followers, required, update, reply)
    }

    pub(crate) fn fail_all(&mut self, error: &str) {
        self.offset_replication.fail_all(error);

        for pending in self.take_pending() {
            if let OffsetMutationCompletion::LeaderCommit(commit) = pending.completion {
                let _ = commit
                    .reply
                    .send(ConsumerOffsetCommitAck::InternalError(error.to_string()));
            }
        }

        for parked in self
            .future_offset_commits
            .drain()
            .flat_map(|(_, commits)| commits)
        {
            if let FutureOffsetCommit::Client(commit) = parked {
                let _ = commit
                    .reply
                    .send(ConsumerOffsetCommitAck::InternalError(error.to_string()));
            }
        }
    }

    // ? why per one consumer offset key, multiple future offset commit
    pub(crate) fn take_future_commits(
        &mut self,
        key: &ConsumerOffsetKey,
    ) -> Vec<FutureOffsetCommit> {
        self.future_offset_commits.remove(key).unwrap_or_default()
    }

    pub(crate) fn take_pending(&mut self) -> Vec<PendingOffsetMutation> {
        std::mem::take(&mut self.pending_offset_mutations)
    }

    pub(crate) fn has_pending_offsets(&self) -> bool {
        !self.pending_offset_mutations.is_empty()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.pending_offset_mutations.is_empty() && self.future_offset_commits.is_empty()
    }
}

#[derive(Debug)]
pub(crate) struct OffsetPlacement {
    pub(crate) segment_key: SegmentKey,
    pub(crate) shard_group_id: ShardGroupId,
    // Desired data replicas for this placement. This also preserves their
    // ordering, including which replica is the leader.
    pub(crate) replicas: Replicas,
    // Consumer-offset readiness for every member of `replicas`.
    pub(crate) replica_states: HashMap<NodeId, OffsetReplicaState>,
    pub(crate) placement_ack_sent: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OffsetReplicaState {
    Joining,
    SnapshotInstalled,
    Ready,
}

impl OffsetPlacement {
    pub(crate) fn leader(&self) -> &NodeId {
        self.replicas
            .leader()
            .expect("an offset placement must have a leader")
    }

    pub(crate) fn can_bootstrap_replicas(&self, segment_key: &SegmentKey) -> bool {
        self.segment_key == *segment_key && self.is_replica_ready(self.leader())
    }

    pub(crate) fn is_replica_ready(&self, node: &NodeId) -> bool {
        self.replica_states.get(node) == Some(&OffsetReplicaState::Ready)
    }

    pub(crate) fn all_replicas_ready(&self) -> bool {
        self.replica_states
            .values()
            .all(|state| *state == OffsetReplicaState::Ready)
    }

    pub(crate) fn compare(&self, other_key: &SegmentKey, other: &Replicas) -> PlacementObservation {
        if self.segment_key == *other_key {
            if &self.replicas != other {
                tracing::warn!(
                    ?other_key,
                    current_replicas = ?self.replicas,
                    observed_replicas = ?other,
                    "ignored conflicting replica set for an existing segment"
                );
            }
            return if &self.replicas == other {
                PlacementObservation::Unchanged
            } else {
                PlacementObservation::Conflict
            };
        }
        if self.segment_key.segment_id >= other_key.segment_id {
            return PlacementObservation::Stale;
        }
        PlacementObservation::Accepted
    }

    pub(crate) fn unready_followers(&self) -> Box<[NodeId]> {
        self.replicas
            .followers()
            .filter(|node| !self.is_replica_ready(node))
            .cloned()
            .collect()
    }
}
