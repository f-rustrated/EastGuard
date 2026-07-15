use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::control_plane::metadata::{RangeId, TopicId};
use crate::control_plane::{NodeId, Replicas};
use crate::data_plane::SegmentKey;
use crate::data_plane::checkpoint::OffsetCheckpointJob;
use crate::data_plane::consumer_offset_management::ledger::{
    ConsumerOffsetKey, ConsumerOffsetPosition, EpochSeal, OffsetLedger, OffsetRecord, StaleEpoch,
};
use crate::data_plane::consumer_offset_management::replication::OffsetReplicationState;

use crate::data_plane::messages::command::{
    CommitConsumerOffset, ConsumerOffsetCommitAck, ReplicaOffsetAck, ReplicaOffsetAckResult,
};

use crate::data_plane::transport::command::DataTransportCommand;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;

use super::types::*;

#[derive(Default)]
pub(crate) struct ConsumerOffsetManager {
    offset_ledger: OffsetLedger,
    offset_placements: HashMap<(TopicId, RangeId), OffsetPlacement>,
    pending_offset_mutations: Vec<PendingOffsetMutation>,
    future_offset_commits: HashMap<ConsumerOffsetKey, Vec<FutureOffsetCommit>>,
    pub(crate) uncheckpointed_offset_lsns: VecDeque<u64>,
    pub(crate) offset_checkpoint_lsn: u64,
    pub(crate) offset_checkpoint_in_flight: bool,
    offset_replication: OffsetReplicationState,
}

impl ConsumerOffsetManager {
    pub(crate) fn new(offset_ledger: OffsetLedger) -> Self {
        Self {
            offset_ledger,
            ..Default::default()
        }
    }

    fn future_entry(&mut self, key: ConsumerOffsetKey) -> &mut Vec<FutureOffsetCommit> {
        self.future_offset_commits.entry(key).or_default()
    }

    pub(crate) fn offset(&self, key: &ConsumerOffsetKey) -> Option<ConsumerOffsetPosition> {
        self.offset_ledger.offset(key)
    }

    pub(crate) fn handle_consumer_offset_commit(
        &mut self,
        cmd: CommitConsumerOffset,
        node_id: &NodeId,
    ) -> bool {
        let replica_set = match self.get_replica_set_if_leader(&cmd.update.key, node_id) {
            Ok(replica_set) => replica_set,
            Err(leader) => {
                let _ = cmd
                    .reply
                    .send(ConsumerOffsetCommitAck::NotWriteLeader(leader));
                return false;
            }
        };
        let sealed_generation = self.latest_generation(&cmd.update.key);
        if cmd.update.generation < sealed_generation {
            let _ = cmd
                .reply
                .send(ConsumerOffsetCommitAck::StaleEpoch(StaleEpoch(
                    sealed_generation,
                )));
            return false;
        }
        if cmd.update.generation > sealed_generation {
            self.future_entry(cmd.update.key.clone())
                .push(FutureOffsetCommit::Client(cmd));

            return false;
        }

        self.pending_offset_mutations
            .push(PendingOffsetMutation::new(
                OffsetRecord::OffsetCommit(cmd.update),
                LeaderOffsetCommitApplied {
                    replica_set,
                    reply: cmd.reply,
                },
            ));
        true
    }

    pub(crate) fn handle_offset_checkpoint_complete(&mut self, checkpointed_lsn: u64) {
        self.offset_checkpoint_lsn = self.offset_checkpoint_lsn.max(checkpointed_lsn);

        // Clear now-safely-persisted LSNs from the uncheckpointed tracking queue
        while self
            .uncheckpointed_offset_lsns
            .front()
            .is_some_and(|lsn| *lsn <= checkpointed_lsn)
        {
            self.uncheckpointed_offset_lsns.pop_front();
        }
        self.offset_checkpoint_in_flight = false;
    }

    // Looks at the oldest uncheckpointed consumer offset commit. If there are consumer offsets in memory that haven't been
    // written to the persistent offset_ledger file, their WAL entries must be preserved.
    pub(crate) fn reclamation_watermark(&self, reclaimable_lsn: u64) -> u64 {
        self.uncheckpointed_offset_lsns
            .front()
            .map(|lsn| lsn.saturating_sub(1))
            .unwrap_or(reclaimable_lsn)
    }

    pub(crate) fn oldest_uncheckpointed_lsn(&self) -> Option<&u64> {
        self.uncheckpointed_offset_lsns.front()
    }

    pub(crate) fn latest_uncheckpointed_lsn(&self) -> Option<&u64> {
        self.uncheckpointed_offset_lsns.back()
    }

    pub(crate) fn raise_offset_checkpoint_job(
        &mut self,
        data_dir: PathBuf,
    ) -> Option<OffsetCheckpointJob> {
        self.offset_checkpoint_in_flight = true;
        Some(OffsetCheckpointJob {
            checkpointed_lsn: *self.latest_uncheckpointed_lsn()?,
            offsets: self.offset_ledger.clone(),
            data_dir,
        })
    }

    pub(crate) fn add_placement(&mut self, segment_key: &SegmentKey, replica_set: Vec<NodeId>) {
        if replica_set.is_empty() {
            return;
        }

        let replicas = Replicas::new(replica_set);
        let leader = replicas.leader().cloned().unwrap();
        self.offset_placements.insert(
            (segment_key.topic_id, segment_key.range_id),
            OffsetPlacement { leader, replicas },
        );
    }

    pub(crate) fn get_replica_set_if_leader(
        &self,
        key: &ConsumerOffsetKey,
        node_id: &NodeId,
    ) -> Result<Replicas, Option<NodeId>> {
        let Some(placement) = self.offset_placements.get(&(key.topic_id, key.range_id)) else {
            return Err(None);
        };
        if placement.leader != *node_id {
            return Err(Some(placement.leader.clone()));
        }
        Ok(placement.replicas.clone())
    }

    pub(crate) fn has_pending_offsets(&self) -> bool {
        !self.pending_offset_mutations.is_empty()
    }

    pub(crate) fn handle_replica_offset_commit(
        &mut self,
        cmd: ReplicaOffsetCommit,
        sealed_generation: GenerationId,
    ) -> bool {
        if cmd.update.generation > sealed_generation {
            self.future_entry(cmd.update.key.clone())
                .push(FutureOffsetCommit::Replica(cmd));
            return false;
        }
        self.pending_offset_mutations
            .push(PendingOffsetMutation::new(
                OffsetRecord::OffsetCommit(cmd.update.clone()),
                cmd,
            ));
        true
    }

    pub(crate) fn handle_replica_offset_ack(&mut self, ack: ReplicaOffsetAck) {
        self.offset_replication.process_ack(ack);
    }

    pub(crate) fn handle_consumer_group_epoch_seal(
        &mut self,
        cmd: EpochSeal,
    ) -> Option<Vec<FutureOffsetCommit>> {
        // Idempotency
        if cmd.generation <= self.latest_generation(&cmd.key) {
            return None;
        }

        self.pending_offset_mutations
            .push(PendingOffsetMutation::new(
                OffsetRecord::EpochSeal(cmd.clone()),
                OffsetMutationCompletion::EpochSeal,
            ));

        Some(
            self.future_offset_commits
                .remove(&cmd.key)
                .unwrap_or_default(),
        )
    }

    pub(crate) fn latest_generation(&self, key: &ConsumerOffsetKey) -> GenerationId {
        self.pending_offset_mutations
            .iter()
            .rev() // Look backwards through the pending queue (newest first)
            .find_map(|pending| {
                // Find the most recent EpochSeal for this specific key
                if let OffsetRecord::EpochSeal(EpochSeal {
                    key: pending_key,
                    generation,
                }) = &pending.record
                    && pending_key == key
                {
                    return Some(*generation);
                }
                None
            })
            // No pending EpochSeals, fall back to the actual ledger's generation
            .unwrap_or_else(|| self.offset_ledger.generation(key))
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

    pub(crate) fn take_pending(&mut self) -> Vec<PendingOffsetMutation> {
        std::mem::take(&mut self.pending_offset_mutations)
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

    pub(crate) fn flush_batch(
        &mut self,
        node_id: &NodeId,
        uncheckpointed_lsn: u64,
    ) -> Vec<DataTransportCommand> {
        let mut transports = vec![];
        let pending = self.take_pending();
        if pending.is_empty() {
            return transports;
        }

        for PendingOffsetMutation { record, completion } in pending {
            self.offset_ledger.apply(record.clone());
            match completion {
                OffsetMutationCompletion::EpochSeal => {}
                OffsetMutationCompletion::LeaderCommit(commit) => {
                    let OffsetRecord::OffsetCommit(offset_commit) = record else {
                        tracing::debug!("leader offset completion must carry an offset commit");
                        continue;
                    };

                    let followers: Vec<NodeId> = commit.replica_set.followers().cloned().collect();
                    if followers.is_empty() {
                        let _ = commit.reply.send(ConsumerOffsetCommitAck::Committed);
                        continue;
                    }

                    let seq = self.offset_replication.begin(
                        followers.clone().into_iter().collect(),
                        offset_commit.clone(),
                        commit.reply,
                    );
                    transports.push(DataTransportCommand::send_to_targets(
                        followers,
                        ReplicaOffsetCommit {
                            seq,
                            replica_set: commit.replica_set,
                            update: offset_commit,
                        },
                    ));
                }
                OffsetMutationCompletion::ReplicaCommit(commit) => {
                    let Some(leader) = commit.replica_set.leader() else {
                        continue;
                    };
                    transports.push(DataTransportCommand::send_to_targets(
                        vec![leader.clone()],
                        ReplicaOffsetAck {
                            seq: commit.seq,
                            update: commit.update,
                            from: node_id.clone(),
                            result: ReplicaOffsetAckResult::Committed,
                        },
                    ));
                }
            }
        }
        self.uncheckpointed_offset_lsns
            .push_back(uncheckpointed_lsn);

        transports
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::control_plane::metadata::EntryId;
    use crate::data_plane::consumer_offset_management::ledger::{
        ConsumerOffsetPosition, ConsumerOffsetUpdate,
    };
    use tokio::sync::oneshot;

    fn key() -> ConsumerOffsetKey {
        ConsumerOffsetKey {
            topic_id: TopicId(1),
            range_id: RangeId(2),
            group_id: "group".into(),
        }
    }

    fn position() -> ConsumerOffsetPosition {
        ConsumerOffsetPosition {
            entry_id: EntryId(3),
            batch_offset: 1,
            absolute_offset: 4,
        }
    }

    #[test]
    fn fail_all_resolves_pending_in_flight_and_future_clients() {
        let mut manager = ConsumerOffsetManager::new(OffsetLedger::default());
        let (pending_reply, mut pending_response) = oneshot::channel();
        manager
            .pending_offset_mutations
            .push(PendingOffsetMutation::new(
                OffsetRecord::OffsetCommit(ConsumerOffsetUpdate {
                    key: key(),
                    generation: GenerationId(1),
                    position: position(),
                }),
                LeaderOffsetCommitApplied {
                    replica_set: Replicas::new(vec![NodeId::new("leader")]),
                    reply: pending_reply,
                },
            ));

        let (future_reply, mut future_response) = oneshot::channel();
        manager.future_offset_commits.insert(
            key(),
            vec![FutureOffsetCommit::Client(CommitConsumerOffset {
                update: ConsumerOffsetUpdate {
                    key: key(),
                    generation: GenerationId(2),
                    position: position(),
                },
                reply: future_reply,
            })],
        );

        let (replication_reply, mut replication_response) = oneshot::channel();
        manager.offset_replication.begin(
            HashSet::from_iter([NodeId::new("follower")]),
            ConsumerOffsetUpdate {
                key: key(),
                generation: GenerationId(1),
                position: position(),
            },
            replication_reply,
        );

        manager.fail_all("WAL failed");

        let expected = ConsumerOffsetCommitAck::InternalError("WAL failed".into());
        assert_eq!(pending_response.try_recv().unwrap(), expected);
        assert_eq!(future_response.try_recv().unwrap(), expected);
        assert_eq!(replication_response.try_recv().unwrap(), expected);
        assert!(manager.pending_offset_mutations.is_empty());
        assert!(manager.future_offset_commits.is_empty());
    }
}
