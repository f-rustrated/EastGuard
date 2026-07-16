use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
use crate::control_plane::{NodeId, Replicas};
use crate::data_plane::SegmentKey;
use crate::data_plane::checkpoint::OffsetCheckpointJob;
use crate::data_plane::consumer_offset_management::ledger::{
    ConsumerOffsetKey, ConsumerOffsetPosition, EpochSeal, OffsetLedger, OffsetRecord, StaleEpoch,
};
use crate::data_plane::consumer_offset_management::replication::OffsetReplicationState;

use crate::data_plane::messages::command::{
    CommitConsumerOffset, ConsumerOffsetCommitAck, ConsumerOffsetReplicated,
    ConsumerOffsetReplicationResult, ConsumerOffsetSnapshotInstalled,
    InstallConsumerOffsetSnapshot,
};
use crate::data_plane::messages::{RequestConsumerOffsetSnapshot, SegmentPlaced};

use crate::data_plane::transport::command::DataTransportCommand;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;

use super::types::*;

#[derive(Default)]
pub(crate) struct ConsumerOffsetManager {
    offset_ledger: OffsetLedger, // durable offsets, epochs, and local placement readiness
    coordination: ConsumerOffsetCoordination, // live placements, pending mutations, parked commits, and replication tracking
    checkpoint: OffsetCheckpointState,        // WAL reclamation and checkpoint progresss
}

#[derive(Default)]
struct ConsumerOffsetCoordination {
    placements: HashMap<(TopicId, RangeId), OffsetPlacement>,
    pending_offset_mutations: Vec<PendingOffsetMutation>,
    future_offset_commits: HashMap<ConsumerOffsetKey, Vec<FutureOffsetCommit>>,
    offset_replication: OffsetReplicationState,
}

#[derive(Default)]
struct OffsetCheckpointState {
    uncheckpointed_lsns: VecDeque<u64>,
    checkpoint_lsn: u64,
    in_flight: bool,
}

impl ConsumerOffsetManager {
    pub(crate) fn new(offset_ledger: OffsetLedger) -> Self {
        Self {
            offset_ledger,
            ..Default::default()
        }
    }

    pub(crate) fn future_entry(&mut self, key: ConsumerOffsetKey) -> &mut Vec<FutureOffsetCommit> {
        self.coordination
            .future_offset_commits
            .entry(key)
            .or_default()
    }

    pub(crate) fn offset(&self, key: &ConsumerOffsetKey) -> Option<ConsumerOffsetPosition> {
        self.offset_ledger.offset(key)
    }

    pub(crate) fn commit_consumer_offset(
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
        let required_followers = self.get_placement_followers(&cmd);
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

        self.push_pending_offset_mutation(PendingOffsetMutation::new(
            OffsetRecord::OffsetCommit(cmd.update),
            LeaderOffsetCommitApplied {
                replica_set,
                required_followers,
                reply: cmd.reply,
            },
        ));

        true
    }

    pub(crate) fn handle_offset_checkpoint_complete(&mut self, checkpointed_lsn: u64) {
        self.checkpoint.checkpoint_lsn = self.checkpoint.checkpoint_lsn.max(checkpointed_lsn);

        // Clear now-safely-persisted LSNs from the uncheckpointed tracking queue
        while self
            .checkpoint
            .uncheckpointed_lsns
            .front()
            .is_some_and(|lsn| *lsn <= checkpointed_lsn)
        {
            self.checkpoint.uncheckpointed_lsns.pop_front();
        }
        self.checkpoint.in_flight = false;
    }

    // Looks at the oldest uncheckpointed consumer offset commit. If there are consumer offsets in memory that haven't been
    // written to the persistent offset_ledger file, their WAL entries must be preserved.
    pub(crate) fn reclamation_watermark(&self, reclaimable_lsn: u64) -> u64 {
        self.checkpoint
            .uncheckpointed_lsns
            .front()
            .map(|lsn| lsn.saturating_sub(1))
            .unwrap_or(reclaimable_lsn)
    }

    pub(crate) fn oldest_uncheckpointed_lsn(&self) -> Option<&u64> {
        self.checkpoint.uncheckpointed_lsns.front()
    }

    fn latest_uncheckpointed_lsn(&self) -> Option<&u64> {
        self.checkpoint.uncheckpointed_lsns.back()
    }

    pub(crate) fn raise_offset_checkpoint_job(
        &mut self,
        data_dir: PathBuf,
    ) -> Option<OffsetCheckpointJob> {
        self.checkpoint.in_flight = true;
        Some(OffsetCheckpointJob {
            checkpointed_lsn: *self.latest_uncheckpointed_lsn()?,
            offsets: self.offset_ledger.clone(),
            data_dir,
        })
    }

    pub(crate) fn offset_checkpoint_in_flight(&self) -> bool {
        self.checkpoint.in_flight
    }

    pub(crate) fn observe_placement(
        &mut self,
        segment_key: SegmentKey,
        shard_group_id: ShardGroupId,
        replicas: &Replicas,
        node_id: &NodeId,
    ) -> PlacementObservation {
        let observation = self.compare_placement(&segment_key, replicas);
        if observation != PlacementObservation::Accepted {
            return observation;
        }

        // need to place!
        let leader = replicas.leader().cloned().unwrap();
        let ready_replicas = if self.offset_ledger.is_placement_ready(&segment_key) {
            HashSet::from([node_id.clone()])
        } else {
            HashSet::new()
        };
        self.coordination.placements.insert(
            segment_key.placement_key(),
            OffsetPlacement {
                segment_key,
                shard_group_id,
                leader,
                replicas: replicas.clone(),
                ready_replicas,
                bootstrap_acked: HashSet::new(),
                placement_ack_sent: false,
            },
        );
        PlacementObservation::Accepted
    }

    fn compare_placement(
        &self,
        segment_key: &SegmentKey,
        replicas: &Replicas,
    ) -> PlacementObservation {
        self.coordination
            .placements
            .get(&(segment_key.topic_id, segment_key.range_id))
            .map(|placement| placement.compare(segment_key, replicas))
            .unwrap_or(PlacementObservation::Accepted)
    }

    /// 1. records placement to [`ConsumerOffsetCoordination`](Self::coordination)
    /// 2. Takes a State Snapshot
    /// 3. Initiates Follower Bootstrapping
    pub(crate) fn install_leader_placement(
        &mut self,
        segment_key: SegmentKey,
        shard_group_id: ShardGroupId,
        replicas: Replicas,
    ) -> Option<DataTransportCommand> {
        let leader = replicas.leader().cloned()?;

        let placement_key = segment_key.placement_key();
        let ready_replicas =
            self.derive_offset_commit_replicas(segment_key, &replicas, placement_key);

        match self.observe_placement(segment_key, shard_group_id, &replicas, &leader) {
            PlacementObservation::Accepted => {
                self.get_placement_mut(&placement_key)
                    .unwrap()
                    .ready_replicas = ready_replicas;
            }
            PlacementObservation::Unchanged => {
                if let Some(placement) = self.get_placement_mut(&placement_key) {
                    placement.placement_ack_sent = false;
                }
            }
            PlacementObservation::Stale | PlacementObservation::Conflict => return None,
        }

        let placement = self
            .get_placement(&placement_key)
            .expect("accepted or unchanged placement must exist");

        if !placement.is_ready(&leader) {
            let targets: Box<[_]> = placement.replicas.followers().cloned().collect();
            if targets.is_empty() {
                return None;
            }
            return Some(DataTransportCommand::send_to_targets(
                targets,
                RequestConsumerOffsetSnapshot {
                    segment_key,
                    replicas: placement.replicas.clone(),
                    requester: leader,
                },
            ));
        }

        let targets = placement.unready_followers();
        if targets.is_empty() {
            return None;
        }
        Some(DataTransportCommand::send_to_targets(
            targets,
            self.raise_snapshot(segment_key, placement.replicas.clone()),
        ))
    }

    fn derive_offset_commit_replicas(
        &self,
        segment_key: SegmentKey,
        replicas: &Replicas,
        placement_key: (TopicId, RangeId),
    ) -> HashSet<NodeId> {
        let leader = replicas.leader().unwrap();
        let retained_replicas: HashSet<_> = replicas.iter().cloned().collect();

        // Only replicas already eligible for offset commits retain that
        // eligibility across placement graduation.
        let mut ready_replicas: HashSet<NodeId> = self
            .get_placement(&placement_key)
            .map(|placement| {
                placement
                    .ready_replicas
                    .intersection(&retained_replicas)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        if segment_key.segment_id == SegmentId(0)
            || self.offset_ledger.is_placement_ready(&segment_key)
        {
            ready_replicas.insert(leader.clone());
        }
        ready_replicas
    }

    fn get_placement(&self, placement_key: &(TopicId, RangeId)) -> Option<&OffsetPlacement> {
        self.coordination.placements.get(placement_key)
    }

    fn get_placement_followers(&mut self, cmd: &CommitConsumerOffset) -> HashSet<NodeId> {
        self.get_placement(&cmd.update.key.placement_key())
            .map(|placement| {
                let mut followers = placement.ready_replicas.clone();
                followers.remove(&placement.leader);
                followers
            })
            .unwrap_or_default()
    }

    fn get_placement_mut(
        &mut self,
        placement_key: &(TopicId, RangeId),
    ) -> Option<&mut OffsetPlacement> {
        self.coordination.placements.get_mut(placement_key)
    }

    pub(crate) fn install_consumer_offsets(
        &mut self,
        cmd: InstallConsumerOffsetSnapshot,
        node_id: &NodeId,
    ) -> Vec<FutureOffsetCommit> {
        let leader = cmd.replica_set.leader().cloned().unwrap();
        let mut bootstrapped_keys = HashSet::new();
        for entry in cmd.entries {
            bootstrapped_keys.insert(entry.key.clone());
            self.coordination
                .pending_offset_mutations
                .push(PendingOffsetMutation::new(
                    OffsetRecord::BootstrapEntry(entry),
                    OffsetMutationCompletion::EpochSeal,
                ));
        }
        self.coordination
            .pending_offset_mutations
            .push(PendingOffsetMutation::new(
                OffsetRecord::PlacementReady(cmd.segment_key),
                ConsumerOffsetSnapshotInstalled {
                    segment_key: cmd.segment_key,
                    from: node_id.clone(),
                    leader,
                },
            ));
        bootstrapped_keys
            .into_iter()
            .flat_map(|key| self.take_future_commits(key))
            .collect()
    }

    pub(crate) fn create_offset_snapshot(
        &self,
        request: RequestConsumerOffsetSnapshot,
        source_node_id: &NodeId,
    ) -> Option<DataTransportCommand> {
        let live_ready = self
            .get_placement(&request.segment_key.placement_key())
            .is_some_and(|placement| {
                placement.segment_key == request.segment_key && placement.is_ready(source_node_id)
            });

        // Reject only when the source is neither live-ready nor durably eligible.
        if !live_ready
            && !self
                .offset_ledger
                .can_source_snapshot_for(&request.segment_key)
        {
            return None;
        }
        Some(DataTransportCommand::send_to_targets(
            vec![request.requester.clone()],
            self.raise_snapshot(request.segment_key, request.replicas),
        ))
    }

    fn raise_snapshot(
        &self,
        segment_key: SegmentKey,
        replica_set: Replicas,
    ) -> InstallConsumerOffsetSnapshot {
        InstallConsumerOffsetSnapshot {
            entries: self
                .offset_ledger
                .snapshot_range(segment_key.topic_id, segment_key.range_id),
            segment_key,
            replica_set,
        }
    }

    pub(crate) fn create_offset_snapshot_for_unready_replicas(
        &self,
        segment_key: SegmentKey,
    ) -> Option<DataTransportCommand> {
        let placement = self.get_placement(&segment_key.placement_key())?;
        if !placement.can_bootstrap_replicas(&segment_key) {
            return None;
        }

        let targets = placement.unready_followers();

        if targets.is_empty() {
            return None;
        }

        Some(DataTransportCommand::send_to_targets(
            targets,
            self.raise_snapshot(segment_key, placement.replicas.clone()),
        ))
    }

    pub(crate) fn handle_bootstrap_ack(&mut self, ack: &ConsumerOffsetSnapshotInstalled) {
        let Some(placement) = self.get_placement_mut(&ack.segment_key.placement_key()) else {
            return;
        };
        if placement.segment_key != ack.segment_key || !placement.replicas.contains(&ack.from) {
            return;
        }
        placement.bootstrap_acked.insert(ack.from.clone());
        self.promote_drained_replicas();
    }

    fn promote_drained_replicas(&mut self) {
        let promotable: Vec<(TopicId, RangeId, NodeId)> = self
            .coordination
            .placements
            .iter()
            .flat_map(|(&(topic_id, range_id), placement)| {
                placement
                    .bootstrap_acked
                    .iter()
                    .filter(|node| !self.coordination.offset_replication.has_pending_for(node))
                    .cloned()
                    .map(move |node| (topic_id, range_id, node))
            })
            .collect();
        for (topic_id, range_id, node) in promotable {
            if let Some(placement) = self.coordination.placements.get_mut(&(topic_id, range_id)) {
                placement.ready_replicas.insert(node);
            }
        }
    }

    pub(crate) fn ready_placement_acks(&mut self, from: &NodeId) -> Vec<SegmentPlaced> {
        let mut acks = Vec::new();
        for placement in self.coordination.placements.values_mut() {
            if !placement.placement_ack_sent
                && placement.leader == *from
                && placement.ready_replicas.len() == placement.replicas.len()
            {
                placement.placement_ack_sent = true;
                acks.push(SegmentPlaced {
                    segment_key: placement.segment_key,
                    shard_group_id: placement.shard_group_id,
                    from: from.clone(),
                });
            }
        }
        acks
    }

    pub(crate) fn get_replica_set_if_leader(
        &self,
        key: &ConsumerOffsetKey,
        node_id: &NodeId,
    ) -> Result<Replicas, Option<NodeId>> {
        let Some(placement) = self.get_placement(&key.placement_key()) else {
            return Err(None);
        };
        if placement.leader != *node_id {
            return Err(Some(placement.leader.clone()));
        }
        if !placement.ready_replicas.contains(node_id) {
            return Err(None);
        }
        Ok(placement.replicas.clone())
    }

    pub(crate) fn has_pending_offsets(&self) -> bool {
        !self.coordination.pending_offset_mutations.is_empty()
    }

    pub(crate) fn push_pending_offset_mutation(&mut self, cmd: impl Into<PendingOffsetMutation>) {
        self.coordination.pending_offset_mutations.push(cmd.into());
    }

    pub(crate) fn handle_replica_offset_ack(&mut self, ack: ConsumerOffsetReplicated) {
        self.coordination.offset_replication.process_ack(ack);
        self.promote_drained_replicas();
    }

    pub(crate) fn handle_consumer_group_epoch_seal(
        &mut self,
        cmd: EpochSeal,
    ) -> Option<Vec<FutureOffsetCommit>> {
        let key = cmd.key.clone();
        // Idempotency
        if cmd.generation <= self.latest_generation(&key) {
            return None;
        }
        self.push_pending_offset_mutation(cmd);
        Some(self.take_future_commits(key))
    }

    pub(crate) fn latest_generation(&self, key: &ConsumerOffsetKey) -> GenerationId {
        self.coordination
            .pending_offset_mutations
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
            // No pending EpochSeals, fall back to the actual ledger's generation
            .unwrap_or_else(|| self.offset_ledger.generation(key))
    }

    pub(crate) fn encode_offsets(&mut self) -> Result<Vec<Vec<u8>>, std::io::Error> {
        match self
            .coordination
            .pending_offset_mutations
            .iter()
            .map(|pending| borsh::to_vec(&pending.record))
            .collect()
        {
            Ok(encoded) => Ok(encoded),
            Err(error) => {
                tracing::error!(?error, "consumer offset WAL encoding failed");
                for pending in std::mem::take(&mut self.coordination.pending_offset_mutations) {
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
        std::mem::take(&mut self.coordination.pending_offset_mutations)
    }

    pub(crate) fn fail_all(&mut self, error: &str) {
        self.coordination.offset_replication.fail_all(error);

        for pending in self.take_pending() {
            if let OffsetMutationCompletion::LeaderCommit(commit) = pending.completion {
                let _ = commit
                    .reply
                    .send(ConsumerOffsetCommitAck::InternalError(error.to_string()));
            }
        }

        for parked in self
            .coordination
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

                    let seq = self.coordination.offset_replication.begin(
                        followers.clone().into_iter().collect(),
                        commit.required_followers,
                        offset_commit.clone(),
                        commit.reply,
                    );
                    transports.push(DataTransportCommand::send_to_targets(
                        followers,
                        ReplicateConsumerOffset {
                            seq,
                            segment_key: self
                                .get_placement(&offset_commit.key.placement_key())
                                .map(|placement| placement.segment_key)
                                .unwrap(),
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
                        ConsumerOffsetReplicated {
                            seq: commit.seq,
                            update: commit.update,
                            from: node_id.clone(),
                            result: ConsumerOffsetReplicationResult::Committed,
                        },
                    ));
                }
                OffsetMutationCompletion::Bootstrap(ack) => {
                    transports.push(DataTransportCommand::send_to_targets(
                        vec![ack.leader.clone()],
                        ack,
                    ));
                }
            }
        }
        self.checkpoint
            .uncheckpointed_lsns
            .push_back(uncheckpointed_lsn);

        transports
    }

    fn take_future_commits(&mut self, key: ConsumerOffsetKey) -> Vec<FutureOffsetCommit> {
        self.coordination
            .future_offset_commits
            .remove(&key)
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn uncheckpointed_offset_lsns(&self) -> &VecDeque<u64> {
        &self.checkpoint.uncheckpointed_lsns
    }

    #[cfg(test)]
    pub(crate) fn offset_checkpoint_lsn(&self) -> u64 {
        self.checkpoint.checkpoint_lsn
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
    use crate::data_plane::messages::DataPlanePeerMessage;
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

    fn segment(id: u64) -> SegmentKey {
        SegmentKey::new(TopicId(1), RangeId(2), id.into())
    }

    #[test]
    fn observed_placement_restores_local_durable_readiness() {
        let local = NodeId::new("b");
        let current = segment(2);
        let mut ledger = OffsetLedger::default();
        ledger.apply(OffsetRecord::PlacementReady(current));
        let mut manager = ConsumerOffsetManager::new(ledger);

        let replicas = Replicas::new(vec![NodeId::new("a"), local.clone(), NodeId::new("c")]);
        manager.observe_placement(current, ShardGroupId(0), &replicas, &local);

        let placement = manager
            .coordination
            .placements
            .get(&(current.topic_id, current.range_id))
            .unwrap();
        assert_eq!(placement.ready_replicas, HashSet::from([local]));
    }

    #[test]
    fn repeated_follower_placement_preserves_live_coordination_state() {
        let local = NodeId::new("b");
        let leader = NodeId::new("a");
        let replicas = Replicas::new(vec![leader, local.clone()]);
        let current = segment(2);
        let mut manager = ConsumerOffsetManager::new(OffsetLedger::default());
        manager.observe_placement(current, ShardGroupId(0), &replicas, &local);
        let live_placement = manager
            .coordination
            .placements
            .get_mut(&(current.topic_id, current.range_id))
            .unwrap();
        live_placement.bootstrap_acked.insert(local.clone());
        live_placement.placement_ack_sent = true;

        manager.observe_placement(current, ShardGroupId(0), &replicas, &local);

        let placement = manager
            .coordination
            .placements
            .get(&(current.topic_id, current.range_id))
            .unwrap();
        assert_eq!(placement.bootstrap_acked, HashSet::from([local]));
        assert!(placement.placement_ack_sent);
    }

    #[test]
    fn stale_follower_batch_cannot_replace_newer_placement() {
        let local = NodeId::new("b");
        let current_replicas = Replicas::new(vec![NodeId::new("a"), local.clone()]);
        let stale_replicas = Replicas::new(vec![NodeId::new("old"), local.clone()]);
        let current = segment(2);
        let stale = segment(1);
        let mut manager = ConsumerOffsetManager::new(OffsetLedger::default());
        manager.observe_placement(current, ShardGroupId(0), &current_replicas, &local);

        manager.observe_placement(stale, ShardGroupId(0), &stale_replicas, &local);

        let placement = manager
            .coordination
            .placements
            .get(&(current.topic_id, current.range_id))
            .unwrap();
        assert_eq!(placement.segment_key, current);
        assert_eq!(placement.replicas, current_replicas);
    }

    #[test]
    fn leader_placement_rejects_stale_and_conflicting_assignments() {
        let leader = NodeId::new("a");
        let follower = NodeId::new("b");
        let current = segment(2);
        let current_replicas = Replicas::new(vec![leader.clone(), follower]);
        let mut manager = ConsumerOffsetManager::new(OffsetLedger::default());

        manager.install_leader_placement(current, ShardGroupId(7), current_replicas.clone());

        assert!(
            manager
                .install_leader_placement(
                    segment(1),
                    ShardGroupId(7),
                    Replicas::new(vec![leader.clone(), NodeId::new("old")]),
                )
                .is_none()
        );
        assert!(
            manager
                .install_leader_placement(
                    current,
                    ShardGroupId(7),
                    Replicas::new(vec![leader, NodeId::new("conflict")]),
                )
                .is_none()
        );

        let placement = manager
            .coordination
            .placements
            .get(&(current.topic_id, current.range_id))
            .unwrap();
        assert_eq!(placement.segment_key, current);
        assert_eq!(placement.replicas, current_replicas);
    }

    #[test]
    fn a_new_unready_leader_cannot_commit_or_publish_snapshots() {
        let old_leader = NodeId::new("a");
        let retained = NodeId::new("b");
        let new_leader = NodeId::new("d");
        let mut manager = ConsumerOffsetManager::new(OffsetLedger::default());
        manager.install_leader_placement(
            segment(1),
            ShardGroupId(7),
            Replicas::new(vec![old_leader, retained.clone()]),
        );
        manager
            .get_placement_mut(&segment(1).placement_key())
            .unwrap()
            .ready_replicas
            .insert(retained.clone());

        let snapshots = manager.install_leader_placement(
            segment(2),
            ShardGroupId(7),
            Replicas::new(vec![new_leader.clone(), retained.clone()]),
        );

        assert!(snapshots.iter().any(|command| matches!(
            command,
            DataTransportCommand::SendToTargets(send)
                if send.targets.contains(&retained)
                    && matches!(
                        send.message,
                        DataPlanePeerMessage::RequestConsumerOffsetSnapshot(_)
                    )
        )));
        let placement = manager.get_placement(&segment(2).placement_key()).unwrap();
        assert_eq!(placement.ready_replicas, HashSet::from([retained]));

        let (reply, mut response) = oneshot::channel();
        assert!(!manager.commit_consumer_offset(
            CommitConsumerOffset {
                update: ConsumerOffsetUpdate {
                    key: key(),
                    generation: GenerationId(0),
                    position: position(),
                },
                reply,
            },
            &new_leader,
        ));
        assert_eq!(
            response.try_recv().unwrap(),
            ConsumerOffsetCommitAck::NotWriteLeader(None)
        );
    }

    #[test]
    fn only_a_durably_ready_replica_can_bootstrap_a_new_leader() {
        let source = NodeId::new("b");
        let new_leader = NodeId::new("d");
        let old_segment = segment(1);
        let new_segment = segment(2);
        let replica_set = Replicas::new(vec![new_leader.clone(), source]);
        let request = RequestConsumerOffsetSnapshot {
            segment_key: new_segment,
            replicas: replica_set.clone(),
            requester: new_leader.clone(),
        };

        let empty = ConsumerOffsetManager::new(OffsetLedger::default());
        assert!(
            empty
                .create_offset_snapshot(request.clone(), &NodeId::new("empty"))
                .is_none()
        );

        let mut ledger = OffsetLedger::default();
        ledger.apply(OffsetRecord::PlacementReady(old_segment));
        ledger.apply(OffsetRecord::EpochSeal(EpochSeal {
            key: key(),
            generation: GenerationId(1),
        }));
        ledger.apply(OffsetRecord::OffsetCommit(ConsumerOffsetUpdate {
            key: key(),
            generation: GenerationId(1),
            position: position(),
        }));
        let source_manager = ConsumerOffsetManager::new(ledger);

        let skipped_request = RequestConsumerOffsetSnapshot {
            segment_key: segment(3),
            replicas: replica_set.clone(),
            requester: new_leader.clone(),
        };
        assert!(
            source_manager
                .create_offset_snapshot(skipped_request, &NodeId::new("b"))
                .is_none(),
            "a replica removed for an intervening placement is stale"
        );

        let DataTransportCommand::SendToTargets(send) = source_manager
            .create_offset_snapshot(request, &NodeId::new("b"))
            .unwrap()
        else {
            panic!("snapshot must be sent directly to the new leader");
        };
        assert_eq!(send.targets.as_ref(), &[new_leader]);
        let DataPlanePeerMessage::InstallConsumerOffsetSnapshot(snapshot) = send.message else {
            panic!("expected an offset snapshot");
        };
        assert_eq!(snapshot.segment_key, new_segment);
        assert_eq!(snapshot.replica_set, replica_set);
        assert_eq!(snapshot.entries.len(), 1);
    }

    #[test]
    fn joining_replica_is_not_required_until_bootstrap_and_commit_drain() {
        let leader = NodeId::new("a");
        let retained = NodeId::new("b");
        let removed = NodeId::new("c");
        let joining = NodeId::new("d");
        let mut ledger = OffsetLedger::default();
        ledger.apply(OffsetRecord::EpochSeal(EpochSeal {
            key: key(),
            generation: GenerationId(1),
        }));
        ledger.apply(OffsetRecord::OffsetCommit(ConsumerOffsetUpdate {
            key: key(),
            generation: GenerationId(1),
            position: position(),
        }));
        let mut manager = ConsumerOffsetManager::new(ledger);
        manager.coordination.placements.insert(
            (TopicId(1), RangeId(2)),
            OffsetPlacement {
                segment_key: segment(1),
                shard_group_id: ShardGroupId(7),
                leader: leader.clone(),
                replicas: Replicas::new(vec![leader.clone(), retained.clone(), removed.clone()]),
                ready_replicas: HashSet::from([leader.clone(), retained.clone(), removed]),
                bootstrap_acked: HashSet::new(),
                placement_ack_sent: true,
            },
        );

        let bootstraps = manager.install_leader_placement(
            segment(2),
            ShardGroupId(7),
            Replicas::new(vec![leader.clone(), retained.clone(), joining.clone()]),
        );
        assert!(bootstraps.is_some(), "only the joining replica bootstraps");

        let advanced = ConsumerOffsetPosition {
            entry_id: EntryId(4),
            batch_offset: 0,
            absolute_offset: 5,
        };
        let update = ConsumerOffsetUpdate {
            key: key(),
            generation: GenerationId(1),
            position: advanced,
        };
        let (reply, mut response) = oneshot::channel();
        assert!(manager.commit_consumer_offset(
            CommitConsumerOffset {
                update: update.clone(),
                reply,
            },
            &leader,
        ));
        let transports = manager.flush_batch(&leader, 1);
        let replicated = transports
            .into_iter()
            .find_map(|transport| match transport {
                DataTransportCommand::SendToTargets(send) => match send.message {
                    DataPlanePeerMessage::ReplicateConsumerOffset(commit) => Some(commit),
                    _ => None,
                },
                _ => None,
            })
            .unwrap();
        assert!(response.try_recv().is_err());

        manager.handle_bootstrap_ack(&ConsumerOffsetSnapshotInstalled {
            segment_key: segment(2),
            from: joining.clone(),
            leader: leader.clone(),
        });
        assert!(manager.ready_placement_acks(&leader).is_empty());

        manager.handle_replica_offset_ack(ConsumerOffsetReplicated {
            seq: replicated.seq,
            update: update.clone(),
            from: retained,
            result: ConsumerOffsetReplicationResult::Committed,
        });
        assert_eq!(
            response.try_recv().unwrap(),
            ConsumerOffsetCommitAck::Committed
        );
        assert!(manager.ready_placement_acks(&leader).is_empty());

        manager.handle_replica_offset_ack(ConsumerOffsetReplicated {
            seq: replicated.seq,
            update,
            from: joining,
            result: ConsumerOffsetReplicationResult::Committed,
        });
        assert_eq!(manager.ready_placement_acks(&leader).len(), 1);
    }

    #[test]
    fn fail_all_resolves_pending_in_flight_and_future_clients() {
        let mut manager = ConsumerOffsetManager::new(OffsetLedger::default());
        let (pending_reply, mut pending_response) = oneshot::channel();
        manager
            .coordination
            .pending_offset_mutations
            .push(PendingOffsetMutation::new(
                OffsetRecord::OffsetCommit(ConsumerOffsetUpdate {
                    key: key(),
                    generation: GenerationId(1),
                    position: position(),
                }),
                LeaderOffsetCommitApplied {
                    replica_set: Replicas::new(vec![NodeId::new("leader")]),
                    required_followers: HashSet::new(),
                    reply: pending_reply,
                },
            ));

        let (future_reply, mut future_response) = oneshot::channel();
        manager.coordination.future_offset_commits.insert(
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
        manager.coordination.offset_replication.begin(
            HashSet::from_iter([NodeId::new("follower")]),
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
        assert!(manager.coordination.pending_offset_mutations.is_empty());
        assert!(manager.coordination.future_offset_commits.is_empty());
    }
}
