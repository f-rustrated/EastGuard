use super::consumer_offsets::state::{
    ConsumerOffsetKey, ConsumerOffsetPosition, ConsumerOffsets, EpochSeal, OffsetRecord, StaleEpoch,
};
use super::state::AuxiliarySnapshot;
use crate::client::EntryId;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
use crate::control_plane::{NodeId, Replicas};
use crate::data_plane::SegmentKey;

use crate::data_plane::auxiliary_states::consumer_offsets::{
    ConsumerOffsetCoordination, OffsetPlacement, OffsetReplicaState,
};
use crate::data_plane::checkpoint::AuxiliaryCheckpointJob;

use crate::data_plane::messages::command::{
    AuthorizedProducerIdentity, CommitConsumerOffset, ConsumerOffsetCommitAck,
    ConsumerOffsetReplicated, ConsumerOffsetReplicationResult, ConsumerOffsetSnapshot,
    ConsumerOffsetSnapshotInstalled,
};
use crate::data_plane::messages::{RequestConsumerOffsetSnapshot, SegmentPlaced};

use crate::data_plane::ProducerAppendIdentity;
use crate::data_plane::auxiliary_states::producer::{AppendKey, ProducerDecision, ProducerTracker};
use crate::data_plane::transport::command::DataTransportCommand;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;

use super::consumer_offsets::types::*;

#[derive(Default)]
pub(crate) struct AuxiliaryStateManager {
    offsets: ConsumerOffsets,
    consumer_coord: ConsumerOffsetCoordination, // live placements, pending mutations, parked commits, and replication tracking
    producer: ProducerTracker,
    checkpoint: AuxiliaryCheckpointState, // WAL reclamation and checkpoint progress
}

#[derive(Default)]
struct AuxiliaryCheckpointState {
    uncheckpointed_lsns: VecDeque<u64>,
    checkpoint_lsn: u64,
    in_flight: bool,
}

impl AuxiliaryStateManager {
    #[cfg(test)]
    pub(crate) fn new(offsets: ConsumerOffsets) -> Self {
        Self::from_recovery(offsets, ProducerTracker::default())
    }

    pub(crate) fn from_recovery(offsets: ConsumerOffsets, producer: ProducerTracker) -> Self {
        Self {
            offsets,
            producer,
            ..Default::default()
        }
    }

    pub(crate) fn verify_producer(
        &mut self,
        segment_key: SegmentKey,
        producer: AuthorizedProducerIdentity,
    ) -> ProducerDecision {
        self.producer.verify(segment_key, producer)
    }

    pub(crate) fn unstage_producer(&mut self, append_key: &AppendKey) {
        self.producer.unstage(append_key);
    }

    pub(crate) fn advance_producer(
        &mut self,
        segment_key: SegmentKey,
        producer: ProducerAppendIdentity,
        entry_id: EntryId,
        lsn: u64,
    ) {
        self.producer.advance(segment_key, producer, entry_id);
        self.mark_auxiliary_state_dirty(lsn);
    }

    pub(crate) fn push_future(&mut self, key: ConsumerOffsetKey, future: FutureOffsetCommit) {
        self.consumer_coord.future_entry(key).push(future);
    }

    pub(crate) fn offset(&self, key: &ConsumerOffsetKey) -> Option<ConsumerOffsetPosition> {
        self.offsets.offset(key)
    }

    pub(crate) fn durable_generation(&self, key: &ConsumerOffsetKey) -> GenerationId {
        self.offsets.generation(key)
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
            self.push_future(cmd.update.key.clone(), FutureOffsetCommit::Client(cmd));

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

    pub(crate) fn handle_auxiliary_checkpoint_complete(&mut self, checkpointed_lsn: u64) {
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

    pub(crate) fn mark_auxiliary_state_dirty(&mut self, lsn: u64) {
        if self.checkpoint.uncheckpointed_lsns.back() != Some(&lsn) {
            self.checkpoint.uncheckpointed_lsns.push_back(lsn);
        }
    }

    // Looks at the oldest uncheckpointed consumer offset commit. If there are consumer offsets in memory that haven't been
    // included in the persistent auxiliary snapshot, their WAL entries must be preserved.
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

    pub(crate) fn raise_auxiliary_checkpoint_job(
        &mut self,
        data_dir: PathBuf,
    ) -> Option<AuxiliaryCheckpointJob> {
        self.checkpoint.in_flight = true;
        Some(AuxiliaryCheckpointJob {
            checkpointed_lsn: *self.latest_uncheckpointed_lsn()?,
            snapshot: AuxiliarySnapshot {
                consumer_offsets: self.offsets.clone(),
                producer_sessions: self.producer.sessions(),
            },
            data_dir,
        })
    }

    #[cfg(any(test, debug_assertions))]
    pub(crate) fn assert_producer_invariants(&self) {
        self.producer.assert_invariants();
    }

    pub(crate) fn auxiliary_checkpoint_in_flight(&self) -> bool {
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
        let local_placement_installed = self.offsets.has_installed_placement(&segment_key);
        let replica_states = replicas
            .iter()
            .cloned()
            .map(|node| {
                let state = if node == *node_id && local_placement_installed {
                    OffsetReplicaState::Ready
                } else {
                    OffsetReplicaState::Joining
                };
                (node, state)
            })
            .collect();
        self.consumer_coord.placements.insert(
            segment_key.placement_key(),
            OffsetPlacement {
                segment_key,
                shard_group_id,
                replicas: replicas.clone(),
                replica_states,
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
        self.consumer_coord
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
        let replica_states =
            self.derive_offset_commit_replicas(segment_key, &replicas, placement_key);

        match self.observe_placement(segment_key, shard_group_id, &replicas, &leader) {
            PlacementObservation::Stale | PlacementObservation::Conflict => return None,
            PlacementObservation::Accepted => {
                let placement = self.get_placement_mut(&placement_key).unwrap();
                placement.replica_states = replica_states;
            }
            PlacementObservation::Unchanged => {
                let placement = self.get_placement_mut(&placement_key).unwrap();
                placement.placement_ack_sent = false;
            }
        }

        let placement = self
            .get_placement(&placement_key)
            .expect("accepted or unchanged placement must exist");

        if !placement.is_replica_ready(&leader) {
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
    ) -> HashMap<NodeId, OffsetReplicaState> {
        let leader = replicas.leader().unwrap();
        let current = self.get_placement(&placement_key);
        let mut replica_states: HashMap<_, _> = replicas
            .iter()
            .cloned()
            .map(|replica| {
                let state = if current.is_some_and(|placement| placement.is_replica_ready(&replica))
                {
                    OffsetReplicaState::Ready
                } else {
                    OffsetReplicaState::Joining
                };
                (replica, state)
            })
            .collect();

        if segment_key.segment_id == SegmentId(0)
            || self.offsets.has_installed_placement(&segment_key)
        {
            replica_states.insert(leader.clone(), OffsetReplicaState::Ready);
        }
        replica_states
    }

    fn get_placement(&self, placement_key: &(TopicId, RangeId)) -> Option<&OffsetPlacement> {
        self.consumer_coord.placements.get(placement_key)
    }

    fn get_placement_followers(&self, cmd: &CommitConsumerOffset) -> HashSet<NodeId> {
        self.get_placement(&cmd.update.key.placement_key())
            .map(|placement| {
                placement
                    .replica_states
                    .keys()
                    .filter(|n| placement.is_replica_ready(n) && *n != placement.leader())
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    fn get_placement_mut(
        &mut self,
        placement_key: &(TopicId, RangeId),
    ) -> Option<&mut OffsetPlacement> {
        self.consumer_coord.placements.get_mut(placement_key)
    }

    pub(crate) fn install_consumer_offsets(
        &mut self,
        cmd: ConsumerOffsetSnapshot,
        node_id: &NodeId,
    ) -> Vec<FutureOffsetCommit> {
        let leader = cmd.replica_set.leader().cloned().unwrap();
        let mut bootstrapped_keys = HashSet::new();
        for entry in cmd.entries {
            bootstrapped_keys.insert(entry.key.clone());
            self.consumer_coord.push_pending(PendingOffsetMutation::new(
                OffsetRecord::BootstrapEntry(entry),
                OffsetMutationCompletion::EpochSeal,
            ));
        }
        self.consumer_coord.push_pending(PendingOffsetMutation::new(
            OffsetRecord::PlacementInstalled(cmd.segment_key),
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
                placement.segment_key == request.segment_key
                    && placement.is_replica_ready(source_node_id)
            });

        // Reject only when the source is neither live-ready nor durably eligible.
        if !live_ready && !self.offsets.can_source_snapshot_for(&request.segment_key) {
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
    ) -> ConsumerOffsetSnapshot {
        ConsumerOffsetSnapshot {
            entries: self
                .offsets
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

    pub(crate) fn handle_snapshot_installed_ack(&mut self, ack: &ConsumerOffsetSnapshotInstalled) {
        let Some(placement) = self.get_placement_mut(&ack.segment_key.placement_key()) else {
            return;
        };
        if placement.segment_key != ack.segment_key || !placement.replicas.contains(&ack.from) {
            return;
        }
        placement
            .replica_states
            .insert(ack.from.clone(), OffsetReplicaState::SnapshotInstalled);
        self.mark_offset_replica_ready_if_caught_up(&ack.from);
    }

    fn mark_offset_replica_ready_if_caught_up(&mut self, node: &NodeId) -> bool {
        if self.consumer_coord.has_pending_replication_for(node) {
            return false;
        }
        let mut readiness_changed = false;
        for placement in self.consumer_coord.placements.values_mut() {
            if placement.replica_states.get(node) == Some(&OffsetReplicaState::SnapshotInstalled) {
                placement
                    .replica_states
                    .insert(node.clone(), OffsetReplicaState::Ready);
                readiness_changed = true;
            }
        }
        readiness_changed
    }

    pub(crate) fn drain_ready_placements(&mut self, from: &NodeId) -> Vec<SegmentPlaced> {
        self.consumer_coord
            .placements
            .values_mut()
            .filter_map(|placement| {
                if !placement.placement_ack_sent
                    && placement.leader() == from
                    && placement.all_replicas_ready()
                {
                    placement.placement_ack_sent = true;
                    return Some(SegmentPlaced {
                        segment_key: placement.segment_key,
                        shard_group_id: placement.shard_group_id,
                        from: from.clone(),
                    });
                }
                None
            })
            .collect()
    }

    pub(crate) fn get_replica_set_if_leader(
        &self,
        key: &ConsumerOffsetKey,
        node_id: &NodeId,
    ) -> Result<Replicas, Option<NodeId>> {
        let Some(placement) = self.get_placement(&key.placement_key()) else {
            return Err(None);
        };
        if placement.leader() != node_id {
            return Err(Some(placement.leader().clone()));
        }
        if !placement.is_replica_ready(node_id) {
            return Err(None);
        }
        Ok(placement.replicas.clone())
    }

    pub(crate) fn has_pending_offsets(&self) -> bool {
        self.consumer_coord.has_pending_offsets()
    }

    pub(crate) fn push_pending_offset_mutation(&mut self, cmd: impl Into<PendingOffsetMutation>) {
        self.consumer_coord.push_pending(cmd.into());
    }

    pub(crate) fn handle_replica_offset_ack(&mut self, ack: ConsumerOffsetReplicated) -> bool {
        let from = ack.from.clone();
        self.consumer_coord.ack_replication(ack);
        self.mark_offset_replica_ready_if_caught_up(&from)
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
        self.consumer_coord
            .latest_generation(key)
            // No pending EpochSeals, fall back to the actual ledger's generation
            .unwrap_or_else(|| self.offsets.generation(key))
    }

    pub(crate) fn encode_offsets(&mut self) -> Result<Vec<Vec<u8>>, std::io::Error> {
        self.consumer_coord.encode_offsets()
    }

    pub(crate) fn take_pending(&mut self) -> Vec<PendingOffsetMutation> {
        self.consumer_coord.take_pending()
    }

    pub(crate) fn fail_inflight_coordination(&mut self, error: &str) {
        self.consumer_coord.fail_all(error);
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
            self.offsets.apply(record.clone());
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

                    let seq = self.consumer_coord.begin_replication(
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
        self.consumer_coord.take_future_commits(&key)
    }

    #[cfg(test)]
    pub(crate) fn uncheckpointed_offset_lsns(&self) -> &VecDeque<u64> {
        &self.checkpoint.uncheckpointed_lsns
    }

    #[cfg(test)]
    pub(crate) fn auxiliary_checkpoint_lsn(&self) -> u64 {
        self.checkpoint.checkpoint_lsn
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::control_plane::metadata::EntryId;
    use crate::data_plane::auxiliary_states::consumer_offsets::state::{
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
        let mut ledger = ConsumerOffsets::default();
        ledger.apply(OffsetRecord::PlacementInstalled(current));
        let mut manager = AuxiliaryStateManager::new(ledger);

        let replicas = Replicas::new(vec![NodeId::new("a"), local.clone(), NodeId::new("c")]);
        manager.observe_placement(current, ShardGroupId(0), &replicas, &local);

        let placement = manager
            .consumer_coord
            .placements
            .get(&(current.topic_id, current.range_id))
            .unwrap();
        assert!(placement.is_replica_ready(&local));
    }

    #[test]
    fn repeated_follower_placement_preserves_live_coordination_state() {
        let local = NodeId::new("b");
        let leader = NodeId::new("a");
        let replicas = Replicas::new(vec![leader, local.clone()]);
        let current = segment(2);
        let mut manager = AuxiliaryStateManager::new(ConsumerOffsets::default());
        manager.observe_placement(current, ShardGroupId(0), &replicas, &local);
        let live_placement = manager
            .consumer_coord
            .placements
            .get_mut(&(current.topic_id, current.range_id))
            .unwrap();
        live_placement
            .replica_states
            .insert(local.clone(), OffsetReplicaState::SnapshotInstalled);
        live_placement.placement_ack_sent = true;

        manager.observe_placement(current, ShardGroupId(0), &replicas, &local);

        let placement = manager
            .consumer_coord
            .placements
            .get(&(current.topic_id, current.range_id))
            .unwrap();
        assert_eq!(
            placement.replica_states.get(&local),
            Some(&OffsetReplicaState::SnapshotInstalled)
        );
        assert!(placement.placement_ack_sent);
    }

    #[test]
    fn stale_follower_batch_cannot_replace_newer_placement() {
        let local = NodeId::new("b");
        let current_replicas = Replicas::new(vec![NodeId::new("a"), local.clone()]);
        let stale_replicas = Replicas::new(vec![NodeId::new("old"), local.clone()]);
        let current = segment(2);
        let stale = segment(1);
        let mut manager = AuxiliaryStateManager::new(ConsumerOffsets::default());
        manager.observe_placement(current, ShardGroupId(0), &current_replicas, &local);

        manager.observe_placement(stale, ShardGroupId(0), &stale_replicas, &local);

        let placement = manager
            .consumer_coord
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
        let mut manager = AuxiliaryStateManager::new(ConsumerOffsets::default());

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
            .consumer_coord
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
        let mut manager = AuxiliaryStateManager::new(ConsumerOffsets::default());
        manager.install_leader_placement(
            segment(1),
            ShardGroupId(7),
            Replicas::new(vec![old_leader, retained.clone()]),
        );
        manager
            .get_placement_mut(&segment(1).placement_key())
            .unwrap()
            .replica_states
            .insert(retained.clone(), OffsetReplicaState::Ready);

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
        assert!(placement.is_replica_ready(&retained));
        assert!(!placement.is_replica_ready(&new_leader));

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

        let empty = AuxiliaryStateManager::new(ConsumerOffsets::default());
        assert!(
            empty
                .create_offset_snapshot(request.clone(), &NodeId::new("empty"))
                .is_none()
        );

        let mut ledger = ConsumerOffsets::default();
        ledger.apply(OffsetRecord::PlacementInstalled(old_segment));
        ledger.apply(OffsetRecord::EpochSeal(EpochSeal {
            key: key(),
            generation: GenerationId(1),
        }));
        ledger.apply(OffsetRecord::OffsetCommit(ConsumerOffsetUpdate {
            key: key(),
            generation: GenerationId(1),
            position: position(),
        }));
        let source_manager = AuxiliaryStateManager::new(ledger);

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
        let mut ledger = ConsumerOffsets::default();
        ledger.apply(OffsetRecord::EpochSeal(EpochSeal {
            key: key(),
            generation: GenerationId(1),
        }));
        ledger.apply(OffsetRecord::OffsetCommit(ConsumerOffsetUpdate {
            key: key(),
            generation: GenerationId(1),
            position: position(),
        }));
        let mut manager = AuxiliaryStateManager::new(ledger);
        manager.consumer_coord.placements.insert(
            (TopicId(1), RangeId(2)),
            OffsetPlacement {
                segment_key: segment(1),
                shard_group_id: ShardGroupId(7),
                replicas: Replicas::new(vec![leader.clone(), retained.clone(), removed.clone()]),
                replica_states: HashMap::from([
                    (leader.clone(), OffsetReplicaState::Ready),
                    (retained.clone(), OffsetReplicaState::Ready),
                    (removed, OffsetReplicaState::Ready),
                ]),
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

        manager.handle_snapshot_installed_ack(&ConsumerOffsetSnapshotInstalled {
            segment_key: segment(2),
            from: joining.clone(),
            leader: leader.clone(),
        });
        assert!(manager.drain_ready_placements(&leader).is_empty());

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
        assert!(manager.drain_ready_placements(&leader).is_empty());

        manager.handle_replica_offset_ack(ConsumerOffsetReplicated {
            seq: replicated.seq,
            update,
            from: joining,
            result: ConsumerOffsetReplicationResult::Committed,
        });
        assert_eq!(manager.drain_ready_placements(&leader).len(), 1);
    }

    #[test]
    fn fail_all_resolves_pending_in_flight_and_future_clients() {
        let mut manager = AuxiliaryStateManager::new(ConsumerOffsets::default());
        let (pending_reply, mut pending_response) = oneshot::channel();
        manager
            .consumer_coord
            .push_pending(PendingOffsetMutation::new(
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
        manager.push_future(
            key(),
            FutureOffsetCommit::Client(CommitConsumerOffset {
                update: ConsumerOffsetUpdate {
                    key: key(),
                    generation: GenerationId(2),
                    position: position(),
                },
                reply: future_reply,
            }),
        );

        let (replication_reply, mut replication_response) = oneshot::channel();
        manager.consumer_coord.begin_replication(
            HashSet::from_iter([NodeId::new("follower")]),
            HashSet::from_iter([NodeId::new("follower")]),
            ConsumerOffsetUpdate {
                key: key(),
                generation: GenerationId(1),
                position: position(),
            },
            replication_reply,
        );

        manager.fail_inflight_coordination("WAL failed");

        let expected = ConsumerOffsetCommitAck::InternalError("WAL failed".into());
        assert_eq!(pending_response.try_recv().unwrap(), expected);
        assert_eq!(future_response.try_recv().unwrap(), expected);
        assert_eq!(replication_response.try_recv().unwrap(), expected);
        assert!(manager.consumer_coord.is_empty());
    }
}
