mod log_state;
mod transient_state;

use crate::control_plane::Replicas;
use crate::control_plane::consensus::messages::{InstallSnapshot, LogMutation};
use crate::control_plane::consensus::raft::log::LogEntry;
use crate::control_plane::consensus::raft::storage::{RaftPersistentState, RaftSnapshot};
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::MetadataCommand;
use crate::control_plane::metadata::event::SegmentReassigned;
use crate::data_plane::SegmentKey;
use crate::data_plane::messages::command::SegmentCaughtUp;
use crate::data_plane::transport::command::DataTransportCommand;
use crate::{client::EntryId, control_plane::NodeId};
use std::{
    collections::{BTreeSet, HashSet},
    ops::RangeInclusive,
};

use log_state::LogState;
pub(crate) use log_state::SnapshotAlreadyStaged;
use transient_state::{InTransitSnapshot, TransientState};
pub(crate) use transient_state::{LeaderlessSegments, PeerState, Role, SNAPSHOT_CHUNK_BYTES};

pub(crate) enum SnapshotInstallOutcome {
    AlreadyInstalled,
    ChunkAccepted { next_offset: u64 },
    Staged,
    ChunkRejected { retry_offset: u64 },
}

/// Owns Raft consensus state while preserving its durable/volatile boundary.
pub(crate) struct ConsensusState {
    log: LogState,
    transient: TransientState,
}

impl ConsensusState {
    pub(crate) fn from_persistent(
        persistent: RaftPersistentState,
        election_jitter_seed: u64,
    ) -> Self {
        let log = LogState::from_persistent(persistent);
        let restored_commit_index = log.last_included_index();
        Self {
            log,
            transient: TransientState::new(election_jitter_seed, restored_commit_index),
        }
    }

    pub(crate) fn take_log_mutations(&mut self) -> Vec<LogMutation> {
        self.log.take_mutations()
    }
    pub(crate) fn last_log_index(&self) -> u64 {
        self.log.last_index()
    }
    pub(crate) fn last_log_term(&self) -> u64 {
        self.log.last_term()
    }
    pub(crate) fn log_term_at(&self, index: u64) -> u64 {
        self.log.term_at(index)
    }
    pub(crate) fn log_entry(&self, index: u64) -> Option<&LogEntry> {
        self.log.get(index)
    }
    pub(crate) fn log_entries_from(&self, index: u64) -> Box<[LogEntry]> {
        self.log.entries_from(index)
    }
    pub(crate) fn append_log(&mut self, entry: LogEntry) {
        self.log.append(entry);
    }
    pub(crate) fn truncate_log_from(&mut self, index: u64) {
        self.log.truncate_from(index);
    }
    pub(crate) fn stabled_index(&self) -> u64 {
        self.log.stabled_index()
    }
    pub(crate) fn last_included_index(&self) -> u64 {
        self.log.last_included_index()
    }
    pub(crate) fn snapshot(&self) -> Option<&RaftSnapshot> {
        self.log.snapshot()
    }
    pub(crate) fn stage_snapshot(
        &mut self,
        snapshot: RaftSnapshot,
    ) -> Result<(), SnapshotAlreadyStaged> {
        self.log.stage_snapshot(snapshot)
    }
    pub(crate) fn take_snapshot_ack_target(&mut self) -> Option<NodeId> {
        self.transient.snapshot_ack_target.take()
    }
    pub(crate) fn apply_snapshot(&mut self) -> Option<RaftSnapshot> {
        self.log.publish_snapshot()
    }
    pub(crate) fn install_snapshot_chunk(
        &mut self,
        req: &InstallSnapshot,
    ) -> SnapshotInstallOutcome {
        if req.header.last_included_index <= self.last_included_index() {
            return SnapshotInstallOutcome::AlreadyInstalled;
        }
        if req.offset == 0 {
            self.transient.in_transit_snapshot = Some(InTransitSnapshot::new(req));
        }
        let Some(in_transit) = self.transient.in_transit_snapshot.as_mut() else {
            return SnapshotInstallOutcome::ChunkRejected { retry_offset: 0 };
        };

        if !in_transit.is_next_chunk(req) {
            return SnapshotInstallOutcome::ChunkRejected {
                retry_offset: in_transit.retry_offset(req),
            };
        }

        let next_offset = in_transit.append(&req.data);
        if !req.done {
            return SnapshotInstallOutcome::ChunkAccepted { next_offset };
        }

        let Some(in_transit_snapshot) = self.transient.in_transit_snapshot.take() else {
            return SnapshotInstallOutcome::ChunkRejected { retry_offset: 0 };
        };
        let Ok((leader, snapshot)) = in_transit_snapshot.finish() else {
            return SnapshotInstallOutcome::ChunkRejected { retry_offset: 0 };
        };
        if self.log.stage_snapshot(snapshot).is_err() {
            return SnapshotInstallOutcome::ChunkRejected { retry_offset: 0 };
        }
        self.transient.snapshot_ack_target = Some(leader);
        SnapshotInstallOutcome::Staged
    }

    /// Highest committed entry that is also locally durable and therefore safe to apply.
    pub(crate) fn ready_to_apply_index(&self) -> u64 {
        self.transient.commit_index.min(self.log.stabled_index())
    }
    pub(crate) fn advance_stabled_index(&mut self, index: u64) {
        self.log.advance_stabled_index(index);
    }
    pub(crate) fn log_entries(&self) -> &[LogEntry] {
        self.log.entries()
    }
    pub(crate) fn current_term(&self) -> u64 {
        self.log.current_term()
    }
    pub(crate) fn voted_for(&self) -> Option<&NodeId> {
        self.log.voted_for()
    }
    pub(crate) fn vote_available_for(&self, node: &NodeId) -> bool {
        self.log.vote_available_for(node)
    }
    pub(crate) fn grant_vote(&mut self, node: NodeId) {
        self.log.grant_vote(node);
    }
    pub(crate) fn advance_term(&mut self, term: u64) -> bool {
        self.log.advance_term(term)
    }

    pub(crate) fn begin_campaign(&mut self, node_id: &NodeId) -> u64 {
        let term = self.log.begin_election(node_id);
        self.transient.begin_campaign();
        term
    }
    pub(crate) fn record_vote(&mut self, term: u64, granted: bool, quorum: u32) -> bool {
        term == self.current_term() && granted && self.transient.record_vote(quorum)
    }
    pub(crate) fn role(&self) -> &Role {
        &self.transient.role
    }
    pub(crate) fn is_leader(&self) -> bool {
        self.transient.role == Role::Leader
    }
    pub(crate) fn current_leader(&self) -> Option<&NodeId> {
        self.transient.current_leader.as_ref()
    }
    pub(crate) fn initialize_leader(&mut self, node: &NodeId, peers: &HashSet<NodeId>) {
        let next_index = self.log.last_index() + 1;
        self.transient.initialize_leader(node, peers, next_index);
    }
    pub(crate) fn reset_for_follower(&mut self) {
        self.transient.reset_for_follower();
    }
    pub(crate) fn recognize_leader(&mut self, leader: NodeId) {
        self.transient.recognize_leader(leader);
    }

    pub(crate) fn commit_index(&self) -> u64 {
        self.transient.commit_index
    }

    pub(crate) fn uncommited_log_range(&self) -> RangeInclusive<u64> {
        self.commit_index() + 1..=self.last_log_index()
    }
    pub(crate) fn set_commit_index(&mut self, index: u64) {
        self.transient.commit_index = index;
    }
    pub(crate) fn advance_follower_commit(&mut self, leader_commit: u64) {
        if leader_commit > self.commit_index() {
            self.transient.commit_index = leader_commit.min(self.last_log_index());
        }
    }
    pub(crate) fn replicated_voter_count(&self, index: u64) -> u32 {
        self.transient.replicated_voter_count(index)
    }
    pub(crate) fn is_peer_caught_up(&self, node: &NodeId) -> bool {
        self.transient.is_peer_caught_up(node)
    }
    pub(crate) fn is_learner_ready_for_promotion(&self, node: &NodeId) -> bool {
        self.transient.is_learner_ready_for_promotion(node)
    }
    pub(crate) fn peer_state(&self, node: &NodeId) -> Option<&PeerState> {
        self.transient
            .peer_states
            .get(node)
            .or_else(|| self.transient.learner_states.get(node))
    }
    pub(crate) fn peer_state_mut(&mut self, node: &NodeId) -> Option<&mut PeerState> {
        self.transient
            .peer_states
            .get_mut(node)
            .or_else(|| self.transient.learner_states.get_mut(node))
    }
    pub(crate) fn is_voter(&self, node: &NodeId) -> bool {
        self.transient.peer_states.contains_key(node)
    }
    pub(crate) fn voter_state_count(&self) -> usize {
        self.transient.peer_states.len()
    }
    pub(crate) fn has_voter_state(&self, node: &NodeId) -> bool {
        self.transient.peer_states.contains_key(node)
    }
    pub(crate) fn voter_states_empty(&self) -> bool {
        self.transient.peer_states.is_empty()
    }
    pub(crate) fn learner_states_empty(&self) -> bool {
        self.transient.learner_states.is_empty()
    }
    pub(crate) fn learner_state_count(&self) -> usize {
        self.transient.learner_states.len()
    }
    pub(crate) fn learner_ids(&self) -> impl Iterator<Item = &NodeId> {
        self.transient.learner_states.keys()
    }
    pub(crate) fn replication_targets(&self) -> Vec<NodeId> {
        self.transient
            .peer_states
            .keys()
            .chain(self.transient.learner_states.keys())
            .cloned()
            .collect()
    }
    pub(crate) fn is_learner(&self, node: &NodeId) -> bool {
        self.transient.learner_states.contains_key(node)
    }
    pub(crate) fn stage_learner(&mut self, node: NodeId, state: PeerState) {
        self.transient.learner_states.insert(node, state);
    }
    pub(crate) fn remove_learner(&mut self, node: &NodeId) -> Option<PeerState> {
        self.transient.learner_states.remove(node)
    }
    pub(crate) fn add_voter_state(&mut self, node: NodeId, state: PeerState) {
        self.transient.peer_states.insert(node, state);
    }
    pub(crate) fn remove_voter_state(&mut self, node: &NodeId) {
        self.transient.peer_states.remove(node);
    }

    pub(crate) fn clear_ring_observation(&mut self) {
        self.transient.ring_observation_streak = None;
    }
    pub(crate) fn record_ring_observation(&mut self, ring: &BTreeSet<NodeId>) -> u32 {
        self.transient.record_ring_observation(ring)
    }
    pub(crate) fn next_election_epoch(&mut self) -> u64 {
        self.transient.advance_election_epoch()
    }
    pub(crate) fn election_epoch(&self) -> u64 {
        self.transient.election_epoch
    }
    pub(crate) fn next_election_jitter(&mut self) -> u32 {
        self.transient.election_jitter.next()
    }

    pub(crate) fn take_pending_proposals(&mut self) -> Vec<MetadataCommand> {
        std::mem::take(&mut self.transient.pending_proposals)
    }
    pub(crate) fn push_pending_proposal(&mut self, command: MetadataCommand) {
        self.transient.pending_proposals.push(command);
    }
    pub(crate) fn extend_pending_proposals(
        &mut self,
        commands: impl IntoIterator<Item = MetadataCommand>,
    ) {
        self.transient.pending_proposals.extend(commands);
    }
    pub(crate) fn take_leaderless_segments(&mut self) -> LeaderlessSegments {
        std::mem::take(&mut self.transient.leaderless_segments)
    }
    pub(crate) fn push_leaderless_segment(&mut self, segment: (SegmentKey, Vec<NodeId>)) {
        self.transient.leaderless_segments.push(segment);
    }
    pub(crate) fn extend_leaderless_segments(&mut self, segments: LeaderlessSegments) {
        self.transient.leaderless_segments.extend(segments);
    }
    pub(crate) fn retain_confirmed_data_leaders(&mut self, active: &HashSet<SegmentKey>) {
        self.transient
            .confirmed_data_leaders
            .retain(|key, _| active.contains(key));
    }
    pub(crate) fn is_data_leader_confirmed(&self, key: &SegmentKey, node: &NodeId) -> bool {
        self.transient.confirmed_data_leaders.get(key) == Some(node)
    }
    pub(crate) fn confirm_data_leader(&mut self, key: SegmentKey, node: NodeId) {
        self.transient.confirmed_data_leaders.insert(key, node);
    }
    pub(crate) fn catch_up_redrives(&self, group_id: ShardGroupId) -> Vec<DataTransportCommand> {
        self.transient.catch_up.redrives(group_id)
    }
    pub(crate) fn confirm_catch_up(&mut self, ack: SegmentCaughtUp) {
        self.transient.catch_up.confirm(ack.segment_key, ack.from);
    }
    pub(crate) fn track_catch_up(&mut self, reassigned: &SegmentReassigned) {
        self.transient.catch_up.track(reassigned);
    }
    pub(crate) fn track_sealed_catch_up(
        &mut self,
        key: SegmentKey,
        start: EntryId,
        end: EntryId,
        replicas: Replicas,
    ) {
        self.transient
            .catch_up
            .track_sealed(key, start, end, replicas);
    }
    #[cfg(test)]
    pub(crate) fn clear_catch_up(&mut self) {
        self.transient.catch_up.clear();
    }
    #[cfg(test)]
    pub(crate) fn catch_up_is_empty(&self) -> bool {
        self.transient.catch_up.is_empty()
    }
}
