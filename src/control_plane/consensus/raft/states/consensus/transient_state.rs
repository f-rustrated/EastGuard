use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::InstallSnapshot;
use crate::control_plane::consensus::raft::catch_up::CatchUpRepairs;
use crate::control_plane::consensus::raft::storage::{
    RaftSnapshot, RaftSnapshotHeader, SnapshotData,
};
use crate::control_plane::metadata::MetadataCommand;
use crate::data_plane::SegmentKey;
use borsh::BorshDeserialize;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};

const ELECTION_JITTER_RANGE: u32 = 20;
pub(crate) const SNAPSHOT_CHUNK_BYTES: usize = 256 * 1024;

/// Segments whose write leader crashed (sole death).
pub(crate) type LeaderlessSegments = Vec<(SegmentKey, Vec<NodeId>)>;

pub(crate) struct ElectionJitter {
    seed: u64,
    counter: u64,
}

impl ElectionJitter {
    fn new(seed: u64) -> Self {
        Self { seed, counter: 0 }
    }

    pub(crate) fn next(&mut self) -> u32 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.seed.hash(&mut hasher);
        self.counter.hash(&mut hasher);
        self.counter += 1;
        (hasher.finish() % ELECTION_JITTER_RANGE as u64) as u32
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate { votes_received: u32 },
    Leader,
}

// Peer tracking is leader-only. `next_index` is the leader's guess;
// `match_index` is confirmed replication progress.
#[derive(Debug, Clone)]
pub(crate) struct PeerState {
    pub(crate) next_index: u64,
    pub(crate) match_index: u64,
}

pub(crate) struct InTransitSnapshot {
    term: u64,
    leader_id: NodeId,
    header: RaftSnapshotHeader,
    bytes: Vec<u8>,
}

impl InTransitSnapshot {
    pub(crate) fn new(req: &InstallSnapshot) -> Self {
        Self {
            term: req.term,
            leader_id: req.leader_id.clone(),
            header: req.header.clone(),
            bytes: Vec::new(),
        }
    }

    pub(crate) fn is_next_chunk(&self, req: &InstallSnapshot) -> bool {
        let next_offset = self.next_offset().saturating_add(req.data.len() as u64);
        self.same_transfer(req)
            && self.is_next_offset(req.offset)
            && req.data.len() <= SNAPSHOT_CHUNK_BYTES
            // Will appending this new chunk cause the total installed data to exceed the declared total
            && next_offset <= self.header.size_bytes
            && req.done == (next_offset == self.header.size_bytes)
    }

    //  Does this chunk start exactly where the last one left off?
    fn is_next_offset(&self, byte_offset: u64) -> bool {
        self.next_offset() == byte_offset
    }

    // Answer:  Does this chunk belong to the exact same snapshot we are currently in the middle of downloading?
    fn same_transfer(&self, req: &InstallSnapshot) -> bool {
        self.term == req.term && self.leader_id == req.leader_id && self.header == req.header
    }

    pub(crate) fn retry_offset(&self, req: &InstallSnapshot) -> u64 {
        if self.same_transfer(req) {
            self.next_offset()
        } else {
            0
        }
    }

    pub(crate) fn append(&mut self, data: &[u8]) -> u64 {
        self.bytes.extend_from_slice(data);
        self.next_offset()
    }

    fn next_offset(&self) -> u64 {
        self.bytes.len() as u64
    }

    pub(crate) fn finish(self) -> Result<(NodeId, RaftSnapshot), ()> {
        if self.bytes.len() as u64 != self.header.size_bytes {
            tracing::error!(
                "Size Mismatch {} {}",
                self.bytes.len(),
                self.header.size_bytes
            );

            return Err(());
        }
        if crc32fast::hash(&self.bytes) != self.header.checksum {
            tracing::error!("Data Corruption Detected - Checksum not match!");
            return Err(());
        }
        let data = SnapshotData::try_from_slice(&self.bytes).map_err(|_| {
            tracing::error!("Invalid Data");
        })?;
        Ok((
            self.leader_id,
            RaftSnapshot::new(
                self.header.last_included_index,
                self.header.last_included_term,
                data,
            ),
        ))
    }
}

pub(crate) struct TransientState {
    pub(crate) commit_index: u64,
    pub(crate) role: Role,
    pub(crate) current_leader: Option<NodeId>,
    pub(crate) peer_states: HashMap<NodeId, PeerState>,
    pub(crate) learner_states: HashMap<NodeId, PeerState>,
    pub(crate) election_epoch: u64,
    pub(crate) election_jitter: ElectionJitter,

    // SegmentKey -> data leader's NodeId: Control plane only actively manages and demands ACKs from the segment's designated Data Leader
    pub(crate) confirmed_data_leaders: HashMap<SegmentKey, NodeId>,
    pub(crate) catch_up: CatchUpRepairs,
    pub(crate) pending_proposals: Vec<MetadataCommand>,
    pub(crate) leaderless_segments: LeaderlessSegments,
    pub(crate) ring_observation_streak: Option<(BTreeSet<NodeId>, u32)>,
    pub(super) in_transit_snapshot: Option<InTransitSnapshot>,
    pub(super) snapshot_ack_target: Option<NodeId>,
}

impl TransientState {
    pub(crate) fn new(election_jitter_seed: u64, commit_index: u64) -> Self {
        Self {
            commit_index,
            role: Role::Follower,
            current_leader: None,
            peer_states: HashMap::new(),
            learner_states: HashMap::new(),
            election_epoch: 0,
            election_jitter: ElectionJitter::new(election_jitter_seed),
            confirmed_data_leaders: HashMap::new(),
            catch_up: CatchUpRepairs::default(),
            pending_proposals: Vec::new(),
            leaderless_segments: Vec::new(),
            ring_observation_streak: None,
            in_transit_snapshot: None,
            snapshot_ack_target: None,
        }
    }

    pub(crate) fn initialize_leader(
        &mut self,
        node_id: &NodeId,
        peers: &HashSet<NodeId>,
        next_index: u64,
    ) {
        self.role = Role::Leader;
        self.current_leader = Some(node_id.clone());
        self.peer_states.clear();
        self.learner_states.clear();
        self.ring_observation_streak = None;
        self.peer_states.extend(peers.iter().cloned().map(|peer| {
            (
                peer,
                PeerState {
                    next_index,
                    match_index: 0,
                },
            )
        }));
    }

    pub(crate) fn begin_campaign(&mut self) {
        self.role = Role::Candidate { votes_received: 1 };
    }

    pub(crate) fn record_vote(&mut self, quorum: u32) -> bool {
        let Role::Candidate { votes_received } = &mut self.role else {
            return false;
        };
        *votes_received += 1;
        *votes_received >= quorum
    }

    pub(crate) fn reset_for_follower(&mut self) {
        self.role = Role::Follower;
        self.current_leader = None;
        self.peer_states.clear();
        self.learner_states.clear();
        self.confirmed_data_leaders.clear();
        self.catch_up.clear();
        self.ring_observation_streak = None;
        self.in_transit_snapshot = None;
        self.snapshot_ack_target = None;
    }

    pub(crate) fn recognize_leader(&mut self, leader: NodeId) {
        if self.role != Role::Follower {
            self.role = Role::Follower;
            self.peer_states.clear();
            self.learner_states.clear();
        }
        self.current_leader = Some(leader);
    }

    pub(crate) fn advance_election_epoch(&mut self) -> u64 {
        self.election_epoch = self.election_epoch.wrapping_add(1);
        self.election_epoch
    }

    pub(crate) fn record_ring_observation(&mut self, ring: &BTreeSet<NodeId>) -> u32 {
        let observations = match self.ring_observation_streak.take() {
            Some((previous, count)) if previous == *ring => count.saturating_add(1),
            _ => 1,
        };
        self.ring_observation_streak = Some((ring.clone(), observations));
        observations
    }

    pub(crate) fn is_peer_caught_up(&self, node_id: &NodeId) -> bool {
        self.peer_states
            .get(node_id)
            .is_some_and(|state| state.match_index >= self.commit_index)
    }

    pub(crate) fn is_learner_ready_for_promotion(&self, node_id: &NodeId) -> bool {
        self.commit_index > 0
            && self
                .learner_states
                .get(node_id)
                .is_some_and(|state| state.match_index >= self.commit_index)
    }

    pub(crate) fn replicated_voter_count(&self, index: u64) -> u32 {
        self.peer_states
            .values()
            .filter(|state| state.match_index >= index)
            .count() as u32
            + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_transition_reinitializes_leader_only_state() {
        let self_id = NodeId::new("node-1");
        let peer = NodeId::new("node-2");
        let mut peers = HashSet::new();
        peers.insert(peer.clone());
        let mut state = TransientState::new(1, 0);

        state.initialize_leader(&self_id, &peers, 7);
        assert_eq!(state.role, Role::Leader);
        assert_eq!(state.current_leader.as_ref(), Some(&self_id));
        assert_eq!(state.peer_states[&peer].next_index, 7);

        state.reset_for_follower();
        assert_eq!(state.role, Role::Follower);
        assert!(state.current_leader.is_none());
        assert!(state.peer_states.is_empty());
        assert!(state.learner_states.is_empty());
    }

    #[test]
    fn snapshot_chunk_is_bound_to_leader_term_and_offset() {
        let request = InstallSnapshot {
            term: 3,
            leader_id: NodeId::new("node-1"),
            header: RaftSnapshotHeader {
                last_included_index: 5,
                last_included_term: 2,
                checksum: 0,
                size_bytes: 2,
            },
            offset: 0,
            data: vec![1].into_boxed_slice(),
            done: false,
        };
        let mut chunk = InTransitSnapshot::new(&request);
        assert!(chunk.is_next_chunk(&request));
        assert_eq!(chunk.append(&request.data), 1);

        let mut next = request.clone();
        next.offset = 1;
        next.done = true;
        assert!(chunk.is_next_chunk(&next));
        next.term += 1;
        assert!(!chunk.is_next_chunk(&next));
        assert_eq!(chunk.retry_offset(&next), 0);
    }
}
