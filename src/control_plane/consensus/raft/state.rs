#![allow(dead_code)]

use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::*;
use crate::control_plane::consensus::raft::command::RaftCommand;
use crate::control_plane::consensus::raft::log::LogEntry;
use crate::control_plane::consensus::raft::storage::RaftPersistentState;
use crate::control_plane::consensus::raft::{compute_replacement_replica_set, now_ms};
use crate::control_plane::membership::{ShardGroupId, TopologyReader};
use crate::control_plane::metadata::state_machine::MetadataStateMachine;
use crate::control_plane::metadata::{
    MetadataCommand, ReplicaSet, RollSegment, TopicMeta, TopicStats,
};
use crate::data_plane::SegmentKey;
use crate::data_plane::messages::command::{SegmentAssignment, SegmentAssignmentAck};
use crate::data_plane::transport::command::DataTransportCommand;
use crate::schedulers::ticker_message::TimerCommand;
#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

const ELECTION_JITTER_RANGE: u32 = 20;

struct ElectionJitter {
    seed: u64,
    counter: u64,
}

impl ElectionJitter {
    fn new(seed: u64) -> Self {
        Self { seed, counter: 0 }
    }

    fn next(&mut self) -> u32 {
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

// Peer tracking (LEADER-ONLY)
// - next_index: Index of the next log entry to send to this peer.
// - match_index: Highest log index known to be replicated on this peer.
// ? Is next_index always match_index +1? Not always. They diverge in two cases:
// - When a node becomes a leader: it sets next_index = last_log_index +1 and match_index = 0, so initially next_index could be 10 while match_index being 0.
// - After rejection: when a follower rejects AppendEntries, the leader decrements next_index to retry with earlier entries - but match_index stays the same until the peer confirms.
// So, `next_index` is the leader's guess and `match_index` is confirmed truth
#[derive(Debug, Clone)]
struct PeerState {
    next_index: u64,
    match_index: u64,
}

// ---------------------------------------------------------------------------
// Raft state machine — pure sync, no I/O
// ---------------------------------------------------------------------------

/// Minimal Raft consensus state machine.
///
/// Follows the same pattern as `Swim`: purely synchronous, no async, no I/O.
/// All outbound packets and timer commands are buffered and drained by the
/// caller (the actor layer).
pub struct Raft {
    // Identity
    pub node_id: NodeId,
    pub shard_group_id: ShardGroupId,
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    pending_log_mutations: Vec<LogMutation>, // must persist
    events: Vec<RaftEvent>,                  // volatile side effects
    commit_index: u64,                       // majority voted
    stabled_index: u64,                      // flushed to disk
    last_applied_index: u64,                 // applied to state machine
    role: Role,
    /// Tracks who the current leader is — set when this node becomes leader
    /// or when a valid `AppendEntries` is received from a leader.
    current_leader: Option<NodeId>,

    peers: HashSet<NodeId>,
    // LEADER-ONLY volatile state
    peer_states: HashMap<NodeId, PeerState>,
    state_machine: MetadataStateMachine,
    election_jitter: ElectionJitter,
    timer_seqs: TimerSeqs,
    /// Election-timer generation. Bumped whenever the election timer is
    /// (re)armed or cancelled; a fired `ElectionTimeout` carrying an older
    /// epoch raced its own cancellation in flight and must be ignored
    election_epoch: u64,
    /// Segments whose data-leader has acked its
    /// `SegmentAssignment`, mapped to the acking node. The heartbeat sweep skips
    /// re-driving a segment whose confirmed node still matches `replica_set[0]`
    confirmed_assignment: HashMap<SegmentKey, NodeId>,
    pending_proposals: Vec<MetadataCommand>,
}

pub(crate) struct TimerSeqs {
    pub election: u64,
    pub rpc: u64,
    pub merge_check: u64,
    pub ring_check: u64,
}

impl Raft {
    pub(crate) fn new(
        node_id: NodeId,
        peers: HashSet<NodeId>,
        persistent: RaftPersistentState,
        election_jitter_seed: u64,
        shard_group_id: ShardGroupId,
        timer_seqs: TimerSeqs,
    ) -> Self {
        let mut raft = Self {
            node_id,
            shard_group_id,
            peers,
            stabled_index: persistent.stabled_index(),
            current_term: persistent.term,
            voted_for: persistent.voted_for,
            log: persistent.log,
            pending_log_mutations: Vec::new(),
            events: Vec::new(),
            commit_index: 0,
            last_applied_index: 0,
            role: Role::Follower,
            current_leader: None,
            state_machine: MetadataStateMachine::default(),
            pending_proposals: Vec::new(),
            peer_states: HashMap::new(),
            election_jitter: ElectionJitter::new(election_jitter_seed),
            timer_seqs,
            election_epoch: 0,
            confirmed_assignment: HashMap::new(),
        };
        raft.reset_election_timer();
        raft
    }

    pub(crate) fn heartbeat_seq(&self) -> u64 {
        self.timer_seqs.rpc
    }

    pub(crate) fn topic_names(&self) -> Box<[String]> {
        self.state_machine.topic_names()
    }

    pub(crate) fn topic_stats(&self) -> Box<[TopicStats]> {
        self.state_machine.topic_stats()
    }

    pub(crate) fn get_topic_by_name(&self, name: &str) -> Option<&TopicMeta> {
        self.state_machine.get_topic_by_name(name)
    }

    pub(crate) fn active_segments_for_node(
        &self,
        node_id: &NodeId,
    ) -> Box<[(SegmentKey, ReplicaSet)]> {
        self.state_machine.active_segments_for_node(node_id)
    }

    /// Every active segment's assignment tuple `(key, replica_set, start_offset)`.
    /// The leader's confirmation-gated assignment sweep (`MultiRaft::build_redrive_cmds`)
    /// turns these into `SegmentAssignment` re-drives for unconfirmed segments.
    pub(crate) fn active_segment_assignments(&self) -> Box<[(SegmentKey, ReplicaSet, u64)]> {
        self.state_machine.active_segment_assignments()
    }

    pub(crate) fn reconcile_peers(
        &mut self,
        topology_reader: &TopologyReader,
        live: &HashSet<NodeId>,
    ) -> bool {
        let mut changed = false;
        let mut peer_nodes: HashSet<NodeId> = self.peers_iter().cloned().collect();

        let dead_count = peer_nodes.iter().filter(|p| !live.contains(*p)).count();

        if dead_count == 0 {
            return changed;
        }

        peer_nodes.insert(self.node_id.clone());
        let replacements =
            topology_reader.ring_replacements_for(self.shard_group_id, &peer_nodes, dead_count);

        for (i, node_id) in peer_nodes
            .into_iter()
            .filter(|p| !live.contains(p))
            .enumerate()
        {
            // get(i) index-pairs the i-th dead peer with the i-th replacement, and gracefully yields None when the replacement pool is shorter than the dead set
            // which is the case the degrade-and-proceed policy exists to handle.
            let replacement = replacements.get(i).cloned();
            changed |= self.propose_replace_peer(node_id.clone(), replacement);
        }

        changed
    }

    /// Live voters that the ring no longer assigns to this group.
    /// Dead voters are deliberately excluded — they are `reconcile_peers`'
    /// jurisdiction, mirroring the live-set guard on the add side.
    pub(crate) fn stale_live_voters(
        &self,
        ring_members: &[NodeId],
        live: &HashSet<NodeId>,
    ) -> Vec<NodeId> {
        let mut stale: Vec<NodeId> = self
            .peers
            .iter()
            .filter(|p| live.contains(*p) && !ring_members.contains(p))
            .cloned()
            .collect();
        stale.sort();
        stale
    }

    /// Leader-side drift telemetry - detection only, no proposals.
    /// Warns when the voter set has ratcheted past the ring's assignment
    /// for this group: live ex-owners present, or more voters than the ring
    /// names. Catching the ratchet in the wild precedes curing it.
    pub(crate) fn log_ring_drift(&self, topology_reader: &TopologyReader, live: &HashSet<NodeId>) {
        if self.role != Role::Leader {
            return;
        }
        let Some(ring_members) = topology_reader.group_ring_members(self.shard_group_id) else {
            return;
        };
        let stale_live = self.stale_live_voters(&ring_members, live);
        let voter_count = self.peers.len() + 1; // +1 for self
        if !stale_live.is_empty() || voter_count > ring_members.len() {
            tracing::warn!(
                node = %self.node_id,
                group = self.shard_group_id.0,
                voters = voter_count,
                ring_members = ring_members.len(),
                stale_live = ?stale_live,
                "ring drift (#135): voter set exceeds the ring's assignment — \
                 quorum is inflated until the stale members are removed",
            );
        }
    }

    /// scan active segments in this group for any whose replica set still names a
    /// node SWIM considers dead, and propose a `RollSegment` to swap in
    /// healthy replacements. Closes the gap where deaths landed during the
    /// no-leader window — the live-leader path (`handle_node_death`) catches
    /// those per-event, this is the backfill sweep on takeover.
    pub(crate) fn reconcile_segments(&mut self, live_set: &HashSet<NodeId>) -> bool {
        let mut changed = false;

        let live_nodes: Vec<NodeId> = live_set.clone().into_iter().collect();

        for (segment_key, old_replica_set) in self.active_segments_with_dead_members(live_set) {
            let dead_in_set: Vec<NodeId> = old_replica_set
                .iter()
                .filter(|n| !live_set.contains(*n))
                .cloned()
                .collect();
            let new_replica_set =
                compute_replacement_replica_set(&old_replica_set, &dead_in_set, &live_nodes);
            // end_entry_id = None: takeover doesn't know the committed offset.
            // The sealed segment's end_offset is corrected later (D5 repair or
            // by the segment leader's subsequent SealRequest carrying it).
            let cmd = RollSegment {
                segment_key,
                sealed_at: now_ms(),
                new_replica_set,
                end_entry_id: None,
            };
            if let Err(e) = self.propose(cmd.into()) {
                tracing::warn!(
                    "Takeover-triggered RollSegment for {:?} on {:?} failed: {:?}",
                    segment_key,
                    self.shard_group_id,
                    e
                );
            } else {
                changed = true;
            }
        }
        changed
    }

    pub(crate) fn handle_node_death(
        &mut self,
        dead_node_id: &NodeId,
        live_set: &HashSet<NodeId>,
        topology_reader: &TopologyReader,
    ) -> bool {
        if !self.is_leader() {
            return false;
        }

        let mut changed = false;
        if self.has_peer(dead_node_id) {
            // Pair the removal with a ring-aware addition so failure
            // tolerance doesn't shrink monotonically.
            let mut excluded: HashSet<NodeId> = self.peers_iter().cloned().collect();
            excluded.insert(self.node_id.clone());

            // Compute the replacement BEFORE proposing the swap — the dying node is
            // still in `raft.peers`, so it's naturally excluded by
            // including the current peer set in the exclusion.
            let replacement = topology_reader
                .ring_replacements_for(self.shard_group_id, &excluded, 1)
                .into_iter()
                .next();

            changed |= self.propose_replace_peer(dead_node_id.clone(), replacement);
        }

        changed || self.reconcile_segments(live_set)
    }

    pub(crate) fn dead_nodes(&mut self, live: &HashSet<NodeId>) -> impl Iterator<Item = &NodeId> {
        self.peers_iter().filter(|p| !live.contains(*p))
    }

    pub(crate) fn active_segments_with_dead_members(
        &self,
        live: &HashSet<NodeId>,
    ) -> Box<[(SegmentKey, ReplicaSet)]> {
        self.state_machine.active_segments_with_dead_members(live)
    }

    pub(crate) fn has_topic(&self, topic_id: &crate::control_plane::metadata::TopicId) -> bool {
        self.state_machine.get_topic(topic_id).is_some()
    }

    pub(crate) fn get_replica_set(&self, key: &SegmentKey) -> Option<ReplicaSet> {
        let topic = self.state_machine.get_topic(&key.topic_id)?;
        let seg = topic.get_active_segment(&key.range_id)?;
        Some(seg.replica_set.clone())
    }

    pub(crate) fn take_events(&mut self) -> Vec<RaftEvent> {
        std::mem::take(&mut self.events)
    }

    pub fn take_log_mutations(&mut self) -> Vec<LogMutation> {
        std::mem::take(&mut self.pending_log_mutations)
    }

    pub(crate) fn take_pending_proposals(&mut self) -> Vec<MetadataCommand> {
        std::mem::take(&mut self.pending_proposals)
    }

    pub(crate) fn last_applied_index(&self) -> u64 {
        self.last_applied_index
    }

    pub(crate) fn log_last_index(&self) -> u64 {
        self.log.last().map_or(0, |e| e.index)
    }

    fn log_last_term(&self) -> u64 {
        self.log.last().map_or(0, |e| e.term)
    }

    fn log_term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        self.log.get((index - 1) as usize).map_or(0, |e| e.term)
    }

    fn log_get(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.log.get((index - 1) as usize)
    }

    fn log_entries_from(&self, start_index: u64) -> Box<[LogEntry]> {
        let last = self.log_last_index();
        if start_index == 0 || start_index > last {
            return Box::new([]);
        }
        self.log[(start_index - 1) as usize..].into()
    }

    fn log_append(&mut self, entry: LogEntry) {
        debug_assert_eq!(
            entry.index,
            self.log_last_index() + 1,
            "log entry index must be contiguous"
        );
        self.pending_log_mutations
            .push(LogMutation::Append(entry.clone()));
        self.log.push(entry);
    }

    fn log_truncate_from(&mut self, from_index: u64) {
        if from_index == 0 || from_index > self.log_last_index() + 1 {
            return;
        }
        self.pending_log_mutations
            .push(LogMutation::TruncateFrom(from_index));
        self.log.truncate((from_index - 1) as usize);
    }

    fn push_hard_state(&mut self) {
        self.pending_log_mutations.push(LogMutation::HardState {
            term: self.current_term,
            voted_for: self.voted_for.clone(),
        });
    }

    pub fn peers_count(&self) -> usize {
        self.peers.len()
    }

    pub fn peers_iter(&self) -> impl Iterator<Item = &NodeId> {
        self.peers.iter()
    }

    pub fn current_leader(&self) -> Option<&NodeId> {
        self.current_leader.as_ref()
    }

    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    pub fn has_peer(&self, node_id: &NodeId) -> bool {
        self.peers.contains(node_id)
    }

    /// Minimum number of nodes needed for a majority (strict majority).
    /// For N nodes: N/2 + 1. Examples: 3→2, 4→3, 5→3.
    fn quorum(&self) -> u32 {
        let total = self.peers.len() as u32 + 1; // +1 for self
        total / 2 + 1
    }

    pub(crate) fn stabled_index(&self) -> u64 {
        self.stabled_index
    }

    // -------------------------------------------------------------------
    // Event handlers (called by actor)
    // -------------------------------------------------------------------

    pub fn handle_timeout(&mut self, event: RaftTimeoutCallback) {
        match event {
            RaftTimeoutCallback::Ignored => {}
            RaftTimeoutCallback::ElectionTimeout { epoch, .. } => {
                // u64::MAX bypasses the staleness check for direct
                // invocations in tests; real timers carry the epoch they
                // were armed with.
                if epoch == self.election_epoch || epoch == u64::MAX {
                    self.start_election();
                    return;
                }
                tracing::debug!(
                    node = %self.node_id,
                    group = self.shard_group_id.0,
                    stale_epoch = epoch,
                    epoch = self.election_epoch,
                    "election: dropped stale election timeout"
                );
            }
            RaftTimeoutCallback::RpcTimeout { .. } => {
                self.send_heartbeats();
            }
            RaftTimeoutCallback::MergeCheckTimeout { now, .. } => {
                self.evaluate_merges(now);
            }
            RaftTimeoutCallback::RingCheckTimeout { .. } => {
                self.reschedule_ring_check();
            }
        }
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    pub fn handle_rpc(&mut self, from: NodeId, rpc: impl Into<RaftRpc>) {
        let rpc = rpc.into();
        match rpc {
            RaftRpc::RequestVote(req) => self.handle_request_vote(from, req),
            RaftRpc::RequestVoteResponse(resp) => self.handle_request_vote_response(resp),
            RaftRpc::AppendEntries(req) => self.handle_append_entries(from, req),
            RaftRpc::AppendEntriesResponse(resp) => self.handle_append_entries_response(resp),
        }
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    // -------------------------------------------------------------------
    // Election
    // -------------------------------------------------------------------

    fn start_election(&mut self) {
        // Leaders don't run election timers — they send heartbeats instead.
        if self.role == Role::Leader {
            return;
        }
        self.current_term += 1;
        self.voted_for = Some(self.node_id.clone());
        self.push_hard_state();

        if self.peers.is_empty() {
            // Single-node cluster: elect self immediately.
            self.become_leader();
            return;
        }

        self.role = Role::Candidate { votes_received: 1 }; // vote for self
        self.reset_election_timer();
        tracing::trace!(
            node = %self.node_id,
            group = self.shard_group_id.0,
            term = self.current_term,
            "election: became candidate, broadcasting RequestVote"
        );

        let req = RequestVote {
            term: self.current_term,
            candidate_id: self.node_id.clone(),
            last_log_index: self.log_last_index(),
            last_log_term: self.log_last_term(),
        };
        for peer_id in self.peers.iter() {
            self.events.push(
                OutboundRaftPacket::new(self.shard_group_id, peer_id.clone(), req.clone()).into(),
            );
        }
    }

    // ! SAFETY : Even without role guard, leaders/candidates step down if term is higher.
    // ! Followers grant or deny - All roles must respond
    fn handle_request_vote(&mut self, from: NodeId, req: RequestVote) {
        // If the request term is newer, step down.
        if req.term > self.current_term {
            self.step_down(req.term);
        }

        let term_ok = req.term == self.current_term;
        let vote_ok = self.vote_available_for(&req.candidate_id);
        let log_ok = self.log_is_up_to_date(req.last_log_index, req.last_log_term);
        let vote_granted = term_ok && vote_ok && log_ok;
        tracing::debug!(
            node = %self.node_id,
            group = self.shard_group_id.0,
            from = %req.candidate_id,
            req_term = req.term,
            term = self.current_term,
            granted = vote_granted,
            term_ok,
            vote_ok,
            log_ok,
            "election: RequestVote received"
        );

        if vote_granted {
            self.voted_for = Some(req.candidate_id);
            self.push_hard_state();
            self.reset_election_timer();
        }

        self.events.push(
            OutboundRaftPacket::new(
                self.shard_group_id,
                from,
                RequestVoteResponse {
                    term: self.current_term,
                    node_id: self.node_id.clone(),
                    vote_granted,
                },
            )
            .into(),
        );
    }

    fn handle_request_vote_response(&mut self, resp: RequestVoteResponse) {
        tracing::debug!(
            node = %self.node_id,
            group = self.shard_group_id.0,
            from = %resp.node_id,
            resp_term = resp.term,
            term = self.current_term,
            granted = resp.vote_granted,
            "election: RequestVoteResponse received"
        );
        if resp.term > self.current_term {
            self.step_down(resp.term);
            return;
        }
        self.count_vote_if_eligible(resp);
    }

    fn count_vote_if_eligible(&mut self, resp: RequestVoteResponse) {
        let Role::Candidate { votes_received } = &mut self.role else {
            return;
        };
        if resp.term != self.current_term || !resp.vote_granted {
            return;
        }
        *votes_received += 1;
        if *votes_received >= self.quorum() {
            self.become_leader();
        }
    }

    fn vote_available_for(&self, candidate_id: &NodeId) -> bool {
        match &self.voted_for {
            None => true,
            Some(id) => id == candidate_id,
        }
    }

    /// §5.4.1: A candidate's log is "at least as up-to-date" if its last
    /// entry has a higher term, or the same term with a >= index.
    fn log_is_up_to_date(&self, last_log_index: u64, last_log_term: u64) -> bool {
        let my_last_term = self.log_last_term();
        let my_last_index = self.log_last_index();

        if last_log_term != my_last_term {
            return last_log_term > my_last_term;
        }
        last_log_index >= my_last_index
    }

    // -------------------------------------------------------------------
    // Leader lifecycle
    // -------------------------------------------------------------------

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.current_leader = Some(self.node_id.clone());
        tracing::debug!(
            node = %self.node_id,
            group = self.shard_group_id.0,
            term = self.current_term,
            "election: became leader"
        );

        self.events.push(
            LeaderChange {
                shard_group_id: self.shard_group_id,
                leader_node_id: self.node_id.clone(),
                term: self.current_term,
            }
            .into(),
        );

        // Cancel election timer, start heartbeat + merge/ring check timers.
        self.cancel_all_timers();
        self.schedule_rpc_timer();
        self.schedule_merge_check_timer();
        self.schedule_ring_check_timer();

        // next_index is set *before* the noop is appended, so it points
        // at the noop's index — causing the first AppendEntries to carry it.
        let next = self.log_last_index() + 1;

        // Peer state tracker needs to be re-initialized on every leadership transition
        self.peer_states.clear();
        for peer_id in self.peers.iter() {
            self.peer_states.insert(
                peer_id.clone(),
                PeerState {
                    next_index: next,
                    match_index: 0,
                },
            );
        }

        // Append a Noop entry at the new term so that all
        // preceding entries from earlier terms can be committed.
        // Without this, old-term entries remain in limbo until a real
        // client proposal arrives.
        self.add_new_entry(RaftCommand::Noop);

        // Send AppendEntries (with the Noop) to all peers.
        self.send_heartbeats();

        // Single-node: commit the Noop immediately (quorum = 1 = self).
        self.try_advance_commit_index();
    }

    fn step_down(&mut self, new_term: u64) {
        debug_assert!(
            new_term >= self.current_term,
            "step_down must never regress the term"
        );
        let was_leader = self.role == Role::Leader;

        tracing::debug!(
            node = %self.node_id,
            group = self.shard_group_id.0,
            new_term,
            was_leader,
            "election: stepping down"
        );

        // Clearing the vote is only safe when the term actually advances;
        // at an equal term this is a pure demotion that must preserve
        // `voted_for`, or the node could vote twice in one term — the same
        // rule `recognize_leader`'s demotion branch follows. All production
        // callers pass strictly newer terms (guarded `>` at every call
        // site); tests use equal-term step_down to depose a leader in place.
        if new_term > self.current_term {
            self.current_term = new_term;
            self.voted_for = None;
            self.push_hard_state();
        }

        self.cancel_leader_timers();
        if was_leader {
            // The election timer resets only on a vote grant or
            // on AppendEntries from the current leader — never on a mere
            // higher-term message, or a doomed candidate can push back a healthy
            // follower's deadline every round (the #133 livelock). Followers and
            // candidates keep their armed deadline; only an ex-leader, which
            // runs no election timer, arms a fresh one.
            self.reset_election_timer();
        }
        self.role = Role::Follower;
        self.current_leader = None;
        self.peer_states.clear();
        self.confirmed_assignment.clear();
    }

    // -------------------------------------------------------------------
    // Log replication
    // -------------------------------------------------------------------

    fn send_heartbeats(&mut self) {
        if self.role != Role::Leader {
            return;
        }

        let peers: Vec<NodeId> = self.peers.iter().cloned().collect();

        for peer_id in peers {
            self.send_append_entries(peer_id);
        }

        self.schedule_rpc_timer();
        self.maybe_redrive_segment_assignments();
    }

    fn maybe_redrive_segment_assignments(&mut self) {
        // rederive Metadata <> Datanode segment assignment
        let active = self.active_segment_assignments();
        let active_keys: HashSet<SegmentKey> = active.iter().map(|(k, _, _)| *k).collect();
        self.confirmed_assignment
            .retain(|k, _| active_keys.contains(k));

        let mut redrives = Vec::new();
        for (segment_key, replica_set, start_entry_id) in active {
            let Some(target) = replica_set.first() else {
                continue;
            };

            // * If data leader acks assignment, it would have been added to confirmed_assignment through Raft::handle_assignment_ack
            if self.confirmed_assignment.get(&segment_key) == Some(target) {
                continue;
            }

            redrives.push(DataTransportCommand::send_to_targets(
                vec![target.clone()],
                SegmentAssignment {
                    segment_key,
                    shard_group_id: self.shard_group_id,
                    replica_set,
                    start_entry_id,
                },
            ));
        }
        if !redrives.is_empty() {
            self.events.push(RaftEvent::RedriveAssignments(redrives));
        }
    }

    pub(crate) fn handle_assignment_ack(&mut self, ack: SegmentAssignmentAck) {
        self.confirmed_assignment.insert(ack.segment_key, ack.from);
    }

    fn send_append_entries(&mut self, peer_id: NodeId) {
        let peer_state = match self.peer_states.get(&peer_id) {
            Some(ps) => ps,
            None => return,
        };

        let prev_log_index = peer_state.next_index.saturating_sub(1);
        let prev_log_term = self.log_term_at(prev_log_index);
        let entries = self.log_entries_from(peer_state.next_index);

        self.events.push(
            OutboundRaftPacket::new(
                self.shard_group_id,
                peer_id,
                AppendEntries {
                    term: self.current_term,
                    leader_id: self.node_id.clone(),
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.commit_index,
                },
            )
            .into(),
        );
    }

    // ! SAFETY:
    // ! - Leaders step down on higher term.
    // ! - Candidates step down on same term : because receiving entries while being a candidate means another node already won.
    // ! - Followers process normally. All roles must respond.
    fn handle_append_entries(&mut self, from: NodeId, req: AppendEntries) {
        if req.term < self.current_term {
            self.reject_append_entries(from);
            return;
        }

        // ! recognizing leader must happen regardless of whether the log entries are accepted
        // ! Even if the log consistency check fails and entries are rejected, the follower should not start a new election.
        self.recognize_leader(&req);

        if !self.check_log_consistency(req.prev_log_index, req.prev_log_term) {
            self.reject_append_entries(from);
            return;
        }
        self.replicate_entries(req.entries);
        self.advance_follower_commit(req.leader_commit);
        self.accept_append_entries(from);
    }

    fn recognize_leader(&mut self, req: &AppendEntries) {
        if req.term > self.current_term {
            self.step_down(req.term);
        } else if self.role != Role::Follower {
            self.role = Role::Follower;
            self.peer_states.clear();
        }
        self.current_leader = Some(req.leader_id.clone());
        self.reset_election_timer();
        tracing::debug!(
            node = %self.node_id,
            group = self.shard_group_id.0,
            term = self.current_term,
            leader = %req.leader_id,
            "election: leader recognized"
        );
    }

    fn check_log_consistency(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
        if prev_log_index == 0 {
            return true;
        }
        let local_term = self.log_term_at(prev_log_index);
        local_term != 0 && local_term == prev_log_term
    }

    fn replicate_entries(&mut self, entries: Box<[LogEntry]>) {
        for entry in entries {
            let existing_term = self.log_term_at(entry.index);
            if existing_term != 0 && existing_term != entry.term {
                self.log_truncate_from(entry.index);
            }
            if entry.index > self.log_last_index() {
                self.log_append(entry);
            }
        }
    }

    fn advance_follower_commit(&mut self, leader_commit: u64) {
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(self.log_last_index());
            self.apply_committed_entries();
        }
    }

    // ! SAFETY
    // ! - term guard
    // ! - only leaders track peer state
    fn handle_append_entries_response(&mut self, resp: AppendEntriesResponse) {
        if resp.term > self.current_term {
            self.step_down(resp.term);
            return;
        }

        if self.role != Role::Leader {
            return;
        }

        if let Some(peer_state) = self.peer_states.get_mut(&resp.node_id) {
            if resp.success {
                peer_state.match_index = resp.last_log_index;
                peer_state.next_index = resp.last_log_index + 1;
                self.try_advance_commit_index();
            } else {
                // Decrement next_index and retry.
                peer_state.next_index = peer_state.next_index.saturating_sub(1).max(1);
                self.send_append_entries(resp.node_id);
            }
        }
    }

    fn accept_append_entries(&mut self, target: NodeId) {
        self.send_append_entries_response(target, true);
    }

    fn reject_append_entries(&mut self, target: NodeId) {
        self.send_append_entries_response(target, false);
    }

    fn send_append_entries_response(&mut self, target: NodeId, success: bool) {
        self.events.push(
            OutboundRaftPacket::new(
                self.shard_group_id,
                target,
                AppendEntriesResponse {
                    term: self.current_term,
                    node_id: self.node_id.clone(),
                    success,
                    last_log_index: self.log_last_index(),
                },
            )
            .into(),
        );
    }

    // A log entry is committed once it is replicated on a
    // "majority" of servers.
    //
    // A leader can only commit entries from its own term.
    // It cannot directly commit entries left over from a previous leader's term, even if they're replicated on a majority.
    // Consider this scenario:
    //   Term 1: Leader-A appends entry at index 3 (term=1), replicates to 2/5 nodes, then crashes
    //   Term 2: Leader-B wins election, never sees index 3, appends its own entries, then crashes
    //   Term 3: Leader-C wins election, sees index 3 (term=1) on 2 nodes
    // If Leader-C were allowed to commit index 3 (term=1) just because it can replicate it to a majority,
    // there's a race: Leader-B in term 2 might have already overwritten index 3 on some nodes with a different entry.
    // Committing the old entry could violate safety.
    //
    // The fix: Leader-C skips term_at(3) = 1 because 1 != current_term(3).
    // Instead, it appends a new entry at its own term. Once that entry is committed on a majority, all preceding entries (including index 3) are implicitly committed too.
    // And that 'implicit commit' does not violate safety because 'new' entry acts as an election shield that physically prevents that overwrite from happening.
    fn try_advance_commit_index(&mut self) {
        let last = self.log_last_index();
        let quorum = self.quorum();

        // Scan top-down: the highest current-term entry with quorum
        // implicitly commits everything below it (log matching property).
        for n in (self.commit_index + 1..=last).rev() {
            if self.log_term_at(n) != self.current_term {
                continue;
            }
            let replication_count = self
                .peer_states
                .values()
                .filter(|ps| ps.match_index >= n)
                .count() as u32
                + 1; // +1 for self

            if replication_count >= quorum {
                self.commit_index = n;
                self.apply_committed_entries();
                return;
            }
        }
    }

    pub(crate) fn advance_stabled_index(&mut self, value: u64) {
        self.stabled_index = self.stabled_index.max(value);
        self.apply_committed_entries();

        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    fn apply_committed_entries(&mut self) {
        while self.last_applied_index < self.commit_index.min(self.stabled_index) {
            self.last_applied_index += 1;
            let Some(entry) = self.log_get(self.last_applied_index).cloned() else {
                tracing::error!(
                    "[{}] committed entry at index {} missing from log",
                    self.node_id,
                    self.last_applied_index
                );
                break;
            };
            match entry.command {
                RaftCommand::Noop => {}
                RaftCommand::Metadata(cmd) => self.apply_metadata_entry(cmd, entry.index),
                RaftCommand::AddPeer(node_id) => self.apply_add_peer(node_id),
                RaftCommand::RemovePeer(node_id) => self.apply_remove_peer(node_id),
            }
        }
    }

    fn apply_metadata_entry(&mut self, cmd: MetadataCommand, index: u64) {
        match self.state_machine.apply(cmd) {
            Ok(result) => {
                tracing::debug!(
                    "[{}] Applied metadata at index {}: {:?}",
                    self.node_id,
                    index,
                    result
                );
                self.events.push(
                    MetadataCommitted {
                        shard_group_id: self.shard_group_id,
                        result,
                        log_index: index,
                        seal_context: None,
                    }
                    .into(),
                );
            }
            Err(e) => tracing::error!(
                "[{}] Metadata apply error at index {}: {:?}",
                self.node_id,
                index,
                e
            ),
        }
        if self.role == Role::Leader {
            for pending_cmd in self.state_machine.take_pending_proposals() {
                self.pending_proposals.push(pending_cmd);
            }
        }
    }

    /// Apply-only helper. Invoked from `apply_committed_entries()` when an
    /// `AddPeer` log entry commits. Never call directly — the peer set is part
    /// of the replicated state machine and must only mutate through the log.
    fn apply_add_peer(&mut self, node_id: NodeId) {
        if node_id == self.node_id {
            return;
        }
        if self.peers.insert(node_id.clone()) && self.role == Role::Leader {
            self.peer_states.insert(
                node_id,
                PeerState {
                    next_index: self.log_last_index() + 1,
                    match_index: 0,
                },
            );
        }
    }

    /// Apply-only helper. Invoked from `apply_committed_entries()` when a
    /// `RemovePeer` log entry commits. Never call directly — the peer set is part
    /// of the replicated state machine and must only mutate through the log.
    fn apply_remove_peer(&mut self, node_id: NodeId) {
        if self.peers.remove(&node_id) {
            self.peer_states.remove(&node_id);
            self.events.push(RaftEvent::DisconnectPeer(node_id));
        }
    }

    fn add_new_entry(&mut self, command: RaftCommand) {
        let entry = LogEntry {
            term: self.current_term,
            index: self.log_last_index() + 1,
            command,
        };
        self.log_append(entry);
    }

    /// Propose a command to the Raft log. Only the leader can accept proposals.
    /// In the DS-RSM context, the flow would be as follows:
    //
    // Client: "Create topic blue on shard #45"
    // -> Shard #45's leader.propose(CreateTopic("blue"))
    // -> Appended to leader's log
    // -> Replicated to shard #45's followers
    // -> Majority ack -> committed
    // -> Applied to MetadataStateMachine → topic blue exists
    /// Returns the log index at which the command was appended on success.
    pub fn propose(&mut self, command: RaftCommand) -> Result<u64, ClientProposalError> {
        if self.role != Role::Leader {
            return Err(ClientProposalError::NotLeader(self.current_leader.clone()));
        }

        self.add_new_entry(command);
        let index = self.log_last_index();

        // Immediately replicate to all peers.
        let peers: Vec<NodeId> = self.peers.iter().cloned().collect();
        for peer_id in peers {
            self.send_append_entries(peer_id);
        }

        // Single-node cluster: no peers to ack, so commit immediately
        // (quorum of 1 = self). For multi-node, this is a no-op because
        // no peer has acked yet.
        self.try_advance_commit_index();

        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
        Ok(index)
    }

    /// Replace a peer at the *intent* level: propose `RemovePeer(remove)`
    /// then a paired `AddPeer(replacement)`, or degrade-and-proceed when no
    /// replacement is available. The two stay as separate log entries —
    /// Raft's single-server-change rule means we cannot atomically swap two
    /// members in one entry without joint consensus, so committing them in
    /// order (each step a one-server change) is what keeps old-config and
    /// new-config majorities overlapping.
    pub(crate) fn propose_replace_peer(
        &mut self,
        remove: NodeId,
        replacement: Option<NodeId>,
    ) -> bool {
        if let Err(e) = self.propose(RaftCommand::RemovePeer(remove.clone())) {
            tracing::debug!(
                "RemovePeer({:?}) on {:?} rejected: {:?} — skipping paired AddPeer",
                remove,
                self.shard_group_id,
                e
            );
            return false;
        }

        let Some(replacement) = replacement else {
            tracing::debug!(
                "Replace on {:?}: no ring-eligible replacement (cluster too small for RF) — group operating below replication factor until capacity is added",
                self.shard_group_id
            );
            return true;
        };
        if let Err(e) = self.propose(RaftCommand::AddPeer(replacement.clone())) {
            tracing::warn!(
                "Paired AddPeer({:?}) on {:?} rejected: {:?}",
                replacement,
                self.shard_group_id,
                e
            );
        }

        true
    }

    fn reset_election_timer(&mut self) {
        self.election_epoch = self.election_epoch.wrapping_add(1);
        self.events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: self.timer_seqs.election,
            }));
        let jitter = self.election_jitter.next();
        tracing::trace!(
            node = %self.node_id,
            group = self.shard_group_id.0,
            epoch = self.election_epoch,
            jitter_ticks = jitter,
            "election: timer armed"
        );
        self.events
            .push(RaftEvent::Timer(TimerCommand::SetSchedule {
                seq: self.timer_seqs.election,
                timer: RaftTimer::election(jitter, self.shard_group_id, self.election_epoch),
            }));
    }

    fn schedule_rpc_timer(&mut self) {
        self.events
            .push(RaftEvent::Timer(TimerCommand::SetSchedule {
                seq: self.timer_seqs.rpc,
                timer: RaftTimer::rpc(self.shard_group_id),
            }));
    }

    fn schedule_merge_check_timer(&mut self) {
        self.events
            .push(RaftEvent::Timer(TimerCommand::SetSchedule {
                seq: self.timer_seqs.merge_check,
                timer: RaftTimer::merge_check(self.shard_group_id),
            }));
    }

    fn schedule_ring_check_timer(&mut self) {
        self.events
            .push(RaftEvent::Timer(TimerCommand::SetSchedule {
                seq: self.timer_seqs.ring_check,
                timer: RaftTimer::ring_check(self.shard_group_id),
            }));
    }

    /// Re-arm the periodic ring check (#135 trigger gap). The ring diff
    /// itself runs in `MultiRaft::reconcile_ring` — it needs the topology
    /// reader, which lives a layer up — so this Raft-side handler owns only
    /// the timer lifecycle, leader-gated like `evaluate_merges`: a deposed
    /// leader lets the timer die (it is re-armed on the next become_leader).
    pub(crate) fn reschedule_ring_check(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        self.schedule_ring_check_timer();
    }

    pub(crate) fn evaluate_merges(&mut self, now: u64) {
        if self.role != Role::Leader {
            return;
        }

        let merge_proposals = self.state_machine.evaluate_merges(now);
        for cmd in merge_proposals {
            self.pending_proposals.push(cmd);
        }

        self.schedule_merge_check_timer();
    }

    /// Cancels the leader-side timers (heartbeat, merge-check, ring-check)
    /// without touching the election timer — used by `step_down` so a
    /// follower or candidate keeps its currently armed election deadline
    /// (#133).
    fn cancel_leader_timers(&mut self) {
        self.events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: self.timer_seqs.rpc,
            }));
        self.events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: self.timer_seqs.merge_check,
            }));
        self.events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: self.timer_seqs.ring_check,
            }));
    }

    pub(crate) fn cancel_all_timers(&mut self) {
        self.election_epoch = self.election_epoch.wrapping_add(1);
        self.events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: self.timer_seqs.election,
            }));
        self.events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: self.timer_seqs.rpc,
            }));
        self.events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: self.timer_seqs.merge_check,
            }));
        self.events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: self.timer_seqs.ring_check,
            }));
    }
}

#[cfg(any(test, debug_assertions))]
impl crate::test_traits::TAssertInvariant for Raft {
    fn assert_invariants(&self) {
        assert!(
            self.last_applied_index <= self.commit_index,
            "last_applied ({}) > commit_index ({})",
            self.last_applied_index,
            self.commit_index,
        );
        assert!(
            self.last_applied_index <= self.stabled_index,
            "last_applied ({}) > stabled_index ({}) — applied a non-durable entry",
            self.last_applied_index,
            self.stabled_index,
        );
        assert!(
            self.commit_index <= self.log_last_index(),
            "commit_index ({}) > log_last_index ({})",
            self.commit_index,
            self.log_last_index(),
        );

        // Log indices are contiguous and 1-based
        for (i, entry) in self.log.iter().enumerate() {
            assert_eq!(
                entry.index,
                (i + 1) as u64,
                "log entry at position {i} has non-contiguous index {}",
                entry.index,
            );
            assert!(
                entry.term <= self.current_term,
                "log entry at index {} has term {} > current_term {}",
                entry.index,
                entry.term,
                self.current_term,
            );
        }

        // Invariant: peer_states exists only on the leader (and matches the peer
        // set when leader). Followers/candidates carry an empty peer_states.
        match self.role {
            Role::Leader => {
                for peer in &self.peers {
                    assert!(
                        self.peer_states.contains_key(peer),
                        "leader missing peer_state for {:?}",
                        peer,
                    );
                }
                assert_eq!(
                    self.peer_states.len(),
                    self.peers.len(),
                    "leader peer_states size ({}) != peers size ({})",
                    self.peer_states.len(),
                    self.peers.len(),
                );
                // Invariant: at most one leader per term. A snapshot can only check
                // the local fragment of this: a leader must have voted for itself this
                // term (and is therefore the only node that could have won this term).
                assert_eq!(
                    self.voted_for.as_ref(),
                    Some(&self.node_id),
                    "leader has voted_for {:?}, expected self ({:?})",
                    self.voted_for,
                    self.node_id,
                );
            }
            Role::Follower | Role::Candidate { .. } => {
                assert!(
                    self.peer_states.is_empty(),
                    "non-leader carries peer_states ({} entries)",
                    self.peer_states.len(),
                );
            }
        }

        // Invariant (partial): self is never in peers. The peer set is otherwise
        // mutated only via apply of committed AddPeer/RemovePeer entries — the
        // discipline itself is enforced by keeping `apply_add_peer`/`apply_remove_peer`
        // as the sole callers of `peers.insert`/`peers.remove` (callers checked at
        // compile time by their private visibility).
        assert!(
            !self.peers.contains(&self.node_id),
            "self found in peers set",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Raft {
        pub(crate) fn propose_noop(&mut self) -> Result<u64, ClientProposalError> {
            self.propose(RaftCommand::Noop)
        }

        pub(crate) fn simulate_flush(&mut self) {
            self.advance_stabled_index(self.log_last_index());
            self.apply_committed_entries();
        }

        pub(crate) fn current_term(&self) -> u64 {
            self.current_term
        }

        pub(crate) fn voted_for(&self) -> Option<NodeId> {
            self.voted_for.clone()
        }

        pub(crate) fn simulate_flush_and_apply(&mut self) {
            self.advance_stabled_index(self.log_last_index());
            self.apply_committed_entries();
        }

        pub(crate) fn state_machine(&self) -> &MetadataStateMachine {
            &self.state_machine
        }
    }
    fn node(id: &str) -> NodeId {
        NodeId::new(id)
    }

    const TEST_SHARD: ShardGroupId = ShardGroupId(0);

    fn packets(raft: &mut Raft) -> Vec<OutboundRaftPacket> {
        raft.take_events()
            .into_iter()
            .filter_map(|e| match e {
                RaftEvent::OutboundRaftPacket(p) => Some(p),
                _ => None,
            })
            .collect()
    }

    fn leader_events(raft: &mut Raft) -> Vec<LeaderChange> {
        raft.take_events()
            .into_iter()
            .filter_map(|e| match e {
                RaftEvent::LeaderChange(lc) => Some(lc),
                _ => None,
            })
            .collect()
    }

    fn drain(raft: &mut Raft) {
        raft.take_events();
    }

    fn test_timer_seqs() -> TimerSeqs {
        TimerSeqs {
            election: 0,
            rpc: 1,
            merge_check: 2,
            ring_check: 3,
        }
    }

    fn single_node_raft() -> Raft {
        Raft::new(
            node("node-1"),
            HashSet::new(),
            RaftPersistentState::default(),
            0,
            TEST_SHARD,
            test_timer_seqs(),
        )
    }

    fn three_node_raft(id: &str) -> Raft {
        let all = ["node-1", "node-2", "node-3"];
        let peers: HashSet<NodeId> = all.iter().filter(|&&n| n != id).map(|&n| node(n)).collect();
        Raft::new(
            node(id),
            peers,
            RaftPersistentState::default(),
            0,
            TEST_SHARD,
            test_timer_seqs(),
        )
    }

    // -------------------------------------------------------------------
    // Single-node election
    // -------------------------------------------------------------------

    #[test]
    fn single_node_elects_self_on_timeout() {
        let mut raft = single_node_raft();
        assert_eq!(raft.role, Role::Follower);

        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });

        assert_eq!(raft.role, Role::Leader);
        assert_eq!(raft.current_term, 1);
        assert_eq!(raft.voted_for, Some(NodeId::new("node-1")));
    }

    #[test]
    fn single_node_repeated_elections_increment_term() {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        assert_eq!(raft.current_term, 1);

        // Step down and trigger another election
        raft.step_down(1);
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        assert_eq!(raft.current_term, 2);
    }

    // -------------------------------------------------------------------
    // RequestVote
    // -------------------------------------------------------------------

    #[test]
    fn candidate_sends_request_vote_to_all_peers() {
        let mut raft = three_node_raft("node-1");

        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });

        assert!(matches!(raft.role, Role::Candidate { votes_received: 1 }));
        assert_eq!(raft.current_term, 1);

        let out = packets(&mut raft);
        assert_eq!(out.len(), 2); // one per peer
        for pkt in &out {
            assert!(matches!(pkt.rpc, RaftRpc::RequestVote(_)));
        }
    }

    #[test]
    fn follower_grants_vote_to_first_candidate() {
        let mut raft = three_node_raft("node-2");

        let req = RequestVote {
            term: 1,
            candidate_id: NodeId::new("node-1"),
            last_log_index: 0,
            last_log_term: 0,
        };
        raft.handle_rpc(node("node-1"), req);

        let out = packets(&mut raft);
        assert_eq!(out.len(), 1);
        match &out[0].rpc {
            RaftRpc::RequestVoteResponse(resp) => {
                assert!(resp.vote_granted);
                assert_eq!(resp.term, 1);
            }
            _ => panic!("expected RequestVoteResponse"),
        }
        assert_eq!(raft.voted_for, Some(NodeId::new("node-1")));
    }

    #[test]
    fn follower_rejects_second_candidate_same_term() {
        let mut raft = three_node_raft("node-3");

        // Vote for node-1
        let req1 = RequestVote {
            term: 1,
            candidate_id: NodeId::new("node-1"),
            last_log_index: 0,
            last_log_term: 0,
        };
        raft.handle_rpc(node("node-1"), req1);
        drain(&mut raft);

        // node-2 asks for vote in same term
        let req2 = RequestVote {
            term: 1,
            candidate_id: NodeId::new("node-2"),
            last_log_index: 0,
            last_log_term: 0,
        };
        raft.handle_rpc(node("node-2"), req2);

        let out = packets(&mut raft);
        match &out[0].rpc {
            RaftRpc::RequestVoteResponse(resp) => {
                assert!(!resp.vote_granted);
            }
            _ => panic!("expected RequestVoteResponse"),
        }
    }

    #[test]
    fn candidate_becomes_leader_on_majority() {
        let mut raft = three_node_raft("node-1");
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);

        // Receive vote from node-2 (now have 2 out of 3 = majority)
        let resp = RequestVoteResponse {
            term: 1,
            node_id: NodeId::new("node-2"),
            vote_granted: true,
        };
        raft.handle_rpc(node("node-2"), resp);

        assert_eq!(raft.role, Role::Leader);
    }

    #[test]
    fn candidate_steps_down_on_higher_term() {
        let mut raft = three_node_raft("node-1");
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);

        let resp = RequestVoteResponse {
            term: 5,
            node_id: NodeId::new("node-2"),
            vote_granted: false,
        };
        raft.handle_rpc(node("node-2"), resp);

        assert_eq!(raft.role, Role::Follower);
        assert_eq!(raft.current_term, 5);
    }

    // -------------------------------------------------------------------
    // Log up-to-date check
    // -------------------------------------------------------------------

    #[test]
    fn rejects_vote_if_candidate_log_is_stale() {
        let mut raft = three_node_raft("node-2");
        // Give node-2 a log entry at term 2
        raft.log_append(LogEntry {
            term: 2,
            index: 1,
            command: RaftCommand::Noop,
        });

        // node-1 requests vote with older log (term 1)
        let req = RequestVote {
            term: 3,
            candidate_id: NodeId::new("node-1"),
            last_log_index: 1,
            last_log_term: 1,
        };
        raft.handle_rpc(node("node-1"), req);

        let out = packets(&mut raft);
        match &out[0].rpc {
            RaftRpc::RequestVoteResponse(resp) => {
                assert!(!resp.vote_granted, "should reject stale log candidate");
            }
            _ => panic!("expected RequestVoteResponse"),
        }
    }

    // -------------------------------------------------------------------
    // AppendEntries
    // -------------------------------------------------------------------

    #[test]
    fn leader_sends_noop_on_election_then_rpc() {
        let mut raft = three_node_raft("node-1");
        // Become leader
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        raft.handle_rpc(
            node("node-2"),
            RequestVoteResponse {
                term: 1,
                node_id: NodeId::new("node-2"),
                vote_granted: true,
            },
        );

        // Initial AppendEntries carry the noop entry
        let initial = packets(&mut raft);
        assert_eq!(initial.len(), 2);
        for pkt in &initial {
            let RaftRpc::AppendEntries(ae) = &pkt.rpc else {
                panic!("expected AppendEntries")
            };
            assert_eq!(ae.entries.len(), 1, "initial AE should carry noop");
            assert_eq!(ae.entries[0].command, RaftCommand::Noop);
            assert_eq!(ae.entries[0].term, 1);
        }

        // Peers ack the noop
        for name in ["node-2", "node-3"] {
            raft.handle_rpc(
                node(name),
                AppendEntriesResponse {
                    term: 1,
                    node_id: node(name),
                    success: true,
                    last_log_index: 1,
                },
            );
        }
        drain(&mut raft);

        // Subsequent heartbeats are empty
        raft.handle_timeout(RaftTimeoutCallback::RpcTimeout {
            shard_group_id: TEST_SHARD,
        });
        let out = packets(&mut raft);
        assert_eq!(out.len(), 2);
        for pkt in &out {
            let RaftRpc::AppendEntries(ae) = &pkt.rpc else {
                panic!("expected AppendEntries")
            };
            assert_eq!(ae.term, 1);
            assert!(ae.entries.is_empty(), "heartbeat after ack should be empty");
        }
    }

    #[test]
    fn follower_accepts_append_entries() {
        let mut raft = three_node_raft("node-2");

        let ae = AppendEntries {
            term: 1,
            leader_id: NodeId::new("node-1"),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Box::new([LogEntry {
                term: 1,
                index: 1,
                command: RaftCommand::Noop,
            }]),
            leader_commit: 0,
        };
        raft.handle_rpc(node("node-1"), ae);

        let out = packets(&mut raft);
        match &out[0].rpc {
            RaftRpc::AppendEntriesResponse(resp) => {
                assert!(resp.success);
                assert_eq!(resp.last_log_index, 1);
            }
            _ => panic!("expected AppendEntriesResponse"),
        }
        assert_eq!(raft.log_last_index(), 1);
    }

    #[test]
    fn follower_rejects_append_entries_with_stale_term() {
        let mut raft = three_node_raft("node-2");
        raft.current_term = 5;

        let ae = AppendEntries {
            term: 3,
            leader_id: NodeId::new("node-1"),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Default::default(),
            leader_commit: 0,
        };
        raft.handle_rpc(node("node-1"), ae);

        let out = packets(&mut raft);
        match &out[0].rpc {
            RaftRpc::AppendEntriesResponse(resp) => {
                assert!(!resp.success);
                assert_eq!(resp.term, 5);
            }
            _ => panic!("expected AppendEntriesResponse"),
        }
    }

    #[test]
    fn follower_rejects_append_entries_with_log_gap() {
        let mut raft = three_node_raft("node-2");

        // Leader says prev_log_index=1 but follower's log is empty
        let ae = AppendEntries {
            term: 1,
            leader_id: NodeId::new("node-1"),
            prev_log_index: 1,
            prev_log_term: 1,
            entries: Box::new([LogEntry {
                term: 1,
                index: 2,
                command: RaftCommand::Noop,
            }]),
            leader_commit: 0,
        };
        raft.handle_rpc(node("node-1"), ae);

        let out = packets(&mut raft);
        match &out[0].rpc {
            RaftRpc::AppendEntriesResponse(resp) => {
                assert!(!resp.success);
            }
            _ => panic!("expected AppendEntriesResponse"),
        }
    }

    #[test]
    fn leader_advances_commit_on_majority_replication() {
        let mut raft = three_node_raft("node-1");

        // Become leader
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        raft.handle_rpc(
            node("node-2"),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                node_id: NodeId::new("node-2"),
                vote_granted: true,
            }),
        );
        drain(&mut raft);

        // Noop is at index 1 (appended on election). Propose adds at index 2.
        raft.propose_noop().unwrap();
        drain(&mut raft);
        assert_eq!(raft.log_last_index(), 2);
        assert_eq!(raft.commit_index, 0);

        // node-2 acknowledges both entries (noop + proposal)
        let resp = AppendEntriesResponse {
            term: 1,
            node_id: NodeId::new("node-2"),
            success: true,
            last_log_index: 2,
        };
        raft.handle_rpc(node("node-2"), resp);

        // Majority achieved (self + node-2 = 2 out of 3)
        assert_eq!(raft.commit_index, 2);
    }

    #[test]
    fn follower_cannot_propose() {
        let mut raft = three_node_raft("node-1");
        assert_eq!(
            raft.propose_noop(),
            Err::<u64, _>(ClientProposalError::NotLeader(None))
        );
    }

    #[test]
    fn follower_propose_returns_leader_hint_when_known() {
        let mut raft = three_node_raft("node-2");
        // Receive AppendEntries from node-1 so follower learns who leader is
        raft.handle_rpc(
            node("node-1"),
            RaftRpc::AppendEntries(AppendEntries {
                term: 1,
                leader_id: node("node-1"),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Default::default(),
                leader_commit: 0,
            }),
        );
        assert_eq!(raft.current_leader(), Some(&node("node-1")));

        assert_eq!(
            raft.propose_noop(),
            Err::<u64, _>(ClientProposalError::NotLeader(Some(node("node-1"))))
        );
    }

    // -------------------------------------------------------------------
    // Commit index forwarded to followers
    // -------------------------------------------------------------------

    #[test]
    fn follower_advances_commit_index_from_leader() {
        let mut raft = three_node_raft("node-2");

        // First: leader sends entry
        let ae = AppendEntries {
            term: 1,
            leader_id: NodeId::new("node-1"),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Box::new([LogEntry {
                term: 1,
                index: 1,
                command: RaftCommand::Noop,
            }]),
            leader_commit: 1,
        };
        raft.handle_rpc(node("node-1"), ae);
        drain(&mut raft);

        assert_eq!(raft.commit_index, 1);
    }

    // -------------------------------------------------------------------
    // Leader retries on rejection
    // -------------------------------------------------------------------

    #[test]
    fn leader_decrements_next_index_on_rejection() {
        let mut raft = three_node_raft("node-1");

        // Become leader
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        raft.handle_rpc(
            node("node-2"),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                node_id: NodeId::new("node-2"),
                vote_granted: true,
            }),
        );
        drain(&mut raft);

        // Add some entries
        raft.propose_noop().unwrap();
        drain(&mut raft);

        // node-2 rejects (log mismatch)
        let resp = AppendEntriesResponse {
            term: 1,
            node_id: NodeId::new("node-2"),
            success: false,
            last_log_index: 0,
        };
        raft.handle_rpc(node("node-2"), resp);

        // Should have retried with decremented next_index
        let out = packets(&mut raft);
        assert!(!out.is_empty());
        assert!(matches!(out[0].rpc, RaftRpc::AppendEntries(_)));
    }

    // -------------------------------------------------------------------
    // Step down
    // -------------------------------------------------------------------

    #[test]
    fn leader_steps_down_on_higher_term_append_entries() {
        let mut raft = three_node_raft("node-1");

        // Become leader at term 1
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        raft.handle_rpc(
            node("node-2"),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                node_id: NodeId::new("node-2"),
                vote_granted: true,
            }),
        );
        drain(&mut raft);
        assert_eq!(raft.role, Role::Leader);

        // Receive AppendEntries from a leader with higher term
        let ae = AppendEntries {
            term: 3,
            leader_id: NodeId::new("node-3"),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Default::default(),
            leader_commit: 0,
        };
        raft.handle_rpc(node("node-3"), ae);

        assert_eq!(raft.role, Role::Follower);
        assert_eq!(raft.current_term, 3);
    }

    // -------------------------------------------------------------------
    // Timeout role safety
    // -------------------------------------------------------------------

    #[test]
    fn leader_ignores_election_timeout() {
        let mut raft = three_node_raft("node-1");

        // Become leader at term 1
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        raft.handle_rpc(
            node("node-2"),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                node_id: NodeId::new("node-2"),
                vote_granted: true,
            }),
        );
        drain(&mut raft);
        assert_eq!(raft.role, Role::Leader);
        assert_eq!(raft.current_term, 1);

        // Stale election timeout arrives — should be ignored
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });

        assert_eq!(
            raft.role,
            Role::Leader,
            "leader must not start a new election"
        );
        assert_eq!(raft.current_term, 1, "term must not increment");
    }

    #[test]
    fn follower_ignores_rpc_timeout() {
        let mut raft = three_node_raft("node-1");
        assert_eq!(raft.role, Role::Follower);

        raft.handle_timeout(RaftTimeoutCallback::RpcTimeout {
            shard_group_id: TEST_SHARD,
        });

        let out = packets(&mut raft);
        assert!(out.is_empty(), "follower must not send heartbeats");
    }

    #[test]
    fn candidate_ignores_rpc_timeout() {
        let mut raft = three_node_raft("node-1");
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        assert!(matches!(raft.role, Role::Candidate { .. }));

        raft.handle_timeout(RaftTimeoutCallback::RpcTimeout {
            shard_group_id: TEST_SHARD,
        });

        let out = packets(&mut raft);
        assert!(out.is_empty(), "candidate must not send heartbeats");
    }

    // -------------------------------------------------------------------
    // current_leader tracking
    // -------------------------------------------------------------------

    #[test]
    fn current_leader_none_initially() {
        let raft = three_node_raft("node-1");
        assert_eq!(raft.current_leader(), None);
    }

    #[test]
    fn current_leader_set_on_becoming_leader() {
        let mut raft = three_node_raft("node-1");
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);

        raft.handle_rpc(
            node("node-2"),
            RequestVoteResponse {
                term: 1,
                node_id: node("node-2"),
                vote_granted: true,
            },
        );
        drain(&mut raft);

        assert_eq!(raft.role, Role::Leader);
        assert_eq!(raft.current_leader(), Some(&node("node-1")));
    }

    #[test]
    fn current_leader_set_on_append_entries() {
        let mut raft = three_node_raft("node-2");
        assert_eq!(raft.current_leader(), None);

        raft.handle_rpc(
            node("node-1"),
            AppendEntries {
                term: 1,
                leader_id: node("node-1"),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Default::default(),
                leader_commit: 0,
            },
        );
        drain(&mut raft);

        assert_eq!(raft.current_leader(), Some(&node("node-1")));
    }

    #[test]
    fn current_leader_cleared_on_step_down() {
        let mut raft = three_node_raft("node-1");
        // Become leader
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        raft.handle_rpc(
            node("node-2"),
            RequestVoteResponse {
                term: 1,
                node_id: node("node-2"),
                vote_granted: true,
            },
        );
        drain(&mut raft);
        assert_eq!(raft.current_leader(), Some(&node("node-1")));

        // Higher-term vote request forces step-down
        raft.handle_rpc(
            node("node-3"),
            RequestVote {
                term: 5,
                candidate_id: node("node-3"),
                last_log_index: 10,
                last_log_term: 5,
            },
        );
        drain(&mut raft);

        assert_eq!(raft.current_leader(), None);
    }

    #[test]
    fn current_leader_updated_on_new_leader_append_entries() {
        let mut raft = three_node_raft("node-3");

        // Learn leader from node-1
        raft.handle_rpc(
            node("node-1"),
            AppendEntries {
                term: 1,
                leader_id: node("node-1"),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Default::default(),
                leader_commit: 0,
            },
        );
        drain(&mut raft);
        assert_eq!(raft.current_leader(), Some(&node("node-1")));

        // New leader at higher term
        raft.handle_rpc(
            node("node-2"),
            AppendEntries {
                term: 2,
                leader_id: node("node-2"),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Default::default(),
                leader_commit: 0,
            },
        );
        drain(&mut raft);
        assert_eq!(raft.current_leader(), Some(&node("node-2")));
    }

    #[test]
    fn quorum_requires_strict_majority() {
        // total = peers + 1 (self). Quorum = total / 2 + 1.
        let expected = [
            // (total_nodes, expected_quorum)
            (1, 1),
            (2, 2),
            (3, 2),
            (4, 3),
            (5, 3),
            (6, 4),
            (7, 4),
        ];

        for (total, want) in expected {
            let peer_count = total - 1;
            let peers: HashSet<NodeId> = (0..peer_count)
                .map(|i| NodeId::new(format!("peer-{i}")))
                .collect();
            let raft = Raft::new(
                NodeId::new("self"),
                peers,
                RaftPersistentState::default(),
                0,
                TEST_SHARD,
                test_timer_seqs(),
            );

            assert_eq!(
                raft.quorum(),
                want,
                "quorum for {total} nodes should be {want}"
            );
        }
    }

    // -------------------------------------------------------------------
    // is_leader / has_peer
    // -------------------------------------------------------------------

    #[test]
    fn is_leader_returns_false_for_follower() {
        let raft = three_node_raft("node-1");
        assert!(!raft.is_leader());
    }

    #[test]
    fn is_leader_returns_true_after_election() {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        assert!(raft.is_leader());
    }

    #[test]
    fn has_peer_checks_membership() {
        let raft = three_node_raft("node-1");
        assert!(raft.has_peer(&node("node-2")));
        assert!(raft.has_peer(&node("node-3")));
        assert!(!raft.has_peer(&node("node-1"))); // self is not in peers
        assert!(!raft.has_peer(&node("node-99")));
    }

    // -------------------------------------------------------------------
    // Membership changes via the Raft log
    // -------------------------------------------------------------------
    //
    // AddPeer/RemovePeer apply through `apply_committed_entries()` on commit.
    // Direct mutation is gone — the peer set is part of the replicated state.

    #[test]
    fn add_peer_log_entry_inserts_into_peers_on_apply() {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        assert!(raft.is_leader());
        assert_eq!(raft.peers_count(), 0);

        raft.propose(RaftCommand::AddPeer(node("node-2"))).unwrap();
        raft.simulate_flush();

        assert!(raft.has_peer(&node("node-2")));
        assert_eq!(raft.peers_count(), 1);
    }

    #[test]
    fn add_peer_log_entry_skips_self() {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);

        raft.propose(RaftCommand::AddPeer(node("node-1"))).unwrap();
        raft.simulate_flush();

        assert!(!raft.has_peer(&node("node-1")));
        assert_eq!(raft.peers_count(), 0);
    }

    #[test]
    fn add_peer_log_entry_leader_initializes_peer_state() {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        assert!(raft.is_leader());

        raft.propose(RaftCommand::AddPeer(node("node-2"))).unwrap();
        raft.simulate_flush();
        drain(&mut raft);

        // Next heartbeat must include AppendEntries to node-2.
        raft.handle_timeout(RaftTimeoutCallback::RpcTimeout {
            shard_group_id: TEST_SHARD,
        });
        let heartbeats = packets(&mut raft);
        let targets: Vec<&NodeId> = heartbeats.iter().map(|p| &p.target).collect();
        assert!(targets.contains(&&node("node-2")));
    }

    #[test]
    fn remove_peer_log_entry_removes_from_peers_on_apply() {
        let mut n1 = three_node_raft("node-1");
        let mut n2 = three_node_raft("node-2");

        n1.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        let vote_reqs = packets(&mut n1);
        for pkt in &vote_reqs {
            if pkt.target == node("node-2") {
                n2.handle_rpc(node("node-1"), pkt.rpc.clone());
            }
        }
        for pkt in packets(&mut n2) {
            n1.handle_rpc(node("node-2"), pkt.rpc);
        }
        assert!(n1.is_leader());
        drain(&mut n1);
        assert!(n1.has_peer(&node("node-3")));

        // Direct apply: simulate the RemovePeer entry committing on this leader
        // without needing a full 3-node simulation. The apply helper is the
        // production code path triggered by `apply_committed_entries()`.
        n1.apply_remove_peer(node("node-3"));

        n1.handle_timeout(RaftTimeoutCallback::RpcTimeout {
            shard_group_id: TEST_SHARD,
        });
        let heartbeats = packets(&mut n1);
        let targets: Vec<&NodeId> = heartbeats.iter().map(|p| &p.target).collect();
        assert!(!targets.contains(&&node("node-3")));
        assert_eq!(n1.peers_count(), 1);
    }

    // -------------------------------------------------------------------
    // Metadata command through Raft log
    // -------------------------------------------------------------------

    #[test]
    fn metadata_command_noop_in_phase2() {
        use crate::control_plane::metadata::command::{CreateTopic, MetadataCommand};
        use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};

        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);

        let cmd = MetadataCommand::CreateTopic(CreateTopic {
            name: "blue".to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
            replica_set: vec![node("node-1"), node("node-2"), node("node-3")],
            created_at: 1000,
        });

        let result = raft.propose(cmd.into());
        assert!(result.is_ok());
        raft.simulate_flush();
        assert!(raft.last_applied_index > 0);
    }

    // -------------------------------------------------------------------
    // LeaderChangeEvent emission
    // -------------------------------------------------------------------

    #[test]
    fn leader_event_emitted_on_election() {
        let mut raft = three_node_raft("node-1");
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);

        // Win election
        raft.handle_rpc(
            node("node-2"),
            RequestVoteResponse {
                term: 1,
                node_id: node("node-2"),
                vote_granted: true,
            },
        );
        assert!(raft.is_leader());

        let events = leader_events(&mut raft);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].shard_group_id, TEST_SHARD);
        assert_eq!(events[0].leader_node_id, node("node-1"));
        assert_eq!(events[0].term, 1);
    }

    #[test]
    fn single_node_leader_event() {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });

        let events = leader_events(&mut raft);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].leader_node_id, node("node-1"));
        assert_eq!(events[0].term, 1);
    }

    #[test]
    fn no_leader_event_on_step_down() {
        let mut raft = three_node_raft("node-1");
        // Become leader
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        raft.handle_rpc(
            node("node-2"),
            RequestVoteResponse {
                term: 1,
                node_id: node("node-2"),
                vote_granted: true,
            },
        );
        drain(&mut raft);
        drain(&mut raft); // drain election event

        // Step down via higher-term vote request
        raft.handle_rpc(
            node("node-3"),
            RequestVote {
                term: 5,
                candidate_id: node("node-3"),
                last_log_index: 10,
                last_log_term: 5,
            },
        );
        drain(&mut raft);

        let events = leader_events(&mut raft);
        assert!(events.is_empty(), "step-down must not emit leader event");
    }

    #[test]
    fn leader_event_correct_term_on_reelection() {
        let mut raft = single_node_raft();

        // First election: term 1
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        let events = leader_events(&mut raft);
        assert_eq!(events[0].term, 1);

        // Step down and re-elect: term 2
        raft.step_down(1);
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });

        let reelection_events = leader_events(&mut raft);
        assert_eq!(reelection_events.len(), 1);
        assert_eq!(reelection_events[0].term, 2);
    }

    // -------------------------------------------------------------------
    // Phase 3 — MetadataStateMachine apply
    // -------------------------------------------------------------------

    use crate::control_plane::metadata::command::{CreateTopic, MetadataCommand};
    use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};

    fn test_create_topic_cmd(name: &str) -> MetadataCommand {
        MetadataCommand::CreateTopic(CreateTopic {
            name: name.to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
            replica_set: vec![node("n1"), node("n2"), node("n3")],
            created_at: 1000,
        })
    }

    #[test]
    fn apply_metadata_command_creates_topic() {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        raft.simulate_flush();

        raft.propose(test_create_topic_cmd("blue").into()).unwrap();
        raft.simulate_flush();

        assert!(raft.state_machine().get_topic_by_name("blue").is_some());
    }

    #[test]
    fn apply_noop_does_not_affect_state_machine() {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        raft.simulate_flush();

        raft.propose_noop().unwrap();
        raft.simulate_flush();

        assert_eq!(raft.state_machine().topic_count(), 0);
    }

    #[test]
    fn apply_duplicate_topic_logs_error_no_panic() {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        raft.simulate_flush();

        raft.propose(test_create_topic_cmd("red").into()).unwrap();
        raft.simulate_flush();

        raft.propose(test_create_topic_cmd("red").into()).unwrap();
        raft.simulate_flush();

        assert_eq!(raft.state_machine().topic_count(), 1);
    }

    #[test]
    fn apply_multiple_topics_independent() {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        raft.simulate_flush();

        raft.propose(test_create_topic_cmd("alpha").into()).unwrap();
        raft.propose(test_create_topic_cmd("beta").into()).unwrap();
        raft.simulate_flush();

        assert_eq!(raft.state_machine().topic_count(), 2);
        assert!(raft.state_machine().get_topic_by_name("alpha").is_some());
        assert!(raft.state_machine().get_topic_by_name("beta").is_some());
    }

    // -------------------------------------------------------------------
    // Figure 8 safety: old-term entries not directly committed
    // -------------------------------------------------------------------

    #[test]
    fn old_term_entries_not_committed_until_current_term_entry_committed() {
        // Follower receives a term-1 entry from a leader that crashes before committing.
        // A new leader at term 2 must NOT commit the term-1 entry directly.
        // It commits only after a term-2 entry achieves quorum.
        let mut raft = three_node_raft("node-1");

        // Receive term-1 entry as a follower (simulating replication from a crashed leader)
        raft.handle_rpc(
            node("node-2"),
            RaftRpc::AppendEntries(AppendEntries {
                term: 1,
                leader_id: node("node-2"),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Box::new([LogEntry {
                    term: 1,
                    index: 1,
                    command: RaftCommand::Noop,
                }]),
                leader_commit: 0,
            }),
        );
        drain(&mut raft);
        raft.simulate_flush();
        assert_eq!(raft.log_last_index(), 1);
        assert_eq!(raft.commit_index, 0);

        // node-1 wins election at term 2
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        raft.handle_rpc(
            node("node-3"),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 2,
                node_id: node("node-3"),
                vote_granted: true,
            }),
        );
        drain(&mut raft);
        assert_eq!(raft.role, Role::Leader);
        assert_eq!(raft.current_term, 2);
        // become_leader appends a Noop at term 2 (index 2)
        assert_eq!(raft.log_last_index(), 2);
        assert_eq!(raft.log_term_at(1), 1);
        assert_eq!(raft.log_term_at(2), 2);

        // node-3 acks only the old term-1 entry (index 1) but not the term-2 entry
        raft.handle_rpc(
            node("node-3"),
            AppendEntriesResponse {
                term: 2,
                node_id: node("node-3"),
                success: true,
                last_log_index: 1,
            },
        );
        // commit_index must NOT advance — the replicated entry is term 1, not current term
        assert_eq!(
            raft.commit_index, 0,
            "term-1 entry must not be directly committed even with majority"
        );

        // node-3 acks the term-2 entry (index 2)
        raft.handle_rpc(
            node("node-3"),
            AppendEntriesResponse {
                term: 2,
                node_id: node("node-3"),
                success: true,
                last_log_index: 2,
            },
        );
        // Now both entries committed (term-2 entry at index 2 has quorum,
        // implicitly committing the term-1 entry at index 1)
        assert_eq!(raft.commit_index, 2);
    }

    #[test]
    fn merge_check_timer_scheduled_on_leadership() {
        let seqs = test_timer_seqs();
        let merge_seq = seqs.merge_check;
        let mut raft = Raft::new(
            node("node-1"),
            HashSet::new(),
            RaftPersistentState::default(),
            0,
            TEST_SHARD,
            seqs,
        );
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });

        let events = raft.take_events();
        let merge_check_set = events.iter().any(|e| match e {
            RaftEvent::Timer(TimerCommand::SetSchedule { seq, timer }) => {
                *seq == merge_seq && timer.shard_group_id == TEST_SHARD
            }
            _ => false,
        });
        assert!(
            merge_check_set,
            "merge_check timer must be scheduled on become_leader"
        );
    }

    #[test]
    fn ring_check_timer_scheduled_on_leadership() {
        let seqs = test_timer_seqs();
        let ring_seq = seqs.ring_check;
        let mut raft = Raft::new(
            node("node-1"),
            HashSet::new(),
            RaftPersistentState::default(),
            0,
            TEST_SHARD,
            seqs,
        );
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });

        let events = raft.take_events();
        let ring_check_set = events.iter().any(|e| match e {
            RaftEvent::Timer(TimerCommand::SetSchedule { seq, timer }) => {
                *seq == ring_seq && timer.shard_group_id == TEST_SHARD
            }
            _ => false,
        });
        assert!(
            ring_check_set,
            "ring_check timer must be scheduled on become_leader"
        );
    }

    #[test]
    fn ring_check_rearm_is_leader_gated() {
        let seqs = test_timer_seqs();
        let ring_seq = seqs.ring_check;
        let mut raft = Raft::new(
            node("node-1"),
            HashSet::new(),
            RaftPersistentState::default(),
            0,
            TEST_SHARD,
            seqs,
        );
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);

        // Leader: the fired check re-arms itself.
        raft.handle_timeout(RaftTimeoutCallback::RingCheckTimeout {
            shard_group_id: TEST_SHARD,
        });
        let rearmed = raft.take_events().iter().any(|e| {
            matches!(
                e,
                RaftEvent::Timer(TimerCommand::SetSchedule { seq, .. }) if *seq == ring_seq
            )
        });
        assert!(rearmed, "leader must re-arm the ring check on fire");

        // Deposed: the timer dies until the next become_leader.
        let term = raft.current_term();
        raft.step_down(term);
        drain(&mut raft);
        raft.handle_timeout(RaftTimeoutCallback::RingCheckTimeout {
            shard_group_id: TEST_SHARD,
        });
        let rearmed_after_depose = raft.take_events().iter().any(|e| {
            matches!(
                e,
                RaftEvent::Timer(TimerCommand::SetSchedule { seq, .. }) if *seq == ring_seq
            )
        });
        assert!(
            !rearmed_after_depose,
            "a non-leader must let the ring check die"
        );
    }

    // -------------------------------------------------------------------
    // Reconciliation: peer pairing + segment catch-up
    //
    // These tests were originally in `multi_raft.rs` but moved here when
    // `reconcile_peers` and `reconcile_segments` became methods on `Raft`.
    // They construct a `Raft` directly, drive it into Leader role, and
    // verify the proposals appear in the log.
    // -------------------------------------------------------------------

    use crate::control_plane::membership::{
        Topology, TopologyConfig, TopologyReader, topology_channel,
    };

    /// Build a `TopologyReader` seeded with `nodes` as live members. The
    /// publisher half is dropped on return — the reader's own Arc keeps the
    /// underlying ArcSwap alive for the test's lifetime.
    fn topology_reader_with(nodes: &[&str]) -> TopologyReader {
        let topology = Topology::new(
            nodes.iter().map(|n| NodeId::new(*n)),
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 3,
            },
        );
        let (_pub_handle, reader) = topology_channel(topology);
        reader
    }

    fn live_set(nodes: &[&str]) -> HashSet<NodeId> {
        nodes.iter().map(|n| NodeId::new(*n)).collect()
    }

    /// Three-node Raft as `id`, then drive it to Leader by simulating one
    /// vote-granted RequestVoteResponse from a peer (majority of 3 = 2 votes
    /// including self).
    fn three_node_raft_as_leader(id: &str) -> Raft {
        let mut raft = three_node_raft(id);
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        let peer = raft
            .peers_iter()
            .next()
            .expect("three_node_raft must have peers")
            .clone();
        let term = raft.current_term;
        raft.handle_rpc(
            peer.clone(),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term,
                node_id: peer,
                vote_granted: true,
            }),
        );
        drain(&mut raft);
        assert_eq!(raft.role, Role::Leader, "must be leader after election");
        raft
    }

    /// Single-node Raft, immediately leader after ElectionTimeout (no peers
    /// to wait for, quorum=1). Used by segment tests so CreateTopic commits
    /// without needing follower RPCs.
    fn single_node_raft_as_leader() -> Raft {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        assert_eq!(raft.role, Role::Leader);
        raft
    }

    /// Proposals in the log after the become_leader noop (index 1).
    fn proposals_after_become_leader(raft: &Raft) -> Vec<RaftCommand> {
        (2..=raft.log_last_index())
            .filter_map(|i| raft.log_get(i).map(|e| e.command.clone()))
            .collect()
    }

    fn create_topic_in_raft(raft: &mut Raft, name: &str, replica_set: Vec<NodeId>) {
        use crate::control_plane::metadata::command::CreateTopic;
        use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
        let rf = replica_set.len();
        let cmd: RaftCommand = MetadataCommand::CreateTopic(CreateTopic {
            name: name.to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: rf as u64,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
            replica_set,
            created_at: 1000,
        })
        .into();
        raft.propose(cmd).expect("CreateTopic propose failed");
        // Drive apply so the state machine sees the committed entry.
        raft.simulate_flush_and_apply();
    }

    #[test]
    fn reconcile_peers_pairs_each_dead_with_replacement() {
        // 3-node raft as node-1 (peers = [node-2, node-3]). Topology has
        // [node-1, node-3, node-4] — so node-2 is dead per SWIM, and node-4
        // is the ring-eligible replacement. Reconciliation must propose
        // RemovePeer(node-2) + AddPeer(node-4).
        let mut raft = three_node_raft_as_leader("node-1");
        let topology = topology_reader_with(&["node-1", "node-3", "node-4"]);
        let live = live_set(&["node-1", "node-3", "node-4"]);

        raft.reconcile_peers(&topology, &live);

        let proposals = proposals_after_become_leader(&raft);
        assert_eq!(
            proposals.len(),
            2,
            "reconciliation must propose RemovePeer + AddPeer (got {:?})",
            proposals
        );
        assert_eq!(proposals[0], RaftCommand::RemovePeer(node("node-2")));
        assert_eq!(
            proposals[1],
            RaftCommand::AddPeer(node("node-4")),
            "node-4 is the only ring-eligible node not already in the group"
        );
    }

    #[test]
    fn reconcile_peers_degrades_when_pool_exhausted() {
        // 3-node raft as node-1 (peers = [node-2, node-3]). Topology has only
        // [node-1, node-3] — node-2 is dead AND no replacement is available.
        // RemovePeer must still fire; AddPeer must not.
        let mut raft = three_node_raft_as_leader("node-1");
        let topology = topology_reader_with(&["node-1", "node-3"]);
        let live = live_set(&["node-1", "node-3"]);

        raft.reconcile_peers(&topology, &live);

        let proposals = proposals_after_become_leader(&raft);
        assert_eq!(
            proposals,
            vec![RaftCommand::RemovePeer(node("node-2"))],
            "exhausted pool must yield only the removal, no paired addition"
        );
    }

    #[test]
    fn stale_live_voters_flags_live_ex_owners_only() {
        // Voters are {node-1 (self), node-2, node-3}; the ring now assigns the
        // group to {node-1, node-3, node-4}. node-2 is the #135 ratchet case
        // only while it is alive — once dead it becomes `reconcile_peers`'
        // jurisdiction and must not be reported here.
        let raft = three_node_raft("node-1");
        let ring = [node("node-1"), node("node-3"), node("node-4")];

        let all_live = live_set(&["node-1", "node-2", "node-3", "node-4"]);
        assert_eq!(
            raft.stale_live_voters(&ring, &all_live),
            vec![node("node-2")],
            "a live voter outside the ring assignment is a stale live voter"
        );

        let node_2_dead = live_set(&["node-1", "node-3", "node-4"]);
        assert!(
            raft.stale_live_voters(&ring, &node_2_dead).is_empty(),
            "dead ex-owners belong to reconcile_peers, not drift detection"
        );

        let ring_matches_voters = [node("node-1"), node("node-2"), node("node-3")];
        assert!(
            raft.stale_live_voters(&ring_matches_voters, &all_live)
                .is_empty(),
            "no drift when voters equal the ring assignment"
        );
    }

    #[test]
    fn reconcile_segments_rolls_segments_with_dead_replicas() {
        // Single-node Raft so CreateTopic commits trivially (quorum=1),
        // letting us seed an active segment. The segment's replica_set
        // [x,y,z] is independent of the Raft peer set — none of x/y/z appear
        // in the live set, so they're all "dead" per SWIM. Takeover-time
        // segment catch-up must propose a RollSegment for that segment.
        let mut raft = single_node_raft_as_leader();
        create_topic_in_raft(
            &mut raft,
            "test-topic",
            vec![node("x"), node("y"), node("z")],
        );
        let before = proposals_after_become_leader(&raft).len();

        let live = live_set(&["node-1"]);
        raft.reconcile_segments(&live);

        let after = proposals_after_become_leader(&raft);
        assert_eq!(
            after.len() - before,
            1,
            "reconcile_segments must propose exactly one RollSegment (got tail: {:?})",
            &after[before..]
        );
        match &after[before] {
            RaftCommand::Metadata(MetadataCommand::RollSegment(roll)) => {
                for member in &roll.new_replica_set {
                    assert!(
                        live.contains(member),
                        "new replica_set must contain only live nodes, found {:?}",
                        member
                    );
                }
                assert_eq!(
                    roll.end_entry_id, None,
                    "takeover-triggered rolls don't know the committed offset"
                );
            }
            other => panic!("expected Metadata(RollSegment), got {:?}", other),
        }
    }

    #[test]
    fn reconcile_segments_noop_when_all_replicas_live() {
        // Same setup, but the live set now includes x/y/z — no dead
        // replicas, so reconcile_segments must not propose anything.
        let mut raft = single_node_raft_as_leader();
        create_topic_in_raft(
            &mut raft,
            "test-topic",
            vec![node("x"), node("y"), node("z")],
        );
        let before = proposals_after_become_leader(&raft).len();

        let live = live_set(&["node-1", "x", "y", "z"]);
        raft.reconcile_segments(&live);

        let after = proposals_after_become_leader(&raft);
        assert_eq!(
            after.len(),
            before,
            "reconcile_segments must be a no-op when all replicas are live (got tail: {:?})",
            &after[before..]
        );
    }
}
