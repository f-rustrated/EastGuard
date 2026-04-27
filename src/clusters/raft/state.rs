#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use crate::clusters::NodeId;
use crate::clusters::raft::log::LogEntry;
use crate::clusters::raft::messages::*;
use crate::clusters::raft::storage::RaftPersistentState;
use crate::clusters::swims::ShardGroupId;
use crate::schedulers::ticker_message::TimerCommand;

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
    peers: HashSet<NodeId>,

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
    // LEADER-ONLY volatile state
    peer_states: HashMap<NodeId, PeerState>,
    election_jitter: u32,
    election_seq: u32,
    heartbeat_seq: u32,
}

impl Raft {
    pub(crate) fn new(
        node_id: NodeId,
        peers: HashSet<NodeId>,
        persistent: RaftPersistentState,
        election_jitter: u32,
        shard_group_id: ShardGroupId,
        election_seq: u32,
        heartbeat_seq: u32,
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
            peer_states: HashMap::new(),
            election_jitter,
            election_seq,
            heartbeat_seq,
        };
        raft.reset_election_timer();
        raft
    }

    pub(crate) fn heartbeat_seq(&self) -> u32 {
        self.heartbeat_seq
    }

    pub(crate) fn take_events(&mut self) -> Vec<RaftEvent> {
        std::mem::take(&mut self.events)
    }

    pub fn take_log_mutations(&mut self) -> Vec<LogMutation> {
        std::mem::take(&mut self.pending_log_mutations)
    }

    // -------------------------------------------------------------------
    // In-memory log helpers (1-based indexing; index 0 means "before log")
    // -------------------------------------------------------------------

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

    fn log_entries_from(&self, start_index: u64) -> Vec<LogEntry> {
        let last = self.log_last_index();
        if start_index == 0 || start_index > last {
            return vec![];
        }
        self.log[(start_index - 1) as usize..].to_vec()
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
            RaftTimeoutCallback::ElectionTimeout { .. } => {
                self.start_election();
            }
            RaftTimeoutCallback::HeartbeatTimeout { .. } => {
                self.send_heartbeats();
            }
        }
    }

    pub fn step(&mut self, from: NodeId, rpc: impl Into<RaftRpc>) {
        let rpc = rpc.into();
        match rpc {
            RaftRpc::RequestVote(req) => self.handle_request_vote(from, req),
            RaftRpc::RequestVoteResponse(resp) => self.handle_request_vote_response(resp),
            RaftRpc::AppendEntries(req) => self.handle_append_entries(from, req),
            RaftRpc::AppendEntriesResponse(resp) => self.handle_append_entries_response(resp),
        }
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

        let vote_granted = req.term == self.current_term
            && self.vote_available_for(&req.candidate_id)
            && self.log_is_up_to_date(req.last_log_index, req.last_log_term);

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

    // ! SAFETY: only candidates count votes.
    fn handle_request_vote_response(&mut self, resp: RequestVoteResponse) {
        if resp.term > self.current_term {
            self.step_down(resp.term);
            return;
        }

        if let Role::Candidate { votes_received } = &mut self.role
            && resp.term == self.current_term
            && resp.vote_granted
        {
            *votes_received += 1;
            if *votes_received >= self.quorum() {
                self.become_leader();
            }
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

        self.events.push(
            LeaderChange {
                shard_group_id: self.shard_group_id,
                leader_node_id: self.node_id.clone(),
                term: self.current_term,
            }
            .into(),
        );

        // Cancel election timer, start heartbeat timer.
        self.cancel_all_timers();
        self.schedule_heartbeat_timer();

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
        self.current_term = new_term;
        self.voted_for = None;
        self.push_hard_state();
        self.role = Role::Follower;
        self.current_leader = None;
        self.peer_states.clear();
        self.cancel_all_timers();
        self.reset_election_timer();
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

        self.schedule_heartbeat_timer();
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
        // Stale term: reject.
        if req.term < self.current_term {
            self.send_append_entries_response(from.clone(), false);
            return;
        }

        // If term is current or newer, recognize leader and reset to follower.
        if req.term > self.current_term {
            self.step_down(req.term);
        } else if self.role != Role::Follower {
            // Same term but we're candidate — step down.
            self.role = Role::Follower;
            self.peer_states.clear();
        }

        self.current_leader = Some(req.leader_id.clone());
        self.reset_election_timer();

        // Log consistency check: prev_log_index must exist with matching term.
        if req.prev_log_index > 0 {
            let local_term = self.log_term_at(req.prev_log_index);
            if local_term == 0 || local_term != req.prev_log_term {
                self.send_append_entries_response(from, false);
                return;
            }
        }

        // Append entries (truncate conflicting suffix first).
        for entry in req.entries {
            let existing_term = self.log_term_at(entry.index);
            if existing_term != 0 && existing_term != entry.term {
                self.log_truncate_from(entry.index);
            }
            if entry.index > self.log_last_index() {
                self.log_append(entry);
            }
        }

        // Advance commit index.
        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.log_last_index());
            self.apply_committed_entries();
        }

        self.send_append_entries_response(from, true);
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
                // TODO: In practice, commit means applying the command to the state machine. The flow
                //  1. Leader receives propose(CreateTopic("blue"))
                //  2. Leader appends to log: [term=3, index=7, cmd=CreateTopic("blue")]
                //  3. Leader replicates to peers via AppendEntries
                //  4. Majority acknowledge → commit_index advances to 7
                //  5. Apply: the state machine executes CreateTopic("blue")  ← this is the real "commit"
            } else {
                // Decrement next_index and retry.
                peer_state.next_index = peer_state.next_index.saturating_sub(1).max(1);
                self.send_append_entries(resp.node_id);
            }
        }
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
    }

    #[cfg(test)]
    pub(crate) fn simulate_flush(&mut self) {
        self.advance_stabled_index(self.log_last_index());
        self.apply_committed_entries();
    }

    /// Apply committed (and persisted) but unapplied log entries.
    /// Phase 3 will dispatch Metadata commands to MetadataStateMachine.
    fn apply_committed_entries(&mut self) {
        while self.last_applied_index < self.commit_index.min(self.stabled_index) {
            self.last_applied_index += 1;
            let entry = match self.log_get(self.last_applied_index) {
                Some(e) => e.clone(),
                None => {
                    tracing::error!(
                        "[{}] committed entry at index {} missing from log",
                        self.node_id,
                        self.last_applied_index
                    );
                    break;
                }
            };
            match entry.command {
                RaftCommand::Noop => {}
                RaftCommand::Metadata(_) => {
                    // Phase 3: dispatch to MetadataStateMachine
                }
            }
        }
    }

    /// Directly add a peer to this Raft instance. Called by MultiRaft on
    /// SWIM `NodeAlive` events — bypasses the log since SWIM is membership authority.
    pub(crate) fn add_peer(&mut self, node_id: NodeId) {
        if node_id == self.node_id {
            return;
        }
        self.peers.insert(node_id.clone());
        if self.role == Role::Leader {
            self.peer_states.insert(
                node_id,
                PeerState {
                    next_index: self.log_last_index() + 1,
                    match_index: 0,
                },
            );
        }
    }

    /// Directly remove a peer from this Raft instance. Called by MultiRaft on
    /// SWIM `NodeDead` events — bypasses the log since SWIM is membership authority.
    pub(crate) fn remove_peer(&mut self, node_id: &NodeId) {
        self.peers.remove(node_id);
        self.peer_states.remove(node_id);
        self.events.push(RaftEvent::DisconnectPeer(node_id.clone()));
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
    pub fn propose(&mut self, command: RaftCommand) -> Result<(), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader);
        }

        self.add_new_entry(command);

        // Immediately replicate to all peers.
        let peers: Vec<NodeId> = self.peers.iter().cloned().collect();
        for peer_id in peers {
            self.send_append_entries(peer_id);
        }

        // Single-node cluster: no peers to ack, so commit immediately
        // (quorum of 1 = self). For multi-node, this is a no-op because
        // no peer has acked yet.
        self.try_advance_commit_index();

        Ok(())
    }

    fn reset_election_timer(&mut self) {
        self.events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: self.election_seq,
            }));
        self.events
            .push(RaftEvent::Timer(TimerCommand::SetSchedule {
                seq: self.election_seq,
                timer: RaftTimer::election(self.election_jitter, self.shard_group_id),
            }));
    }

    fn schedule_heartbeat_timer(&mut self) {
        self.events
            .push(RaftEvent::Timer(TimerCommand::SetSchedule {
                seq: self.heartbeat_seq,
                timer: RaftTimer::heartbeat(self.shard_group_id),
            }));
    }

    pub(crate) fn cancel_all_timers(&mut self) {
        self.events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: self.election_seq,
            }));
        self.events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: self.heartbeat_seq,
            }));
    }

    #[cfg(test)]
    pub(crate) fn current_term(&self) -> u64 {
        self.current_term
    }

    #[cfg(test)]
    pub(crate) fn voted_for(&self) -> Option<NodeId> {
        self.voted_for.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn single_node_raft() -> Raft {
        Raft::new(
            node("node-1"),
            HashSet::new(),
            RaftPersistentState::default(),
            0,
            TEST_SHARD,
            0,
            1,
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
            0,
            1,
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
        });
        assert_eq!(raft.current_term, 1);

        // Step down and trigger another election
        raft.step_down(1);
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
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
        raft.step(node("node-1"), req);

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
        raft.step(node("node-1"), req1);
        drain(&mut raft);

        // node-2 asks for vote in same term
        let req2 = RequestVote {
            term: 1,
            candidate_id: NodeId::new("node-2"),
            last_log_index: 0,
            last_log_term: 0,
        };
        raft.step(node("node-2"), req2);

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
        });
        drain(&mut raft);

        // Receive vote from node-2 (now have 2 out of 3 = majority)
        let resp = RequestVoteResponse {
            term: 1,
            node_id: NodeId::new("node-2"),
            vote_granted: true,
        };
        raft.step(node("node-2"), resp);

        assert_eq!(raft.role, Role::Leader);
    }

    #[test]
    fn candidate_steps_down_on_higher_term() {
        let mut raft = three_node_raft("node-1");
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
        });
        drain(&mut raft);

        let resp = RequestVoteResponse {
            term: 5,
            node_id: NodeId::new("node-2"),
            vote_granted: false,
        };
        raft.step(node("node-2"), resp);

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
        raft.step(node("node-1"), req);

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
    fn leader_sends_noop_on_election_then_heartbeats() {
        let mut raft = three_node_raft("node-1");
        // Become leader
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
        });
        drain(&mut raft);
        raft.step(
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
            raft.step(
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
        raft.handle_timeout(RaftTimeoutCallback::HeartbeatTimeout {
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
            entries: vec![LogEntry {
                term: 1,
                index: 1,
                command: RaftCommand::Noop,
            }],
            leader_commit: 0,
        };
        raft.step(node("node-1"), ae);

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
            entries: vec![],
            leader_commit: 0,
        };
        raft.step(node("node-1"), ae);

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
            entries: vec![LogEntry {
                term: 1,
                index: 2,
                command: RaftCommand::Noop,
            }],
            leader_commit: 0,
        };
        raft.step(node("node-1"), ae);

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
        });
        drain(&mut raft);
        raft.step(
            node("node-2"),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                node_id: NodeId::new("node-2"),
                vote_granted: true,
            }),
        );
        drain(&mut raft);

        // Noop is at index 1 (appended on election). Propose adds at index 2.
        raft.propose(RaftCommand::Noop).unwrap();
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
        raft.step(node("node-2"), resp);

        // Majority achieved (self + node-2 = 2 out of 3)
        assert_eq!(raft.commit_index, 2);
    }

    #[test]
    fn follower_cannot_propose() {
        let mut raft = three_node_raft("node-1");
        assert_eq!(
            raft.propose(RaftCommand::Noop),
            Err(ProposeError::NotLeader)
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
            entries: vec![LogEntry {
                term: 1,
                index: 1,
                command: RaftCommand::Noop,
            }],
            leader_commit: 1,
        };
        raft.step(node("node-1"), ae);
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
        });
        drain(&mut raft);
        raft.step(
            node("node-2"),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                node_id: NodeId::new("node-2"),
                vote_granted: true,
            }),
        );
        drain(&mut raft);

        // Add some entries
        raft.propose(RaftCommand::Noop).unwrap();
        drain(&mut raft);

        // node-2 rejects (log mismatch)
        let resp = AppendEntriesResponse {
            term: 1,
            node_id: NodeId::new("node-2"),
            success: false,
            last_log_index: 0,
        };
        raft.step(node("node-2"), resp);

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
        });
        drain(&mut raft);
        raft.step(
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
            entries: vec![],
            leader_commit: 0,
        };
        raft.step(node("node-3"), ae);

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
        });
        drain(&mut raft);
        raft.step(
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
        });

        assert_eq!(
            raft.role,
            Role::Leader,
            "leader must not start a new election"
        );
        assert_eq!(raft.current_term, 1, "term must not increment");
    }

    #[test]
    fn follower_ignores_heartbeat_timeout() {
        let mut raft = three_node_raft("node-1");
        assert_eq!(raft.role, Role::Follower);

        raft.handle_timeout(RaftTimeoutCallback::HeartbeatTimeout {
            shard_group_id: TEST_SHARD,
        });

        let out = packets(&mut raft);
        assert!(out.is_empty(), "follower must not send heartbeats");
    }

    #[test]
    fn candidate_ignores_heartbeat_timeout() {
        let mut raft = three_node_raft("node-1");
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
        });
        drain(&mut raft);
        assert!(matches!(raft.role, Role::Candidate { .. }));

        raft.handle_timeout(RaftTimeoutCallback::HeartbeatTimeout {
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
        });
        drain(&mut raft);

        raft.step(
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

        raft.step(
            node("node-1"),
            AppendEntries {
                term: 1,
                leader_id: node("node-1"),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
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
        });
        drain(&mut raft);
        raft.step(
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
        raft.step(
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
        raft.step(
            node("node-1"),
            AppendEntries {
                term: 1,
                leader_id: node("node-1"),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
        );
        drain(&mut raft);
        assert_eq!(raft.current_leader(), Some(&node("node-1")));

        // New leader at higher term
        raft.step(
            node("node-2"),
            AppendEntries {
                term: 2,
                leader_id: node("node-2"),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
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
                0,
                1,
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
    // Direct peer mutation: add_peer / remove_peer
    // -------------------------------------------------------------------

    #[test]
    fn add_peer_inserts_into_peers() {
        let mut raft = single_node_raft();
        assert_eq!(raft.peers_count(), 0);

        raft.add_peer(node("node-2"));

        assert!(raft.has_peer(&node("node-2")));
        assert_eq!(raft.peers_count(), 1);
    }

    #[test]
    fn add_peer_skips_self() {
        let mut raft = single_node_raft();
        raft.add_peer(node("node-1")); // node-1 is self
        assert!(!raft.has_peer(&node("node-1")));
        assert_eq!(raft.peers_count(), 0);
    }

    #[test]
    fn add_peer_leader_initializes_peer_state() {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
        });
        drain(&mut raft);
        assert!(raft.is_leader());

        raft.add_peer(node("node-2"));
        assert!(raft.has_peer(&node("node-2")));

        // Next heartbeat must include AppendEntries to node-2.
        raft.handle_timeout(RaftTimeoutCallback::HeartbeatTimeout {
            shard_group_id: TEST_SHARD,
        });
        let heartbeats = packets(&mut raft);
        let targets: Vec<&NodeId> = heartbeats.iter().map(|p| &p.target).collect();
        assert!(targets.contains(&&node("node-2")));
    }

    #[test]
    fn remove_peer_removes_from_peers() {
        let mut raft = three_node_raft("node-1");
        assert!(raft.has_peer(&node("node-3")));

        raft.remove_peer(&node("node-3"));

        assert!(!raft.has_peer(&node("node-3")));
        assert_eq!(raft.peers_count(), 1);
    }

    #[test]
    fn remove_peer_clears_peer_state_for_leader() {
        let mut n1 = three_node_raft("node-1");
        let mut n2 = three_node_raft("node-2");

        n1.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
        });
        let vote_reqs = packets(&mut n1);
        for pkt in &vote_reqs {
            if pkt.target == node("node-2") {
                n2.step(node("node-1"), pkt.rpc.clone());
            }
        }
        for pkt in packets(&mut n2) {
            n1.step(node("node-2"), pkt.rpc);
        }
        assert!(n1.is_leader());
        drain(&mut n1);

        n1.remove_peer(&node("node-3"));

        // After removal, next heartbeat must NOT target node-3.
        n1.handle_timeout(RaftTimeoutCallback::HeartbeatTimeout {
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
        use crate::clusters::metadata::command::{CreateTopic, MetadataCommand};
        use crate::clusters::metadata::strategy::{PartitionStrategy, StoragePolicy};

        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
        });
        drain(&mut raft);

        let cmd = RaftCommand::Metadata(MetadataCommand::CreateTopic(CreateTopic {
            name: "blue".to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
            replica_set: vec![node("node-1"), node("node-2"), node("node-3")],
            created_at: 1000,
        }));

        let result = raft.propose(cmd);
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
        });
        drain(&mut raft);

        // Win election
        raft.step(
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
        });
        drain(&mut raft);
        raft.step(
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
        raft.step(
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
        });
        let events = leader_events(&mut raft);
        assert_eq!(events[0].term, 1);

        // Step down and re-elect: term 2
        raft.step_down(1);
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: TEST_SHARD,
        });

        let events = leader_events(&mut raft);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].term, 2);
    }
}
