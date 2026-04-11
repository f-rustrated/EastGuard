#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::clusters::NodeId;
use crate::raft::log::MemLog;
use crate::raft::messages::*;
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
    pub addr: SocketAddr,
    peers: HashMap<NodeId, SocketAddr>,

    current_term: u64,
    voted_for: Option<NodeId>,
    log: MemLog,

    commit_index: u64,
    role: Role,

    // LEADER-ONLY volatile state
    peer_states: HashMap<NodeId, PeerState>,

    pending_outbound: Vec<OutboundRaftPacket>,
    pending_timer_commands: Vec<TimerCommand<RaftTimer>>,

    election_jitter: u32,
}

/// Fixed timer seq values. Raft only ever has one election timer and one
/// heartbeat timer active at a time, so we use well-known constants instead
/// of a rolling counter.
const ELECTION_TIMER_SEQ: u32 = 0;
const HEARTBEAT_TIMER_SEQ: u32 = 1;

impl Raft {
    pub(crate) fn new(
        node_id: NodeId,
        addr: SocketAddr,
        peers: HashMap<NodeId, SocketAddr>,
        election_jitter: u32,
    ) -> Self {
        let mut raft = Self {
            node_id,
            addr,
            peers,
            current_term: 0,
            voted_for: None,
            log: MemLog::default(),
            commit_index: 0,
            role: Role::Follower,
            peer_states: HashMap::new(),
            pending_outbound: Vec::new(),
            pending_timer_commands: Vec::new(),
            election_jitter,
        };
        raft.reset_election_timer();
        raft
    }

    pub(crate) fn take_outbound(&mut self) -> Vec<OutboundRaftPacket> {
        std::mem::take(&mut self.pending_outbound)
    }

    pub(crate) fn take_timer_commands(&mut self) -> Vec<TimerCommand<RaftTimer>> {
        std::mem::take(&mut self.pending_timer_commands)
    }

    /// Minimum number of nodes needed for a majority (strict majority).
    /// For N nodes: N/2 + 1. Examples: 3→2, 4→3, 5→3.
    fn quorum(&self) -> u32 {
        let total = self.peers.len() as u32 + 1; // +1 for self
        total / 2 + 1
    }

    // -------------------------------------------------------------------
    // Event handlers (called by actor)
    // -------------------------------------------------------------------

    pub fn handle_timeout(&mut self, event: RaftTimeoutCallback) {
        match event {
            RaftTimeoutCallback::ElectionTimeout => {
                self.start_election();
            }
            RaftTimeoutCallback::HeartbeatTimeout => {
                self.send_heartbeats();
            }
        }
    }

    pub fn step(&mut self, src: SocketAddr, rpc: RaftRpc) {
        match rpc {
            RaftRpc::RequestVote(req) => self.handle_request_vote(src, req),
            RaftRpc::RequestVoteResponse(resp) => self.handle_request_vote_response(resp),
            RaftRpc::AppendEntries(req) => self.handle_append_entries(src, req),
            RaftRpc::AppendEntriesResponse(resp) => self.handle_append_entries_response(src, resp),
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

        if self.peers.is_empty() {
            // Single-node cluster: elect self immediately.
            self.become_leader();
            return;
        }

        self.role = Role::Candidate { votes_received: 1 }; // vote for self
        self.reset_election_timer();

        for &peer_addr in self.peers.values() {
            self.pending_outbound.push(OutboundRaftPacket::new(
                peer_addr,
                RequestVote {
                    term: self.current_term,
                    candidate_id: self.node_id.clone(),
                    last_log_index: self.log.last_index(),
                    last_log_term: self.log.last_term(),
                },
            ));
        }
    }

    // ! SAFETY : Even without role guard, leaders/candidates step down if term is higher.
    // ! Followers grant or deny - All roles must respond
    fn handle_request_vote(&mut self, src: SocketAddr, req: RequestVote) {
        // If the request term is newer, step down.
        if req.term > self.current_term {
            self.step_down(req.term);
        }

        let vote_granted = req.term == self.current_term
            && self.vote_available_for(&req.candidate_id)
            && self.log_is_up_to_date(req.last_log_index, req.last_log_term);

        if vote_granted {
            self.voted_for = Some(req.candidate_id);
            self.reset_election_timer();
        }

        self.pending_outbound.push(OutboundRaftPacket::new(
            src,
            RequestVoteResponse {
                term: self.current_term,
                vote_granted,
            },
        ));
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
        let my_last_term = self.log.last_term();
        let my_last_index = self.log.last_index();

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

        // Initialize peer states per §5.3.
        let next = self.log.last_index() + 1;
        self.peer_states.clear();
        for peer_id in self.peers.keys() {
            self.peer_states.insert(
                peer_id.clone(),
                PeerState {
                    next_index: next,
                    match_index: 0,
                },
            );
        }

        // Cancel election timer, start heartbeat timer.
        self.cancel_all_timers();
        self.schedule_heartbeat_timer();

        // Send initial empty AppendEntries (heartbeat) to all peers.
        self.send_heartbeats();
    }

    fn step_down(&mut self, new_term: u64) {
        self.current_term = new_term;
        self.voted_for = None;
        self.role = Role::Follower;
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

        let peers: Vec<(NodeId, SocketAddr)> = self
            .peers
            .iter()
            .map(|(id, addr)| (id.clone(), *addr))
            .collect();

        for (peer_id, peer_addr) in peers {
            self.send_append_entries(peer_id, peer_addr);
        }

        self.schedule_heartbeat_timer();
    }

    fn send_append_entries(&mut self, peer_id: NodeId, peer_addr: SocketAddr) {
        let peer_state = match self.peer_states.get(&peer_id) {
            Some(ps) => ps,
            None => return,
        };

        let prev_log_index = peer_state.next_index.saturating_sub(1);
        let prev_log_term = self.log.term_at(prev_log_index);
        let entries = self.log.entries_from(peer_state.next_index).to_vec();

        self.pending_outbound.push(OutboundRaftPacket::new(
            peer_addr,
            AppendEntries {
                term: self.current_term,
                leader_id: self.node_id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        ));
    }

    // ! SAFETY:
    // ! - Leaders step down on higher term.
    // ! - Candidates step down on same term : because receiving entries while being a candidate means another node already won.
    // ! - Followers process normally. All roles must respond.
    fn handle_append_entries(&mut self, src: SocketAddr, req: AppendEntries) {
        // Stale term: reject.
        if req.term < self.current_term {
            self.send_append_entries_response(src, false);
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

        self.reset_election_timer();

        // Log consistency check: prev_log_index must exist with matching term.
        if req.prev_log_index > 0 {
            let local_term = self.log.term_at(req.prev_log_index);
            if local_term == 0 || local_term != req.prev_log_term {
                self.send_append_entries_response(src, false);
                return;
            }
        }

        // Append entries (truncate conflicting suffix first).
        for entry in &req.entries {
            let existing_term = self.log.term_at(entry.index);
            if existing_term != 0 && existing_term != entry.term {
                self.log.truncate_from(entry.index);
            }
            if entry.index > self.log.last_index() {
                self.log.append(entry.clone());
            }
        }

        // Advance commit index.
        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.log.last_index());
        }

        self.send_append_entries_response(src, true);
    }

    // ! SAFETY
    // ! - term guard
    // ! - only leaders track peer state
    fn handle_append_entries_response(&mut self, src: SocketAddr, resp: AppendEntriesResponse) {
        if resp.term > self.current_term {
            self.step_down(resp.term);
            return;
        }

        if self.role != Role::Leader {
            return;
        }

        let peer_id = match self.peers.iter().find(|&(_, addr)| *addr == src) {
            Some((id, _)) => id.clone(),
            None => return,
        };

        if let Some(peer_state) = self.peer_states.get_mut(&peer_id) {
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
                let peer_addr = self.peers[&peer_id];
                self.send_append_entries(peer_id, peer_addr);
            }
        }
    }

    fn send_append_entries_response(&mut self, target: SocketAddr, success: bool) {
        self.pending_outbound.push(OutboundRaftPacket::new(
            target,
            AppendEntriesResponse {
                term: self.current_term,
                success,
                last_log_index: self.log.last_index(),
            },
        ));
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
        let last = self.log.last_index();
        for n in (self.commit_index + 1)..=last {
            if self.log.term_at(n) != self.current_term {
                continue;
            }
            let replication_count = self
                .peer_states
                .values()
                .filter(|ps| ps.match_index >= n)
                .count() as u32
                + 1; // +1 for self

            if replication_count >= self.quorum() {
                self.commit_index = n;
            }
        }
    }

    /// Propose a command to the Raft log. Only the leader can accept proposals.
    /// In the DS-RSM context, the flow would be as follows:
    //
    // Client: "Create topic blue on shard #45"
    // -> Shard #45's leader.propose(CreateTopic("blue"))
    // -> Appended to leader's log
    // -> Replicated to shard #45's followers
    // -> Majority ack -> committed
    // -> Applied to CoordinatorStateMachine → topic blue exists
    pub fn propose(&mut self, command: RaftCommand) -> Result<(), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader);
        }

        let entry = LogEntry {
            term: self.current_term,
            index: self.log.last_index() + 1,
            command,
        };
        self.log.append(entry);

        // Immediately replicate to all peers.
        let peers: Vec<(NodeId, SocketAddr)> = self
            .peers
            .iter()
            .map(|(id, addr)| (id.clone(), *addr))
            .collect();
        for (peer_id, peer_addr) in peers {
            self.send_append_entries(peer_id, peer_addr);
        }

        Ok(())
    }

    fn reset_election_timer(&mut self) {
        self.pending_timer_commands
            .push(TimerCommand::CancelSchedule {
                seq: ELECTION_TIMER_SEQ,
            });
        self.pending_timer_commands.push(TimerCommand::SetSchedule {
            seq: ELECTION_TIMER_SEQ,
            timer: RaftTimer::election(self.election_jitter),
        });
    }

    fn schedule_heartbeat_timer(&mut self) {
        self.pending_timer_commands.push(TimerCommand::SetSchedule {
            seq: HEARTBEAT_TIMER_SEQ,
            timer: RaftTimer::heartbeat(),
        });
    }

    fn cancel_all_timers(&mut self) {
        self.pending_timer_commands
            .push(TimerCommand::CancelSchedule {
                seq: ELECTION_TIMER_SEQ,
            });
        self.pending_timer_commands
            .push(TimerCommand::CancelSchedule {
                seq: HEARTBEAT_TIMER_SEQ,
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    fn single_node_raft() -> Raft {
        Raft::new(NodeId::new("node-1"), addr(8001), HashMap::new(), 0)
    }

    fn three_node_raft(id: &str, port: u16) -> (Raft, HashMap<NodeId, SocketAddr>) {
        let mut all_nodes: HashMap<NodeId, SocketAddr> = HashMap::new();
        all_nodes.insert(NodeId::new("node-1"), addr(8001));
        all_nodes.insert(NodeId::new("node-2"), addr(8002));
        all_nodes.insert(NodeId::new("node-3"), addr(8003));

        let my_id = NodeId::new(id);
        let mut peers = all_nodes.clone();
        peers.remove(&my_id);

        let raft = Raft::new(my_id, addr(port), peers, 0);
        (raft, all_nodes)
    }

    // -------------------------------------------------------------------
    // Single-node election
    // -------------------------------------------------------------------

    #[test]
    fn single_node_elects_self_on_timeout() {
        let mut raft = single_node_raft();
        assert_eq!(raft.role, Role::Follower);

        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);

        assert_eq!(raft.role, Role::Leader);
        assert_eq!(raft.current_term, 1);
        assert_eq!(raft.voted_for, Some(NodeId::new("node-1")));
    }

    #[test]
    fn single_node_repeated_elections_increment_term() {
        let mut raft = single_node_raft();
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);
        assert_eq!(raft.current_term, 1);

        // Step down and trigger another election
        raft.step_down(1);
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);
        assert_eq!(raft.current_term, 2);
    }

    // -------------------------------------------------------------------
    // RequestVote
    // -------------------------------------------------------------------

    #[test]
    fn candidate_sends_request_vote_to_all_peers() {
        let (mut raft, _) = three_node_raft("node-1", 8001);

        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);

        assert!(matches!(raft.role, Role::Candidate { votes_received: 1 }));
        assert_eq!(raft.current_term, 1);

        let out = raft.take_outbound();
        assert_eq!(out.len(), 2); // one per peer
        for pkt in &out {
            assert!(matches!(pkt.rpc, RaftRpc::RequestVote(_)));
        }
    }

    #[test]
    fn follower_grants_vote_to_first_candidate() {
        let (mut raft, _) = three_node_raft("node-2", 8002);

        let req = RequestVote {
            term: 1,
            candidate_id: NodeId::new("node-1"),
            last_log_index: 0,
            last_log_term: 0,
        };
        raft.step(addr(8001), RaftRpc::RequestVote(req));

        let out = raft.take_outbound();
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
        let (mut raft, _) = three_node_raft("node-3", 8003);

        // Vote for node-1
        let req1 = RequestVote {
            term: 1,
            candidate_id: NodeId::new("node-1"),
            last_log_index: 0,
            last_log_term: 0,
        };
        raft.step(addr(8001), RaftRpc::RequestVote(req1));
        let _ = raft.take_outbound();

        // node-2 asks for vote in same term
        let req2 = RequestVote {
            term: 1,
            candidate_id: NodeId::new("node-2"),
            last_log_index: 0,
            last_log_term: 0,
        };
        raft.step(addr(8002), RaftRpc::RequestVote(req2));

        let out = raft.take_outbound();
        match &out[0].rpc {
            RaftRpc::RequestVoteResponse(resp) => {
                assert!(!resp.vote_granted);
            }
            _ => panic!("expected RequestVoteResponse"),
        }
    }

    #[test]
    fn candidate_becomes_leader_on_majority() {
        let (mut raft, _) = three_node_raft("node-1", 8001);
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);
        let _ = raft.take_outbound();

        // Receive vote from node-2 (now have 2 out of 3 = majority)
        let resp = RequestVoteResponse {
            term: 1,
            vote_granted: true,
        };
        raft.step(addr(8002), RaftRpc::RequestVoteResponse(resp));

        assert_eq!(raft.role, Role::Leader);
    }

    #[test]
    fn candidate_steps_down_on_higher_term() {
        let (mut raft, _) = three_node_raft("node-1", 8001);
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);
        let _ = raft.take_outbound();

        let resp = RequestVoteResponse {
            term: 5,
            vote_granted: false,
        };
        raft.step(addr(8002), RaftRpc::RequestVoteResponse(resp));

        assert_eq!(raft.role, Role::Follower);
        assert_eq!(raft.current_term, 5);
    }

    // -------------------------------------------------------------------
    // Log up-to-date check
    // -------------------------------------------------------------------

    #[test]
    fn rejects_vote_if_candidate_log_is_stale() {
        let (mut raft, _) = three_node_raft("node-2", 8002);
        // Give node-2 a log entry at term 2
        raft.log.append(LogEntry {
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
        raft.step(addr(8001), RaftRpc::RequestVote(req));

        let out = raft.take_outbound();
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
    fn leader_sends_heartbeats_on_timeout() {
        let (mut raft, _) = three_node_raft("node-1", 8001);
        // Become leader
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);
        let _ = raft.take_outbound();
        raft.step(
            addr(8002),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                vote_granted: true,
            }),
        );
        // Leader sends initial heartbeats upon election
        let initial = raft.take_outbound();
        assert_eq!(initial.len(), 2);

        // Heartbeat timeout triggers more
        raft.handle_timeout(RaftTimeoutCallback::HeartbeatTimeout);
        let out = raft.take_outbound();
        assert_eq!(out.len(), 2);
        for pkt in &out {
            match &pkt.rpc {
                RaftRpc::AppendEntries(ae) => {
                    assert_eq!(ae.term, 1);
                    assert!(ae.entries.is_empty()); // heartbeat
                }
                _ => panic!("expected AppendEntries"),
            }
        }
    }

    #[test]
    fn follower_accepts_append_entries() {
        let (mut raft, _) = three_node_raft("node-2", 8002);

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
        raft.step(addr(8001), RaftRpc::AppendEntries(ae));

        let out = raft.take_outbound();
        match &out[0].rpc {
            RaftRpc::AppendEntriesResponse(resp) => {
                assert!(resp.success);
                assert_eq!(resp.last_log_index, 1);
            }
            _ => panic!("expected AppendEntriesResponse"),
        }
        assert_eq!(raft.log.last_index(), 1);
    }

    #[test]
    fn follower_rejects_append_entries_with_stale_term() {
        let (mut raft, _) = three_node_raft("node-2", 8002);
        raft.current_term = 5;

        let ae = AppendEntries {
            term: 3,
            leader_id: NodeId::new("node-1"),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        raft.step(addr(8001), RaftRpc::AppendEntries(ae));

        let out = raft.take_outbound();
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
        let (mut raft, _) = three_node_raft("node-2", 8002);

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
        raft.step(addr(8001), RaftRpc::AppendEntries(ae));

        let out = raft.take_outbound();
        match &out[0].rpc {
            RaftRpc::AppendEntriesResponse(resp) => {
                assert!(!resp.success);
            }
            _ => panic!("expected AppendEntriesResponse"),
        }
    }

    #[test]
    fn leader_advances_commit_on_majority_replication() {
        let (mut raft, _) = three_node_raft("node-1", 8001);

        // Become leader
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);
        let _ = raft.take_outbound();
        raft.step(
            addr(8002),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                vote_granted: true,
            }),
        );
        let _ = raft.take_outbound();

        // Propose a command
        raft.propose(RaftCommand::Noop).unwrap();
        let _ = raft.take_outbound();
        assert_eq!(raft.log.last_index(), 1);
        assert_eq!(raft.commit_index, 0);

        // node-2 acknowledges
        let resp = AppendEntriesResponse {
            term: 1,
            success: true,
            last_log_index: 1,
        };
        raft.step(addr(8002), RaftRpc::AppendEntriesResponse(resp));

        // Majority achieved (self + node-2 = 2 out of 3)
        assert_eq!(raft.commit_index, 1);
    }

    #[test]
    fn follower_cannot_propose() {
        let (mut raft, _) = three_node_raft("node-1", 8001);
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
        let (mut raft, _) = three_node_raft("node-2", 8002);

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
        raft.step(addr(8001), RaftRpc::AppendEntries(ae));
        let _ = raft.take_outbound();

        assert_eq!(raft.commit_index, 1);
    }

    // -------------------------------------------------------------------
    // Leader retries on rejection
    // -------------------------------------------------------------------

    #[test]
    fn leader_decrements_next_index_on_rejection() {
        let (mut raft, _) = three_node_raft("node-1", 8001);

        // Become leader
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);
        let _ = raft.take_outbound();
        raft.step(
            addr(8002),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                vote_granted: true,
            }),
        );
        let _ = raft.take_outbound();

        // Add some entries
        raft.propose(RaftCommand::Noop).unwrap();
        let _ = raft.take_outbound();

        // node-2 rejects (log mismatch)
        let resp = AppendEntriesResponse {
            term: 1,
            success: false,
            last_log_index: 0,
        };
        raft.step(addr(8002), RaftRpc::AppendEntriesResponse(resp));

        // Should have retried with decremented next_index
        let out = raft.take_outbound();
        assert!(!out.is_empty());
        assert!(matches!(out[0].rpc, RaftRpc::AppendEntries(_)));
    }

    // -------------------------------------------------------------------
    // Step down
    // -------------------------------------------------------------------

    #[test]
    fn leader_steps_down_on_higher_term_append_entries() {
        let (mut raft, _) = three_node_raft("node-1", 8001);

        // Become leader at term 1
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);
        let _ = raft.take_outbound();
        raft.step(
            addr(8002),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                vote_granted: true,
            }),
        );
        let _ = raft.take_outbound();
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
        raft.step(addr(8003), RaftRpc::AppendEntries(ae));

        assert_eq!(raft.role, Role::Follower);
        assert_eq!(raft.current_term, 3);
    }

    // -------------------------------------------------------------------
    // Timeout role safety
    // -------------------------------------------------------------------

    #[test]
    fn leader_ignores_election_timeout() {
        let (mut raft, _) = three_node_raft("node-1", 8001);

        // Become leader at term 1
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);
        let _ = raft.take_outbound();
        raft.step(
            addr(8002),
            RaftRpc::RequestVoteResponse(RequestVoteResponse {
                term: 1,
                vote_granted: true,
            }),
        );
        let _ = raft.take_outbound();
        assert_eq!(raft.role, Role::Leader);
        assert_eq!(raft.current_term, 1);

        // Stale election timeout arrives — should be ignored
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);

        assert_eq!(
            raft.role,
            Role::Leader,
            "leader must not start a new election"
        );
        assert_eq!(raft.current_term, 1, "term must not increment");
    }

    #[test]
    fn follower_ignores_heartbeat_timeout() {
        let (mut raft, _) = three_node_raft("node-1", 8001);
        assert_eq!(raft.role, Role::Follower);

        raft.handle_timeout(RaftTimeoutCallback::HeartbeatTimeout);

        let out = raft.take_outbound();
        assert!(out.is_empty(), "follower must not send heartbeats");
    }

    #[test]
    fn candidate_ignores_heartbeat_timeout() {
        let (mut raft, _) = three_node_raft("node-1", 8001);
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout);
        let _ = raft.take_outbound();
        assert!(matches!(raft.role, Role::Candidate { .. }));

        raft.handle_timeout(RaftTimeoutCallback::HeartbeatTimeout);

        let out = raft.take_outbound();
        assert!(out.is_empty(), "candidate must not send heartbeats");
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
            let peers = (0..peer_count)
                .map(|i| (NodeId::new(format!("peer-{i}")), addr(9000 + i as u16)))
                .collect();
            let raft = Raft::new(NodeId::new("self"), addr(8000), peers, 0);

            assert_eq!(
                raft.quorum(),
                want,
                "quorum for {total} nodes should be {want}"
            );
        }
    }
}
