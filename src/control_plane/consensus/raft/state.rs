#![allow(dead_code)]

use crate::control_plane::consensus::messages::*;
use crate::control_plane::consensus::raft::catch_up::CatchUpRepairs;
use crate::control_plane::consensus::raft::command::RaftCommand;
use crate::control_plane::consensus::raft::errors::{EvictionError, ProposalError};
use crate::control_plane::consensus::raft::log::LogEntry;
use crate::control_plane::consensus::raft::storage::RaftPersistentState;
use crate::control_plane::consensus::raft::{compute_replacement_replica_set, now_ms};
use crate::control_plane::membership::{ShardGroupId, TopologyReader};
use crate::control_plane::metadata::command::DeleteSegments;
use crate::control_plane::metadata::event::ApplyResult;
use crate::control_plane::metadata::state_machine::MetadataStateMachine;
use crate::control_plane::metadata::{
    ConsumerGroupAssignment, ConsumerMemberId, EntryId, MetadataCommand, RangeId, ReassignSegment,
    RollSegment, SegmentId, TopicId, TopicMeta, TopicStats,
};
use crate::control_plane::{NodeId, Replicas};
use crate::data_plane::SegmentKey;
use crate::data_plane::messages::command::{CatchUpAck, SegmentAssignment, SegmentAssignmentAck};
use crate::data_plane::transport::command::DataTransportCommand;
use crate::schedulers::ticker_message::TimerCommand;
#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};

const ELECTION_JITTER_RANGE: u32 = 20;

/// #135: The number of consecutive, identical ring observations needed
/// before we can safely evict a live ex-owner.
///
/// Observations happen during routine RingChecks (~every 30s) and during
/// leadership changes. Requiring 3 observations guarantees about a minute
/// of ring stability. This buffer ensures we don't accidentally evict a
/// legitimate owner just because our local snapshot was briefly incorrect
/// during a rebalance.
pub(crate) const RING_STABLE_OBSERVATIONS: u32 = 3;

/// Segments whose write leader crashed (sole death),
pub(crate) type LeaderlessSegments = Vec<(SegmentKey, Vec<NodeId>)>;

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

    // -- LOG STATE
    current_term: u64,
    voted_for: Option<NodeId>,
    log: Vec<LogEntry>,
    stabled_index: u64, // flushed to disk

    // -- APPLICATION STATE
    state_machine: MetadataStateMachine,
    last_applied_index: u64, // applied to state machine

    // Transient State (Volatile Consensus & Coordinator Metadata)
    commit_index: u64, // majority voted
    role: Role,
    /// Tracks who the current leader is — set when this node becomes leader
    /// or when a valid `AppendEntries` is received from a leader.
    current_leader: Option<NodeId>,
    peers: HashSet<NodeId>,
    // LEADER-ONLY volatile state
    peer_states: HashMap<NodeId, PeerState>,
    /// LEADER-ONLY: non-voting members the leader is catching up before promotion.
    /// Replicated to (like peers) but **excluded from the commit quorum** — a node
    /// that can't yet participate (e.g. a freshly ring-assigned host without a local
    /// instance) is staged here and never counted, so it can't freeze the group. It is
    /// promoted to a voting peer via a committed `AddPeer` only once caught up
    /// (`maybe_promote_learner`). Re-derived on takeover (leader-volatile). Disjoint
    /// from `peers`; empty on followers.
    learner_states: HashMap<NodeId, PeerState>,
    /// Election-timer generation. Bumped whenever the election timer is
    /// (re)armed or cancelled; a fired `ElectionTimeout` carrying an older
    /// epoch raced its own cancellation in flight and must be ignored
    election_epoch: u64,
    election_jitter: ElectionJitter,
    /// Segments whose data-leader has acked its
    /// `SegmentAssignment`, mapped to the acking node. The heartbeat sweep skips
    /// re-driving a segment whose confirmed node still matches `replica_set[0]`
    confirmed_assignment: HashMap<SegmentKey, NodeId>,
    /// In-flight sealed-segment repairs. Seeded at `ReassignSegment` apply; the
    /// heartbeat sweep re-drives until acked. Leader-volatile — cleared on step-down.
    catch_up: CatchUpRepairs,
    pending_proposals: Vec<MetadataCommand>,
    leaderless_segments: LeaderlessSegments,
    /// Tracks the ring membership seen during the last check, along with a count
    /// of how many times we've seen this exact membership in a row (#135).
    ///
    /// This state is "leader-volatile," meaning it gets wiped clean whenever a
    /// new leader takes over. This guarantees that a new leader must observe a
    /// stable ring over time to "re-earn confidence" before it is allowed to
    /// evict anyone.
    ring_observation_streak: Option<(BTreeSet<NodeId>, u32)>,

    pending_log_mutations: Vec<LogMutation>, // must persist
    events: Vec<RaftEvent>,                  // volatile side effects

    timer_seqs: TimerSeqs,
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
            state_machine: MetadataStateMachine::new(shard_group_id),
            pending_proposals: Vec::new(),
            leaderless_segments: Vec::new(),
            peer_states: HashMap::new(),
            learner_states: HashMap::new(),
            election_jitter: ElectionJitter::new(election_jitter_seed),
            timer_seqs,
            election_epoch: 0,
            confirmed_assignment: HashMap::new(),
            catch_up: CatchUpRepairs::default(),
            ring_observation_streak: None,
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

    pub(crate) fn get_consumer_group_assignment(
        &self,
        topic_name: &str,
        group_id: &str,
        member_id: ConsumerMemberId,
    ) -> Option<ConsumerGroupAssignment> {
        self.state_machine
            .get_consumer_group_assignment(topic_name, group_id, member_id)
    }

    pub(crate) fn active_segments_for_node(
        &self,
        node_id: &NodeId,
    ) -> Box<[(SegmentKey, Replicas)]> {
        self.state_machine.active_segments_for_node(node_id)
    }

    /// Every active segment's assignment tuple `(key, replica_set, start_offset)`.
    /// The leader's confirmation-gated assignment sweep (`MultiRaft::build_redrive_cmds`)
    /// turns these into `SegmentAssignment` re-drives for unconfirmed segments.
    pub(crate) fn active_segment_assignments(&self) -> Box<[(SegmentKey, Replicas, EntryId)]> {
        self.state_machine.active_segment_assignments()
    }

    /// Full reconciliation against the current topology: assert the ring-assigned
    /// peers, replace dead peers, repair segments, evict stale live ex-owners
    /// (#135), and log ring drift. Returns whether anything was proposed (the
    /// caller marks the group dirty);
    ///
    /// leaderless segments are stashed for the caller to drain (`take_leaderless_segments`) and drive.
    ///
    /// Run on takeover and the periodic ring check.
    ///
    /// `target_members` is the ring members to assert as peers via `AddPeer` (only the live, non-self ones are proposed).
    ///
    /// The caller picks the shape:
    /// - **takeover** passes the group's *full* member set — re-assert everything
    ///   to heal genesis divergence (#133/#134);
    /// - the **ring check** passes only members *not already peers* — just the
    ///   delta, so re-asserting present members doesn't spam the log;
    /// - **`None`** skips the add step (the group is no longer in the ring
    ///   snapshot); the rest of the reconcile still runs.
    pub(crate) fn reconcile(
        &mut self,
        topology: &TopologyReader,
        // Ring members to assert as peers (see doc above); `None` skips the add step.
        target_members: Option<Box<[NodeId]>>,
    ) -> bool {
        let mut changed = false;
        let live_set: HashSet<NodeId> = topology.live_nodes().into_iter().collect();
        if let Some(members) = target_members {
            for member in members.iter() {
                // skip self && dead peer
                if *member == self.node_id || !live_set.contains(member) {
                    continue;
                }
                // Stage the ring member as a non-voting learner; it's promoted to a
                // voter once caught up. Never added straight to the quorum — an
                // un-participating ring member would otherwise freeze commits.
                if self.stage_learner(member.clone()) {
                    changed = true;
                }
            }
        }
        changed |= self.reconcile_peers(topology, &live_set);
        changed |= self.reconcile_segments(&live_set);
        changed |= self.refill_under_replicated_segments(topology);
        changed |= self.reconcile_retention_deletes();

        // Record a ring observation every run, whether routine RingCheck or
        // leadership change. During a leadership change, evictions are paused
        // until future RingChecks — the system waits for a fresh stability window
        // and for the new membership to commit. That delay gives "add-before-
        // remove" for free, with no special logic.
        changed |= self.reconcile_stale_live_peers(topology, &live_set).is_ok();
        self.log_ring_drift(topology, &live_set);
        changed
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

    /// Returns `true` if there is a pending membership change (`AddPeer` or
    /// `RemovePeer`) that has been appended to the log but not yet committed.
    ///
    /// We only allow one configuration change in flight at a time per group.
    /// Because of this rule, stale-peer removals must wait in line. This
    /// naturally enforces our "add-before-remove" ordering: if an `AddPeer`
    /// is proposed during a check, any `RemovePeer` proposed later in that
    /// same check is delayed until the add successfully commits.
    fn has_uncommitted_membership_change(&self) -> bool {
        (self.commit_index + 1..=self.log_last_index()).any(|i| {
            matches!(
                self.log_get(i).map(|e| &e.command),
                Some(RaftCommand::AddPeer(_) | RaftCommand::RemovePeer(_))
            )
        })
    }

    /// Safely evicts up to one *live* ex-owner per pass (Issue #135).
    ///
    /// This function only targets nodes that are alive, currently
    /// acting as voters, but no longer belong to the ring. Dead peers are
    /// handled separately by `reconcile_peers`. These two functions can run
    /// in any order without conflict.
    ///
    /// To prevent accidental data loss or instability, all of the following
    /// safety gates must pass before an eviction occurs:
    /// 1. The current node is the leader, and the group still exists in the ring.
    /// 2. The ring membership has been stable for `RING_STABLE_OBSERVATIONS` checks.
    /// 3. The leader is still a member of the ring (if the leader is the ex-owner, it should transfer leadership, not evict itself).
    /// 4. There are no uncommitted configuration changes in flight.
    /// 5. All current ring members are alive, are voters, and have caught up on the log. This ensures we don't drop below our replication factor or strand committed data.
    /// 6. We only remove one peer per pass, picking the lowest `NodeId` for consistency.
    ///
    /// Note on `AddPeer`: Unlike replacing a dead node, this removal is not
    /// paired with an `AddPeer`. Because gate #5 ensures all current members
    /// are active voters, this eviction just safely shrinks the group back
    /// to its standard replication factor.
    #[tracing::instrument(level = "trace", skip_all)]
    pub(crate) fn reconcile_stale_live_peers(
        &mut self,
        topology_reader: &TopologyReader,
        live: &HashSet<NodeId>,
    ) -> Result<(), EvictionError> {
        if self.role != Role::Leader {
            return Err(EvictionError::NotLeader);
        }

        let Some(ring_members) = topology_reader.group_ring_members(self.shard_group_id) else {
            self.ring_observation_streak = None;
            return Err(EvictionError::GroupNotFound);
        };
        let ring: BTreeSet<NodeId> = ring_members.iter().cloned().collect();

        // Gate 2: consecutive identical observations.
        let observations = self.record_ring_observation(&ring);
        if observations < RING_STABLE_OBSERVATIONS {
            return Err(EvictionError::WaitingForStability);
        }

        // Gate 3: Check leader validity BEFORE doing any work/allocations
        if !ring.contains(&self.node_id) {
            return Err(EvictionError::LeaderNotInRing);
        }

        let victim = self
            .peers
            .iter()
            .filter(|p| live.contains(*p) && !ring.contains(*p))
            .min()
            .cloned();

        let Some(victim) = victim else {
            return Err(EvictionError::NoStalePeers);
        };

        // Gate 4: Raft only allows one membership change at a time.
        if self.has_uncommitted_membership_change() {
            return Err(EvictionError::ConfigChangeInFlight);
        }

        // Gate 5. Before the leader removes an ex-owner, it must verify that every legitimate member currently in the ring is online,
        // fully integrated, and has a complete copy of all committed data.
        for member in &ring {
            if *member == self.node_id {
                continue;
            }
            if !live.contains(member) || !self.peers.contains(member) {
                return Err(EvictionError::RingMemberNotReady);
            }

            // It checks if a specific node has a complete, up-to-date copy of all permanently saved data.
            let in_sync = self
                .peer_states
                .get(member)
                .is_some_and(|ps| ps.match_index >= self.commit_index);
            if !in_sync {
                return Err(EvictionError::FollowersLagging);
            }
        }

        // Gate 6. Propose the removal of the single victim.

        match self.propose(RaftCommand::RemovePeer(victim.clone())) {
            Ok(_) => {
                tracing::info!(
                    node = %self.node_id,
                    group = self.shard_group_id.0,
                    victim = %victim,
                    "evicting live ex-owner after ring stability window",
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(?victim, error = ?e, "Stale-peer RemovePeer rejected");
                Err(EvictionError::ProposalRejected)
            }
        }
    }

    #[inline(always)]
    fn record_ring_observation(&mut self, ring: &BTreeSet<NodeId>) -> u32 {
        let observations = match self.ring_observation_streak.take() {
            Some((prev, n)) if prev == *ring => n.saturating_add(1),
            _ => 1,
        };
        self.ring_observation_streak = Some((ring.clone(), observations));
        observations
    }

    /// Repair this group's segments whose replica set still names a dead node:
    ///   - active, one replica died → stash for seal-end recovery (rolled later)
    ///   - active, multiple replicas died → roll now with an unknown end (`RollSegment`)
    ///   - sealed (known end) → swap the replica set (`ReassignSegment`); catch-up refills
    ///
    /// Runs per-death (`handle_node_death`) and as the takeover backfill sweep.
    pub(crate) fn reconcile_segments(&mut self, live_set: &HashSet<NodeId>) -> bool {
        let mut to_roll: Vec<(SegmentKey, Replicas)> = Vec::new();
        for (key, rs) in self.active_segments_with_dead_members(live_set).into_vec() {
            if Self::only_replica_dead(&rs, live_set) {
                let survivors = rs
                    .iter()
                    .filter(|n| live_set.contains(*n))
                    .cloned()
                    .collect();
                self.leaderless_segments.push((key, survivors));
            } else {
                to_roll.push((key, rs));
            }
        }

        let mut leaderless_segments = vec![];
        for (key, rs) in self.boundary_unknown_segments() {
            let survivors = rs
                .iter()
                .filter(|n| live_set.contains(*n))
                .cloned()
                .collect();
            leaderless_segments.push((key, survivors));
        }
        self.leaderless_segments.extend(leaderless_segments);

        let sealed = self.sealed_segments_with_dead_members(live_set);
        let mut changed = false;

        // Roll the others: seal + reopen with a healthy set. `end_entry_id = None`
        // — a follower-death/takeover roll doesn't know the committed offset.
        changed |= self.repair_segments(
            to_roll.into_boxed_slice(),
            live_set,
            |segment_key, new_replica_set| {
                RollSegment {
                    segment_key,
                    sealed_at: now_ms(),
                    new_replica_set,
                    end_entry_id: None,
                }
                .into()
            },
        );

        // Sealed (known-end): swap the replica set in place; catch-up re-replicates
        // to the new node. Boundary-unknown seals are excluded by the scan — see
        // `docs/data-plane/leader_crash_seal_boundary.md`.
        changed |= self.repair_segments(sealed, live_set, |segment_key, new_replica_set| {
            ReassignSegment {
                segment_key,
                replica_set: new_replica_set,
            }
            .into()
        });
        changed
    }

    fn repair_segments<F>(
        &mut self,
        segments: Box<[(SegmentKey, Replicas)]>,
        live_set: &HashSet<NodeId>,
        build_cmd: F,
    ) -> bool
    where
        F: Fn(SegmentKey, Replicas) -> MetadataCommand,
    {
        let live_nodes: Vec<NodeId> = live_set.iter().cloned().collect();
        let mut changed = false;
        for (segment_key, curr_rs) in segments {
            let dead_in_set: Vec<NodeId> = curr_rs
                .iter()
                .filter(|n| !live_set.contains(*n))
                .cloned()
                .collect();
            let new_replica_set =
                compute_replacement_replica_set(&curr_rs, &dead_in_set, &live_nodes);
            match self.propose(build_cmd(segment_key, new_replica_set).into()) {
                Ok(_) => changed = true,
                Err(e) => tracing::warn!(
                    "segment repair for {:?} on {:?} rejected: {:?}",
                    segment_key,
                    self.shard_group_id,
                    e
                ),
            }
        }
        changed
    }

    /// Re-fill known-end sealed segments left under-replicated by an earlier death
    fn refill_under_replicated_segments(&mut self, topology: &TopologyReader) -> bool {
        let targets: Box<[(SegmentKey, Replicas)]> = self
            .state_machine
            .topics
            .values()
            .flat_map(|t| t.under_replicated_sealed_segments(topology.replication_factor()))
            .collect();

        let mut changed = false;
        for (segment_key, mut replica_set) in targets {
            let excluded: HashSet<NodeId> = replica_set.iter().cloned().collect();
            let need = topology.replication_factor() - replica_set.len(); // > 0 by the query's `len < rf` filter
            let additions = topology.ring_replacements_for(self.shard_group_id, &excluded, need);
            if additions.is_empty() {
                continue; // ring can't grow it yet — retried on the next ring check
            }

            replica_set.extend(&additions);

            let cmd = ReassignSegment {
                segment_key,
                replica_set,
            };

            if let Err(e) = self.propose(cmd.into()) {
                tracing::warn!(
                    "under-replication re-fill for {:?} on {:?} rejected: {:?}",
                    segment_key,
                    self.shard_group_id,
                    e
                );
                continue;
            }
            changed = true;
        }
        changed
    }

    /// Retention sweep (D7): leader-only, on the ring-check cadence. For each owned
    /// range compute the oldest-first prefix of sealed segments expired under the
    /// topic's `retention_ms` (against the wall clock, the one age-based decision —
    /// leader-only so cross-node skew can't diverge replicas), and propose
    /// `DeleteSegments`. Topics with no retention policy contribute nothing.
    fn reconcile_retention_deletes(&mut self) -> bool {
        let now = now_ms();
        let targets: Vec<(TopicId, RangeId, Box<[SegmentId]>)> = self
            .state_machine
            .topics
            .values()
            .flat_map(|t| {
                let topic_id = t.id;
                t.expired_segment_prefixes(now)
                    .into_iter()
                    .map(move |(range_id, ids)| (topic_id, range_id, ids))
            })
            .collect();

        let mut changed = false;
        for (topic_id, range_id, segment_ids) in targets {
            let cmd = DeleteSegments {
                topic_id,
                range_id,
                segment_ids,
            };
            if let Err(e) = self.propose(cmd.into()) {
                tracing::warn!(
                    "retention delete for {:?}/{:?} on {:?} rejected: {:?}",
                    topic_id,
                    range_id,
                    self.shard_group_id,
                    e
                );
                continue;
            }
            changed = true;
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

        let segments_changed = self.reconcile_segments(live_set);
        changed || segments_changed
    }

    pub(crate) fn dead_nodes(&mut self, live: &HashSet<NodeId>) -> impl Iterator<Item = &NodeId> {
        self.peers_iter().filter(|p| !live.contains(*p))
    }

    /// Active segments across all topics whose `replica_set` contains at
    /// least one non-live member.
    pub(crate) fn active_segments_with_dead_members(
        &self,
        live: &HashSet<NodeId>,
    ) -> Box<[(SegmentKey, Replicas)]> {
        self.state_machine
            .topics
            .values()
            .flat_map(|t| t.active_segments_with_dead_members(live))
            .collect()
    }

    /// Sealed (known-end) segments across all topics whose `replica_set` names a
    /// non-live member — D5 sealed-segment repair candidates.
    pub(crate) fn sealed_segments_with_dead_members(
        &self,
        live: &HashSet<NodeId>,
    ) -> Box<[(SegmentKey, Replicas)]> {
        self.state_machine
            .topics
            .values()
            .flat_map(|t| t.sealed_segments_with_dead_members(live))
            .collect()
    }

    pub(crate) fn boundary_unknown_segments(
        &self,
    ) -> impl Iterator<Item = (SegmentKey, Vec<NodeId>)> {
        self.state_machine
            .topics
            .values()
            .flat_map(|t| t.boundary_unknown_segments())
    }

    /// A single missing replica is recoverable: the remaining replicas can
    /// report the all-ack committed minimum before the successor is opened.
    fn only_replica_dead(replica_set: &[NodeId], live: &HashSet<NodeId>) -> bool {
        replica_set.iter().filter(|n| !live.contains(*n)).count() == 1
    }

    pub(crate) fn has_topic(&self, topic_id: &TopicId) -> bool {
        self.state_machine.get_topic(topic_id).is_some()
    }

    pub(crate) fn get_replica_set(&self, key: &SegmentKey) -> Option<Replicas> {
        let topic = self.state_machine.get_topic(&key.topic_id)?;
        let range = topic.ranges.get(&key.range_id)?;
        let seg = range.segments.get(&key.segment_id)?;
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

    /// Drain the leaderless segments found by `reconcile_segments` — the actor
    /// drives seal-end recovery (poll survivors, seal at the recovered end).
    pub(crate) fn take_leaderless_segments(&mut self) -> Vec<(SegmentKey, Vec<NodeId>)> {
        std::mem::take(&mut self.leaderless_segments)
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

    /// A node the leader is catching up as a non-voting learner (not yet a voter).
    #[cfg(test)]
    pub(crate) fn is_learner(&self, node_id: &NodeId) -> bool {
        self.learner_states.contains_key(node_id)
    }

    #[cfg(test)]
    pub(crate) fn learner_count(&self) -> usize {
        self.learner_states.len()
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
        // Learners are leader-volatile catch-up state; reconcile re-stages them.
        self.learner_states.clear();
        // #135: ring confidence is leader-volatile — re-earn it.
        self.ring_observation_streak = None;
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
        // #135: a deposed leader's ring observations die with its term.
        self.ring_observation_streak = None;
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
        self.learner_states.clear();
        self.confirmed_assignment.clear();
        self.catch_up.clear();
    }

    // -------------------------------------------------------------------
    // Log replication
    // -------------------------------------------------------------------

    fn send_heartbeats(&mut self) {
        if self.role != Role::Leader {
            return;
        }

        let peers: Vec<NodeId> = self.replication_targets();

        for peer_id in peers {
            self.send_append_entries(peer_id);
        }

        self.schedule_rpc_timer();
        self.maybe_redrive_segment_assignments();
        self.maybe_redrive_catch_ups();
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

    /// Re-drive the catch-up sweep — the sealed-segment analogue of
    /// `maybe_redrive_segment_assignments`. See `.claude/rules/raft-actor.md` #9.
    fn maybe_redrive_catch_ups(&mut self) {
        let redrives = self.catch_up.redrives(self.shard_group_id);
        if !redrives.is_empty() {
            self.events.push(RaftEvent::RedriveAssignments(redrives));
        }
    }

    /// A member confirmed it holds a reassigned sealed segment; routed here from
    /// `MultiRaft`.
    pub(crate) fn handle_catch_up_ack(&mut self, ack: CatchUpAck) {
        self.catch_up.confirm(ack.segment_key, ack.from);
    }

    /// Takeover backstop: re-seed catch-up for every known-end sealed segment this
    /// node leads. The tracker is leader-volatile (empty after takeover), so a
    /// repair in flight when leadership changed would otherwise be stranded; the
    /// heartbeat sweep re-drives the re-seeded set (already-complete members
    /// full-match-ack cheaply). See `.claude/rules/raft-actor.md` #9.
    pub(crate) fn reseed_catch_up(&mut self) {
        let sealed: Vec<_> = self
            .state_machine
            .topics
            .values()
            .flat_map(|t| t.known_end_sealed_segments())
            .collect();
        for (segment_key, start, end, replica_set) in sealed {
            self.catch_up
                .track_sealed(segment_key, start, end, replica_set);
        }
    }

    /// The periodic shard-leader re-announce for the ring-check backstop (#135
    /// class) — `None` unless this node leads the group. Announce-only: the actor
    /// forwards it to SWIM, never reconcile.
    pub(crate) fn shard_leader_refresh(&self) -> Option<RaftEvent> {
        self.is_leader().then(|| {
            RaftEvent::ShardLeaderRefresh(LeaderChange {
                shard_group_id: self.shard_group_id,
                leader_node_id: self.node_id.clone(),
                term: self.current_term,
            })
        })
    }

    /// Everyone the leader replicates to: voting peers plus catching-up learners.
    fn replication_targets(&self) -> Vec<NodeId> {
        self.peer_states
            .keys()
            .chain(self.learner_states.keys())
            .cloned()
            .collect()
    }

    /// Stage a node as a non-voting learner the leader catches up before promoting it
    /// to a voter. No-op if it's self, already a voter, or already a learner. Starts
    /// catch-up immediately. Excluded from the commit quorum until promoted, so a node
    /// that never participates (e.g. a freshly ring-assigned host with no local group
    /// instance yet) can never freeze the group. Leader-only.
    pub(crate) fn stage_learner(&mut self, node: NodeId) -> bool {
        if self.role != Role::Leader
            || node == self.node_id
            || self.peers.contains(&node)
            || self.learner_states.contains_key(&node)
        {
            return false;
        }
        self.learner_states.insert(
            node.clone(),
            PeerState {
                next_index: self.log_last_index() + 1,
                match_index: 0,
            },
        );
        self.send_append_entries(node);
        true
    }

    /// Once a learner has replicated every committed entry, promote it to a voting peer
    /// via a committed `AddPeer` (apply moves it out of `learner_states`). Gated by the
    /// one-conf-change-at-a-time rule. A learner that never catches up (match stays 0)
    /// never reaches the bar, so it stays a non-voter and never blocks quorum.
    fn maybe_promote_learner(&mut self, node: &NodeId) {
        if self.has_uncommitted_membership_change() {
            return;
        }
        let Some(ls) = self.learner_states.get(node) else {
            return;
        };
        if self.commit_index > 0 && ls.match_index >= self.commit_index {
            let _ = self.propose(RaftCommand::AddPeer(node.clone()));
        }
    }

    fn send_append_entries(&mut self, peer_id: NodeId) {
        let peer_state = match self
            .peer_states
            .get(&peer_id)
            .or_else(|| self.learner_states.get(&peer_id))
        {
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
            self.learner_states.clear();
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

        // The responder may be a voting peer or a catching-up learner.
        let node_id = resp.node_id.clone();
        let is_voter = self.peer_states.contains_key(&node_id);
        let peer_state = if is_voter {
            self.peer_states.get_mut(&node_id)
        } else {
            self.learner_states.get_mut(&node_id)
        };
        if let Some(peer_state) = peer_state {
            if resp.success {
                peer_state.match_index = resp.last_log_index;
                peer_state.next_index = resp.last_log_index + 1;
                self.try_advance_commit_index();
                // A caught-up learner graduates to a voting peer.
                if !is_voter {
                    self.maybe_promote_learner(&node_id);
                }
            } else {
                // Decrement next_index and retry.
                peer_state.next_index = peer_state.next_index.saturating_sub(1).max(1);
                self.send_append_entries(node_id);
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

    #[tracing::instrument(level = "debug", skip_all, fields(
        group = self.shard_group_id.0,
        from = self.last_applied_index + 1,
        to = self.commit_index.min(self.stabled_index),
    ))]
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

            // ? At this point if apply failed when things are already committed, what'd that mean?
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

                if self.is_leader()
                    && let ApplyResult::SegmentReassigned(r) = &result
                {
                    self.catch_up.track(r);
                }
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
            self.pending_proposals
                .extend(self.state_machine.take_pending_proposals());
        }
    }

    /// Apply-only helper. Invoked from `apply_committed_entries()` when an
    /// `AddPeer` log entry commits. Never call directly — the peer set is part
    /// of the replicated state machine and must only mutate through the log.
    fn apply_add_peer(&mut self, node_id: NodeId) {
        if node_id == self.node_id {
            return;
        }
        // Promotion: a learner graduating to a voter carries its catch-up progress, so
        // the new voter isn't reset to match_index 0 (which would stall commits anew).
        let carried = self.learner_states.remove(&node_id);
        if self.peers.insert(node_id.clone()) && self.role == Role::Leader {
            let state = carried.unwrap_or(PeerState {
                next_index: self.log_last_index() + 1,
                match_index: 0,
            });
            self.peer_states.insert(node_id, state);
        }
    }

    /// Apply-only helper. Invoked from `apply_committed_entries()` when a
    /// `RemovePeer` log entry commits. Never call directly — the peer set is part
    /// of the replicated state machine and must only mutate through the log.
    fn apply_remove_peer(&mut self, node_id: NodeId) {
        self.learner_states.remove(&node_id);
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
    #[tracing::instrument(level = "trace", skip_all, fields(group = self.shard_group_id.0, command = ?command))]
    pub fn propose(&mut self, command: RaftCommand) -> Result<u64, ProposalError> {
        if self.role != Role::Leader {
            return Err(ProposalError::NotLeader(self.current_leader.clone()));
        }

        self.add_new_entry(command);
        let index = self.log_last_index();

        // Immediately replicate to all peers.
        let peers: Vec<NodeId> = self.replication_targets();
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

    /// Replace a dead peer: propose a committed `RemovePeer(remove)` (a single-server
    /// change the live majority can always commit), then **stage** the replacement as a
    /// non-voting learner rather than adding it straight to the quorum. The learner is
    /// promoted to a voter via a committed `AddPeer` only once it has caught up
    /// (`maybe_promote_learner`) — so a replacement that can't yet participate (e.g. no
    /// local group instance) never joins the quorum and can't freeze the group.
    /// Degrades to remove-only when no replacement is available.
    #[tracing::instrument(level = "debug", skip_all, fields(
        group = self.shard_group_id.0,
        remove = %remove,
        replacement = ?replacement,
    ))]
    pub(crate) fn propose_replace_peer(
        &mut self,
        remove: NodeId,
        replacement: Option<NodeId>,
    ) -> bool {
        if let Err(e) = self.propose(RaftCommand::RemovePeer(remove.clone())) {
            tracing::debug!(
                "RemovePeer({:?}) on {:?} rejected: {:?} — skipping replacement staging",
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
        // Stage the replacement as a non-voting learner (promoted once caught up), not
        // added straight to the quorum alongside the just-removed dead node.
        self.stage_learner(replacement);

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
                // Invariant: learners are non-voting and disjoint from voters — a node
                // is never both — and self is never a learner. Learners are replicated
                // to but excluded from the commit quorum until promoted via `AddPeer`.
                for learner in self.learner_states.keys() {
                    assert!(
                        !self.peers.contains(learner),
                        "node {:?} is both a voter and a learner",
                        learner,
                    );
                    assert_ne!(learner, &self.node_id, "self staged as a learner");
                }
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
                assert!(
                    self.learner_states.is_empty(),
                    "non-leader carries learner_states ({} entries)",
                    self.learner_states.len(),
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
    use crate::data_plane::messages::command::CatchUpAssignment;

    impl Raft {
        pub(crate) fn propose_noop(&mut self) -> Result<u64, ProposalError> {
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
            Err::<u64, _>(ProposalError::NotLeader(None))
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
            Err::<u64, _>(ProposalError::NotLeader(Some(node("node-1"))))
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
                retention_ms: Some(3_600_000),
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
            replica_set: Replicas::new(vec![node("node-1"), node("node-2"), node("node-3")]),
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
                retention_ms: Some(3_600_000),
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
            replica_set: Replicas::new(vec![node("n1"), node("n2"), node("n3")]),
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
                retention_ms: Some(3_600_000),
                replication_factor: rf as u64,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
            replica_set: Replicas::new(replica_set),
            created_at: 1000,
        })
        .into();
        raft.propose(cmd).expect("CreateTopic propose failed");
        // Drive apply so the state machine sees the committed entry.
        raft.simulate_flush_and_apply();
    }

    #[test]
    fn reconcile_peers_removes_dead_and_stages_replacement_as_learner() {
        // 3-node raft as node-1 (peers = [node-2, node-3]). Topology has
        // [node-1, node-3, node-4] — node-2 is dead, node-4 is the ring replacement.
        // Reconciliation proposes RemovePeer(node-2) and **stages node-4 as a learner**
        // (promoted to a voter only once caught up), never a paired AddPeer.
        let mut raft = three_node_raft_as_leader("node-1");
        let topology = topology_reader_with(&["node-1", "node-3", "node-4"]);
        let live = live_set(&["node-1", "node-3", "node-4"]);

        raft.reconcile_peers(&topology, &live);

        let proposals = proposals_after_become_leader(&raft);
        assert_eq!(
            proposals,
            vec![RaftCommand::RemovePeer(node("node-2"))],
            "reconciliation proposes only RemovePeer; node-4 is staged as a learner (got {:?})",
            proposals
        );
        assert!(
            raft.is_learner(&node("node-4")),
            "node-4 (the ring replacement) must be staged as a non-voting learner"
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

    // -------------------------------------------------------------------
    // #135 stale-live-peer eviction: gate-by-gate coverage.
    //
    // The 3-node ring at RF=3 assigns every group all three nodes, so any
    // real group id taken from the reader has members {node-1..3} — which
    // lets these tests line a Raft's shard_group_id up with an actual ring
    // group (the synthetic TEST_SHARD never resolves on the ring).
    // -------------------------------------------------------------------

    /// Leader over a real ring group plus the named extra "stale" voters.
    /// Ring peers have granted the election; nothing is acked yet, so
    /// commit_index is 0 until the test drives `ack_to_last`.
    fn ring_raft_with_stale(stale_names: &[&str]) -> (Raft, TopologyReader, Replicas) {
        let reader = topology_reader_with(&["node-1", "node-2", "node-3"]);
        let group = reader
            .shard_groups_for_node(&node("node-1"))
            .first()
            .expect("node-1 must own at least one group")
            .clone();

        let mut peers: HashSet<NodeId> = group
            .replicas
            .iter()
            .filter(|m| **m != node("node-1"))
            .cloned()
            .collect();
        for stale in stale_names {
            peers.insert(node(stale));
        }

        let mut raft = Raft::new(
            node("node-1"),
            peers,
            RaftPersistentState::default(),
            0,
            group.id,
            test_timer_seqs(),
        );
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: group.id,
            epoch: u64::MAX,
        });
        let term = raft.current_term();
        for peer in group.replicas.iter().filter(|m| **m != node("node-1")) {
            raft.handle_rpc(
                peer.clone(),
                RaftRpc::RequestVoteResponse(RequestVoteResponse {
                    term,
                    node_id: peer.clone(),
                    vote_granted: true,
                }),
            );
        }
        drain(&mut raft);
        assert_eq!(raft.role, Role::Leader, "must be leader after election");

        let members = group.replicas.clone();
        (raft, reader, members)
    }

    /// Simulate `peer` confirming replication up to the leader's last index.
    fn ack_to_last(raft: &mut Raft, peer: &NodeId) {
        let term = raft.current_term();
        let last = raft.log_last_index();
        raft.handle_rpc(
            peer.clone(),
            RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
                term,
                node_id: peer.clone(),
                success: true,
                last_log_index: last,
            }),
        );
        drain(raft);
    }

    fn ring_peers(members: &[NodeId]) -> Vec<NodeId> {
        members
            .iter()
            .filter(|m| **m != node("node-1"))
            .cloned()
            .collect()
    }

    #[test]
    fn stale_live_peer_evicted_after_ring_stability_window() {
        let (mut raft, topology, members) = ring_raft_with_stale(&["node-9"]);
        for peer in ring_peers(&members) {
            ack_to_last(&mut raft, &peer);
        }
        raft.simulate_flush_and_apply();
        let live = live_set(&["node-1", "node-2", "node-3", "node-9"]);

        // The window itself parks the eviction…
        for _ in 0..RING_STABLE_OBSERVATIONS - 1 {
            assert!(
                raft.reconcile_stale_live_peers(&topology, &live).is_err(),
                "no eviction before the stability window completes"
            );
        }
        // …and the pass that completes it evicts exactly the stale member.
        assert!(raft.reconcile_stale_live_peers(&topology, &live).is_ok());

        assert_eq!(
            proposals_after_become_leader(&raft),
            vec![RaftCommand::RemovePeer(node("node-9"))],
            "pure shrink back to RF: a removal with no paired addition"
        );
    }

    #[test]
    fn eviction_parks_behind_in_flight_config_change_then_proceeds() {
        let (mut raft, topology, members) = ring_raft_with_stale(&["node-9"]);
        for peer in ring_peers(&members) {
            ack_to_last(&mut raft, &peer);
        }
        raft.simulate_flush_and_apply();
        let live = live_set(&["node-1", "node-2", "node-3", "node-9"]);

        for _ in 0..RING_STABLE_OBSERVATIONS - 1 {
            assert!(raft.reconcile_stale_live_peers(&topology, &live).is_err());
        }

        // A membership entry is in flight: the window is complete, but the
        // one-config-change-at-a-time gate must park the eviction.
        raft.propose(RaftCommand::AddPeer(node("node-7"))).unwrap();
        assert!(
            raft.reconcile_stale_live_peers(&topology, &live).is_err(),
            "uncommitted AddPeer must park the eviction"
        );

        // Once the add commits (ring peers ack, entry applies), the next
        // pass may evict — add-before-remove falls out of the gate.
        for peer in ring_peers(&members) {
            ack_to_last(&mut raft, &peer);
        }
        raft.simulate_flush_and_apply();
        assert!(raft.reconcile_stale_live_peers(&topology, &live).is_ok());

        assert_eq!(
            proposals_after_become_leader(&raft),
            vec![
                RaftCommand::AddPeer(node("node-7")),
                RaftCommand::RemovePeer(node("node-9")),
            ],
        );
    }

    #[test]
    fn one_eviction_per_pass_smallest_node_first() {
        let (mut raft, topology, members) = ring_raft_with_stale(&["node-9", "node-8"]);
        for peer in ring_peers(&members) {
            ack_to_last(&mut raft, &peer);
        }
        raft.simulate_flush_and_apply();
        let live = live_set(&["node-1", "node-2", "node-3", "node-8", "node-9"]);

        for _ in 0..RING_STABLE_OBSERVATIONS - 1 {
            assert!(raft.reconcile_stale_live_peers(&topology, &live).is_err());
        }
        assert!(raft.reconcile_stale_live_peers(&topology, &live).is_ok());

        // The second stale member must wait: the first removal is in flight.
        assert!(
            raft.reconcile_stale_live_peers(&topology, &live).is_err(),
            "second eviction must park behind the uncommitted first"
        );

        assert_eq!(
            proposals_after_become_leader(&raft),
            vec![RaftCommand::RemovePeer(node("node-8"))],
            "one eviction per pass, smallest NodeId first"
        );
    }

    #[test]
    fn eviction_parks_while_a_ring_member_lags() {
        let (mut raft, topology, members) = ring_raft_with_stale(&["node-9"]);
        let peers = ring_peers(&members);
        let (acked, lagging) = (&peers[0], &peers[1]);

        // Commit advances via the stale member's ack, leaving one ring
        // member behind the committed prefix.
        ack_to_last(&mut raft, acked);
        ack_to_last(&mut raft, &node("node-9"));
        raft.simulate_flush_and_apply();
        let live = live_set(&["node-1", "node-2", "node-3", "node-9"]);

        for _ in 0..RING_STABLE_OBSERVATIONS + 2 {
            assert!(
                raft.reconcile_stale_live_peers(&topology, &live).is_err(),
                "a lagging ring member must park the eviction"
            );
        }

        // The laggard catches up; the very next pass may evict.
        ack_to_last(&mut raft, lagging);
        assert!(raft.reconcile_stale_live_peers(&topology, &live).is_ok());
        assert_eq!(
            proposals_after_become_leader(&raft),
            vec![RaftCommand::RemovePeer(node("node-9"))],
        );
    }

    #[test]
    fn ring_instability_restarts_the_observation_window() {
        let (mut raft, topology, members) = ring_raft_with_stale(&["node-9"]);
        for peer in ring_peers(&members) {
            ack_to_last(&mut raft, &peer);
        }
        raft.simulate_flush_and_apply();
        let live = live_set(&["node-1", "node-2", "node-3", "node-9"]);

        for _ in 0..RING_STABLE_OBSERVATIONS - 1 {
            assert!(raft.reconcile_stale_live_peers(&topology, &live).is_err());
        }

        // A divergent snapshot lands mid-window (rebalance in progress):
        // whatever this group looks like on the shrunken ring — different
        // membership or vanished outright — confidence must restart.
        let shrunken = topology_reader_with(&["node-1", "node-2"]);
        assert!(raft.reconcile_stale_live_peers(&shrunken, &live).is_err());

        // Back on the stable ring, the full window is owed again.
        for _ in 0..RING_STABLE_OBSERVATIONS - 1 {
            assert!(
                raft.reconcile_stale_live_peers(&topology, &live).is_err(),
                "instability must restart the window, not resume it"
            );
        }
        assert!(raft.reconcile_stale_live_peers(&topology, &live).is_ok());
        assert_eq!(
            proposals_after_become_leader(&raft),
            vec![RaftCommand::RemovePeer(node("node-9"))],
        );
    }

    #[test]
    fn dead_stale_member_is_left_to_reconcile_peers() {
        let (mut raft, topology, members) = ring_raft_with_stale(&["node-9"]);
        for peer in ring_peers(&members) {
            ack_to_last(&mut raft, &peer);
        }
        raft.simulate_flush_and_apply();
        // node-9 is an ex-owner AND dead: not this path's jurisdiction.
        let live = live_set(&["node-1", "node-2", "node-3"]);

        for _ in 0..RING_STABLE_OBSERVATIONS + 2 {
            assert!(raft.reconcile_stale_live_peers(&topology, &live).is_err());
        }
        assert!(
            proposals_after_become_leader(&raft).is_empty(),
            "dead voters belong to reconcile_peers' replace pairing"
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
                for member in &roll.new_replica_set.0 {
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

    #[test]
    fn reconcile_reassigns_sealed_segments_with_dead_replicas() {
        use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};

        // Single-node leader so CreateTopic + RollSegment commit and apply
        // trivially (quorum = 1).
        let mut raft = single_node_raft_as_leader();
        create_topic_in_raft(&mut raft, "t", vec![node("x"), node("y"), node("z")]);

        // Seal segment 0 with a KNOWN end, handing the new (active) segment a LIVE
        // replica set — so afterward only the sealed segment is under-replicated.
        let seg0 = SegmentKey::new(TopicId(0), RangeId(0), SegmentId(0));
        raft.propose(
            MetadataCommand::RollSegment(RollSegment {
                segment_key: seg0,
                sealed_at: 2000,
                new_replica_set: Replicas::new(vec![node("node-1")]),
                end_entry_id: Some(100.into()),
            })
            .into(),
        )
        .expect("RollSegment propose failed");
        raft.simulate_flush_and_apply();

        let before = proposals_after_become_leader(&raft).len();
        let live = live_set(&["node-1"]); // x/y/z dead; the active segment's node-1 lives
        raft.reconcile_segments(&live);
        let after = proposals_after_become_leader(&raft);

        let reassigns: Vec<_> = after[before..]
            .iter()
            .filter_map(|c| match c {
                RaftCommand::Metadata(MetadataCommand::ReassignSegment(r)) => Some(r),
                _ => None,
            })
            .collect();
        assert_eq!(
            reassigns.len(),
            1,
            "exactly one ReassignSegment for the sealed segment (tail: {:?})",
            &after[before..]
        );
        assert_eq!(reassigns[0].segment_key, seg0);
        for member in &reassigns[0].replica_set.0 {
            assert!(
                live.contains(member),
                "reassigned replica_set must be all-live, found {:?}",
                member
            );
        }
    }

    #[test]
    fn node_death_reconciles_peer_and_sealed_replica_together() {
        use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};

        let mut raft = three_node_raft_as_leader("node-1");
        let dead = raft.peers_iter().next().expect("peer exists").clone();
        let survivor = raft
            .peers_iter()
            .find(|peer| **peer != dead)
            .expect("second peer exists")
            .clone();
        let spare = node("spare");

        create_topic_in_raft(
            &mut raft,
            "t",
            vec![dead.clone(), survivor.clone(), node("node-1")],
        );
        ack_to_last(&mut raft, &survivor);
        raft.simulate_flush_and_apply();

        let seg0 = SegmentKey::new(TopicId(0), RangeId(0), SegmentId(0));
        raft.propose(
            MetadataCommand::RollSegment(RollSegment {
                segment_key: seg0,
                sealed_at: 2000,
                new_replica_set: Replicas::new(vec![
                    node("node-1"),
                    survivor.clone(),
                    spare.clone(),
                ]),
                end_entry_id: Some(100.into()),
            })
            .into(),
        )
        .expect("RollSegment propose failed");
        ack_to_last(&mut raft, &survivor);
        raft.simulate_flush_and_apply();

        let before = proposals_after_become_leader(&raft).len();
        let live = HashSet::from([node("node-1"), survivor, spare]);
        let topology = topology_reader_with(&["node-1", "spare"]);
        assert!(raft.handle_node_death(&dead, &live, &topology));

        let tail = &proposals_after_become_leader(&raft)[before..];
        assert!(
            tail.iter()
                .any(|command| matches!(command, RaftCommand::RemovePeer(peer) if peer == &dead)),
            "peer removal was not proposed: {tail:?}",
        );
        assert!(
            tail.iter().any(|command| matches!(
                command,
                RaftCommand::Metadata(MetadataCommand::ReassignSegment(reassign))
                    if reassign.segment_key == seg0
            )),
            "sealed data replica repair was skipped: {tail:?}",
        );
    }

    // ── Capacity-return re-fill of under-replicated sealed segments (raft-actor.md #10) ──

    /// Single-node leader of a *real* ring group id, so `ring_replacements_for` resolves
    /// against the topology (unlike `single_node_raft_as_leader`'s synthetic `TEST_SHARD`).
    /// Single-node → quorum 1, so CreateTopic / RollSegment commit and apply trivially.
    fn single_node_leader_of(group_id: ShardGroupId) -> Raft {
        let mut raft = Raft::new(
            node("node-1"),
            HashSet::new(),
            RaftPersistentState::default(),
            0,
            group_id,
            test_timer_seqs(),
        );
        raft.handle_timeout(RaftTimeoutCallback::ElectionTimeout {
            shard_group_id: group_id,
            epoch: u64::MAX,
        });
        drain(&mut raft);
        assert_eq!(raft.role, Role::Leader);
        raft
    }

    /// Topic "t" in `raft` with segment 0 sealed at a known end carrying `sealed_set`
    /// (plus a fresh active segment on the same set) — so only the sealed segment's
    /// replication level is under test.
    fn seal_segment_zero_with(raft: &mut Raft, sealed_set: Vec<NodeId>) -> SegmentKey {
        use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
        create_topic_in_raft(raft, "t", sealed_set.clone());
        let seg0 = SegmentKey::new(
            TopicId((raft.shard_group_id.0) << 32),
            RangeId(0),
            SegmentId(0),
        );
        raft.propose(
            MetadataCommand::RollSegment(RollSegment {
                segment_key: seg0,
                sealed_at: 2000,
                new_replica_set: Replicas::new(sealed_set),
                end_entry_id: Some(100.into()),
            })
            .into(),
        )
        .expect("RollSegment propose failed");
        raft.simulate_flush_and_apply();
        seg0
    }

    fn reassigns_in(proposals: &[RaftCommand]) -> Vec<(SegmentKey, Replicas)> {
        proposals
            .iter()
            .filter_map(|c| match c {
                RaftCommand::Metadata(MetadataCommand::ReassignSegment(r)) => {
                    Some((r.segment_key, r.replica_set.clone()))
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn refill_grows_under_replicated_sealed_segment_to_a_ring_member() {
        // A sealed segment shrunk to [node-1, node-2] (len 2 < RF 3) with NO dead member —
        // exactly what an earlier death-with-no-replacement leaves behind. The ring for the
        // group is {node-1, node-2, node-3}, so re-fill must reassign it to add node-3.
        let reader = topology_reader_with(&["node-1", "node-2", "node-3"]);
        let group = reader
            .shard_groups_for_node(&node("node-1"))
            .first()
            .expect("node-1 must own a group")
            .clone();
        let mut raft = single_node_leader_of(group.id);
        let seg0 = seal_segment_zero_with(&mut raft, vec![node("node-1"), node("node-2")]);

        let before = proposals_after_become_leader(&raft).len();
        assert!(
            raft.refill_under_replicated_segments(&reader),
            "an under-replicated sealed segment with a ring member to spare must re-fill"
        );
        let after = proposals_after_become_leader(&raft);
        let reassigns = reassigns_in(&after[before..]);

        assert_eq!(
            reassigns.len(),
            1,
            "exactly one ReassignSegment (tail: {:?})",
            &after[before..]
        );
        let (key, new_set) = &reassigns[0];
        assert_eq!(*key, seg0, "the re-fill must target the sealed segment");
        assert_eq!(new_set.len(), 3, "grown back to RF");
        for n in ["node-1", "node-2", "node-3"] {
            assert!(
                new_set.contains(&node(n)),
                "{n} must be in the re-filled set {new_set:?}"
            );
        }
    }

    #[test]
    fn refill_is_a_noop_for_a_segment_already_at_rf() {
        // Sealed at full RF (3 members) → not under-replicated → nothing proposed. This is
        // what stops ordinary ring drift from churning well-replicated segments.
        let reader = topology_reader_with(&["node-1", "node-2", "node-3"]);
        let group = reader
            .shard_groups_for_node(&node("node-1"))
            .first()
            .expect("node-1 must own a group")
            .clone();
        let mut raft = single_node_leader_of(group.id);
        seal_segment_zero_with(
            &mut raft,
            vec![node("node-1"), node("node-2"), node("node-3")],
        );

        let before = proposals_after_become_leader(&raft).len();
        assert!(
            !raft.refill_under_replicated_segments(&reader),
            "a segment already at RF must not be re-filled"
        );
        assert_eq!(
            proposals_after_become_leader(&raft).len(),
            before,
            "no proposal for an at-RF segment"
        );
    }

    #[test]
    fn refill_is_a_noop_when_the_ring_cannot_grow_the_set() {
        // Under-replicated [node-1, node-2] (len 2 < RF 3) but the ring has only those two
        // nodes — every ring member is already in the set, so there is nothing to add.
        // Re-fill proposes nothing; the segment waits for capacity (next ring check).
        let reader = topology_reader_with(&["node-1", "node-2"]);
        let group = reader
            .shard_groups_for_node(&node("node-1"))
            .first()
            .expect("node-1 must own a group")
            .clone();
        let mut raft = single_node_leader_of(group.id);
        seal_segment_zero_with(&mut raft, vec![node("node-1"), node("node-2")]);

        let before = proposals_after_become_leader(&raft).len();
        assert!(
            !raft.refill_under_replicated_segments(&reader),
            "no ring member outside the set → no proposal"
        );
        assert_eq!(
            proposals_after_become_leader(&raft).len(),
            before,
            "segment stays under-replicated until the ring grows"
        );
    }

    // ── Catch-up re-drive: seed at ReassignSegment apply, sweep until acked ──

    /// Create a topic, seal segment 0 at a known end, reassign it to `new_set`,
    /// and apply — leaving one in-flight catch-up repair seeded in the leader's
    /// tracker (the heartbeat sweep will re-drive it).
    fn raft_with_seeded_catch_up(new_set: Vec<NodeId>) -> (Raft, SegmentKey) {
        use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
        let mut raft = single_node_raft_as_leader();
        create_topic_in_raft(&mut raft, "t", vec![node("x"), node("y"), node("z")]);
        let seg0 = SegmentKey::new(TopicId(0), RangeId(0), SegmentId(0));
        raft.propose(
            MetadataCommand::RollSegment(RollSegment {
                segment_key: seg0,
                sealed_at: 2000,
                new_replica_set: Replicas::new(vec![node("node-1")]),
                end_entry_id: Some(100.into()),
            })
            .into(),
        )
        .expect("RollSegment propose failed");
        raft.simulate_flush_and_apply();
        raft.propose(
            MetadataCommand::ReassignSegment(ReassignSegment {
                segment_key: seg0,
                replica_set: Replicas::new(new_set),
            })
            .into(),
        )
        .expect("ReassignSegment propose failed");
        raft.simulate_flush_and_apply();
        (raft, seg0)
    }

    /// Run the catch-up re-drive sweep and collect (target, assignment) pairs.
    fn drain_catch_up_redrives(raft: &mut Raft) -> Vec<(NodeId, CatchUpAssignment)> {
        use crate::data_plane::messages::command::DataPlaneInterNodeCommand;
        raft.maybe_redrive_catch_ups();
        let mut out = Vec::new();
        for event in raft.take_events() {
            let RaftEvent::RedriveAssignments(cmds) = event else {
                continue;
            };
            for cmd in cmds {
                if let DataTransportCommand::SendToTargets(s) = cmd {
                    let target = s.targets[0].clone();
                    if let DataPlaneInterNodeCommand::CatchUpAssignment(a) = s.message {
                        out.push((target, a));
                    }
                }
            }
        }
        out
    }

    #[test]
    fn catch_up_redrives_to_every_unconfirmed_member() {
        let members = vec![node("node-1"), node("y"), node("z")];
        let (mut raft, seg0) = raft_with_seeded_catch_up(members.clone());

        let redrives = drain_catch_up_redrives(&mut raft);
        let mut targets: Vec<NodeId> = redrives.iter().map(|(t, _)| t.clone()).collect();
        targets.sort();
        let mut expected = members.clone();
        expected.sort();
        assert_eq!(targets, expected, "re-drive reaches every member");

        // Each assignment carries the segment's sealed bounds + full replica set
        // (the receiver picks its own source from it).
        for (_, a) in &redrives {
            assert_eq!(a.segment_key, seg0);
            assert_eq!(a.shard_group_id, TEST_SHARD);
            assert_eq!(a.start_entry_id, 0.into());
            assert_eq!(a.sealed_end_entry_id, 100.into());
            assert_eq!(a.replica_set.0, members.clone().into_boxed_slice());
        }
    }

    #[test]
    fn catch_up_repairs_dropped_on_step_down() {
        let (mut raft, _) = raft_with_seeded_catch_up(vec![node("node-1"), node("y")]);
        // A higher term deposes the leader → leader-volatile tracker is cleared.
        raft.step_down(raft.current_term + 1);
        assert!(raft.catch_up.is_empty());
        assert!(drain_catch_up_redrives(&mut raft).is_empty());
    }

    #[test]
    fn reseed_catch_up_reconstructs_the_tracker_on_takeover() {
        let members = vec![node("node-1"), node("y"), node("z")];
        let (mut raft, seg0) = raft_with_seeded_catch_up(members.clone());

        // Simulate the takeover gap: the leader-volatile tracker is empty, but the
        // sealed segment is still under-replicated in the state machine.
        raft.catch_up.clear();
        assert!(drain_catch_up_redrives(&mut raft).is_empty());

        raft.reseed_catch_up();

        let redrives = drain_catch_up_redrives(&mut raft);
        for (_, a) in &redrives {
            assert_eq!(a.segment_key, seg0);
            assert_eq!(a.sealed_end_entry_id, 100.into());
        }
        let mut targets: Vec<NodeId> = redrives.iter().map(|(t, _)| t.clone()).collect();
        targets.sort();
        let mut expected = members;
        expected.sort();
        assert_eq!(
            targets, expected,
            "reseed re-drives every member of the sealed segment"
        );
    }

    #[test]
    fn reconcile_redrives_seal_recovery_for_unknown_end() {
        use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};

        // Same shape, but seal with end_entry_id = None — a SWIM-death-style seal
        // whose committed end the coordinator never learned.
        let mut raft = single_node_raft_as_leader();
        create_topic_in_raft(&mut raft, "t", vec![node("x"), node("y"), node("z")]);
        let seg0 = SegmentKey::new(TopicId(0), RangeId(0), SegmentId(0));
        raft.propose(
            MetadataCommand::RollSegment(RollSegment {
                segment_key: seg0,
                sealed_at: 2000,
                new_replica_set: Replicas::new(vec![node("y"), node("z"), node("node-1")]),
                end_entry_id: None,
            })
            .into(),
        )
        .expect("RollSegment propose failed");
        raft.simulate_flush_and_apply();
        assert_eq!(
            raft.get_replica_set(&seg0.with_segment_id(SegmentId(1))),
            Some(Replicas::new(vec![node("y"), node("z"), node("node-1")]))
        );

        let before = proposals_after_become_leader(&raft).len();
        // The crashed write leader may already have bounced by the time this
        // backstop runs; metadata's successor set identifies the true survivors.
        let live = live_set(&["node-1", "x", "y", "z"]);
        raft.reconcile_segments(&live);
        let after = proposals_after_become_leader(&raft);

        // Dead replicas, but an unknown end → excluded from repair
        // (`leader_crash_seal_boundary.md`). Nothing proposed for it.
        let reassigns = after[before..]
            .iter()
            .filter(|c| {
                matches!(
                    c,
                    RaftCommand::Metadata(MetadataCommand::ReassignSegment(_))
                )
            })
            .count();
        assert_eq!(
            reassigns,
            0,
            "boundary-unknown sealed segment must not be reassigned (tail: {:?})",
            &after[before..]
        );
        assert_eq!(
            raft.take_leaderless_segments(),
            vec![(seg0, vec![node("y"), node("z")])],
            "unknown boundary remains a recovery candidate until corrected"
        );
    }

    #[test]
    fn only_replica_dead_classifies_the_death() {
        let rs = vec![node("x"), node("y"), node("z")]; // x is the write leader
        assert!(
            Raft::only_replica_dead(&rs, &live_set(&["y", "z"])),
            "only the leader (x) died → recoverable leader crash"
        );
        assert!(
            Raft::only_replica_dead(&rs, &live_set(&["x", "z"])),
            "one dead follower is also recoverable before rolling"
        );
        assert!(
            !Raft::only_replica_dead(&rs, &live_set(&["z"])),
            "leader x and follower y died → multi-death fallback"
        );
    }

    #[test]
    fn reconcile_segments_defers_leaderless_to_recovery() {
        // Active segment 0 has replica_set [x, y, z]; x is the write leader.
        let mut raft = single_node_raft_as_leader();
        create_topic_in_raft(&mut raft, "t", vec![node("x"), node("y"), node("z")]);

        let before = proposals_after_become_leader(&raft).len();
        let live = live_set(&["node-1", "y", "z"]); // leader x crashed
        raft.reconcile_segments(&live);

        // No immediate roll — the segment is stashed for seal-end recovery.
        let after = proposals_after_become_leader(&raft);
        let rolls = after[before..]
            .iter()
            .filter(|c| matches!(c, RaftCommand::Metadata(MetadataCommand::RollSegment(_))))
            .count();
        assert_eq!(
            rolls,
            0,
            "leaderless segment is deferred, not rolled (tail: {:?})",
            &after[before..]
        );

        let candidates = raft.take_leaderless_segments();
        assert_eq!(candidates.len(), 1, "the leaderless segment is stashed");
        assert_eq!(
            candidates[0].1,
            vec![node("y"), node("z")],
            "with its surviving replicas in replica-set order"
        );
    }

    #[test]
    fn reconcile_segments_rolls_active_on_multi_death() {
        let mut raft = single_node_raft_as_leader();
        create_topic_in_raft(&mut raft, "t", vec![node("x"), node("y"), node("z")]);

        let before = proposals_after_become_leader(&raft).len();
        let live = live_set(&["node-1", "z"]); // x and y dead → multi-death
        raft.reconcile_segments(&live);
        let after = proposals_after_become_leader(&raft);

        let rolls: Vec<_> = after[before..]
            .iter()
            .filter_map(|c| match c {
                RaftCommand::Metadata(MetadataCommand::RollSegment(r)) => Some(r),
                _ => None,
            })
            .collect();
        assert_eq!(
            rolls.len(),
            1,
            "multi-death falls back to the unknown-end roll (tail: {:?})",
            &after[before..]
        );
        assert_eq!(
            rolls[0].end_entry_id, None,
            "fallback roll has no known end"
        );
        assert!(
            raft.take_leaderless_segments().is_empty(),
            "multi-death is not a sole-leader-crash candidate"
        );
    }
}
