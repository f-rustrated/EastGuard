use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::{
    CoordinatorSealRequest, DeferredConsumerGroupAssignment, DeferredReply, InboundRaftRpc,
    LogMutation, MetadataProposal, MultiRaftActorCommand, RaftEvent, RaftProtocolMessage,
    RaftTimeoutCallback,
};
use crate::control_plane::consensus::raft::errors::ProposalError;
use crate::control_plane::consensus::raft::state::{LeaderlessSegments, Raft, TimerSeqs};
use crate::control_plane::consensus::raft::storage::RaftStorage;
use crate::control_plane::consensus::raft::{compute_replacement_replica_set, now_ms};
use crate::control_plane::consensus::seal_recovery::{SealEndRecovery, SealEndStep};
use crate::control_plane::membership::{ShardGroup, ShardGroupId, TopologyReader};
use crate::control_plane::metadata::command::RollSegment;
use crate::control_plane::metadata::{
    ConsumerGroupAssignment, EntryId, TopicId, TopicMeta, TopicStats,
};
use crate::data_plane::SegmentKey;
use crate::data_plane::messages::command::{
    CatchUpAck, SealBoundaryQuery, SealBoundaryReport, SegmentAssignmentAck,
};
use crate::data_plane::transport::command::DataTransportCommand;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use tokio::sync::oneshot;
use uuid::Uuid;

// dirty is owned here because it's a persistence concern — flush() drains it.
pub(crate) struct MultiRaft {
    node_id: NodeId,
    election_jitter_seed: u64,
    storage: Box<dyn RaftStorage>,
    groups: BTreeMap<ShardGroupId, Raft>,
    seq_counter: RaftTimerTokenGenerator,
    dirty: BTreeSet<ShardGroupId>,
    pending_events: Vec<RaftEvent>,
    deferred: Vec<DeferredReply>,
    /// Senders waiting for a specific (shard, log index) to be committed and applied.
    pending_proposes: BTreeMap<(ShardGroupId, u64), oneshot::Sender<Result<(), ProposalError>>>,

    pending_seals: PendingSealTracker,
    /// Leader-crash seal-end recoveries (one per leader-crashed segment).
    seal_recovery: SealEndRecovery,

    topology: TopologyReader,
}

#[derive(Debug)]
pub(crate) struct SealContext {
    pub(crate) requester: NodeId,
    pub(crate) segment_key: SegmentKey,
}
// Bookkeeping for in-flight seal proposals.
#[derive(Default)]
struct PendingSealTracker {
    by_group: HashMap<ShardGroupId, BTreeMap<u64, SealContext>>,
    by_key: HashSet<SegmentKey>,
}

impl PendingSealTracker {
    fn contains(&self, key: &SegmentKey) -> bool {
        self.by_key.contains(key)
    }

    fn insert(&mut self, group_id: ShardGroupId, log_index: u64, seal: SealContext) {
        self.by_key.insert(seal.segment_key);
        self.by_group
            .entry(group_id)
            .or_default()
            .insert(log_index, seal);
    }
    fn remove(&mut self, group_id: ShardGroupId, log_index: u64) -> Option<SealContext> {
        let group_map = self.by_group.get_mut(&group_id)?;
        let seal = group_map.remove(&log_index)?;

        // Clean up the empty map to prevent memory leaks over time
        if group_map.is_empty() {
            self.by_group.remove(&group_id);
        }

        self.by_key.remove(&seal.segment_key);
        Some(seal)
    }

    fn pop_seal_context(&mut self, group_id: ShardGroupId, log_index: u64) -> Option<SealContext> {
        self.remove(group_id, log_index)
    }

    /// Drop pending seal contexts for `group_id` whose log indices were
    /// truncated by a new leader. Prevents unbounded growth of `by_key` /
    /// `by_index` when proposals are rejected by truncation.
    fn drop_from(&mut self, group_id: ShardGroupId, from_index: u64) {
        if let Some(group_map) = self.by_group.get_mut(&group_id) {
            let stale = group_map.split_off(&from_index);
            for seal in stale.into_values() {
                self.by_key.remove(&seal.segment_key);
            }

            if group_map.is_empty() {
                self.by_group.remove(&group_id);
            }
        }
    }

    /// Drop every pending seal context for `group_id`. Called when a leader
    /// steps down — any not-yet-committed proposals will either be truncated
    /// by the new leader or commit there, but this replica will no longer be
    /// the one to dispatch the SealResponse.
    fn drop_group(&mut self, group_id: ShardGroupId) {
        if let Some(stale_group) = self.by_group.remove(&group_id) {
            for seal in stale_group.into_values() {
                self.by_key.remove(&seal.segment_key);
            }
        }
    }
}

impl MultiRaft {
    pub(crate) fn new(
        node_id: NodeId,
        election_jitter_seed: u64,
        storage: Box<dyn RaftStorage>,
        topology: TopologyReader,
    ) -> Self {
        Self {
            node_id,
            election_jitter_seed,
            storage,
            groups: BTreeMap::new(),
            seq_counter: RaftTimerTokenGenerator::default(),
            dirty: BTreeSet::new(),
            pending_events: Vec::new(),
            deferred: Vec::new(),
            pending_proposes: BTreeMap::new(),
            pending_seals: PendingSealTracker::default(),
            seal_recovery: SealEndRecovery::default(),
            topology,
        }
    }

    pub(crate) fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// This runs a reconciliation process to stabilize the group.
    /// Importantly, takeover does NOT trust its own peer set.
    #[tracing::instrument(level = "debug", skip_all, fields(group = shard_group_id.0))]
    pub(crate) fn reconcile_on_leadership_change(&mut self, shard_group_id: ShardGroupId) {
        let target_members = self.topology.group_ring_members(shard_group_id);

        self.reconcile_membership(shard_group_id, target_members);

        // Takeover backstop: the catch-up tracker is leader-volatile, so a repair
        // in flight when leadership changed left it empty here. Re-seed it; the
        // heartbeat sweep re-drives until each member confirms.
        if let Some(raft) = self.groups.get_mut(&shard_group_id)
            && raft.is_leader()
        {
            raft.reseed_catch_up();
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(group = shard_group_id.0))]
    fn handle_ringcheck_timeout(&mut self, shard_group_id: ShardGroupId) {
        let Some(raft) = self.groups.get(&shard_group_id) else {
            return;
        };

        // * Tick-path filter: only chase ring deltas. Asserting already-present
        // * members is the takeover path's genesis-divergence heal (#133/#134);
        // * repeating it every interval would spam the log with no-op AddPeers.
        // * hash_peer() saves recurring log spam, covered by takeover
        let target_members = self
            .topology
            .group_ring_members(shard_group_id)
            .map(|members| {
                members
                    .iter()
                    .filter(|m| !raft.has_peer(m))
                    .cloned()
                    .collect::<Box<[NodeId]>>()
            });

        self.reconcile_membership(shard_group_id, target_members);

        // Backstop the one-shot `AnnounceShardLeader` (#135 class): re-announce our
        // leadership each ring check so a node that missed the original gossip
        // converges (without it, its empty shard-leader map drops every
        // `SendToCoordinator` — seals, catch-up acks — forever). Spread across
        // groups by the per-group timer; receivers that already know it no-op
        // (term-monotonic). Announce-only — emphatically not a reconcile.
        if let Some(event) = self
            .groups
            .get(&shard_group_id)
            .and_then(|g| g.shard_leader_refresh())
        {
            self.pending_events.push(event);
        }
    }

    fn reconcile_membership(
        &mut self,
        shard_group_id: ShardGroupId,
        target_members: Option<Box<[NodeId]>>,
    ) {
        let Some(raft) = self.groups.get_mut(&shard_group_id) else {
            return;
        };
        if !raft.is_leader() {
            return;
        }

        if raft.reconcile(&self.topology, target_members) {
            self.dirty.insert(shard_group_id);
        }

        let leaderless = raft.take_leaderless_segments();

        self.seal_recovery
            .drop_stale_in_group(shard_group_id, &leaderless);
        self.drive_seal_end_recovery(shard_group_id, leaderless);
    }

    pub(crate) fn process(&mut self, cmd: MultiRaftActorCommand) {
        match cmd {
            MultiRaftActorCommand::ProtocolMessage(c) => {
                self.handle_consensus(c);
            }
            MultiRaftActorCommand::GetLeader { group_id, reply } => {
                let result = self.get_leader(group_id);
                self.deferred.push(DeferredReply::GetLeader(reply, result));
            }
            MultiRaftActorCommand::GetPeers { group_id, reply } => {
                let result = self.get_peers(group_id);
                self.deferred.push(DeferredReply::GetPeers(reply, result));
            }
            MultiRaftActorCommand::ClientProposal { propose, reply } => {
                self.propose(propose, reply);
            }

            MultiRaftActorCommand::GetTopics { reply } => {
                let topics = self.get_topics();
                self.deferred.push(DeferredReply::GetTopics(reply, topics));
            }
            MultiRaftActorCommand::GetTopicStats { reply } => {
                let stats = self.get_topic_stats();
                self.deferred
                    .push(DeferredReply::GetTopicStats(reply, stats));
            }
            MultiRaftActorCommand::GetTopicMetadata { topic_name, reply } => {
                let meta = self.get_topic_metadata(&topic_name);
                self.deferred
                    .push(DeferredReply::GetTopicMetadata(reply, meta));
            }
            MultiRaftActorCommand::GetConsumerGroupAssignment(query) => {
                let value = self.get_consumer_group_assignment(
                    &query.topic_name,
                    &query.group_id,
                    query.member_id,
                );
                self.deferred
                    .push(DeferredReply::GetConsumerGroupAssignment(
                        DeferredConsumerGroupAssignment {
                            reply: query.reply,
                            value,
                        },
                    ));
            }
            MultiRaftActorCommand::Coordinator(req) => {
                self.handle_seal_request(req);
            }
            MultiRaftActorCommand::AssignmentAck(ack) => {
                self.handle_assignment_ack(ack);
            }
            MultiRaftActorCommand::SealBoundaryReport(report) => {
                self.handle_seal_boundary_report(report);
            }
            MultiRaftActorCommand::CatchUpAck(ack) => {
                self.handle_catch_up_ack(ack);
            }
        }
    }

    pub(crate) fn fire_deferred(&mut self) {
        for reply in self.deferred.drain(..) {
            match reply {
                DeferredReply::GetLeader(sender, v) => {
                    let _ = sender.send(v);
                }
                DeferredReply::GetPeers(sender, v) => {
                    let _ = sender.send(v);
                }
                DeferredReply::Propose(sender, v) => {
                    let _ = sender.send(v);
                }
                DeferredReply::GetTopics(sender, v) => {
                    let _ = sender.send(v);
                }
                DeferredReply::GetTopicStats(sender, v) => {
                    let _ = sender.send(v);
                }
                DeferredReply::GetTopicMetadata(sender, v) => {
                    let _ = sender.send(v);
                }
                DeferredReply::GetConsumerGroupAssignment(deferred) => {
                    let _ = deferred.reply.send(deferred.value);
                }
            }
        }
    }

    pub(crate) fn handle_consensus(&mut self, cmd: impl Into<RaftProtocolMessage>) {
        match cmd.into() {
            RaftProtocolMessage::InboundRaftRpc(cmd) => self.handle_rpc(cmd),
            RaftProtocolMessage::Timeout(cb) => self.handle_timeout(cb),
            RaftProtocolMessage::EnsureGroup(cmd) => self.add_group(&cmd.group),
            RaftProtocolMessage::RemoveGroup(cmd) => self.remove_group(cmd.group_id),
            RaftProtocolMessage::HandleNodeDeath(node_id) => self.handle_node_death(node_id),
            RaftProtocolMessage::HandleNodeJoin(node_id) => self.add_node(node_id),
        }
    }

    fn add_group(&mut self, group: &ShardGroup) {
        if self.groups.contains_key(&group.id) {
            return;
        }
        if !group.members.contains(&self.node_id) {
            return;
        }

        let peers: HashSet<NodeId> = group
            .members
            .iter()
            .filter(|id| *id != &self.node_id)
            .cloned()
            .collect();

        let election_jitter_seed = self.create_election_jitter_seed(group);

        let persistent = self.storage.load_state(group.id.0);

        let timer_seqs = TimerSeqs {
            election: self.seq_counter.generate(),
            rpc: self.seq_counter.generate(),
            merge_check: self.seq_counter.generate(),
            ring_check: self.seq_counter.generate(),
        };

        let raft = Raft::new(
            self.node_id.clone(),
            peers,
            persistent,
            election_jitter_seed,
            group.id,
            timer_seqs,
        );

        tracing::info!(
            "[{}] Created Raft group {:?} with {} peers",
            self.node_id,
            group.id,
            raft.peers_count()
        );

        self.groups.insert(group.id, raft);
        self.dirty.insert(group.id);
    }

    fn create_election_jitter_seed(&mut self, group: &ShardGroup) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.election_jitter_seed.hash(&mut hasher);
        self.node_id.hash(&mut hasher);
        group.id.hash(&mut hasher);
        hasher.finish()
    }

    fn remove_group(&mut self, group_id: ShardGroupId) {
        let Some(mut raft) = self.groups.remove(&group_id) else {
            return;
        };
        raft.cancel_all_timers();
        self.pending_events.extend(raft.take_events());

        let pending_keys: Box<[_]> = self
            .pending_proposes
            .keys()
            .filter(|(gid, _)| *gid == group_id)
            .cloned()
            .collect();
        for key in pending_keys {
            if let Some(sender) = self.pending_proposes.remove(&key) {
                let _ = sender.send(Err(ProposalError::ShardGroupRemoved));
            }
        }

        self.storage.delete_group(group_id);

        tracing::info!("[{}] Removed Raft group {:?}", self.node_id, group_id);
    }

    #[tracing::instrument(level = "trace", skip_all, fields(group = cmd.shard_group_id.0, from = %cmd.from))]
    fn handle_rpc(&mut self, cmd: InboundRaftRpc) {
        if let Some(raft) = self.groups.get_mut(&cmd.shard_group_id) {
            raft.handle_rpc(cmd.from, cmd.rpc);
            self.dirty.insert(cmd.shard_group_id);
        }
    }

    #[tracing::instrument(level = "trace", skip_all, fields(cb = ?cb))]
    fn handle_timeout(&mut self, cb: RaftTimeoutCallback) {
        let shard_id = match &cb {
            RaftTimeoutCallback::Ignored => return,
            RaftTimeoutCallback::ElectionTimeout { shard_group_id, .. }
            | RaftTimeoutCallback::RpcTimeout { shard_group_id }
            | RaftTimeoutCallback::MergeCheckTimeout { shard_group_id, .. }
            | RaftTimeoutCallback::RingCheckTimeout { shard_group_id } => *shard_group_id,
        };

        // The ring diff needs `self.topology`, which the Raft state machine
        // deliberately doesn't hold — run it here, then forward the callback
        // so the leader-gated re-arm stays inside `Raft` like MergeCheck's.
        if matches!(cb, RaftTimeoutCallback::RingCheckTimeout { .. }) {
            self.handle_ringcheck_timeout(shard_id);
        }

        if let Some(raft) = self.groups.get_mut(&shard_id) {
            raft.handle_timeout(cb);
            self.dirty.insert(shard_id);
        }
    }

    fn handle_assignment_ack(&mut self, ack: SegmentAssignmentAck) {
        let Some(raft) = self.groups.get_mut(&ack.shard_group_id) else {
            return;
        };
        raft.handle_assignment_ack(ack);
    }

    fn get_leader(&self, group_id: ShardGroupId) -> Option<NodeId> {
        self.groups
            .get(&group_id)
            .and_then(|r| r.current_leader().cloned())
    }

    fn get_peers(&self, group_id: ShardGroupId) -> Box<[NodeId]> {
        self.groups
            .get(&group_id)
            .map(|r| r.peers_iter().cloned().collect())
            .unwrap_or_default()
    }

    fn get_topics(&self) -> Box<[String]> {
        self.groups
            .values()
            .flat_map(|raft| raft.topic_names())
            .collect()
    }

    fn get_topic_stats(&self) -> Box<[TopicStats]> {
        self.groups
            .values()
            .flat_map(|raft| raft.topic_stats())
            .collect()
    }

    fn get_topic_metadata(&self, name: &str) -> Option<TopicMeta> {
        self.groups
            .values()
            .find_map(|raft| raft.get_topic_by_name(name).cloned())
    }

    fn get_consumer_group_assignment(
        &self,
        topic_name: &str,
        group_id: &str,
        member_id: Uuid,
    ) -> Option<ConsumerGroupAssignment> {
        self.groups
            .values()
            .find_map(|raft| raft.get_consumer_group_assignment(topic_name, group_id, member_id))
    }

    // Full scan over groups is acceptable — node death is rare (~6-7s SWIM detection)
    // and the scan is O(groups) with cheap has_peer/is_leader checks.
    //
    // Membership changes flow through the Raft log: only the leader proposes
    // `RemovePeer`, and followers apply on commit. Direct mutation would let
    // replicas disagree on quorum size during gossip convergence. See
    // `docs/metadata-management/metadata_management_roadmap.md` Phase 3d.
    //
    // The live set is loaded fresh from the topology snapshot — drift between
    // event emission and processing is absorbed naturally, and there's one
    // canonical source of "current cluster shape" (the topology reader).
    #[tracing::instrument(level = "debug", skip_all, fields(dead = %dead_node_id))]
    fn handle_node_death(&mut self, dead_node_id: NodeId) {
        let live_set: HashSet<NodeId> = self.topology.live_nodes().into_iter().collect();
        let mut reconciled: HashMap<ShardGroupId, LeaderlessSegments> = HashMap::new();
        for (group_id, raft) in self.groups.iter_mut() {
            if raft.handle_node_death(&dead_node_id, &live_set, &self.topology) {
                self.dirty.insert(*group_id);
            }
            raft.log_ring_drift(&self.topology, &live_set);
            reconciled.insert(*group_id, raft.take_leaderless_segments());
        }

        // Across every group we just reconciled, drop gathers whose segment is no
        // longer leaderless — a group left with none drops all of its gathers.
        self.seal_recovery.drop_stale_reconciled(&reconciled);
        for (shard_group_id, leaderless) in reconciled {
            if !leaderless.is_empty() {
                self.drive_seal_end_recovery(shard_group_id, leaderless);
            }
        }
    }

    fn handle_seal_request(&mut self, req: CoordinatorSealRequest) {
        let seal = &req.request;
        if self.pending_seals.contains(&seal.segment_key) {
            return;
        }

        let Some(group_id) = self.find_group_for_topic(&seal.segment_key.topic_id) else {
            tracing::warn!(
                "SealRequest for unknown topic {:?}",
                seal.segment_key.topic_id
            );
            return;
        };
        let Some(raft) = self.groups.get_mut(&group_id) else {
            return;
        };

        let Some(old_replica_set) = raft.get_replica_set(&seal.segment_key) else {
            tracing::warn!("SealRequest for unknown segment {:?}", seal.segment_key);
            return;
        };

        let live_nodes = self.topology.live_nodes();

        let new_replica_set =
            compute_replacement_replica_set(&old_replica_set, &seal.failed_nodes, &live_nodes);

        let cmd = RollSegment {
            segment_key: seal.segment_key,
            sealed_at: now_ms(),
            new_replica_set,
            end_entry_id: Some(seal.end_entry_id),
        };

        match raft.propose(cmd.into()) {
            Ok(log_index) => {
                self.pending_seals.insert(
                    group_id,
                    log_index,
                    SealContext {
                        requester: seal.from.clone(),
                        segment_key: seal.segment_key,
                    },
                );
                self.dirty.insert(group_id);
            }
            Err(e) => {
                tracing::debug!("SealRequest proposal rejected: {:?}", e);
            }
        }
    }

    /// Drive the seal-end recovery subsystem for `group`'s leaderless `segments`
    /// (callers prune stale gathers first), executing the steps it returns.
    fn drive_seal_end_recovery(&mut self, group: ShardGroupId, segments: LeaderlessSegments) {
        for step in self.seal_recovery.advance(group, segments) {
            self.execute_seal_step(step);
        }
    }

    /// Feed a survivor's durable extent into the subsystem; if it completes a
    /// gather, execute the resulting seal.
    fn handle_seal_boundary_report(&mut self, report: SealBoundaryReport) {
        match self
            .seal_recovery
            .record(report.segment_key, report.from, report.durable_end)
        {
            Ok(Some(step)) => self.execute_seal_step(step),
            Ok(None) => {}
            Err(error) => tracing::warn!(
                segment = ?report.segment_key,
                ?error,
                "rejected seal-boundary report",
            ),
        }
    }

    /// Route a catch-up confirmation to the owning group's `Raft`, which clears the
    /// member and prunes the repair once all confirm. Mirrors `handle_assignment_ack`.
    fn handle_catch_up_ack(&mut self, ack: CatchUpAck) {
        let Some(raft) = self.groups.get_mut(&ack.shard_group_id) else {
            return;
        };
        raft.handle_catch_up_ack(ack);
    }

    fn execute_seal_step(&mut self, step: SealEndStep) {
        match step {
            SealEndStep::Query {
                segment_key,
                targets,
            } => self.emit_seal_boundary_queries(segment_key, targets),
            SealEndStep::Seal {
                shard_group_id,
                segment_key,
                end,
                leader,
            } => self.propose_recovered_roll(shard_group_id, segment_key, end, leader),
        }
    }

    /// Propose the recovery roll: seal the crashed segment at `end` and reopen
    /// with a healthy set led by `recency_leader` (the most-complete survivor).
    /// `end = None` reproduces the unknown-end fallback. Already-sealed
    /// boundary-unknown segments accept the same command as an idempotent late
    /// correction, so proposal loss during a leadership change can be retried.
    fn propose_recovered_roll(
        &mut self,
        group_id: ShardGroupId,
        segment_key: SegmentKey,
        end: Option<EntryId>,
        recency_leader: Option<NodeId>,
    ) {
        let Some(raft) = self.groups.get_mut(&group_id) else {
            return;
        };
        let Some(old_replica_set) = raft.get_replica_set(&segment_key) else {
            return;
        };
        let live_nodes = self.topology.live_nodes();
        let dead: Vec<NodeId> = old_replica_set
            .iter()
            .filter(|n| !live_nodes.contains(*n))
            .cloned()
            .collect();
        let mut new_replica_set =
            compute_replacement_replica_set(&old_replica_set, &dead, &live_nodes);

        // Recency-based leader selection: the most-complete survivor leads.
        if let Some(leader) = recency_leader
            && let Some(pos) = new_replica_set.iter().position(|n| *n == leader)
        {
            let node = new_replica_set.remove(pos);
            new_replica_set.insert(0, node);
        }

        let cmd = RollSegment {
            segment_key,
            sealed_at: now_ms(),
            new_replica_set,
            end_entry_id: end,
        };
        match raft.propose(cmd.into()) {
            Ok(_) => {
                self.dirty.insert(group_id);
            }
            Err(e) => tracing::warn!("recovered roll for {:?} rejected: {:?}", segment_key, e),
        }
    }

    fn emit_seal_boundary_queries(&mut self, segment_key: SegmentKey, targets: Vec<NodeId>) {
        if targets.is_empty() {
            return;
        }
        let cmd = DataTransportCommand::send_to_targets(
            targets.to_vec(),
            SealBoundaryQuery {
                segment_key,
                coordinator: self.node_id.clone(),
            },
        );
        self.pending_events
            .push(RaftEvent::SealBoundaryQueries(vec![cmd]));
    }

    fn find_group_for_topic(&self, topic_id: &TopicId) -> Option<ShardGroupId> {
        self.groups
            .iter()
            .find_map(|(gid, raft)| raft.has_topic(topic_id).then_some(*gid))
    }

    /// Compute affected groups (the ones this new node now participates in)
    /// from the fresh topology snapshot, then propose `AddPeer` for groups
    /// this node already leads and create new groups it newly joins. Loading
    /// fresh means the join sees the latest cluster shape — including any
    /// topology drift between the SWIM event and this handler running.
    #[tracing::instrument(level = "debug", skip_all, fields(node = %node_id))]
    fn add_node(&mut self, node_id: NodeId) {
        let affected_groups = self.topology.shard_groups_for_node(&node_id);
        if affected_groups.is_empty() {
            return;
        }
        self.add_peer_to_existing_groups(&node_id, &affected_groups);

        // A join is precisely the moment reassignment can mint a live
        // ex-owner (#135): the joiner displaces a still-alive member from a
        // group's ring assignment. Detection only — scan is O(groups × RF)
        // and joins are rare.
        let live_set: HashSet<NodeId> = self.topology.live_nodes().into_iter().collect();
        for raft in self.groups.values() {
            raft.log_ring_drift(&self.topology, &live_set);
        }
    }

    fn add_peer_to_existing_groups(&mut self, node_id: &NodeId, groups: &[ShardGroup]) {
        for group in groups {
            self.add_group(group);
            let Some(raft) = self.groups.get_mut(&group.id) else {
                continue;
            };
            if !raft.is_leader() || raft.has_peer(node_id) {
                continue;
            }
            // Stage the joining node as a non-voting learner; it's promoted to a voter
            // once it has caught up (never added straight to the quorum).
            if raft.stage_learner(node_id.clone()) {
                self.dirty.insert(group.id);
            }
        }
    }

    fn propose(
        &mut self,
        cmd: MetadataProposal,
        reply: oneshot::Sender<Result<(), ProposalError>>,
    ) {
        let gid = cmd.shard_group_id;
        match self.propose_internal(cmd) {
            Ok(index) => {
                self.pending_proposes.insert((gid, index), reply);
            }
            Err(e) => {
                self.deferred.push(DeferredReply::Propose(reply, Err(e)));
            }
        }
    }

    fn propose_internal(&mut self, cmd: MetadataProposal) -> Result<u64, ProposalError> {
        match self.groups.get_mut(&cmd.shard_group_id) {
            Some(raft) => {
                let result = raft.propose(cmd.command.into());
                self.dirty.insert(cmd.shard_group_id);
                result
            }
            None => Err(ProposalError::ShardNotFound),
        }
    }

    pub(crate) fn flush(&mut self) -> Vec<RaftEvent> {
        let dirty: Vec<ShardGroupId> = std::mem::take(&mut self.dirty).into_iter().collect();
        self.flush_auto_proposals(&dirty);
        let (mutations, last_indices) = self.collect_mutations(&dirty);
        self.persist_and_advance(mutations, last_indices);
        self.resolve_pending_proposes(&dirty);
        std::mem::take(&mut self.pending_events)
    }

    fn flush_auto_proposals(&mut self, dirty: &[ShardGroupId]) {
        for id in dirty {
            let Some(raft) = self.groups.get_mut(id) else {
                continue;
            };
            for cmd in raft.take_pending_proposals() {
                if let Err(e) = raft.propose(cmd.into()) {
                    tracing::warn!("Auto-proposal rejected for {:?}: {:?}", id, e);
                }
            }
        }
    }

    fn collect_mutations(
        &mut self,
        dirty: &[ShardGroupId],
    ) -> (
        Vec<(ShardGroupId, LogMutation)>,
        BTreeMap<ShardGroupId, u64>,
    ) {
        let mut mutations = vec![];
        let mut last_indices = BTreeMap::new();
        for id in dirty {
            let Some(raft) = self.groups.get_mut(id) else {
                continue;
            };

            last_indices.insert(*id, raft.log_last_index());
            for log in raft.take_log_mutations() {
                if let LogMutation::TruncateFrom(from_index) = &log {
                    // Rare and destructive: a new leader is overwriting this
                    // replica's conflicting log suffix. Worth a trace — both
                    // the data loss and the dropped seal contexts.
                    tracing::debug!(
                        group = id.0,
                        from = *from_index,
                        "log truncation: dropping conflicting suffix and its pending seals",
                    );
                    self.pending_seals.drop_from(*id, *from_index);
                }
                mutations.push((*id, log));
            }
            // A deposed leader won't finalize either kind of in-flight work, so
            // drop both here (the reliable step-down checkpoint — the ring-check
            // that would otherwise prune is leader-gated and stops firing). A new
            // leader re-derives: re-proposes the seal / re-polls the survivors.
            if !raft.is_leader() {
                // pending_seals: its SealResponse won't come from here.
                self.pending_seals.drop_group(*id);
                // seal-end gathers: we can't propose their recovery roll.
                self.seal_recovery.drop_group(*id);
            }
            self.route_group_events(*id);
        }
        (mutations, last_indices)
    }

    /// Drain a group's buffered events into `pending_events`. `MetadataCommitted`
    /// is dropped on non-leaders (only the current leader dispatches to the data
    /// plane — see invariant 3); seal context is attached for the leader. Called
    /// both after step-driven apply (`collect_mutations`) and after the durability
    /// gate advances apply (`persist_and_advance`).
    fn route_group_events(&mut self, id: ShardGroupId) {
        let Some(raft) = self.groups.get_mut(&id) else {
            return;
        };
        for mut event in raft.take_events() {
            if let RaftEvent::MetadataCommitted(_) = event
                && !raft.is_leader()
            {
                continue;
            }

            if let RaftEvent::MetadataCommitted(committed) = &mut event {
                // For leader to send a SealResponse back to the right client.
                committed.seal_context =
                    self.pending_seals.pop_seal_context(id, committed.log_index);
            }
            self.pending_events.push(event);
        }
    }

    fn resolve_pending_proposes(&mut self, dirty: &[ShardGroupId]) {
        for id in dirty {
            let Some(raft) = self.groups.get(id) else {
                continue;
            };
            let last_applied = raft.last_applied_index();

            // 1. Resolve applied entries
            loop {
                // Find the first key in the applied range.
                let first_key = self
                    .pending_proposes
                    .range((*id, 0)..=(*id, last_applied))
                    .map(|(&k, _)| k)
                    .next();

                let Some(key) = first_key else {
                    break; // No more applied entries for this shard
                };

                if let Some(sender) = self.pending_proposes.remove(&key) {
                    let _ = sender.send(Ok(()));
                }
            }

            // 2. Reject orphaned entries ONLY if we lost leadership
            if !raft.is_leader() {
                loop {
                    // Find the first key GREATER than last_applied for this shard.
                    let first_key = self
                        .pending_proposes
                        .range((*id, last_applied.saturating_add(1))..)
                        .take_while(|((shard_id, _), _)| *shard_id == *id)
                        .map(|(&k, _)| k)
                        .next();

                    let Some(key) = first_key else {
                        break; // No more orphaned entries for this shard
                    };

                    if let Some(sender) = self.pending_proposes.remove(&key) {
                        let _ = sender.send(Err(ProposalError::NotLeader(None)));
                    }
                }
            }
        }
    }

    fn persist_and_advance(
        &mut self,
        mutations: Vec<(ShardGroupId, LogMutation)>,
        last_indices: BTreeMap<ShardGroupId, u64>,
    ) {
        if mutations.is_empty() {
            return;
        }
        self.storage.persist_mutations(mutations.into_boxed_slice());
        for (id, last_log_index) in last_indices {
            if let Some(raft) = self.groups.get_mut(&id) {
                // Advancing the durability gate re-attempts apply for entries that
                // committed before they were persisted. Drain any events that apply
                // produced — `collect_mutations` already ran this round, so without
                // this they'd sit undrained until the next flush cycle.
                raft.advance_stabled_index(last_log_index);
            }
            self.route_group_events(id);
        }
    }
}

#[derive(Debug, Default)]
struct RaftTimerTokenGenerator(u64);
impl RaftTimerTokenGenerator {
    fn generate(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(1);
        self.0
    }
}

#[cfg(test)]
impl MultiRaft {
    fn stabled_for(&self, id: ShardGroupId) -> u64 {
        self.groups.get(&id).map_or(0, |r| r.stabled_index())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::control_plane::consensus::raft::storage::RaftPersistentState;
    use crate::control_plane::consensus::seal_recovery;
    use crate::impls::metadata_storage::MetadataStorage;
    use crate::schedulers::ticker_message::TimerCommand;
    use std::collections::BTreeSet;

    fn node(id: &str) -> NodeId {
        NodeId::new(id)
    }

    fn shard(id: u64, members: Vec<NodeId>) -> ShardGroup {
        ShardGroup {
            id: ShardGroupId(id),
            members,
        }
    }

    fn timer_seqs(events: &[RaftEvent]) -> Vec<u64> {
        events
            .iter()
            .filter_map(|e| match e {
                RaftEvent::Timer(TimerCommand::SetSchedule { seq, .. }) => Some(*seq),
                _ => None,
            })
            .collect()
    }

    fn temp_storage() -> (Box<dyn RaftStorage>, std::path::PathBuf) {
        let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let db = MetadataStorage::open(path.clone());
        (Box::new(db), path)
    }

    fn new_store(node_id: NodeId, storage: Box<dyn RaftStorage>) -> MultiRaft {
        use crate::control_plane::membership::{Topology, TopologyConfig, topology_channel};
        // Tests just need a valid topology reader; empty topology is fine —
        // these tests don't exercise reconciliation or ring picks.
        let topology = Topology::new(
            std::iter::empty(),
            TopologyConfig {
                vnodes_per_pnode: 4,
                replication_factor: 1,
            },
        );
        let (_pub_handle, reader) = topology_channel(topology);
        MultiRaft::new(node_id, 0, storage, reader)
    }

    #[test]
    fn timer_seqs_are_unique_across_groups() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);

        store.add_group(&shard(1, vec![me.clone(), node("n2")]));
        store.add_group(&shard(2, vec![me.clone(), node("n2")]));

        let events = store.flush();
        let seqs = timer_seqs(&events);
        let unique: BTreeSet<u64> = seqs.iter().cloned().collect();
        assert_eq!(seqs.len(), unique.len());
    }

    #[test]
    fn add_group_registers_in_memory() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);

        store.add_group(&shard(42, vec![me.clone(), node("n2")]));

        assert!(store.groups.contains_key(&ShardGroupId(42)));
    }

    #[test]
    fn remove_group_deletes_data() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);

        store.add_group(&shard(42, vec![me.clone()]));
        store.handle_consensus(RaftProtocolMessage::Timeout(
            RaftTimeoutCallback::ElectionTimeout {
                shard_group_id: ShardGroupId(42),
                epoch: u64::MAX,
            },
        ));
        store.flush();
        assert!(store.storage.load_state(42) != Default::default());

        store.remove_group(ShardGroupId(42));
        assert!(store.storage.load_state(42) == Default::default());
    }

    #[test]
    fn add_group_noop_when_not_member() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);

        store.add_group(&shard(42, vec![node("n2"), node("n3")]));

        assert!(!store.groups.contains_key(&ShardGroupId(42)));
    }

    #[test]
    fn election_jitter_is_distinct_per_replica_and_stable_per_group() {
        let (storage_one, _) = temp_storage();
        let (storage_two, _) = temp_storage();
        let mut one = new_store(node("n1"), storage_one);
        let mut two = new_store(node("n2"), storage_two);
        let group = shard(42, vec![node("n1"), node("n2")]);

        let one_seed = one.create_election_jitter_seed(&group);
        assert_eq!(one_seed, one.create_election_jitter_seed(&group));
        assert_ne!(one_seed, two.create_election_jitter_seed(&group));
    }

    /// The ring-check re-announces shard leadership (the #135 backstop for the
    /// one-shot `AnnounceShardLeader`) only when this node leads the group — a
    /// follower's ring-check emits nothing.
    #[test]
    fn ring_check_re_announces_leadership_only_when_leader() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);

        // Group 1: single-node → leader on election. Group 2: two-node, no
        // election → this node stays a follower.
        store.add_group(&shard(1, vec![me.clone()]));
        store.handle_consensus(RaftProtocolMessage::Timeout(
            RaftTimeoutCallback::ElectionTimeout {
                shard_group_id: ShardGroupId(1),
                epoch: u64::MAX,
            },
        ));
        store.add_group(&shard(2, vec![me.clone(), node("n2")]));
        store.flush(); // drain the initial LeaderChange + add_group dirt

        // Leader's ring-check → exactly one re-announce, for its group.
        store.handle_consensus(RaftProtocolMessage::Timeout(
            RaftTimeoutCallback::RingCheckTimeout {
                shard_group_id: ShardGroupId(1),
            },
        ));
        assert_eq!(
            shard_leader_refreshes(&store.flush()),
            vec![(ShardGroupId(1), me.clone())],
            "leader re-announces its shard leadership on the ring-check"
        );

        // Follower's ring-check → nothing.
        store.handle_consensus(RaftProtocolMessage::Timeout(
            RaftTimeoutCallback::RingCheckTimeout {
                shard_group_id: ShardGroupId(2),
            },
        ));
        assert!(
            shard_leader_refreshes(&store.flush()).is_empty(),
            "a follower does not re-announce"
        );
    }

    fn shard_leader_refreshes(events: &[RaftEvent]) -> Vec<(ShardGroupId, NodeId)> {
        events
            .iter()
            .filter_map(|e| {
                if let RaftEvent::ShardLeaderRefresh(lc) = e {
                    Some((lc.shard_group_id, lc.leader_node_id.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    #[test]
    fn restart_recovers_persisted_state() {
        let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let me = node("n1");

        {
            let db = MetadataStorage::open(path.clone());
            let mut store = new_store(me.clone(), Box::new(db));
            store.add_group(&shard(1, vec![me.clone()]));
            store.add_group(&shard(2, vec![me.clone()]));
            store.handle_consensus(RaftProtocolMessage::Timeout(
                RaftTimeoutCallback::ElectionTimeout {
                    shard_group_id: ShardGroupId(1),
                    epoch: u64::MAX,
                },
            ));
            store.handle_consensus(RaftProtocolMessage::Timeout(
                RaftTimeoutCallback::ElectionTimeout {
                    shard_group_id: ShardGroupId(2),
                    epoch: u64::MAX,
                },
            ));
            store.flush();
        }

        let db = MetadataStorage::open(path);
        assert!(db.load_state(1) != Default::default());
        assert!(db.load_state(2) != Default::default());
    }

    // -----------------------------------------------------------------------
    // Unit 2 — flush() writes to RocksDB
    // -----------------------------------------------------------------------

    use crate::control_plane::consensus::messages::{AppendEntries, RaftRpc};
    use crate::control_plane::consensus::raft::command::RaftCommand;
    use crate::control_plane::consensus::raft::log::LogEntry;

    const TEST_GROUP_ID: ShardGroupId = ShardGroupId(42);

    /// Elect n1 as leader of a single-node shard group. Single-node clusters
    /// become leader immediately on ElectionTimeout (no peers to wait for).
    fn elect_leader(store: &mut MultiRaft) {
        store.handle_consensus(RaftProtocolMessage::Timeout(
            RaftTimeoutCallback::ElectionTimeout {
                shard_group_id: TEST_GROUP_ID,
                epoch: u64::MAX,
            },
        ));
    }

    fn propose_noop(store: &mut MultiRaft) {
        let raft = store.groups.get_mut(&TEST_GROUP_ID).unwrap();
        raft.propose_noop().expect("propose_noop failed");
        store.dirty.insert(TEST_GROUP_ID);
    }

    fn read_entry(store: &MultiRaft, index: u64) -> Option<LogEntry> {
        let state = store.storage.load_state(TEST_GROUP_ID.0);
        state.log.into_iter().find(|e| e.index == index)
    }

    fn read_hard_state(store: &MultiRaft) -> RaftPersistentState {
        store.storage.load_state(TEST_GROUP_ID.0)
    }

    #[test]
    fn flush_persists_log_entry() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));

        elect_leader(&mut store);
        store.flush(); // persist election HardState

        propose_noop(&mut store);
        store.flush();

        // The noop appended by become_leader is index 1; our proposal is index 2.
        let entry = read_entry(&store, 2).expect("entry 2 must be in RocksDB after flush");
        assert_eq!(entry.index, 2);
    }

    #[test]
    fn flush_persists_hard_state() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));

        elect_leader(&mut store);
        store.flush();

        let state = read_hard_state(&store);
        assert_eq!(state.term, 1, "term must be 1 after first election");
    }

    #[test]
    fn stabled_advances_after_flush() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));

        assert_eq!(store.stabled_for(TEST_GROUP_ID), 0);

        elect_leader(&mut store);
        store.flush();
        let after_election = store.stabled_for(TEST_GROUP_ID);

        propose_noop(&mut store);
        store.flush();
        let after_propose = store.stabled_for(TEST_GROUP_ID);

        assert!(
            after_propose > after_election,
            "stabled must advance after appending an entry"
        );
    }

    #[test]
    fn flush_noop_when_not_dirty() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));
        store.flush(); // consume dirty from add_group

        // No state changes — flush should be a no-op and return no events.
        let events = store.flush();
        assert!(events.is_empty());
    }

    #[test]
    fn flush_truncate_removes_entries_from_rocksdb() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let n2 = node("n2");
        let mut store = new_store(me.clone(), storage);
        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone(), n2.clone()]));
        store.flush(); // consume add_group dirty

        // n2 is acting as leader at term 1.  Send two entries to n1 (follower).
        store.handle_consensus(InboundRaftRpc {
            shard_group_id: TEST_GROUP_ID,
            from: n2.clone(),
            rpc: RaftRpc::AppendEntries(AppendEntries {
                term: 1,
                leader_id: n2.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Box::new([
                    LogEntry {
                        term: 1,
                        index: 1,
                        command: RaftCommand::Noop,
                    },
                    LogEntry {
                        term: 1,
                        index: 2,
                        command: RaftCommand::Noop,
                    },
                ]),
                leader_commit: 0,
            }),
        });
        store.flush();
        assert!(
            read_entry(&store, 1).is_some(),
            "entry 1 must exist before truncation"
        );
        assert!(
            read_entry(&store, 2).is_some(),
            "entry 2 must exist before truncation"
        );

        // n2 re-sends from index 1 at term 2, conflicting with the existing term-1 entries.
        // Raft truncates from index 1 and replaces with the new entry.
        store.handle_consensus(InboundRaftRpc {
            shard_group_id: TEST_GROUP_ID,
            from: n2.clone(),
            rpc: RaftRpc::AppendEntries(AppendEntries {
                term: 2,
                leader_id: n2.clone(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Box::new([LogEntry {
                    term: 2,
                    index: 1,
                    command: RaftCommand::Noop,
                }]),
                leader_commit: 0,
            }),
        });
        store.flush();

        let new_entry = read_entry(&store, 1).expect("replacement entry 1 must be in RocksDB");
        assert_eq!(new_entry.term, 2, "entry 1 must be the term-2 replacement");
        assert!(
            read_entry(&store, 2).is_none(),
            "truncated entry 2 must be gone from RocksDB"
        );
    }

    #[test]
    fn restart_restores_log_entries_and_stabled_index() {
        let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let me = node("n1");

        let expected_last_index;
        {
            let db = MetadataStorage::open(path.clone());
            let mut store = new_store(me.clone(), Box::new(db));
            store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));
            elect_leader(&mut store);
            store.flush();
            propose_noop(&mut store);
            store.flush();
            expected_last_index = store.groups.get(&TEST_GROUP_ID).unwrap().log_last_index();
        }

        let db = MetadataStorage::open(path);
        let mut store = new_store(me.clone(), Box::new(db));
        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));

        let raft = store.groups.get(&TEST_GROUP_ID).unwrap();
        assert_eq!(
            raft.log_last_index(),
            expected_last_index,
            "log must be restored from RocksDB after restart"
        );
        assert_eq!(
            raft.stabled_index(),
            expected_last_index,
            "stabled_index must equal last restored log entry"
        );
    }

    #[test]
    fn restart_restores_hard_state() {
        let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let me = node("n1");
        {
            let db = MetadataStorage::open(path.clone());
            let mut store = new_store(me.clone(), Box::new(db));
            store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));
            elect_leader(&mut store);
            store.flush();
        }

        let db = MetadataStorage::open(path);
        let mut store = new_store(me.clone(), Box::new(db));
        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));
        let raft = store.groups.get(&TEST_GROUP_ID).unwrap();
        assert_eq!(raft.current_term(), 1);
        assert_eq!(raft.voted_for(), Some(node("n1")));
    }

    #[test]
    fn propose_metadata_command_via_multi_raft() {
        use crate::control_plane::metadata::command::{CreateTopic, MetadataCommand};
        use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};

        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));

        elect_leader(&mut store);
        store.flush();

        let cmd = MetadataCommand::CreateTopic(CreateTopic {
            name: "test-topic".to_string(),
            storage_policy: StoragePolicy {
                retention_ms: Some(3_600_000),
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
            replica_set: vec![node("n1"), node("n2"), node("n3")],
            created_at: 1000,
        });

        store
            .propose_internal(MetadataProposal {
                shard_group_id: TEST_GROUP_ID,
                command: cmd,
            })
            .expect("metadata propose failed");
        store.flush();

        let entry = read_entry(&store, 2).expect("metadata command entry must be persisted");
        assert_eq!(entry.index, 2);
    }

    #[test]
    fn groups_are_independent_no_cross_contamination() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);

        // Single-node groups so election succeeds immediately
        store.add_group(&shard(1, vec![me.clone()]));
        store.add_group(&shard(2, vec![me.clone()]));
        store.flush();

        // Elect leader in group 1 only
        store.handle_consensus(RaftProtocolMessage::Timeout(
            RaftTimeoutCallback::ElectionTimeout {
                shard_group_id: ShardGroupId(1),
                epoch: u64::MAX,
            },
        ));
        store.flush();

        // Propose noop to group 1
        let raft = store.groups.get_mut(&ShardGroupId(1)).unwrap();
        raft.propose_noop().unwrap();
        store.dirty.insert(ShardGroupId(1));
        store.flush();

        let g1 = store.groups.get(&ShardGroupId(1)).unwrap();
        assert_eq!(g1.current_term(), 1);
        assert!(g1.log_last_index() >= 2);

        let g2 = store.groups.get(&ShardGroupId(2)).unwrap();
        assert_eq!(g2.current_term(), 0, "group 2 term must be unaffected");
        assert_eq!(g2.log_last_index(), 0, "group 2 log must be empty");
    }

    #[test]
    fn stale_metadata_proposal_rejected_gracefully() {
        use crate::control_plane::metadata::command::{CreateTopic, MetadataCommand};
        use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};

        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);
        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));

        elect_leader(&mut store);
        store.flush();

        let make_cmd = || {
            MetadataCommand::CreateTopic(CreateTopic {
                name: "blue".to_string(),
                storage_policy: StoragePolicy {
                    retention_ms: Some(3_600_000),
                    replication_factor: 3,
                    partition_strategy: PartitionStrategy::AutoSplit,
                },
                replica_set: vec![node("n1"), node("n2"), node("n3")],
                created_at: 1000,
            })
        };

        // First proposal succeeds
        let _ = store.propose_internal(MetadataProposal {
            shard_group_id: TEST_GROUP_ID,
            command: make_cmd(),
        });
        store.flush();

        // Duplicate proposal: must not panic, state machine rejects at apply
        let _ = store.propose_internal(MetadataProposal {
            shard_group_id: TEST_GROUP_ID,
            command: make_cmd(),
        });
        store.flush();

        let raft = store.groups.get(&TEST_GROUP_ID).unwrap();
        assert_eq!(
            raft.state_machine().topic_count(),
            1,
            "duplicate must not create a second topic"
        );
    }

    // ---------------------------------------------------------------------
    // Reconciliation pairing: removals are paired with ring-aware additions
    // so the group's failure budget doesn't shrink monotonically.
    // ---------------------------------------------------------------------

    /// Build a MultiRaft with a topology populated by `all_nodes`. The publisher
    /// half is dropped; the reader's own Arc keeps the ArcSwap alive for the
    /// test's lifetime. Use this whenever a test needs the reader's queries
    /// (live_nodes / ring_replacements_for / shard_groups_for_node) to return
    /// non-empty results.
    fn new_store_with_topology(
        node_id: NodeId,
        storage: Box<dyn RaftStorage>,
        all_nodes: &[NodeId],
    ) -> MultiRaft {
        use crate::control_plane::membership::{Topology, TopologyConfig, topology_channel};
        let topology = Topology::new(
            all_nodes.iter().cloned(),
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 3,
            },
        );
        let (_pub_handle, reader) = topology_channel(topology);
        MultiRaft::new(node_id, 0, storage, reader)
    }

    /// `new_store_with_topology`, but the publisher half is kept so the test
    /// can simulate a rebalance by publishing a fresh snapshot — exactly what
    /// `SwimActor` does on a membership change.
    fn new_store_with_topology_publisher(
        node_id: NodeId,
        storage: Box<dyn RaftStorage>,
        all_nodes: &[NodeId],
    ) -> (
        MultiRaft,
        std::sync::Arc<arc_swap::ArcSwap<crate::control_plane::membership::Topology>>,
    ) {
        use crate::control_plane::membership::{Topology, TopologyConfig, topology_channel};
        let topology = Topology::new(
            all_nodes.iter().cloned(),
            TopologyConfig {
                vnodes_per_pnode: 64,
                replication_factor: 3,
            },
        );
        let (pub_handle, reader) = topology_channel(topology);
        (MultiRaft::new(node_id, 0, storage, reader), pub_handle)
    }

    /// Drive `TEST_GROUP_ID` to elect this node as leader by simulating
    /// vote-granted RequestVoteResponses from a majority of `peers`. Mirrors
    /// the pattern used in `raft/state.rs` tests: ElectionTimeout transitions
    /// to Candidate, then enough RequestVoteResponses tip past quorum.
    fn elect_leader_with_peers(store: &mut MultiRaft, peers: &[NodeId]) {
        use crate::control_plane::consensus::messages::{RaftRpc, RequestVoteResponse};

        elect_leader(store);

        // Cluster size = peers.len() + 1 (self). Self has already voted for
        // itself in become_candidate, so we need `cluster_size / 2` more votes
        // to clear majority.
        let cluster_size = peers.len() + 1;
        let needed = cluster_size / 2;
        let raft = store.groups.get_mut(&TEST_GROUP_ID).unwrap();
        let term = raft.current_term();
        for peer in peers.iter().take(needed) {
            raft.handle_rpc(
                peer.clone(),
                RaftRpc::RequestVoteResponse(RequestVoteResponse {
                    term,
                    node_id: peer.clone(),
                    vote_granted: true,
                }),
            );
        }
        store.dirty.insert(TEST_GROUP_ID);
    }

    /// `elect_leader_with_peers`, but for an arbitrary group id. Ring-derived
    /// ids (rather than the synthetic `TEST_GROUP_ID`) are needed whenever a
    /// test must line a Raft group up with a real topology snapshot.
    fn elect_leader_for(store: &mut MultiRaft, gid: ShardGroupId, peers: &[NodeId]) {
        use crate::control_plane::consensus::messages::{RaftRpc, RequestVoteResponse};

        store.handle_consensus(RaftProtocolMessage::Timeout(
            RaftTimeoutCallback::ElectionTimeout {
                shard_group_id: gid,
                epoch: u64::MAX,
            },
        ));

        let cluster_size = peers.len() + 1;
        let needed = cluster_size / 2;
        let raft = store.groups.get_mut(&gid).unwrap();
        let term = raft.current_term();
        for peer in peers.iter().take(needed) {
            raft.handle_rpc(
                peer.clone(),
                RaftRpc::RequestVoteResponse(RequestVoteResponse {
                    term,
                    node_id: peer.clone(),
                    vote_granted: true,
                }),
            );
        }
        store.dirty.insert(gid);
    }

    /// All proposals committed so far on `TEST_GROUP_ID`, in log order.
    /// Skips the bootstrap noop appended by `become_leader`.
    fn proposals_after_become_leader(store: &MultiRaft) -> Vec<RaftCommand> {
        let state = store.storage.load_state(TEST_GROUP_ID.0);
        state
            .log
            .into_iter()
            .skip(1) // index 1 is the become_leader noop
            .map(|e| e.command)
            .collect()
    }

    #[test]
    fn handle_node_death_removes_voter_and_stages_replacement_as_learner() {
        // 5 nodes on the ring, group has 3. On a member's death the leader proposes
        // ONLY RemovePeer (committed); the ring-picked replacement is **staged as a
        // non-voting learner**, not added straight to the voting quorum (which would
        // freeze commits if it can't yet participate). It's promoted via AddPeer only
        // once caught up — which can't happen in this no-replication unit test, so it
        // stays a learner.
        let (storage, _) = temp_storage();
        let me = node("me");
        let peers = vec![node("n2"), node("n3")];
        let group_members = std::iter::once(me.clone())
            .chain(peers.iter().cloned())
            .collect::<Vec<_>>();
        let all_nodes = vec![me.clone(), node("n2"), node("n3"), node("n4"), node("n5")];
        let mut store = new_store_with_topology(me.clone(), storage, &all_nodes);

        store.add_group(&shard(TEST_GROUP_ID.0, group_members.clone()));
        elect_leader_with_peers(&mut store, &peers);
        store.flush();

        store.handle_node_death(node("n2"));
        store.flush();

        // Log carries only the removal — no immediate AddPeer.
        let proposals = proposals_after_become_leader(&store);
        assert_eq!(
            proposals,
            vec![RaftCommand::RemovePeer(node("n2"))],
            "death proposes only RemovePeer; the replacement is staged as a learner (got {:?})",
            proposals
        );

        // The ring-picked replacement (n4 or n5) is a non-voting learner, not a voter.
        let raft = store.groups.get(&TEST_GROUP_ID).expect("group exists");
        assert_eq!(
            raft.learner_count(),
            1,
            "exactly one ring replacement staged as a learner"
        );
        assert!(
            raft.is_learner(&node("n4")) || raft.is_learner(&node("n5")),
            "a ring non-member must be staged as a learner"
        );
    }

    #[test]
    fn handle_node_death_degrades_when_pool_exhausted() {
        // Cluster is exactly [me, n2] — once n2 dies, no replacement is available.
        // RemovePeer must still be proposed; nothing is staged.
        let (storage, _) = temp_storage();
        let me = node("me");
        let peers = vec![node("n2")];
        let all_nodes = vec![me.clone(), node("n2")];
        let mut store = new_store_with_topology(me.clone(), storage, &all_nodes);

        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone(), node("n2")]));
        elect_leader_with_peers(&mut store, &peers);
        store.flush();

        store.handle_node_death(node("n2"));
        store.flush();

        let proposals = proposals_after_become_leader(&store);
        assert_eq!(
            proposals,
            vec![RaftCommand::RemovePeer(node("n2"))],
            "exhausted pool must yield only the removal"
        );
        let raft = store.groups.get(&TEST_GROUP_ID).expect("group exists");
        assert_eq!(
            raft.learner_count(),
            0,
            "no replacement available → nothing staged"
        );
    }

    #[test]
    fn ring_check_adds_new_ring_member_under_stable_leader() {
        use crate::control_plane::membership::{Topology, TopologyConfig};

        // The #135 trigger gap: reconciliation used to run only on takeover
        // or SWIM join/death *as seen by this handler chain*. Here the leader
        // stays healthy across a rebalance (n4 joins the ring), so no
        // takeover ever re-runs the ring diff — the periodic RingCheck alone
        // must converge the add direction.
        let (storage, _) = temp_storage();
        let n1 = node("n1");
        let old_nodes = [node("n1"), node("n2"), node("n3")];
        let config = TopologyConfig {
            vnodes_per_pnode: 64,
            replication_factor: 3,
        };

        // Deterministic group pick (murmur3 ring): a group n1 owns in the old
        // ring whose new-ring assignment includes the joiner n4. With 192
        // candidate groups and n4 owning a quarter of all vnode positions,
        // such a group always exists.
        let old_topology = Topology::new(old_nodes.iter().cloned(), config.clone());
        let new_topology = Topology::new(
            old_nodes.iter().cloned().chain([node("n4")]),
            config.clone(),
        );
        let gid = old_topology
            .shard_groups_for_node(&n1)
            .into_iter()
            .map(|g| g.id)
            .find(|id| {
                new_topology
                    .group(*id)
                    .is_some_and(|g| g.members.contains(&node("n4")))
            })
            .expect("some group of n1 must gain n4 after the join");

        let (mut store, publisher) =
            new_store_with_topology_publisher(n1.clone(), storage, &old_nodes);

        // Old ring is 3 nodes at RF=3, so every group's membership is exactly
        // {n1, n2, n3}.
        store.add_group(&shard(gid.0, old_nodes.to_vec()));
        elect_leader_for(&mut store, gid, &[node("n2"), node("n3")]);
        store.flush();

        // The rebalance lands; the leader is healthy, so no takeover fires.
        publisher.store(std::sync::Arc::new(new_topology));

        store.handle_consensus(RaftProtocolMessage::Timeout(
            RaftTimeoutCallback::RingCheckTimeout {
                shard_group_id: gid,
            },
        ));
        store.flush();

        // The learner model stages the newly-assigned member as a non-voting learner
        // (catch-up before promotion) rather than adding it straight to the voting
        // quorum. It's promoted via AddPeer once caught up — which can't happen for a
        // node with no real replica here — so it stays a learner, never a voter.
        let raft = store.groups.get(&gid).expect("group exists");
        assert!(
            raft.is_learner(&node("n4")),
            "stable-leader ring check must stage the newly assigned member as a learner"
        );
        let log = store.storage.load_state(gid.0).log;
        assert!(
            !log.iter()
                .any(|e| e.command == RaftCommand::AddPeer(node("n4"))),
            "the learner must not be added straight to the quorum (no immediate AddPeer, log: {:?})",
            log.iter().map(|e| &e.command).collect::<Vec<_>>()
        );
    }

    #[test]
    fn ring_check_evicts_live_ex_owner_after_stability_window() {
        use crate::control_plane::consensus::messages::{AppendEntriesResponse, RaftRpc};
        use crate::control_plane::consensus::raft::state::RING_STABLE_OBSERVATIONS;

        // Ring has [n1, n2, n3, n9]; pick a group the ring assigns to
        // {n1, n2, n3} — n9 is alive (on the ring) but NOT an owner of this
        // group. Seed the Raft group with n9 as a voter anyway: the #135
        // ratchet, as left behind by an earlier reassignment.
        let (storage, _) = temp_storage();
        let n1 = node("n1");
        let all_nodes = [node("n1"), node("n2"), node("n3"), node("n9")];
        let mut store = new_store_with_topology(n1.clone(), storage, &all_nodes);

        let gid = store
            .topology
            .shard_groups_for_node(&n1)
            .iter()
            .find(|g| !g.members.contains(&node("n9")))
            .map(|g| g.id)
            .expect("some group of n1 must exclude n9");

        store.add_group(&shard(gid.0, all_nodes.to_vec()));
        elect_leader_for(&mut store, gid, &[node("n2"), node("n3"), node("n9")]);
        store.flush();

        // Ring members confirm replication up to the leader's last entry so
        // the caught-up gate holds; the stale n9 never acks.
        {
            let raft = store.groups.get_mut(&gid).unwrap();
            let term = raft.current_term();
            let last = raft.log_last_index();
            for peer in [node("n2"), node("n3")] {
                raft.handle_rpc(
                    peer.clone(),
                    RaftRpc::AppendEntriesResponse(AppendEntriesResponse {
                        term,
                        node_id: peer,
                        success: true,
                        last_log_index: last,
                    }),
                );
            }
            store.dirty.insert(gid);
        }
        store.flush();

        // Tick past the stability window, plus extra passes that must park
        // behind the in-flight removal: exactly one eviction, ever.
        for _ in 0..RING_STABLE_OBSERVATIONS + 2 {
            store.handle_consensus(RaftProtocolMessage::Timeout(
                RaftTimeoutCallback::RingCheckTimeout {
                    shard_group_id: gid,
                },
            ));
            store.flush();
        }

        let log = store.storage.load_state(gid.0).log;
        let removals = log
            .iter()
            .filter(|e| e.command == RaftCommand::RemovePeer(node("n9")))
            .count();
        assert_eq!(
            removals,
            1,
            "exactly one gated eviction of the live ex-owner (log: {:?})",
            log.iter().map(|e| &e.command).collect::<Vec<_>>()
        );
        assert!(
            !log.iter()
                .any(|e| e.command == RaftCommand::AddPeer(node("n9"))),
            "the eviction must not be paired with an AddPeer"
        );
    }

    // --- Leader-crash seal-end recovery -----------------------------------

    fn create_topic_via_store(store: &mut MultiRaft, name: &str, replica_set: Vec<NodeId>) {
        use crate::control_plane::metadata::command::{CreateTopic, MetadataCommand};
        use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
        let rf = replica_set.len() as u64;
        store
            .propose_internal(MetadataProposal {
                shard_group_id: TEST_GROUP_ID,
                command: MetadataCommand::CreateTopic(CreateTopic {
                    name: name.to_string(),
                    storage_policy: StoragePolicy {
                        retention_ms: Some(3_600_000),
                        replication_factor: rf,
                        partition_strategy: PartitionStrategy::AutoSplit,
                    },
                    replica_set,
                    created_at: 1000,
                }),
            })
            .expect("CreateTopic propose failed");
        store.flush(); // commit + persist + apply (single-node)
    }

    /// Targets a `SealBoundaryQuery` for `seg` was fanned out to (sorted).
    fn seal_boundary_query_targets(events: &[RaftEvent], seg: SegmentKey) -> Vec<NodeId> {
        use crate::data_plane::messages::command::DataPlaneInterNodeCommand;
        let mut out = Vec::new();
        for e in events {
            let RaftEvent::SealBoundaryQueries(cmds) = e else {
                continue;
            };
            for cmd in cmds {
                if let DataTransportCommand::SendToTargets(s) = cmd
                    && let DataPlaneInterNodeCommand::SealBoundaryQuery(q) = &s.message
                    && q.segment_key == seg
                {
                    out.extend(s.targets.iter().cloned());
                }
            }
        }
        out.sort();
        out
    }

    fn recovered_rolls(store: &MultiRaft, from: usize) -> Vec<RollSegment> {
        use crate::control_plane::metadata::command::MetadataCommand;
        proposals_after_become_leader(store)
            .into_iter()
            .skip(from)
            .filter_map(|c| match c {
                RaftCommand::Metadata(MetadataCommand::RollSegment(r)) => Some(r),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn seal_end_gather_seals_at_min_with_recency_leader() {
        use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};

        // Coordinator n1 leads the shard group; the segment's data replicas are
        // [x, y, z] with x as write leader. x is absent from the live cluster.
        let (storage, _tmp) = temp_storage();
        let me = node("n1");
        let mut store =
            new_store_with_topology(me.clone(), storage, &[node("n1"), node("y"), node("z")]);
        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));
        elect_leader(&mut store);
        store.flush();
        create_topic_via_store(&mut store, "t", vec![node("x"), node("y"), node("z")]);

        let seg0 = SegmentKey::new(TopicId((TEST_GROUP_ID.0) << 32), RangeId(0), SegmentId(0));

        // x (the leader) crashed: the death drives reconcile, which finds the
        // leaderless segment and starts a seal-end gather querying both
        // survivors.
        store.handle_node_death(node("x"));
        assert!(store.seal_recovery.contains(&seg0));
        let events = store.flush();
        assert_eq!(
            seal_boundary_query_targets(&events, seg0),
            vec![node("y"), node("z")],
            "query fans out to both survivors"
        );

        let before = proposals_after_become_leader(&store).len();

        // Survivors report durable extents; the second completes the gather.
        store.handle_seal_boundary_report(SealBoundaryReport {
            segment_key: seg0,
            from: node("y"),
            durable_end: Some(EntryId(50)),
        });
        store.handle_seal_boundary_report(SealBoundaryReport {
            segment_key: seg0,
            from: node("z"),
            durable_end: Some(EntryId(40)),
        });
        store.flush();

        let rolls = recovered_rolls(&store, before);
        assert_eq!(rolls.len(), 1, "the completed gather proposes one roll");
        assert_eq!(rolls[0].segment_key, seg0);
        assert_eq!(
            rolls[0].end_entry_id,
            Some(EntryId(40)),
            "seal at the min of survivor durable extents"
        );
        assert_eq!(
            rolls[0].new_replica_set.first(),
            Some(&node("y")),
            "the most-recent survivor (extent 50) leads the new segment"
        );
    }

    #[test]
    fn seal_end_gather_expires_to_unknown_end() {
        use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};

        let (storage, _tmp) = temp_storage();
        let me = node("n1");
        let mut store =
            new_store_with_topology(me.clone(), storage, &[node("n1"), node("y"), node("z")]);
        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));
        elect_leader(&mut store);
        store.flush();
        create_topic_via_store(&mut store, "t", vec![node("x"), node("y"), node("z")]);

        let seg0 = SegmentKey::new(TopicId((TEST_GROUP_ID.0) << 32), RangeId(0), SegmentId(0));
        let before = proposals_after_become_leader(&store).len();

        // No reports ever arrive: each death-driven reconcile re-drives the
        // gather; after the attempt budget it falls back to an unknown-end roll.
        for _ in 0..(seal_recovery::GATHER_ATTEMPTS + 2) {
            store.handle_node_death(node("x"));
        }
        store.flush();

        let rolls = recovered_rolls(&store, before);
        assert_eq!(rolls.len(), 1, "expiry proposes exactly one fallback roll");
        assert_eq!(rolls[0].segment_key, seg0);
        assert_eq!(
            rolls[0].end_entry_id, None,
            "an expired gather seals with an unknown end (today's fallback)"
        );
    }

    #[test]
    fn seal_end_gather_dropped_on_step_down() {
        use crate::control_plane::consensus::messages::{AppendEntries, RaftRpc};
        use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};

        let (storage, _tmp) = temp_storage();
        let me = node("n1");
        let mut store =
            new_store_with_topology(me.clone(), storage, &[node("n1"), node("y"), node("z")]);
        store.add_group(&shard(TEST_GROUP_ID.0, vec![me.clone()]));
        elect_leader(&mut store);
        store.flush();
        create_topic_via_store(&mut store, "t", vec![node("x"), node("y"), node("z")]);

        let seg0 = SegmentKey::new(TopicId((TEST_GROUP_ID.0) << 32), RangeId(0), SegmentId(0));
        store.handle_node_death(node("x"));
        assert!(
            store.seal_recovery.contains(&seg0),
            "the leader started a seal-end gather"
        );

        // A higher-term AppendEntries deposes n1. The drain-time `!is_leader`
        // checkpoint must drop the orphaned gather — it can no longer propose the
        // recovery roll, and the leader-gated ring-check won't fire to prune it.
        store.handle_consensus(InboundRaftRpc {
            shard_group_id: TEST_GROUP_ID,
            from: node("n2"),
            rpc: RaftRpc::AppendEntries(AppendEntries {
                term: 99,
                leader_id: node("n2"),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Box::new([]),
                leader_commit: 0,
            }),
        });
        store.flush();

        assert!(
            store.seal_recovery.is_empty(),
            "a deposed leader drops its seal-end gathers"
        );
    }
}
