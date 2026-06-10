use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::{
    ClientProposalError, CoordinatorSealRequest, DeferredReply, InboundRaftRpc, LogMutation,
    MetadataProposal, MultiRaftActorCommand, RaftEvent, RaftProtocolMessage, RaftTimeoutCallback,
};
use crate::control_plane::consensus::raft::command::RaftCommand;
use crate::control_plane::consensus::raft::state::{Raft, TimerSeqs};
use crate::control_plane::consensus::raft::storage::RaftStorage;
use crate::control_plane::consensus::raft::{compute_replacement_replica_set, now_ms};
use crate::control_plane::membership::{ShardGroup, ShardGroupId, TopologyReader};
use crate::control_plane::metadata::command::RollSegment;
use crate::control_plane::metadata::{TopicId, TopicMeta, TopicStats};
use crate::data_plane::SegmentKey;
use crate::data_plane::messages::command::SegmentAssignmentAck;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use tokio::sync::oneshot;

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
    pending_proposes:
        BTreeMap<(ShardGroupId, u64), oneshot::Sender<Result<(), ClientProposalError>>>,

    pending_seals: PendingSealTracker,

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
            topology,
        }
    }

    pub(crate) fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Invoked on `LeaderChange → self` for takeover reconciliation:
    /// 1. assert the group's current ring membership into the log, healing
    ///    replicas whose genesis peer set was seeded from a partial ring;
    /// 2. close the gap where the previous leader died before proposing the
    ///    removals (or their pairings) for peers SWIM no longer considers live.
    pub(crate) fn reconcile_on_leadership_change(&mut self, shard_group_id: ShardGroupId) {
        let Some(raft) = self.groups.get_mut(&shard_group_id) else {
            return;
        };
        if !raft.is_leader() {
            return;
        }

        let live_set: HashSet<NodeId> = self.topology.live_nodes().into_iter().collect();

        // Genesis peer sets are seeded from each replica's *local* ring
        // snapshot at creation and can diverge on a partially-joined ring
        // (#133: livelocks the group once that leader dies).
        // Peer sets mutate only through the log, so on takeover assert the group's ring membership as AddPeer entries:
        // `apply_add_peer` is idempotent and self-skipping, so healthy
        // replicas no-op while divergent ones heal. Dead members are skipped
        // replacement is `reconcile_peers`'s job, rejoin is `add_node`'s.
        let mut membership_asserted = false;

        if let Some(members) = self
            .topology
            .resolve_nodes_in_group(&self.node_id, shard_group_id)
        {
            for member in members {
                if member == self.node_id || !live_set.contains(&member) {
                    continue;
                }
                if raft.propose(RaftCommand::AddPeer(member.clone())).is_ok() {
                    membership_asserted = true;
                } else {
                    tracing::warn!(
                        "Takeover membership assert AddPeer({:?}) on {:?} rejected",
                        member,
                        shard_group_id,
                    );
                }
            }
        }

        let peer_reconciled = raft.reconcile_peers(&self.topology, &live_set);
        let segment_reconciled = raft.reconcile_segments(&live_set);

        if membership_asserted || peer_reconciled || segment_reconciled {
            self.dirty.insert(shard_group_id);
        };
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
            MultiRaftActorCommand::Coordinator(req) => {
                self.handle_seal_request(req);
            }
            MultiRaftActorCommand::AssignmentAck(ack) => {
                self.handle_assignment_ack(ack);
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
            }
        }
    }

    pub(crate) fn handle_consensus(&mut self, cmd: impl Into<RaftProtocolMessage>) {
        match cmd.into() {
            RaftProtocolMessage::InboundRaftRpc(cmd) => self.handle_rpc(cmd),
            RaftProtocolMessage::Timeout(cb) => self.handle_timeout(cb),
            RaftProtocolMessage::EnsureGroup(cmd) => self.add_group(cmd.group),
            RaftProtocolMessage::RemoveGroup(cmd) => self.remove_group(cmd.group_id),
            RaftProtocolMessage::HandleNodeDeath(cmd) => self.handle_node_death(cmd.dead_node_id),
            RaftProtocolMessage::HandleNodeJoin(cmd) => self.add_node(cmd.new_node_id),
        }
    }

    fn add_group(&mut self, group: ShardGroup) {
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

        let election_jitter_seed = self.create_election_jitter_seed(&group);

        let persistent = self.storage.load_state(group.id.0);

        let timer_seqs = TimerSeqs {
            election: self.seq_counter.generate(),
            heartbeat: self.seq_counter.generate(),
            merge_check: self.seq_counter.generate(),
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
        group.id.hash(&mut hasher);
        hasher.finish()
    }

    fn remove_group(&mut self, group_id: ShardGroupId) {
        let Some(mut raft) = self.groups.remove(&group_id) else {
            return;
        };
        raft.cancel_all_timers();
        self.pending_events.extend(raft.take_events());

        let pending_keys: Vec<_> = self
            .pending_proposes
            .keys()
            .filter(|(gid, _)| *gid == group_id)
            .cloned()
            .collect();
        for key in pending_keys {
            if let Some(sender) = self.pending_proposes.remove(&key) {
                let _ = sender.send(Err(ClientProposalError::ShardGroupRemoved));
            }
        }

        self.storage.delete_group(group_id);

        tracing::info!("[{}] Removed Raft group {:?}", self.node_id, group_id);
    }

    fn handle_rpc(&mut self, cmd: InboundRaftRpc) {
        if let Some(raft) = self.groups.get_mut(&cmd.shard_group_id) {
            raft.handle_rpc(cmd.from, cmd.rpc);
            self.dirty.insert(cmd.shard_group_id);
        }
    }

    fn handle_timeout(&mut self, cb: RaftTimeoutCallback) {
        let shard_id = match &cb {
            RaftTimeoutCallback::Ignored => return,
            RaftTimeoutCallback::ElectionTimeout { shard_group_id, .. }
            | RaftTimeoutCallback::RpcTimeout { shard_group_id }
            | RaftTimeoutCallback::MergeCheckTimeout { shard_group_id, .. } => *shard_group_id,
        };

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

    fn get_peers(&self, group_id: ShardGroupId) -> Vec<NodeId> {
        self.groups
            .get(&group_id)
            .map(|r| r.peers_iter().cloned().collect())
            .unwrap_or_default()
    }

    fn get_topics(&self) -> Vec<String> {
        self.groups
            .values()
            .flat_map(|raft| raft.topic_names())
            .collect()
    }

    fn get_topic_stats(&self) -> Vec<TopicStats> {
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

    // Full scan over groups is acceptable — node death is rare (~6-7s SWIM detection)
    // and the scan is O(groups) with cheap has_peer/is_leader checks.
    //
    // Membership changes flow through the Raft log: only the leader proposes
    // `RemovePeer`, and followers apply on commit. Direct mutation would let
    // replicas disagree on quorum size during gossip convergence. See
    // `diagrams/metadata-management/metadata_management_roadmap.md` Phase 3d.
    //
    // The live set is loaded fresh from the topology snapshot — drift between
    // event emission and processing is absorbed naturally, and there's one
    // canonical source of "current cluster shape" (the topology reader).
    fn handle_node_death(&mut self, dead_node_id: NodeId) {
        let live_set: HashSet<NodeId> = self.topology.live_nodes().into_iter().collect();
        for (group_id, raft) in self.groups.iter_mut() {
            if raft.handle_node_death(&dead_node_id, &live_set, &self.topology) {
                self.dirty.insert(*group_id);
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
    fn add_node(&mut self, node_id: NodeId) {
        let affected_groups: Vec<ShardGroup> = self.topology.shard_groups_for_node(&node_id);
        if affected_groups.is_empty() {
            return;
        }
        self.add_peer_to_existing_groups(&node_id, &affected_groups);
        self.ensure_new_groups(affected_groups);
    }

    fn add_peer_to_existing_groups(&mut self, node_id: &NodeId, groups: &[ShardGroup]) {
        for group in groups {
            let Some(raft) = self.groups.get_mut(&group.id) else {
                continue;
            };
            if !raft.is_leader() || raft.has_peer(node_id) {
                continue;
            }
            if let Err(e) = raft.propose(RaftCommand::AddPeer(node_id.clone())) {
                tracing::warn!(
                    "Join-triggered AddPeer({:?}) on {:?} rejected: {:?}",
                    node_id,
                    group.id,
                    e
                );
                continue;
            }
            self.dirty.insert(group.id);
        }
    }

    fn ensure_new_groups(&mut self, groups: Vec<ShardGroup>) {
        for group in groups {
            if !self.groups.contains_key(&group.id) && group.members.contains(&self.node_id) {
                self.add_group(group);
            }
        }
    }

    fn propose(
        &mut self,
        cmd: MetadataProposal,
        reply: oneshot::Sender<Result<(), ClientProposalError>>,
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

    fn propose_internal(&mut self, cmd: MetadataProposal) -> Result<u64, ClientProposalError> {
        match self.groups.get_mut(&cmd.shard_group_id) {
            Some(raft) => {
                let result = raft.propose(cmd.command.into());
                self.dirty.insert(cmd.shard_group_id);
                result
            }
            None => Err(ClientProposalError::ShardNotFound),
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
                    self.pending_seals.drop_from(*id, *from_index);
                }
                mutations.push((*id, log));
            }
            // If this replica is no longer leader, drop any outstanding seal
            // contexts — the SealResponse for those proposals will not come from
            // here. Avoids unbounded growth of `pending_seals`
            if !raft.is_leader() {
                self.pending_seals.drop_group(*id);
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
                        let _ = sender.send(Err(ClientProposalError::NotLeader(None)));
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
        self.storage.persist_mutations(mutations);
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

        store.add_group(shard(1, vec![me.clone(), node("n2")]));
        store.add_group(shard(2, vec![me.clone(), node("n2")]));

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

        store.add_group(shard(42, vec![me.clone(), node("n2")]));

        assert!(store.groups.contains_key(&ShardGroupId(42)));
    }

    #[test]
    fn remove_group_deletes_data() {
        let (storage, _) = temp_storage();
        let me = node("n1");
        let mut store = new_store(me.clone(), storage);

        store.add_group(shard(42, vec![me.clone()]));
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

        store.add_group(shard(42, vec![node("n2"), node("n3")]));

        assert!(!store.groups.contains_key(&ShardGroupId(42)));
    }

    #[test]
    fn restart_recovers_persisted_state() {
        let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let me = node("n1");

        {
            let db = MetadataStorage::open(path.clone());
            let mut store = new_store(me.clone(), Box::new(db));
            store.add_group(shard(1, vec![me.clone()]));
            store.add_group(shard(2, vec![me.clone()]));
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
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

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
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

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
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

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
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));
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
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone(), n2.clone()]));
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
                entries: vec![
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
                ],
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
                entries: vec![LogEntry {
                    term: 2,
                    index: 1,
                    command: RaftCommand::Noop,
                }],
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
            store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));
            elect_leader(&mut store);
            store.flush();
            propose_noop(&mut store);
            store.flush();
            expected_last_index = store.groups.get(&TEST_GROUP_ID).unwrap().log_last_index();
        }

        let db = MetadataStorage::open(path);
        let mut store = new_store(me.clone(), Box::new(db));
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

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
            store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));
            elect_leader(&mut store);
            store.flush();
        }

        let db = MetadataStorage::open(path);
        let mut store = new_store(me.clone(), Box::new(db));
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));
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
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

        elect_leader(&mut store);
        store.flush();

        let cmd = MetadataCommand::CreateTopic(CreateTopic {
            name: "test-topic".to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
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
        store.add_group(shard(1, vec![me.clone()]));
        store.add_group(shard(2, vec![me.clone()]));
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
        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone()]));

        elect_leader(&mut store);
        store.flush();

        let make_cmd = || {
            MetadataCommand::CreateTopic(CreateTopic {
                name: "blue".to_string(),
                storage_policy: StoragePolicy {
                    retention_ms: 3_600_000,
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
    fn handle_node_death_pairs_remove_with_ring_aware_add() {
        // Cluster has 5 nodes on the ring; the test group has 3 of them.
        // After the leader processes the death of one member, it must propose
        // RemovePeer for the dead node AND AddPeer for a ring-picked replacement
        // chosen from the 2 non-member nodes.
        let (storage, _) = temp_storage();
        let me = node("me");
        let peers = vec![node("n2"), node("n3")];
        let group_members = std::iter::once(me.clone())
            .chain(peers.iter().cloned())
            .collect::<Vec<_>>();
        let all_nodes = vec![me.clone(), node("n2"), node("n3"), node("n4"), node("n5")];
        let mut store = new_store_with_topology(me.clone(), storage, &all_nodes);

        store.add_group(shard(TEST_GROUP_ID.0, group_members.clone()));
        elect_leader_with_peers(&mut store, &peers);
        store.flush();

        store.handle_node_death(node("n2"));
        store.flush();

        let proposals = proposals_after_become_leader(&store);
        assert_eq!(
            proposals.len(),
            2,
            "death of a peer must propose exactly RemovePeer + paired AddPeer (got {:?})",
            proposals
        );
        assert_eq!(proposals[0], RaftCommand::RemovePeer(node("n2")));

        let add_target = match &proposals[1] {
            RaftCommand::AddPeer(t) => t.clone(),
            other => panic!("expected AddPeer paired with RemovePeer, got {:?}", other),
        };
        let current: HashSet<NodeId> = group_members.iter().cloned().collect();
        assert!(
            !current.contains(&add_target),
            "AddPeer target {:?} must not be a current group member",
            add_target
        );
        assert_ne!(
            add_target,
            node("n2"),
            "AddPeer target must not be the just-removed peer"
        );
        assert!(
            add_target == node("n4") || add_target == node("n5"),
            "AddPeer target must be one of the ring's non-member nodes, got {:?}",
            add_target
        );
    }

    #[test]
    fn handle_node_death_degrades_when_pool_exhausted() {
        // Cluster is exactly [me, n2] — once n2 dies, no replacement is
        // available. RemovePeer must still be proposed; AddPeer must not.
        let (storage, _) = temp_storage();
        let me = node("me");
        let peers = vec![node("n2")];
        let all_nodes = vec![me.clone(), node("n2")];
        let mut store = new_store_with_topology(me.clone(), storage, &all_nodes);

        store.add_group(shard(TEST_GROUP_ID.0, vec![me.clone(), node("n2")]));
        elect_leader_with_peers(&mut store, &peers);
        store.flush();

        store.handle_node_death(node("n2"));
        store.flush();

        let proposals = proposals_after_become_leader(&store);
        assert_eq!(
            proposals,
            vec![RaftCommand::RemovePeer(node("n2"))],
            "exhausted pool must yield only the removal, no paired addition"
        );
    }
}
