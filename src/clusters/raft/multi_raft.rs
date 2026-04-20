use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::clusters::NodeId;
use crate::clusters::raft::messages::{
    MultiRaftCommand, MultiRaftReply, ProposeError, RaftCommand, RaftEvent, RaftRpc,
    RaftTimeoutCallback,
};
use crate::clusters::raft::state::Raft;
use crate::clusters::swims::{ShardGroup, ShardGroupId};
use crate::schedulers::ticker_message::TimerCommand;

// dirty is owned here because it's a persistence concern — flush() drains it.
pub(crate) struct MultiRaft {
    node_id: NodeId,
    groups: HashMap<ShardGroupId, Raft>,
    seq_counter: RaftTimerTokenGenerator,
    dirty: HashSet<ShardGroupId>,
    pending_events: Vec<RaftEvent>,
}

impl MultiRaft {
    pub(crate) fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            groups: HashMap::new(),
            seq_counter: RaftTimerTokenGenerator::default(),
            dirty: HashSet::new(),
            pending_events: Vec::new(),
        }
    }

    pub(crate) fn handle_command(&mut self, cmd: MultiRaftCommand) -> MultiRaftReply {
        use MultiRaftCommand::*;
        match cmd {
            PacketReceived {
                shard_group_id,
                from,
                rpc,
            } => {
                self.step(shard_group_id, from, rpc);
                MultiRaftReply::None
            }
            Timeout(cb) => {
                self.handle_timeout(cb);
                MultiRaftReply::None
            }
            EnsureGroup { group } => {
                self.add_group(group);
                MultiRaftReply::None
            }
            RemoveGroup { group_id } => {
                self.remove_group(group_id);
                MultiRaftReply::None
            }
            GetLeader { group_id } => MultiRaftReply::GetLeader(self.get_leader(group_id)),
            Propose {
                shard_group_id,
                command,
            } => MultiRaftReply::Propose(self.propose(shard_group_id, command)),
            HandleNodeDeath { dead_node_id } => {
                self.remove_node(dead_node_id);
                MultiRaftReply::None
            }
            HandleNodeJoin {
                new_node_id,
                affected_groups,
            } => {
                self.add_node(new_node_id, affected_groups);
                MultiRaftReply::None
            }
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

        let jitter = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            self.node_id.hash(&mut hasher);
            (hasher.finish() % 20) as u32
        };

        let election_seq = self.seq_counter.generate();
        let heartbeat_seq = self.seq_counter.generate();

        let raft = Raft::new(
            self.node_id.clone(),
            peers,
            jitter,
            group.id,
            election_seq,
            heartbeat_seq,
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

    fn remove_group(&mut self, group_id: ShardGroupId) {
        let Some(raft) = self.groups.remove(&group_id) else {
            return;
        };

        self.pending_events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: raft.election_seq(),
            }));
        self.pending_events
            .push(RaftEvent::Timer(TimerCommand::CancelSchedule {
                seq: raft.heartbeat_seq(),
            }));

        tracing::info!("[{}] Removed Raft group {:?}", self.node_id, group_id);
    }

    fn step(&mut self, shard_id: ShardGroupId, from: NodeId, rpc: RaftRpc) {
        if let Some(raft) = self.groups.get_mut(&shard_id) {
            raft.step(from, rpc);
            self.dirty.insert(shard_id);
        }
    }

    fn handle_timeout(&mut self, cb: RaftTimeoutCallback) {
        let shard_id = match &cb {
            RaftTimeoutCallback::Ignored => return,
            RaftTimeoutCallback::ElectionTimeout { shard_group_id } => *shard_group_id,
            RaftTimeoutCallback::HeartbeatTimeout { shard_group_id } => *shard_group_id,
        };
        if let Some(raft) = self.groups.get_mut(&shard_id) {
            raft.handle_timeout(cb);
            self.dirty.insert(shard_id);
        }
    }

    fn get_leader(&self, group_id: ShardGroupId) -> Option<NodeId> {
        self.groups
            .get(&group_id)
            .and_then(|r| r.current_leader().cloned())
    }

    fn remove_node(&mut self, node_id: NodeId) {
        self.pending_events
            .push(RaftEvent::DisconnectPeer(node_id.clone()));

        let affected: Vec<ShardGroupId> = self
            .groups
            .iter()
            .filter(|(_, r)| r.is_leader() && r.has_peer(&node_id))
            .map(|(id, _)| *id)
            .collect();

        for group_id in &affected {
            if let Some(raft) = self.groups.get_mut(group_id) {
                let _ = raft.propose(RaftCommand::RemovePeer(node_id.clone()));
            }
        }

        self.dirty.extend(affected);
    }

    fn add_node(&mut self, node_id: NodeId, affected_groups: Vec<ShardGroup>) {
        for group in &affected_groups {
            if let Some(raft) = self.groups.get_mut(&group.id)
                && raft.is_leader()
                && !raft.has_peer(&node_id)
            {
                let _ = raft.propose(RaftCommand::AddPeer(node_id.clone()));
                self.dirty.insert(group.id);
            }
        }

        for group in affected_groups {
            if !self.groups.contains_key(&group.id) && group.members.contains(&self.node_id) {
                self.add_group(group);
            }
        }
    }

    fn propose(
        &mut self,
        shard_id: ShardGroupId,
        command: RaftCommand,
    ) -> Result<(), ProposeError> {
        match self.groups.get_mut(&shard_id) {
            Some(raft) => {
                let result = raft.propose(command);
                self.dirty.insert(shard_id);
                result
            }
            None => Err(ProposeError::NotLeader),
        }
    }

    pub(crate) fn flush(&mut self) -> Vec<RaftEvent> {
        for id in std::mem::take(&mut self.dirty) {
            let Some(raft) = self.groups.get_mut(&id) else {
                continue;
            };
            // RocksDB integration point: replace the discard below with a batch write.
            // take_log_mutations() returns Vec<LogMutation> — iterate and apply each variant:
            //   LogMutation::Append(entry)        → cf_log.put((id, entry.index), bincode(entry))
            //   LogMutation::TruncateFrom(index)  → delete_range cf_log (id, index)..(id, u64::MAX)
            //   LogMutation::HardState { .. }     → cf_meta.put((id, "hard_state"), bincode(...))
            // Batch all mutations for all dirty groups into one WriteBatch before db.write().
            let _ = raft.take_log_mutations();
            self.pending_events.extend(raft.take_events());
        }

        std::mem::take(&mut self.pending_events)
    }
}

#[derive(Debug, Default)]
struct RaftTimerTokenGenerator(u32);
impl RaftTimerTokenGenerator {
    fn generate(&mut self) -> u32 {
        self.0 = self.0.wrapping_add(1);
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn node(id: &str) -> NodeId {
        NodeId::new(id)
    }

    fn shard(id: u64, members: Vec<NodeId>) -> ShardGroup {
        ShardGroup {
            id: ShardGroupId(id),
            members,
        }
    }

    fn timer_seqs(events: &[RaftEvent]) -> Vec<u32> {
        events
            .iter()
            .filter_map(|e| match e {
                RaftEvent::Timer(TimerCommand::SetSchedule { seq, .. }) => Some(*seq),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn timer_seqs_are_unique_across_groups() {
        let me = node("n1");
        let mut store = MultiRaft::new(me.clone());

        store.add_group(shard(1, vec![me.clone(), node("n2")]));
        store.add_group(shard(2, vec![me.clone(), node("n2")]));

        let events = store.flush();
        let seqs = timer_seqs(&events);
        let unique: HashSet<u32> = seqs.iter().cloned().collect();
        assert_eq!(seqs.len(), unique.len());
    }
}
