use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::clusters::NodeId;
use crate::clusters::raft::messages::{
    ELECTION_TIMER_SEQ, HEARTBEAT_TIMER_SEQ, LeaderChange, OutboundRaftPacket, ProposeError,
    RaftCommand, RaftEvent, RaftRpc, RaftTimeoutCallback, RaftTimer,
};
use crate::clusters::raft::state::Raft;
use crate::clusters::swims::{ShardGroup, ShardGroupId};
use crate::schedulers::ticker_message::{TickerCommand, TimerCommand};

pub(crate) struct ShardGroupState {
    pub raft: Raft,
    pub election_seq: u32,  // global ticker seq for election timer
    pub heartbeat_seq: u32, // global ticker seq for heartbeat timer
}

// dirty is owned here because it's a persistence concern — flush() drains it.
pub(crate) struct MultiRaft {
    node_id: NodeId,
    groups: HashMap<ShardGroupId, ShardGroupState>,
    seq_counter: RaftTimerTokenGenerator,
    dirty: HashSet<ShardGroupId>,
    packets_by_target: HashMap<NodeId, Vec<OutboundRaftPacket>>,
    pending_timer_cmds: Vec<TickerCommand<RaftTimer>>,
    pending_leader_changes: Vec<LeaderChange>,
}

impl MultiRaft {
    pub(crate) fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            groups: HashMap::new(),
            seq_counter: RaftTimerTokenGenerator::default(),
            dirty: HashSet::new(),
            packets_by_target: HashMap::new(),
            pending_timer_cmds: Vec::new(),
            pending_leader_changes: Vec::new(),
        }
    }

    pub(crate) fn add_group(&mut self, group: ShardGroup) {
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

        let raft = Raft::new(self.node_id.clone(), peers, jitter, group.id);

        tracing::info!(
            "[{}] Created Raft group {:?} with {} peers",
            self.node_id,
            group.id,
            raft.peers_count()
        );

        self.groups.insert(
            group.id,
            ShardGroupState {
                raft,
                election_seq,
                heartbeat_seq,
            },
        );
        self.dirty.insert(group.id);
    }

    pub(crate) fn remove_group(&mut self, group_id: ShardGroupId) {
        let Some(state) = self.groups.remove(&group_id) else {
            return;
        };

        self.pending_timer_cmds.push(
            TimerCommand::CancelSchedule {
                seq: state.election_seq,
            }
            .into(),
        );
        self.pending_timer_cmds.push(
            TimerCommand::CancelSchedule {
                seq: state.heartbeat_seq,
            }
            .into(),
        );

        tracing::info!("[{}] Removed Raft group {:?}", self.node_id, group_id);
    }

    pub(crate) fn step(&mut self, shard_id: ShardGroupId, from: NodeId, rpc: RaftRpc) {
        if let Some(state) = self.groups.get_mut(&shard_id) {
            state.raft.step(from, rpc);
            self.dirty.insert(shard_id);
        }
    }

    pub(crate) fn handle_timeout(&mut self, cb: RaftTimeoutCallback) {
        let shard_id = match &cb {
            RaftTimeoutCallback::Ignored => return,
            RaftTimeoutCallback::ElectionTimeout { shard_group_id } => *shard_group_id,
            RaftTimeoutCallback::HeartbeatTimeout { shard_group_id } => *shard_group_id,
        };
        if let Some(state) = self.groups.get_mut(&shard_id) {
            state.raft.handle_timeout(cb);
            self.dirty.insert(shard_id);
        }
    }

    pub(crate) fn get_leader(&self, group_id: ShardGroupId) -> Option<NodeId> {
        self.groups
            .get(&group_id)
            .and_then(|s| s.raft.current_leader().cloned())
    }

    pub(crate) fn remove_node(&mut self, node_id: NodeId) {
        let affected: Vec<ShardGroupId> = self
            .groups
            .iter()
            .filter(|(_, s)| s.raft.is_leader() && s.raft.has_peer(&node_id))
            .map(|(id, _)| *id)
            .collect();

        for group_id in &affected {
            if let Some(state) = self.groups.get_mut(group_id) {
                let _ = state.raft.propose(RaftCommand::RemovePeer(node_id.clone()));
            }
        }

        self.dirty.extend(affected);
    }

    pub(crate) fn add_node(&mut self, node_id: NodeId, affected_groups: Vec<ShardGroup>) {
        for group in &affected_groups {
            if let Some(state) = self.groups.get_mut(&group.id)
                && state.raft.is_leader()
                && !state.raft.has_peer(&node_id)
            {
                let _ = state.raft.propose(RaftCommand::AddPeer(node_id.clone()));
                self.dirty.insert(group.id);
            }
        }

        for group in affected_groups {
            if !self.groups.contains_key(&group.id) && group.members.contains(&self.node_id) {
                self.add_group(group);
            }
        }
    }

    pub(crate) fn propose(
        &mut self,
        shard_id: ShardGroupId,
        command: RaftCommand,
    ) -> Result<(), ProposeError> {
        match self.groups.get_mut(&shard_id) {
            Some(state) => {
                let result = state.raft.propose(command);
                self.dirty.insert(shard_id);
                result
            }
            None => Err(ProposeError::NotLeader),
        }
    }

    pub(crate) fn flush(&mut self) {
        for id in std::mem::take(&mut self.dirty) {
            let state = self.groups.get_mut(&id).unwrap();
            // RocksDB integration point: replace the discard below with a batch write.
            // take_log_mutations() returns Vec<LogMutation> — iterate and apply each variant:
            //   LogMutation::Append(entry)        → cf_log.put((id, entry.index), bincode(entry))
            //   LogMutation::TruncateFrom(index)  → delete_range cf_log (id, index)..(id, u64::MAX)
            //   LogMutation::HardState { .. }     → cf_meta.put((id, "hard_state"), bincode(...))
            // Batch all mutations for all dirty groups into one WriteBatch before db.write().
            let _ = state.raft.take_log_mutations();
            let events = state.raft.take_events();
            for event in events {
                self.route_event(id, event);
            }
        }
    }

    pub(crate) fn take_all_outbound(&mut self) -> HashMap<NodeId, Vec<OutboundRaftPacket>> {
        std::mem::take(&mut self.packets_by_target)
    }

    pub(crate) fn take_all_timer_commands(&mut self) -> Vec<TickerCommand<RaftTimer>> {
        std::mem::take(&mut self.pending_timer_cmds)
    }

    pub(crate) fn take_leader_changes(&mut self) -> Vec<LeaderChange> {
        std::mem::take(&mut self.pending_leader_changes)
    }

    fn route_event(&mut self, group_id: ShardGroupId, event: RaftEvent) {
        match event {
            RaftEvent::OutboundRaftPacket(pkt) => {
                self.packets_by_target
                    .entry(pkt.target.clone())
                    .or_default()
                    .push(pkt);
            }
            RaftEvent::Timer(cmd) => {
                if let Some(cmd) = self.translate_timer_seq(group_id, cmd) {
                    self.pending_timer_cmds.push(cmd.into());
                }
            }
            RaftEvent::LeaderChange(lc) => {
                self.pending_leader_changes.push(lc);
            }
        }
    }

    fn translate_timer_seq(
        &self,
        group_id: ShardGroupId,
        cmd: TimerCommand<RaftTimer>,
    ) -> Option<TimerCommand<RaftTimer>> {
        let state = self.groups.get(&group_id)?;
        Some(match cmd {
            TimerCommand::SetSchedule { seq, timer } => {
                let global_seq = if seq == ELECTION_TIMER_SEQ {
                    state.election_seq
                } else if seq == HEARTBEAT_TIMER_SEQ {
                    state.heartbeat_seq
                } else {
                    return None;
                };
                TimerCommand::SetSchedule {
                    seq: global_seq,
                    timer,
                }
            }
            TimerCommand::CancelSchedule { seq } => {
                let global_seq = if seq == ELECTION_TIMER_SEQ {
                    state.election_seq
                } else if seq == HEARTBEAT_TIMER_SEQ {
                    state.heartbeat_seq
                } else {
                    return None;
                };
                TimerCommand::CancelSchedule { seq: global_seq }
            }
        })
    }
}

#[derive(Debug, Default)]
struct RaftTimerTokenGenerator(u32);
impl RaftTimerTokenGenerator {
    // Lifecycle:
    //   - add_group() → allocates 2 unique seqs, marks dirty → flush() drains Raft::new()'s election timer (from reset_election_timer()) through translation layer
    //   - remove_group() → cancels both global seqs in ticker
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

    fn set_seqs(cmds: &[TickerCommand<RaftTimer>]) -> Vec<u32> {
        cmds.iter()
            .filter_map(|c| match c {
                TickerCommand::Schedule(TimerCommand::SetSchedule { seq, .. }) => Some(*seq),
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
        store.flush();

        let seqs = set_seqs(&store.take_all_timer_commands());
        let unique: HashSet<u32> = seqs.iter().cloned().collect();
        assert_eq!(seqs.len(), unique.len());
    }
}
