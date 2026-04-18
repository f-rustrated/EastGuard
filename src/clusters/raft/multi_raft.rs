use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::clusters::NodeId;
use crate::clusters::raft::messages::{
    OutboundRaftPacket, RaftRpc, RaftTimeoutCallback, RaftTimer,
};
use crate::clusters::raft::raft::Raft;
use crate::clusters::swims::{ShardGroup, ShardGroupId};
use crate::schedulers::ticker_message::TickerCommand;

pub(crate) struct ShardGroupState {
    pub raft: Raft,
    pub election_seq: u32,  // global ticker seq for election timer
    pub heartbeat_seq: u32, // global ticker seq for heartbeat timer
}

// dirty is owned here because it's a persistence concern — flush() drains it.
pub(crate) struct MultiRaftStore {
    node_id: NodeId,
    groups: HashMap<ShardGroupId, ShardGroupState>,
    seq_counter: u32,
    dirty: HashSet<ShardGroupId>, // maybe we can separate dirty markings for storage / transport
    packets_by_target: HashMap<NodeId, Vec<OutboundRaftPacket>>,
    pending_timer_cmds: Vec<TickerCommand<RaftTimer>>,
}

impl MultiRaftStore {
    pub(crate) fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            groups: HashMap::new(),
            seq_counter: 0,
            dirty: HashSet::new(),
            packets_by_target: HashMap::new(),
            pending_timer_cmds: Vec::new(),
        }
    }

    pub(crate) fn ensure_group(&mut self, group: ShardGroup) {
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

        let election_seq = self.alloc_seq();
        let heartbeat_seq = self.alloc_seq();

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
            crate::schedulers::ticker_message::TimerCommand::CancelSchedule {
                seq: state.election_seq,
            }
            .into(),
        );
        self.pending_timer_cmds.push(
            crate::schedulers::ticker_message::TimerCommand::CancelSchedule {
                seq: state.heartbeat_seq,
            }
            .into(),
        );

        tracing::info!("[{}] Removed Raft group {:?}", self.node_id, group_id);
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

    pub(crate) fn step(&mut self, shard_id: ShardGroupId, from: NodeId, rpc: RaftRpc) {
        if let Some(state) = self.groups.get_mut(&shard_id) {
            state.raft.step(from, rpc);
            self.dirty.insert(shard_id);
        }
    }

    fn alloc_seq(&mut self) -> u32 {
        self.seq_counter = self.seq_counter.wrapping_add(1);
        self.seq_counter
    }
}
