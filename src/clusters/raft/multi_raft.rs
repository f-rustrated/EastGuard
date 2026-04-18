use crate::clusters::NodeId;
use crate::clusters::raft::messages::{
    OutboundRaftPacket, RaftRpc, RaftTimeoutCallback, RaftTimer,
};
use crate::clusters::raft::raft::Raft;
use crate::clusters::swims::{ShardGroup, ShardGroupId};
use crate::schedulers::ticker_message::TickerCommand;
use std::collections::{HashMap, HashSet};

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
    dirty: HashSet<ShardGroupId>,
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
}
