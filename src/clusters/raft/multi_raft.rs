use std::collections::{HashMap, HashSet};
use crate::clusters::NodeId;
use crate::clusters::raft::messages::{OutboundRaftPacket, RaftTimer};
use crate::clusters::raft::raft::Raft;
use crate::clusters::swims::{ShardGroup, ShardGroupId};
use crate::schedulers::ticker_message::TickerCommand;

pub(crate) struct ShardGroupState {
    pub raft: Raft,
    pub election_seq: u32,  // global ticker seq for election timer
    pub heartbeat_seq: u32, // global ticker seq for heartbeat timer
}

pub(crate) struct MultiRaftStore {
    node_id: NodeId,
    groups: HashMap<ShardGroupId, ShardGroupState>,
    seq_counter: u32,
    dirty: HashSet<ShardGroupId>,
    packets_by_target: HashMap<NodeId, Vec<OutboundRaftPacket>>,
    pending_timer_cmds: Vec<TickerCommand<RaftTimer>>
}