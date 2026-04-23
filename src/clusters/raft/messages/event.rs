use crate::clusters::NodeId;
use crate::clusters::raft::log::LogEntry;
use crate::clusters::raft::messages::rpc::OutboundRaftPacket;
use crate::clusters::raft::messages::timer::RaftTimer;
use crate::clusters::swims::ShardGroupId;
use crate::impl_from_variant;
use crate::schedulers::ticker_message::TimerCommand;

#[derive(Debug, Clone)]
pub struct LeaderChange {
    pub shard_group_id: ShardGroupId,
    pub leader_node_id: NodeId,
    pub term: u64,
}

#[derive(Debug, Clone, Default)]
pub struct RaftPersistentState {
    pub(crate) term: u64,
    pub(crate) voted_for: Option<NodeId>,
    pub(crate) log: Vec<LogEntry>,
}

impl RaftPersistentState {
    pub(crate) fn stabled_index(&self) -> u64 {
        self.log.last().map_or(0, |e| e.index)
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum LogMutation {
    Append(LogEntry),
    TruncateFrom(u64),
    HardState { term: u64, voted_for: Option<NodeId> },
}

#[derive(Debug)]
pub enum RaftEvent {
    OutboundRaftPacket(OutboundRaftPacket),
    Timer(TimerCommand<RaftTimer>),
    LeaderChange(LeaderChange),
    DisconnectPeer(NodeId),
}

impl_from_variant!(RaftEvent, LeaderChange, OutboundRaftPacket);
