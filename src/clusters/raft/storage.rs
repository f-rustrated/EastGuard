use crate::clusters::NodeId;
use crate::clusters::raft::log::LogEntry;
use crate::clusters::raft::messages::LogMutation;
use crate::clusters::swims::ShardGroupId;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
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

pub(crate) trait RaftStorage: Send {
    fn load_state(&self, group_id: u64) -> RaftPersistentState;
    fn persist_mutations(&self, mutations: Vec<(ShardGroupId, LogMutation)>);
    fn delete_group(&self, group_id: ShardGroupId);
}
