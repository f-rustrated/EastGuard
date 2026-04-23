use crate::clusters::NodeId;
use crate::clusters::raft::log::LogEntry;

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
