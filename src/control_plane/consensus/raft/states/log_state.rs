use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::LogMutation;
use crate::control_plane::consensus::raft::log::LogEntry;
use crate::control_plane::consensus::raft::storage::RaftPersistentState;

pub(crate) struct LogState {
    current_term: u64,
    voted_for: Option<NodeId>,
    entries: Vec<LogEntry>,
    stabled_index: u64,
    unflushed_mutations: Vec<LogMutation>,
}

impl LogState {
    pub(crate) fn from_persistent(persistent: RaftPersistentState) -> Self {
        Self {
            stabled_index: persistent.stabled_index(),
            current_term: persistent.term,
            voted_for: persistent.voted_for,
            entries: persistent.log,
            unflushed_mutations: Vec::new(),
        }
    }

    pub(crate) fn current_term(&self) -> u64 {
        self.current_term
    }

    pub(crate) fn voted_for(&self) -> Option<&NodeId> {
        self.voted_for.as_ref()
    }

    pub(crate) fn vote_available_for(&self, candidate_id: &NodeId) -> bool {
        self.voted_for
            .as_ref()
            .is_none_or(|voted_for| voted_for == candidate_id)
    }

    pub(crate) fn begin_election(&mut self, node_id: &NodeId) -> u64 {
        self.current_term += 1;
        self.voted_for = Some(node_id.clone());
        self.buffer_hard_state();
        self.current_term
    }

    pub(crate) fn grant_vote(&mut self, candidate_id: NodeId) {
        self.voted_for = Some(candidate_id);
        self.buffer_hard_state();
    }

    pub(crate) fn advance_term(&mut self, new_term: u64) -> bool {
        if new_term <= self.current_term {
            return false;
        }
        self.current_term = new_term;
        self.voted_for = None;
        self.buffer_hard_state();
        true
    }

    pub(crate) fn stabled_index(&self) -> u64 {
        self.stabled_index
    }

    pub(crate) fn advance_stabled_index(&mut self, index: u64) {
        self.stabled_index = self.stabled_index.max(index);
    }

    pub(crate) fn entries(&self) -> &[LogEntry] {
        &self.entries
    }

    pub(crate) fn last_index(&self) -> u64 {
        self.entries.last().map_or(0, |entry| entry.index)
    }

    pub(crate) fn last_term(&self) -> u64 {
        self.entries.last().map_or(0, |entry| entry.term)
    }

    pub(crate) fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        self.get(index).map_or(0, |entry| entry.term)
    }

    pub(crate) fn get(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    pub(crate) fn entries_from(&self, start_index: u64) -> Box<[LogEntry]> {
        if start_index == 0 || start_index > self.last_index() {
            return Box::new([]);
        }
        self.entries[(start_index - 1) as usize..].into()
    }

    pub(crate) fn append(&mut self, entry: LogEntry) {
        debug_assert_eq!(
            entry.index,
            self.last_index() + 1,
            "log entry index must be contiguous"
        );
        self.unflushed_mutations
            .push(LogMutation::Append(entry.clone()));
        self.entries.push(entry);
    }

    pub(crate) fn truncate_from(&mut self, from_index: u64) {
        if from_index == 0 || from_index > self.last_index() + 1 {
            return;
        }
        self.unflushed_mutations
            .push(LogMutation::TruncateFrom(from_index));
        self.entries.truncate((from_index - 1) as usize);
    }

    fn buffer_hard_state(&mut self) {
        self.unflushed_mutations.push(LogMutation::HardState {
            term: self.current_term,
            voted_for: self.voted_for.clone(),
        });
    }

    pub(crate) fn take_mutations(&mut self) -> Vec<LogMutation> {
        std::mem::take(&mut self.unflushed_mutations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::consensus::raft::command::RaftCommand;

    #[test]
    fn append_and_truncate_emit_persistence_mutations() {
        let mut state = LogState::from_persistent(RaftPersistentState::default());
        let entry = LogEntry {
            term: 1,
            index: 1,
            command: RaftCommand::Noop,
        };

        state.append(entry.clone());
        state.truncate_from(1);

        assert_eq!(state.last_index(), 0);
        let mutations = state.take_mutations();
        assert!(matches!(
            &mutations[..],
            [
                LogMutation::Append(appended),
                LogMutation::TruncateFrom(1)
            ] if appended == &entry
        ));
    }

    #[test]
    fn election_transitions_buffer_hard_state() {
        let node = NodeId::new("node-1");
        let candidate = NodeId::new("node-2");
        let mut state = LogState::from_persistent(RaftPersistentState::default());

        assert_eq!(state.begin_election(&node), 1);
        assert_eq!(state.voted_for(), Some(&node));
        assert!(!state.vote_available_for(&candidate));

        assert!(state.advance_term(2));
        assert!(state.voted_for().is_none());
        state.grant_vote(candidate.clone());
        assert_eq!(state.voted_for(), Some(&candidate));

        let mutations = state.take_mutations();
        assert_eq!(mutations.len(), 3);
        assert!(matches!(
            mutations.last(),
            Some(LogMutation::HardState {
                term: 2,
                voted_for: Some(voted_node),
            }) if voted_node == &candidate
        ));
    }
}
