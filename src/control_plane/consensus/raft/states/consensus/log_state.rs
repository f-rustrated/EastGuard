use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::LogMutation;
use crate::control_plane::consensus::raft::log::LogEntry;
use crate::control_plane::consensus::raft::storage::{RaftPersistentState, RaftSnapshot};

pub(crate) struct LogState {
    current_term: u64,
    voted_for: Option<NodeId>,
    entries: Vec<LogEntry>,
    snapshot: SnapshotState,
    stabled_index: u64,
    unflushed_mutations: Vec<LogMutation>,
}

#[derive(Debug)]
pub(crate) struct SnapshotAlreadyStaged;

enum SnapshotState {
    None,
    // Staged represents a transition between two durable states.
    // for example,
    //  - Active(S10) is currently persisted.
    //  - A newer S20 is created or received.
    //  - Before persistence completes, state becomes Staged { active: S10, snapshot: S20 }.
    //  - After persistence, it becomes Active(S20).
    Staged {
        active: Option<RaftSnapshot>,
        snapshot: RaftSnapshot,
    },
    Active(RaftSnapshot),
}

impl SnapshotState {
    fn active(&self) -> Option<&RaftSnapshot> {
        match self {
            Self::Staged { active, .. } => active.as_ref(),
            Self::Active(snapshot) => Some(snapshot),
            Self::None => None,
        }
    }
}

impl LogState {
    pub(crate) fn from_persistent(persistent: RaftPersistentState) -> Self {
        let stabled_index = persistent.stabled_index();
        Self {
            stabled_index,
            current_term: persistent.term,
            voted_for: persistent.voted_for,
            entries: persistent.log,
            snapshot: persistent
                .snapshot
                .map_or(SnapshotState::None, SnapshotState::Active),
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

    pub(crate) fn last_included_index(&self) -> u64 {
        self.snapshot
            .active()
            .map_or(0, |snapshot| snapshot.meta.last_included_index)
    }

    fn last_included_term(&self) -> u64 {
        self.snapshot
            .active()
            .map_or(0, |snapshot| snapshot.meta.last_included_term)
    }

    pub(crate) fn overwrite_with(&mut self, snapshot: RaftSnapshot) {
        let index = snapshot.meta.last_included_index;
        if index <= self.last_included_index() {
            return;
        }
        let retained_from = self.entries.partition_point(|entry| entry.index <= index);
        self.entries.drain(..retained_from);
        self.snapshot = SnapshotState::Active(snapshot);
        self.stabled_index = self.stabled_index.max(index);
    }

    pub(crate) fn stage_snapshot(
        &mut self,
        snapshot: RaftSnapshot,
    ) -> Result<(), SnapshotAlreadyStaged> {
        if let SnapshotState::Staged {
            snapshot: staged, ..
        } = &self.snapshot
        {
            return if staged == &snapshot {
                Ok(())
            } else {
                Err(SnapshotAlreadyStaged)
            };
        }
        let active = match std::mem::replace(&mut self.snapshot, SnapshotState::None) {
            SnapshotState::None => None,
            SnapshotState::Active(active) => Some(active),
            staged @ SnapshotState::Staged { .. } => {
                self.snapshot = staged;
                return Err(SnapshotAlreadyStaged);
            }
        };
        self.unflushed_mutations
            .push(LogMutation::Snapshot(snapshot.clone()));
        self.snapshot = SnapshotState::Staged { active, snapshot };
        Ok(())
    }

    pub(crate) fn finish_snapshot(&mut self) -> Option<RaftSnapshot> {
        let SnapshotState::Staged { snapshot, .. } =
            std::mem::replace(&mut self.snapshot, SnapshotState::None)
        else {
            return None;
        };
        self.overwrite_with(snapshot.clone());
        Some(snapshot)
    }

    pub(crate) fn snapshot(&self) -> Option<&RaftSnapshot> {
        self.snapshot.active()
    }

    pub(crate) fn advance_stabled_index(&mut self, index: u64) {
        self.stabled_index = self.stabled_index.max(index);
    }

    pub(crate) fn entries(&self) -> &[LogEntry] {
        &self.entries
    }

    pub(crate) fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map_or(self.last_included_index(), |entry| entry.index)
    }

    pub(crate) fn last_term(&self) -> u64 {
        self.entries
            .last()
            .map_or(self.last_included_term(), |entry| entry.term)
    }

    pub(crate) fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        if index == self.last_included_index() {
            return self.last_included_term();
        }
        self.get(index).map_or(0, |entry| entry.term)
    }

    pub(crate) fn get(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.last_included_index() {
            return None;
        }
        self.entries
            .get((index - self.last_included_index() - 1) as usize)
    }

    pub(crate) fn entries_from(&self, start_index: u64) -> Box<[LogEntry]> {
        if start_index <= self.last_included_index() || start_index > self.last_index() {
            return Box::new([]);
        }
        self.entries[(start_index - self.last_included_index() - 1) as usize..].into()
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
        if from_index <= self.last_included_index() || from_index > self.last_index() + 1 {
            return;
        }
        self.unflushed_mutations
            .push(LogMutation::TruncateFrom(from_index));
        self.entries
            .truncate((from_index - self.last_included_index() - 1) as usize);
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
    use crate::control_plane::{
        consensus::raft::{
            command::RaftCommand, states::metadata_state::MetadataState, storage::SnapshotData,
        },
        membership::ShardGroupId,
    };

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

    #[test]
    fn compacted_log_keeps_logical_boundary_and_suffix_indices() {
        let snapshot = RaftSnapshot::new(
            2,
            1,
            SnapshotData {
                metadata: MetadataState::new(ShardGroupId(1)).snapshot(),
                peers: Box::new([]),
            },
        );
        let mut state = LogState::from_persistent(RaftPersistentState {
            term: 2,
            voted_for: None,
            log: vec![LogEntry {
                term: 2,
                index: 3,
                command: RaftCommand::Noop,
            }],
            snapshot: Some(snapshot),
        });

        assert_eq!(state.term_at(2), 1);
        assert_eq!(state.last_index(), 3);
        assert_eq!(state.get(3).map(|entry| entry.index), Some(3));
        state.truncate_from(3);
        assert_eq!(state.last_index(), 2);
        assert_eq!(state.last_term(), 1);
    }

    #[test]
    fn staging_the_same_snapshot_is_idempotent() {
        let snapshot = RaftSnapshot::new(
            2,
            1,
            SnapshotData {
                metadata: MetadataState::new(ShardGroupId(1)).snapshot(),
                peers: Box::new([]),
            },
        );
        let mut state = LogState::from_persistent(RaftPersistentState::default());

        assert!(state.stage_snapshot(snapshot.clone()).is_ok());
        assert!(state.stage_snapshot(snapshot).is_ok());
        assert_eq!(state.take_mutations().len(), 1);
    }
}
