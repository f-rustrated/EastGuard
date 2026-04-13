use crate::raft::interface::{LogError, LogStore};
use crate::raft::log::LogEntry;

use super::Index;

#[derive(Debug, Default)]
pub struct MemoryLogStore {
    entries: Vec<LogEntry>,
}

impl LogStore for MemoryLogStore {
    fn append_log(&mut self, entry: LogEntry) -> Result<(), LogError> {
        self.entries.push(entry);
        Ok(())
    }

    fn get_range(&self, start: Index, end: Index) -> Result<Vec<LogEntry>, LogError> {
        if start == 0 || start > end {
            return Ok(vec![]);
        }
        let last = self.get_last()?.map_or(0, |e| e.index);
        if start > last {
            return Ok(vec![]);
        }
        let end = end.min(last);
        Ok(self.entries[(start - 1) as usize..=(end - 1) as usize].to_vec())
    }

    fn get(&self, index: Index) -> Result<Option<LogEntry>, LogError> {
        if index == 0 {
            return Ok(None);
        }
        Ok(self.entries.get((index - 1) as usize).cloned())
    }

    fn get_last(&self) -> Result<Option<LogEntry>, LogError> {
        Ok(self.entries.last().cloned())
    }

    fn truncate_log(&mut self, from: Index) -> Result<(), LogError> {
        if from == 0 {
            return Ok(());
        }
        let last = self.get_last()?.map_or(0, |e| e.index);
        if from > last {
            return Ok(());
        }
        self.entries.truncate((from - 1) as usize);
        Ok(())
    }

    /// No-op for in-memory storage — there is nothing to flush.
    fn sync(&mut self) -> Result<(), LogError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::messages::RaftCommand;

    fn entry(term: u64, index: Index) -> LogEntry {
        LogEntry {
            term,
            index,
            command: RaftCommand::Noop,
        }
    }

    #[test]
    fn empty_engine_returns_none() {
        let eng = MemoryLogStore::default();
        assert!(eng.get_last().unwrap().is_none());
        assert!(eng.get(1).unwrap().is_none());
        assert!(eng.get_range(1, 5).unwrap().is_empty());
    }

    #[test]
    fn append_and_get() {
        let mut eng = MemoryLogStore::default();
        eng.append_log(entry(1, 1)).unwrap();
        eng.append_log(entry(2, 2)).unwrap();

        assert_eq!(eng.get(1).unwrap().unwrap().term, 1);
        assert_eq!(eng.get_last().unwrap().unwrap().index, 2);
        assert!(eng.get(3).unwrap().is_none());
    }

    #[test]
    fn truncate_removes_tail() {
        let mut eng = MemoryLogStore::default();
        eng.append_log(entry(1, 1)).unwrap();
        eng.append_log(entry(2, 2)).unwrap();
        eng.append_log(entry(3, 3)).unwrap();

        eng.truncate_log(2).unwrap();

        assert_eq!(eng.get_last().unwrap().unwrap().index, 1);
        assert!(eng.get(2).unwrap().is_none());
    }
}
