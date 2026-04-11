use super::{Entry, Index, StorageEngine, StorageError};

#[derive(Debug, Default)]
pub struct MemEngine {
    entries: Vec<Entry>,
}

impl StorageEngine for MemEngine {
    fn append_log(&mut self, entry: Entry) -> Result<(), StorageError> {
        self.entries.push(entry);
        Ok(())
    }

    fn get_range(&self, start: Index, end: Index) -> Result<Vec<Entry>, StorageError> {
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

    fn get(&self, index: Index) -> Result<Option<Entry>, StorageError> {
        if index == 0 {
            return Ok(None);
        }
        Ok(self.entries.get((index - 1) as usize).cloned())
    }

    fn get_last(&self) -> Result<Option<Entry>, StorageError> {
        Ok(self.entries.last().cloned())
    }

    fn truncate_log(&mut self, from: Index) -> Result<(), StorageError> {
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
    fn sync(&mut self) -> Result<(), StorageError> {
        Ok(())
    }
}
