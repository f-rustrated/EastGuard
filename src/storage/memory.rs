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

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: Index, data: &[u8]) -> Entry {
        Entry { index, data: data.to_vec() }
    }

    #[test]
    fn empty_engine_returns_none() {
        let eng = MemEngine::default();
        assert!(eng.get_last().unwrap().is_none());
        assert!(eng.get(1).unwrap().is_none());
        assert!(eng.get_range(1, 5).unwrap().is_empty());
    }

    #[test]
    fn append_and_get() {
        let mut eng = MemEngine::default();
        eng.append_log(entry(1, b"a")).unwrap();
        eng.append_log(entry(2, b"b")).unwrap();

        assert_eq!(eng.get(1).unwrap().unwrap().data, b"a");
        assert_eq!(eng.get_last().unwrap().unwrap().index, 2);
        assert!(eng.get(3).unwrap().is_none());
    }

    #[test]
    fn truncate_removes_tail() {
        let mut eng = MemEngine::default();
        eng.append_log(entry(1, b"a")).unwrap();
        eng.append_log(entry(2, b"b")).unwrap();
        eng.append_log(entry(3, b"c")).unwrap();

        eng.truncate_log(2).unwrap();

        assert_eq!(eng.get_last().unwrap().unwrap().index, 1);
        assert!(eng.get(2).unwrap().is_none());
    }
}
