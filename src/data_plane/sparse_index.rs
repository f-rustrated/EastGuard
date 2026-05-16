use std::io;

pub trait SparseIndex: Send + Sync + 'static {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()>;
    fn seek_for_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)>;
}

impl SparseIndex for rocksdb::DB {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        rocksdb::DB::put(self, key, value).map_err(io::Error::other)
    }

    fn seek_for_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        let mut iter = self.raw_iterator();
        iter.seek_for_prev(key);
        if !iter.valid() {
            return None;
        }
        let k = iter.key()?.to_vec();
        let v = iter.value()?.to_vec();
        Some((k, v))
    }
}

#[cfg(test)]
pub struct MockSparseIndex {
    entries: std::sync::Mutex<std::collections::BTreeMap<Vec<u8>, Vec<u8>>>,
}

#[cfg(test)]
impl MockSparseIndex {
    pub fn new() -> Self {
        MockSparseIndex {
            entries: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        }
    }
}

#[cfg(test)]
impl SparseIndex for MockSparseIndex {
    fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.entries
            .lock()
            .unwrap()
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn seek_for_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        let entries = self.entries.lock().unwrap();
        entries
            .range(..=key.to_vec())
            .next_back()
            .map(|(k, v)| (k.clone(), v.clone()))
    }
}
