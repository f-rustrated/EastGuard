use std::io;

use crate::data_plane::record::SegmentKey;

pub struct SparseEntry {
    key: Vec<u8>,
    byte_position: [u8; 8],
}
impl SparseEntry {
    pub(crate) fn new(
        segment_key: SegmentKey,
        logical_offset: u64,
        byte_position: [u8; 8],
    ) -> Self {
        let (shard_group_id, range_id, segment_id) = segment_key;
        let mut key = Vec::with_capacity(32);
        key.extend_from_slice(&shard_group_id.0.to_be_bytes());
        key.extend_from_slice(&range_id.0.to_be_bytes());
        key.extend_from_slice(&segment_id.0.to_be_bytes());
        key.extend_from_slice(&logical_offset.to_be_bytes());
        Self { key, byte_position }
    }
}

pub trait SparseIndex: Send + Sync + 'static {
    fn put_batch(&self, entries: Vec<SparseEntry>) -> io::Result<()>;
    fn seek_for_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)>;
}

impl SparseIndex for rocksdb::DB {
    fn put_batch(&self, entries: Vec<SparseEntry>) -> io::Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        for entry in &entries {
            batch.put(&entry.key, entry.byte_position);
        }
        self.write(batch).map_err(io::Error::other)
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
    fn put_batch(&self, entries: Vec<SparseEntry>) -> io::Result<()> {
        let mut map = self.entries.lock().unwrap();
        for entry in entries {
            map.insert(entry.key, entry.byte_position.to_vec());
        }
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
