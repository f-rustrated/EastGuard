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
        let mut key = Vec::with_capacity(32);
        key.extend_from_slice(&segment_key.shard_group_id.to_be_bytes());
        key.extend_from_slice(&segment_key.range_id.to_be_bytes());
        key.extend_from_slice(&segment_key.segment_id.to_be_bytes());
        key.extend_from_slice(&logical_offset.to_be_bytes());
        Self { key, byte_position }
    }
}

pub trait SparseIndex: Send + Sync + 'static {
    fn put_batch(&self, entries: Vec<SparseEntry>) -> io::Result<()>;
    fn seek_index(&self, segment_key: SegmentKey, target_offset: u64) -> u64;
}

impl SparseIndex for rocksdb::DB {
    fn put_batch(&self, entries: Vec<SparseEntry>) -> io::Result<()> {
        let mut batch = rocksdb::WriteBatch::default();
        for entry in &entries {
            batch.put(&entry.key, entry.byte_position);
        }
        self.write(batch).map_err(io::Error::other)
    }

    fn seek_index(&self, segment_key: SegmentKey, target_offset: u64) -> u64 {
        let seek_key = encode_key(segment_key, target_offset);

        let mut iter = self.raw_iterator();
        iter.seek_for_prev(&seek_key);

        if iter.valid()
            && let Some(key) = iter.key()
            && key.len() >= 24
            && key[..24] == seek_key[..24]
            && let Some(value) = iter.value()
            && value.len() == 8
        {
            return u64::from_be_bytes(value.try_into().unwrap());
        }
        0
    }
}

#[inline]
fn encode_key(segment_key: SegmentKey, offset: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(32);
    key.extend_from_slice(&segment_key.shard_group_id.to_be_bytes());
    key.extend_from_slice(&segment_key.range_id.to_be_bytes());
    key.extend_from_slice(&segment_key.segment_id.to_be_bytes());
    key.extend_from_slice(&offset.to_be_bytes());
    key
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

    fn seek_index(&self, segment_key: SegmentKey, target_offset: u64) -> u64 {
        let seek_key = encode_key(segment_key, target_offset);
        let entries = self.entries.lock().unwrap();

        if let Some((key, value)) = entries.range(..=seek_key.clone()).next_back()
            && key.len() >= 24
            && key[..24] == seek_key[..24]
            && value.len() == 8
        {
            return u64::from_be_bytes(value.as_slice().try_into().unwrap());
        }

        0
    }
}
