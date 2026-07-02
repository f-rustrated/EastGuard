use crate::control_plane::metadata::RangeId;

/// A record returned to the consumer application.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub range_id: RangeId,
    pub offset: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl ConsumerRecord {
    pub fn key_match<'a>(&'a self, key: impl Iterator<Item = &'a u8>) -> bool {
        self.key.iter().eq(key)
    }
}
