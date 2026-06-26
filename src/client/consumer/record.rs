/// A record returned to the consumer application.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub range_id: u64,
    pub offset: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
