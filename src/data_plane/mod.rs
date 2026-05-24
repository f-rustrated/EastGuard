use std::path::{Path, PathBuf};

use bincode::de::{BorrowDecoder, Decoder, read::Reader};
use bincode::enc::Encoder;
use bincode::enc::write::Writer;
use bincode::error::{DecodeError, EncodeError};
use bytes::Bytes;

use crate::clusters::metadata::{RangeId, SegmentId, TopicId};
use crate::smart_pointer;

pub(crate) mod actor;

pub(crate) mod checkpoint;
pub(crate) mod cold_read;
pub(crate) mod messages;
pub(crate) mod sparse_index;
pub(crate) mod state;
pub(crate) mod states;
pub(crate) mod timer;
pub(crate) mod transport;
pub(crate) mod wal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bincode::Encode, bincode::Decode)]
pub struct SegmentKey {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub segment_id: SegmentId,
}

#[derive(Debug, Clone)]
pub struct EntryPayload(Bytes);

impl EntryPayload {
    pub fn new(data: Bytes) -> Self {
        Self(data)
    }
}

smart_pointer!(EntryPayload, Bytes);

impl From<Bytes> for EntryPayload {
    fn from(b: Bytes) -> Self {
        Self(b)
    }
}

impl From<Vec<u8>> for EntryPayload {
    fn from(v: Vec<u8>) -> Self {
        Self(Bytes::from(v))
    }
}

impl bincode::Encode for EntryPayload {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        bincode::Encode::encode(&(self.0.len() as u64), encoder)?;
        encoder.writer().write(&self.0)
    }
}

impl<C> bincode::Decode<C> for EntryPayload {
    fn decode<D: Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let len: u64 = bincode::Decode::decode(decoder)?;
        let mut buf = vec![0u8; len as usize];
        decoder.reader().read(&mut buf)?;
        Ok(Self(Bytes::from(buf)))
    }
}

impl<'de, C> bincode::BorrowDecode<'de, C> for EntryPayload {
    fn borrow_decode<D: BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let len: u64 = bincode::Decode::decode(decoder)?;
        let mut buf = vec![0u8; len as usize];
        decoder.reader().read(&mut buf)?;
        Ok(Self(Bytes::from(buf)))
    }
}

impl SegmentKey {
    pub fn new(topic_id: TopicId, range_id: RangeId, segment_id: SegmentId) -> Self {
        Self {
            topic_id,
            range_id,
            segment_id,
        }
    }

    pub fn file_path(&self, data_dir: &Path) -> PathBuf {
        data_dir
            .join(self.topic_id.to_string())
            .join(self.range_id.to_string())
            .join(format!("{}.seg", *self.segment_id))
    }

    pub fn with_segment_id(&self, segment_id: SegmentId) -> Self {
        Self {
            topic_id: self.topic_id,
            range_id: self.range_id,
            segment_id,
        }
    }
}
