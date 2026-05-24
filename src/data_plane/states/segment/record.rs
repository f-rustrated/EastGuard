use std::io;

use bytes::{BufMut, Bytes, BytesMut};

use crate::clusters::metadata::{RangeId, SegmentId, TopicId};
use crate::data_plane::{EntryPayload, SegmentKey};

const ROUTING_HEADER_SIZE: usize = 36;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutingHeader {
    topic_id: TopicId,
    range_id: RangeId,
    segment_id: SegmentId,
    pub(crate) entry_id: u64,
    pub(crate) record_count: u32,
}

impl RoutingHeader {
    pub(crate) fn new(key: SegmentKey, entry_id: u64, record_count: u32) -> Self {
        Self {
            topic_id: key.topic_id,
            range_id: key.range_id,
            segment_id: key.segment_id,
            entry_id,
            record_count,
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.topic_id.0);
        buf.put_u64(*self.range_id);
        buf.put_u64(self.segment_id.0);
        buf.put_u64(self.entry_id);
        buf.put_u32(self.record_count);
    }

    pub(crate) fn build_wal_payload(&self, entry_data: &[u8]) -> Bytes {
        let mut buf = BytesMut::with_capacity(ROUTING_HEADER_SIZE + entry_data.len());
        self.encode(&mut buf);
        buf.put_slice(entry_data);
        buf.freeze()
    }

    pub(crate) fn decode(data: &[u8]) -> io::Result<Self> {
        if data.len() < ROUTING_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "routing header too short",
            ));
        }
        Ok(RoutingHeader {
            topic_id: TopicId(u64::from_be_bytes(data[0..8].try_into().unwrap())),
            range_id: RangeId(u64::from_be_bytes(data[8..16].try_into().unwrap())),
            segment_id: SegmentId(u64::from_be_bytes(data[16..24].try_into().unwrap())),
            entry_id: u64::from_be_bytes(data[24..32].try_into().unwrap()),
            record_count: u32::from_be_bytes(data[32..36].try_into().unwrap()),
        })
    }
}

pub(crate) struct StagedEntry {
    pub(crate) data: EntryPayload,
    pub(crate) record_count: u32,
    pub(crate) segment_key: SegmentKey,
}

impl StagedEntry {
    pub(crate) fn new(data: EntryPayload, record_count: u32, segment_key: SegmentKey) -> Self {
        Self {
            data,
            record_count,
            segment_key,
        }
    }

    pub(crate) fn byte_len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_header_roundtrip() {
        let header = RoutingHeader {
            topic_id: TopicId(42),
            range_id: RangeId(7),
            segment_id: SegmentId(3),
            entry_id: 999,
            record_count: 50,
        };
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        assert_eq!(buf.len(), ROUTING_HEADER_SIZE);
        let decoded = RoutingHeader::decode(&buf).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn wal_payload_build_and_strip() {
        let header = RoutingHeader {
            topic_id: TopicId(1),
            range_id: RangeId(2),
            segment_id: SegmentId(3),
            entry_id: 100,
            record_count: 5,
        };
        let entry_data = b"opaque entry payload";
        let payload = header.build_wal_payload(entry_data);

        let decoded_header = RoutingHeader::decode(&payload).expect("valid routing header");
        let decoded_data = payload.slice(ROUTING_HEADER_SIZE..);

        assert_eq!(header, decoded_header);
        assert_eq!(&decoded_data[..], entry_data);
    }
}
