use std::io;

use bytes::{BufMut, Bytes, BytesMut};

use crate::clusters::metadata::{RangeId, SegmentId};
use crate::clusters::swims::ShardGroupId;
use crate::data_plane::SegmentKey;

const ROUTING_HEADER_SIZE: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RoutingHeader {
    shard_group_id: ShardGroupId,
    range_id: RangeId,
    segment_id: SegmentId,
    pub(crate) logical_offset: u64,
}

impl RoutingHeader {
    fn new(key: SegmentKey, offset: u64) -> Self {
        Self {
            shard_group_id: key.shard_group_id,
            range_id: key.range_id,
            segment_id: key.segment_id,
            logical_offset: offset,
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.shard_group_id.0);
        buf.put_u64(*self.range_id);
        buf.put_u64(self.segment_id.0);
        buf.put_u64(self.logical_offset);
    }

    fn build_wal_payload(&self, user_data: &[u8]) -> Bytes {
        let mut buf = BytesMut::with_capacity(ROUTING_HEADER_SIZE + user_data.len());
        self.encode(&mut buf);
        buf.put_slice(user_data);
        buf.freeze()
    }

    fn decode(data: &[u8]) -> io::Result<Self> {
        if data.len() < ROUTING_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "routing header too short",
            ));
        }
        Ok(RoutingHeader {
            shard_group_id: ShardGroupId(u64::from_be_bytes(data[0..8].try_into().unwrap())),
            range_id: RangeId(u64::from_be_bytes(data[8..16].try_into().unwrap())),
            segment_id: SegmentId(u64::from_be_bytes(data[16..24].try_into().unwrap())),
            logical_offset: u64::from_be_bytes(data[24..32].try_into().unwrap()),
        })
    }
}

pub(crate) struct StagingRecord {
    pub(crate) user_data: Bytes,
    header: RoutingHeader,
}

impl StagingRecord {
    pub(crate) fn new(payload: Bytes, segment_key: SegmentKey, offset: u64) -> Self {
        Self {
            user_data: payload,
            header: RoutingHeader::new(segment_key, offset),
        }
    }

    pub(crate) fn logical_offset(&self) -> u64 {
        self.header.logical_offset
    }

    pub(crate) fn build_wal_payload(&self) -> Bytes {
        self.header.build_wal_payload(&self.user_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_header_roundtrip() {
        let header = RoutingHeader {
            shard_group_id: ShardGroupId(42),
            range_id: RangeId(7),
            segment_id: SegmentId(3),
            logical_offset: 999,
        };
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        let decoded = RoutingHeader::decode(&buf).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn wal_payload_build_and_strip() {
        let header = RoutingHeader {
            shard_group_id: ShardGroupId(1),
            range_id: RangeId(2),
            segment_id: SegmentId(3),
            logical_offset: 100,
        };
        let user_data = b"user payload";
        let payload = header.build_wal_payload(user_data);

        let decoded_header = RoutingHeader::decode(&payload).expect("valid routing header");
        let decoded_data = payload.slice(ROUTING_HEADER_SIZE..);

        assert_eq!(header, decoded_header);
        assert_eq!(&decoded_data[..], user_data);
    }
}
