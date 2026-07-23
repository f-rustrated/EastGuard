use std::io;

use bytes::{BufMut, Bytes, BytesMut};

use crate::control_plane::metadata::{EntryId, RangeId, SegmentId, TopicId};
use crate::data_plane::{PayloadBytes, ProducerAppendIdentity, SegmentKey};

const ROUTING_HEADER_MAGIC: u32 = 0x4547_5032;
const ROUTING_HEADER_SIZE: usize = 81;
// Current frame: magic | legacy routing fields | producer-present |
// producer UUID | incarnation | expiry | sequence | digest.
const MAGIC_SIZE: usize = 4;
const TOPIC_OFFSET: usize = 0;
const RANGE_OFFSET: usize = 8;
const SEGMENT_OFFSET: usize = 16;
const ENTRY_OFFSET: usize = 24;
const RECORD_COUNT_OFFSET: usize = 32;
const PRODUCER_PRESENT_OFFSET: usize = 36;
const PRODUCER_ID_OFFSET: usize = 37;
const PRODUCER_INCARNATION_OFFSET: usize = 53;
const PRODUCER_EXPIRY_OFFSET: usize = 57;
const PRODUCER_SEQUENCE_OFFSET: usize = 65;
const PRODUCER_DIGEST_OFFSET: usize = 73;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutingHeader {
    topic_id: TopicId,
    range_id: RangeId,
    segment_id: SegmentId,
    pub(crate) entry_id: EntryId,
    pub(crate) record_count: u32,
    pub(crate) producer_identity: Option<ProducerAppendIdentity>,
}

impl RoutingHeader {
    pub(crate) fn new(key: SegmentKey, entry_id: EntryId, record_count: u32) -> Self {
        Self {
            topic_id: key.topic_id,
            range_id: key.range_id,
            segment_id: key.segment_id,
            entry_id,
            record_count,
            producer_identity: None,
        }
    }

    pub(crate) fn with_producer(mut self, producer: Option<ProducerAppendIdentity>) -> Self {
        self.producer_identity = producer;
        self
    }

    pub(crate) fn segment_key(&self) -> SegmentKey {
        SegmentKey::new(self.topic_id, self.range_id, self.segment_id)
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32(ROUTING_HEADER_MAGIC);
        buf.put_u64(self.topic_id.0);
        buf.put_u64(*self.range_id);
        buf.put_u64(self.segment_id.0);
        buf.put_u64(*self.entry_id);
        buf.put_u32(self.record_count);
        match self.producer_identity {
            Some(producer) => {
                buf.put_u8(1);
                buf.put_slice(producer.producer_id.as_bytes());
                buf.put_u32(producer.incarnation);
                buf.put_u64(producer.expires_at);
                buf.put_u64(producer.sequence);
                buf.put_u32(producer.digest);
            }
            None => {
                buf.put_u8(0);
                buf.put_bytes(0, 40);
            }
        }
    }

    pub(crate) fn build_wal_payload(&self, entry_data: &[u8]) -> Bytes {
        let mut buf = BytesMut::with_capacity(ROUTING_HEADER_SIZE + entry_data.len());
        self.encode(&mut buf);
        buf.put_slice(entry_data);
        buf.freeze()
    }

    fn decode_with_size(data: &[u8]) -> io::Result<(Self, usize)> {
        if data.len() < ROUTING_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "routing header too short",
            ));
        }

        // Verify magic header
        if u32::from_be_bytes(data[0..4].try_into().unwrap()) != ROUTING_HEADER_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid routing header magic",
            ));
        }

        // Parse fixed fields
        let body = &data[MAGIC_SIZE..];
        let topic_id = TopicId(Self::parse_u64(&body[TOPIC_OFFSET..]));
        let range_id = RangeId(Self::parse_u64(&body[RANGE_OFFSET..]));
        let segment_id = SegmentId(Self::parse_u64(&body[SEGMENT_OFFSET..]));
        let entry_id = EntryId(Self::parse_u64(&body[ENTRY_OFFSET..]));
        let record_count = Self::parse_u32(&body[RECORD_COUNT_OFFSET..]);

        // Parse optional producer identity
        let producer_identity =
            (body[PRODUCER_PRESENT_OFFSET] == 1).then(|| ProducerAppendIdentity {
                producer_id: uuid::Uuid::from_slice(
                    &body[PRODUCER_ID_OFFSET..PRODUCER_INCARNATION_OFFSET],
                )
                .unwrap(),
                incarnation: Self::parse_u32(&body[PRODUCER_INCARNATION_OFFSET..]),
                expires_at: Self::parse_u64(&body[PRODUCER_EXPIRY_OFFSET..]),
                sequence: Self::parse_u64(&body[PRODUCER_SEQUENCE_OFFSET..]),
                digest: Self::parse_u32(&body[PRODUCER_DIGEST_OFFSET..]),
            });

        Ok((
            RoutingHeader {
                topic_id,
                range_id,
                segment_id,
                entry_id,
                record_count,
                producer_identity,
            },
            ROUTING_HEADER_SIZE,
        ))
    }

    fn parse_u32(src: &[u8]) -> u32 {
        u32::from_be_bytes(src[..4].try_into().unwrap())
    }

    fn parse_u64(src: &[u8]) -> u64 {
        u64::from_be_bytes(src[..8].try_into().unwrap())
    }

    /// Splits a WAL record payload (as built by [`RoutingHeader::build_wal_payload`])
    /// back into its routing header and the bare entry data that follows it — the
    /// inverse of `build_wal_payload`. Recovery replay uses it to route a WAL
    /// record by its header while writing only the bare data the segment keeps.
    pub(crate) fn split_wal_payload(payload: &[u8]) -> io::Result<(Self, &[u8])> {
        let (header, size) = Self::decode_with_size(payload)?;
        Ok((header, &payload[size..]))
    }

    #[cfg(test)]
    pub(crate) fn decode(data: &[u8]) -> io::Result<Self> {
        Self::decode_with_size(data).map(|(header, _)| header)
    }
}

pub(crate) struct StagedEntry {
    pub(crate) data: PayloadBytes,
    pub(crate) record_count: u32,
    pub(crate) segment_key: SegmentKey,
    pub(crate) producer_identity: Option<ProducerAppendIdentity>,
}

impl StagedEntry {
    pub(crate) fn new(
        data: PayloadBytes,
        record_count: u32,
        segment_key: SegmentKey,
        producer_identity: Option<ProducerAppendIdentity>,
    ) -> Self {
        Self {
            data,
            record_count,
            segment_key,
            producer_identity,
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
            entry_id: EntryId(999),
            record_count: 50,
            producer_identity: None,
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
            entry_id: EntryId(100),
            record_count: 5,
            producer_identity: None,
        };
        let entry_data = b"opaque entry payload";
        let payload = header.build_wal_payload(entry_data);

        let decoded_header = RoutingHeader::decode(&payload).expect("valid routing header");
        let decoded_data = payload.slice(ROUTING_HEADER_SIZE..);

        assert_eq!(header, decoded_header);
        assert_eq!(&decoded_data[..], entry_data);
    }

    #[test]
    fn producer_identity_roundtrips_inside_the_data_wal_record() {
        let producer = ProducerAppendIdentity {
            producer_id: uuid::Uuid::new_v4(),
            incarnation: 3,
            expires_at: 99,
            sequence: 7,
            digest: 42,
        };
        let header = RoutingHeader::new(
            SegmentKey::new(TopicId(1), RangeId(2), SegmentId(3)),
            EntryId(4),
            5,
        )
        .with_producer(Some(producer));
        let payload = header.build_wal_payload(b"data");
        let (decoded, data) = RoutingHeader::split_wal_payload(&payload).unwrap();

        assert_eq!(decoded.producer_identity, Some(producer));
        assert_eq!(data, b"data");
    }
}
