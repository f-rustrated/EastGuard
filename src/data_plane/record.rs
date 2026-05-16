use std::io::{self, Read, Write};

use bytes::{BufMut, Bytes, BytesMut};

use crate::clusters::metadata::{RangeId, SegmentId};
use crate::clusters::swims::ShardGroupId;

pub type SegmentKey = (ShardGroupId, RangeId, SegmentId);

const HEADER_SIZE: usize = 9; // crc32(4) + type(1) + length(4)
const TRAILING_LENGTH_SIZE: usize = 4;
const ROUTING_HEADER_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    Data = 0,
    BatchEnd = 1,
}

impl RecordType {
    fn from_u8(v: u8) -> io::Result<Self> {
        match v {
            0 => Ok(RecordType::Data),
            1 => Ok(RecordType::BatchEnd),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown record type",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRoutingHeader {
    shard_group_id: ShardGroupId,
    range_id: RangeId,
    segment_id: SegmentId,
    pub(crate) logical_offset: u64,
}

impl DataRoutingHeader {
    pub(super) fn new((shard_group_id, range_id, segment_id): SegmentKey, offset: u64) -> Self {
        Self {
            shard_group_id,
            range_id,
            segment_id,
            logical_offset: offset,
        }
    }
    pub(super) fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.shard_group_id.0);
        buf.put_u64(self.range_id.0);
        buf.put_u64(self.segment_id.0);
        buf.put_u64(self.logical_offset);
    }

    pub(super) fn build_wal_payload(&self, user_data: &[u8]) -> Bytes {
        let mut buf = BytesMut::with_capacity(ROUTING_HEADER_SIZE + user_data.len());
        self.encode(&mut buf);
        buf.put_slice(user_data);
        buf.freeze()
    }

    pub(super) fn decode(data: &[u8]) -> io::Result<Self> {
        if data.len() < ROUTING_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "routing header too short",
            ));
        }
        Ok(DataRoutingHeader {
            shard_group_id: ShardGroupId(u64::from_be_bytes(data[0..8].try_into().unwrap())),
            range_id: RangeId(u64::from_be_bytes(data[8..16].try_into().unwrap())),
            segment_id: SegmentId(u64::from_be_bytes(data[16..24].try_into().unwrap())),
            logical_offset: u64::from_be_bytes(data[24..32].try_into().unwrap()),
        })
    }

    pub(crate) fn segment_key(&self) -> SegmentKey {
        (self.shard_group_id, self.range_id, self.segment_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub record_type: RecordType,
    pub payload: Bytes,
}

impl Record {
    pub(super) fn data(payload: Bytes) -> Self {
        Record {
            record_type: RecordType::Data,
            payload,
        }
    }

    pub(super) fn batch_end() -> Self {
        Record {
            record_type: RecordType::BatchEnd,
            payload: Bytes::new(),
        }
    }

    pub(super) fn encoded_size(&self) -> usize {
        HEADER_SIZE + self.payload.len() + TRAILING_LENGTH_SIZE
    }

    pub(super) fn encode_to(&self, writer: &mut impl Write) -> io::Result<()> {
        let payload_len = self.payload.len() as u32;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[self.record_type as u8]);
        hasher.update(&payload_len.to_be_bytes());
        hasher.update(&self.payload);
        let crc = hasher.finalize();

        writer.write_all(&crc.to_be_bytes())?;
        writer.write_all(&[self.record_type as u8])?;
        writer.write_all(&payload_len.to_be_bytes())?;
        writer.write_all(&self.payload)?;
        writer.write_all(&payload_len.to_be_bytes())?;
        Ok(())
    }

    pub(super) fn decode_from(reader: &mut impl Read) -> io::Result<Self> {
        let mut header = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header)?;

        let stored_crc = u32::from_be_bytes(header[0..4].try_into().unwrap());
        let record_type = RecordType::from_u8(header[4])?;
        let payload_len = u32::from_be_bytes(header[5..9].try_into().unwrap()) as usize;

        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload)?;

        let mut trailing_len = [0u8; TRAILING_LENGTH_SIZE];
        reader.read_exact(&mut trailing_len)?;
        let trailing = u32::from_be_bytes(trailing_len);
        if trailing as usize != payload_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing length mismatch",
            ));
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[record_type as u8]);
        hasher.update(&(payload_len as u32).to_be_bytes());
        hasher.update(&payload);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
        }

        Ok(Record {
            record_type,
            payload: Bytes::from(payload),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SegmentRecordBatch {
    pub records: Vec<Record>,
    pub start_offset: u64,
    pub end_offset: u64,
    pub lsn: u64,
}

pub(crate) struct BufferedRecord {
    pub(crate) user_data: Bytes,
    header: DataRoutingHeader,
}

impl BufferedRecord {
    pub(crate) fn new(payload: Bytes, segment_key: SegmentKey, offset: u64) -> Self {
        Self {
            user_data: payload,
            header: DataRoutingHeader::new(segment_key, offset),
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
    fn record_roundtrip() {
        let record = Record::data(Bytes::from("hello world"));
        let mut buf = Vec::new();
        record.encode_to(&mut buf).unwrap();

        let decoded = Record::decode_from(&mut &buf[..]).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn batch_end_roundtrip() {
        let record = Record::batch_end();
        let mut buf = Vec::new();
        record.encode_to(&mut buf).unwrap();

        let decoded = Record::decode_from(&mut &buf[..]).unwrap();
        assert_eq!(record, decoded);
    }

    #[test]
    fn crc_detects_corruption() {
        let record = Record::data(Bytes::from("test data"));
        let mut buf = Vec::new();
        record.encode_to(&mut buf).unwrap();

        buf[HEADER_SIZE + 2] ^= 0xFF;

        let result = Record::decode_from(&mut &buf[..]);
        assert!(result.is_err());
    }

    #[test]
    fn routing_header_roundtrip() {
        let header = DataRoutingHeader {
            shard_group_id: ShardGroupId(42),
            range_id: RangeId(7),
            segment_id: SegmentId(3),
            logical_offset: 999,
        };
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        let decoded = DataRoutingHeader::decode(&buf).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn wal_payload_build_and_strip() {
        let header = DataRoutingHeader {
            shard_group_id: ShardGroupId(1),
            range_id: RangeId(2),
            segment_id: SegmentId(3),
            logical_offset: 100,
        };
        let user_data = b"user payload";
        let payload = header.build_wal_payload(user_data);

        let decoded_header = DataRoutingHeader::decode(&payload).expect("valid routing header");
        let decoded_data = payload.slice(ROUTING_HEADER_SIZE..);

        assert_eq!(header, decoded_header);
        assert_eq!(&decoded_data[..], user_data);
    }
}
