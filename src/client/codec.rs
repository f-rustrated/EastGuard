use crate::client::producer::buffers::{PendingRecord, Records};
use borsh::{BorshDeserialize, BorshSerialize};

/// Compression codec for opaque entry payloads.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize, clap::ValueEnum,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum CompressionCodec {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

impl CompressionCodec {
    pub fn from_u8(tag: u8) -> Result<Self, std::io::Error> {
        match tag {
            0 => Ok(CompressionCodec::None),
            1 => Ok(CompressionCodec::Lz4),
            2 => Ok(CompressionCodec::Zstd),
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown compression codec tag: {}", other),
            )),
        }
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
        match self {
            CompressionCodec::None => Ok(data.to_vec()),
            CompressionCodec::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
            CompressionCodec::Zstd => zstd::encode_all(data, 0),
        }
    }

    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
        match self {
            CompressionCodec::None => Ok(data.to_vec()),
            CompressionCodec::Lz4 => lz4_flex::decompress_size_prepended(data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
            CompressionCodec::Zstd => zstd::decode_all(data),
        }
    }

    /// Helper to encode a batch of records into an opaque EntryPayload with a 1-byte codec tag.
    pub fn encode_payload(&self, records: &[PendingRecord]) -> Result<Vec<u8>, std::io::Error> {
        let serialized = PendingRecord::serialize_batch(records);
        let compressed = self.compress(&serialized)?;
        let mut payload = Vec::with_capacity(1 + compressed.len());
        payload.push(*self as u8);
        payload.extend_from_slice(&compressed);
        Ok(payload)
    }

    /// Helper to decode an opaque EntryPayload (with 1-byte codec tag) into records.
    pub fn decode_payload(payload: &[u8], record_count: u32) -> Result<Records, std::io::Error> {
        if payload.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Empty payload, missing codec tag",
            ));
        }
        let codec = Self::from_u8(payload[0])?;
        let decompressed = codec.decompress(&payload[1..])?;
        PendingRecord::deserialize_batch(&decompressed, record_count)
    }
}
