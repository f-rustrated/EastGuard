use crate::client::producer::buffers::{MAX_BATCH_BYTES, PendingRecord, Records};
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
        Self::check_decoded_size(data.len())?;
        match self {
            CompressionCodec::None => Ok(data.to_vec()),
            CompressionCodec::Lz4 => Ok(lz4_flex::compress_prepend_size(data)),
            CompressionCodec::Zstd => zstd::encode_all(data, 0),
        }
    }

    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
        match self {
            CompressionCodec::None => {
                Self::check_decoded_size(data.len())?;
                Ok(data.to_vec())
            }
            CompressionCodec::Lz4 => {
                let (size, compressed) = lz4_flex::block::uncompressed_size(data)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                Self::check_decoded_size(size)?;
                let mut output = vec![0; size];
                let decoded = lz4_flex::decompress_into(compressed, &mut output)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                if decoded != size {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "LZ4 decoded size differs from the declared size",
                    ));
                }
                Ok(output)
            }
            CompressionCodec::Zstd => zstd::bulk::decompress(data, MAX_BATCH_BYTES),
        }
    }

    fn check_decoded_size(size: usize) -> Result<(), std::io::Error> {
        if size > MAX_BATCH_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Decoded batch exceeds the 4 MiB limit",
            ));
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn record_counts_and_lengths_are_checked_before_allocation() {
        for (payload, count) in [(&[0][..], u32::MAX), (&[0; 9][..], 2), (&[0; 10][..], 1)] {
            assert_eq!(
                CompressionCodec::decode_payload(payload, count)
                    .unwrap_err()
                    .kind(),
                ErrorKind::InvalidData
            );
        }
        assert_eq!(
            CompressionCodec::decode_payload(&[0; 9], 1)
                .unwrap()
                .as_ref(),
            &[(Vec::new(), Vec::new())]
        );
        assert!(PendingRecord::deserialize_batch(&[255; 8], 1).is_err());
        assert!(
            CompressionCodec::decode_payload(&[0], 0)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn codecs_enforce_the_decoded_byte_limit() {
        let oversized = vec![0; MAX_BATCH_BYTES + 1];
        let at_limit = &oversized[..MAX_BATCH_BYTES];
        for codec in [
            CompressionCodec::None,
            CompressionCodec::Lz4,
            CompressionCodec::Zstd,
        ] {
            let encoded = codec.compress(at_limit).unwrap();
            assert_eq!(codec.decompress(&encoded).unwrap(), at_limit);
            assert!(codec.compress(&oversized).is_err());
        }

        // Only a size prefix and an empty block: reject before LZ4 allocates.
        assert!(
            CompressionCodec::Lz4
                .decompress(&[255, 255, 255, 255, 0])
                .is_err()
        );
        assert!(CompressionCodec::Lz4.decompress(&[0, 0, 0]).is_err());
        let mut understated = lz4_flex::compress_prepend_size(b"payload");
        understated[..4].copy_from_slice(&1u32.to_le_bytes());
        assert!(CompressionCodec::Lz4.decompress(&understated).is_err());
        assert!(
            CompressionCodec::Lz4
                .decompress(&lz4_flex::compress_prepend_size(&oversized))
                .is_err()
        );
        // encode_all creates a streaming frame without a declared content size.
        assert!(
            CompressionCodec::Zstd
                .decompress(&zstd::encode_all(oversized.as_slice(), 0).unwrap())
                .is_err()
        );
        assert!(CompressionCodec::None.decompress(&oversized).is_err());
    }
}
