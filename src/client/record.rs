/// A single key-value record produced or consumed by the client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl ClientRecord {
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Serialize a slice of records into a byte buffer.
    pub fn serialize_batch(records: &[ClientRecord]) -> Vec<u8> {
        let mut buf = Vec::new();
        for r in records {
            buf.extend_from_slice(&(r.key.len() as u32).to_be_bytes());
            buf.extend_from_slice(&r.key);
            buf.extend_from_slice(&(r.value.len() as u32).to_be_bytes());
            buf.extend_from_slice(&r.value);
        }
        buf
    }

    /// Deserialize a byte buffer into a vector of records.
    pub fn deserialize_batch(mut buf: &[u8], count: u32) -> Result<Box<[ClientRecord]>, std::io::Error> {
        let mut records = Vec::with_capacity(count as usize);
        for _ in 0..count {
            if buf.len() < 4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Unexpected EOF reading key length",
                ));
            }
            let key_len = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;
            buf = &buf[4..];

            if buf.len() < key_len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Unexpected EOF reading key data",
                ));
            }
            let key = buf[..key_len].to_vec();
            buf = &buf[key_len..];

            if buf.len() < 4 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Unexpected EOF reading value length",
                ));
            }
            let val_len = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;
            buf = &buf[4..];

            if buf.len() < val_len {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Unexpected EOF reading value data",
                ));
            }
            let value = buf[..val_len].to_vec();
            buf = &buf[val_len..];

            records.push(ClientRecord { key, value });
        }
        Ok(records.into_boxed_slice())
    }
}
