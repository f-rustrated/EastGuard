use std::io::ErrorKind;

use anyhow::bail;
use bytes::{Buf, BytesMut};
use tokio::{
    io::AsyncReadExt,
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

use crate::{config::SERDE_CONFIG, connections::reqeust::SessionRequest};

pub struct ClientStreamWriter(pub(crate) OwnedWriteHalf);

#[derive(Debug)]
pub struct ClientStreamReader {
    pub(crate) stream: OwnedReadHalf,
    // Persistent Buffer - instead of creating a new buffer for every message,
    // we keep one buffer
    buffer: BytesMut,
}

impl ClientStreamReader {
    pub fn new(stream: OwnedReadHalf) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(1024),
        }
    }
    pub async fn read_bytes(&mut self) -> Result<BytesMut, std::io::Error> {
        loop {
            // 1. Attempt to parse what we already have buffered
            //    If we received multiple messages in one packet, this ensures we process them all
            //    before reading from the socket again.
            if let Some(msg) = self.parse_frame()? {
                return Ok(msg);
            }

            // 2. If no full frame is available, read more data from the socket.
            //    'read_buf' automatically appends to the BytesMut
            let n = self.stream.read_buf(&mut self.buffer).await?;
            if 0 == n {
                // If we hit EOF but still have partial data, that's an error
                return if self.buffer.is_empty() {
                    Err(std::io::Error::new(
                        ErrorKind::ConnectionAborted,
                        "Connection closed",
                    ))
                } else {
                    Err(std::io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "Partial frame received",
                    ))
                };
            }
        }
    }

    /// Tries to split off a full message from the internal buffer.
    /// Returns Ok(None) if we need more data.
    pub fn parse_frame(&mut self) -> Result<Option<BytesMut>, std::io::Error> {
        const MAX_MSG_SIZE: usize = 4 * 1024 * 1024; // 4MB

        // 1. Do we have enough for the length prefix (4bytes)?
        if self.buffer.len() < 4 {
            return Ok(None);
        }

        // 2. Peek the length integer (without consuming bytes yet)
        // Use a slice so we don't consume the bytes from self.buffer yet.
        let mut len_bytes = &self.buffer[..4];
        let len = len_bytes.get_u32() as usize;

        // 3. Security check: prevent massive allocations from malicious clients
        if len > MAX_MSG_SIZE {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("Message length {len} exceeds maximum allowed {MAX_MSG_SIZE}"),
            ));
        }

        // 4. Do we have the full message body in the buffer?
        let total_frame_len = 4 + len;
        if self.buffer.len() < total_frame_len {
            if self.buffer.capacity() < total_frame_len {
                // Exact fit: "I know 1MB is coming, reserve exactly 1MB right now."
                self.buffer.reserve(total_frame_len - self.buffer.len());
            }
            return Ok(None); // Not enough data yet, go back to reading
        }

        // 5. Consume header and split off the body
        self.buffer.advance(4); // Discard the length prefix

        // 'split-to' is Zero-Copy-ish, it just grabs the pointer to the existing memory
        let msg = self.buffer.split_to(len);

        // Whatever is left in self.buffer stays there for the next call.
        Ok(Some(msg))
    }

    pub async fn read_request<U>(&mut self) -> anyhow::Result<U>
    where
        U: bincode::Decode<()>,
    {
        let body = self.read_bytes().await;
        if let Err(err) = body.as_ref() {
            bail!(err.to_string())
        }

        let body = body.unwrap();
        let (request, _) = bincode::decode_from_slice(&body, SERDE_CONFIG)?;
        Ok(request)
    }

    pub(crate) async fn handle_client_stream(mut self) {
        loop {
            // * extract queries
            let query_io = self.read_request::<SessionRequest>().await;
        }
    }
}
