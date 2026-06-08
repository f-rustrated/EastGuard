/// # Client ↔ Server request_id protocol
///
/// 1. **Client assigns** — before sending each request, the client obtains a
///    monotonically increasing ID from a per-connection `u64` counter.
/// 2. **Client tracks** — the ID is stored in a `HashMap<u64, oneshot::Sender<Response>>`
///    of in-flight requests, keyed by `request_id`.
/// 3. **Server echoes** — the server reads `request_id` from the incoming frame and
///    writes it back unchanged in the response frame. The server is stateless with
///    respect to `request_id`.
/// 4. **Client matches** — on receiving a response frame, the client looks up
///    `request_id` in the in-flight map and delivers the response to the right waiter.
///
/// This allows multiple requests to be in-flight on a single connection simultaneously,
/// with responses arriving in any order.
use crate::connections::{LEN_PREFIX_SIZE, REQUEST_ID_SIZE};

use std::io::ErrorKind;

use crate::net::OwnedReadHalf;
use bytes::{Buf, BytesMut};
use tokio::io::AsyncReadExt;

use crate::config::SERDE_CONFIG;

pub struct ClientStreamReader {
    pub(crate) stream: OwnedReadHalf,
    buffer: BytesMut,
}

impl ClientStreamReader {
    pub fn new(stream: OwnedReadHalf) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(1024),
        }
    }
    pub async fn read_bytes(&mut self) -> Result<(u64, BytesMut), std::io::Error> {
        loop {
            if let Some(msg) = self.parse_frame()? {
                return Ok(msg);
            }
            self.read_more().await?;
        }
    }

    async fn read_more(&mut self) -> Result<(), std::io::Error> {
        let n: usize = self.stream.read_buf(&mut self.buffer).await?;
        if n == 0 {
            let kind = if self.buffer.is_empty() {
                ErrorKind::ConnectionAborted
            } else {
                ErrorKind::UnexpectedEof
            };
            return Err(std::io::Error::new(kind, "Connection closed"));
        }
        Ok(())
    }

    /// Tries to split off a full frame from the internal buffer.
    /// Frame layout: `[len: u32][request_id: u64][payload: len-REQUEST_ID_SIZE bytes]`
    /// Returns `Ok(None)` if more data is needed.
    pub fn parse_frame(&mut self) -> Result<Option<(u64, BytesMut)>, std::io::Error> {
        const MAX_MSG_SIZE: usize = 4 * 1024 * 1024; // 4MB

        // 1. Do we have enough for the length prefix?
        if self.buffer.len() < LEN_PREFIX_SIZE {
            return Ok(None);
        }

        // 2. Peek the length integer (without consuming bytes yet).
        //    len covers request_id + payload.
        let mut len_bytes = &self.buffer[..LEN_PREFIX_SIZE];
        let len = len_bytes.get_u32() as usize;

        // 3. Security check: prevent massive allocations from malicious clients.
        if len > MAX_MSG_SIZE {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("Message length {len} exceeds maximum allowed {MAX_MSG_SIZE}"),
            ));
        }

        // 4. len must cover at least the request_id field.
        if len < REQUEST_ID_SIZE {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Frame too small: len={len}, minimum is {REQUEST_ID_SIZE} (request_id field)"
                ),
            ));
        }

        // 5. Do we have the full frame in the buffer?
        let total_frame_len = LEN_PREFIX_SIZE + len;
        if self.buffer.len() < total_frame_len {
            if self.buffer.capacity() < total_frame_len {
                self.buffer.reserve(total_frame_len - self.buffer.len());
            }
            return Ok(None);
        }

        // 6. Consume length prefix, extract request_id, then split off payload.
        self.buffer.advance(LEN_PREFIX_SIZE);
        let request_id = self.buffer.get_u64();
        let payload = self.buffer.split_to(len - REQUEST_ID_SIZE);

        Ok(Some((request_id, payload)))
    }

    pub async fn read_request<U>(&mut self) -> anyhow::Result<(u64, U)>
    where
        U: bincode::Decode<()>,
    {
        let (request_id, body) = self.read_bytes().await?;
        let (request, _) = bincode::decode_from_slice(&body, SERDE_CONFIG)?;
        Ok((request_id, request))
    }
}
