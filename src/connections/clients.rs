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
use std::{io::ErrorKind, mem::size_of, net::SocketAddr};

const LEN_PREFIX_SIZE: usize = size_of::<u32>();
const REQUEST_ID_SIZE: usize = size_of::<u64>();

use crate::{
    clusters::{
        NodeId,
        raft::actor::RaftSender,
        raft::messages::ProposeError,
        swims::{ShardGroupId, SwimQueryCommand, actor::SwimSender},
    },
    connections::request::{
        ConnectionRequests, ProposeRequest, ProposeResponse, QueryCommand, ShardInfoResponse, TopicSummary,
    },
    net::{OwnedReadHalf, OwnedWriteHalf, TcpStream},
};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::SERDE_CONFIG;

pub struct ClientStreamWriter {
    pub(crate) stream: OwnedWriteHalf,
    swim_sender: SwimSender,
    raft_sender: RaftSender,
}

impl ClientStreamWriter {
    pub(crate) fn new(
        write_half: OwnedWriteHalf,
        swim_sender: SwimSender,
        raft_sender: RaftSender,
    ) -> Self {
        Self {
            stream: write_half,
            swim_sender,
            raft_sender,
        }
    }

    pub(crate) async fn write<T: bincode::Encode>(
        &mut self,
        request_id: u64,
        data: &T,
    ) -> anyhow::Result<()> {
        let encoded = bincode::encode_to_vec(data, SERDE_CONFIG)?;
        let len = (REQUEST_ID_SIZE + encoded.len()) as u32;
        self.stream.write_all(&len.to_be_bytes()).await.expect("write len failed");
        self.stream
            .write_all(&request_id.to_be_bytes())
            .await
            .expect("write request_id failed");
        self.stream
            .write_all(&encoded)
            .await
            .expect("write encoded failed");
        Ok(())
    }

    pub(crate) async fn dispatch(
        &mut self,
        request_id: u64,
        request: ConnectionRequests,
    ) -> anyhow::Result<()> {
        match request {
            ConnectionRequests::Connection(_) => Ok(()),
            ConnectionRequests::Query(q) => self.handle_query(request_id, q).await,
            ConnectionRequests::Propose(req) => self.handle_propose(request_id, req).await,
        }
    }

    async fn handle_query(
        &mut self,
        request_id: u64,
        query_type: QueryCommand,
    ) -> anyhow::Result<()> {
        match query_type {
            QueryCommand::GetMembers => self.handle_get_members(request_id).await,
            QueryCommand::GetShardInfo { key } => self.handle_get_shard_info(request_id, key).await,
            QueryCommand::GetShardLeader { shard_group_id } => {
                self.handle_get_shard_leader(request_id, shard_group_id).await
            }
            QueryCommand::GetTopics => self.handle_get_topics(request_id).await,
        }
    }

    async fn handle_get_members(&mut self, request_id: u64) -> anyhow::Result<()> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.swim_sender
            .send(SwimQueryCommand::GetMembers { reply: send })
            .await?;
        let result = recv.await?;
        self.write(request_id, &result).await
    }

    async fn handle_get_shard_leader(
        &mut self,
        request_id: u64,
        shard_group_id: u64,
    ) -> anyhow::Result<()> {
        let leader = self
            .raft_sender
            .get_leader(ShardGroupId(shard_group_id))
            .await
            .map(|n| n.to_string());
        self.write(request_id, &leader).await
    }

    async fn handle_get_topics(&mut self, request_id: u64) -> anyhow::Result<()> {
        let topics: Vec<TopicSummary> = self
            .raft_sender
            .get_topics()
            .await
            .into_iter()
            .map(|name| TopicSummary { name })
            .collect();
        self.write(request_id, &topics).await
    }

    async fn handle_get_shard_info(&mut self, request_id: u64, key: Vec<u8>) -> anyhow::Result<()> {
        let response = self
            .swim_sender
            .get_shard_info(key)
            .await?
            .map(|(group, leader)| ShardInfoResponse {
                shard_group_id: group.id.0,
                leader_node_id: leader.as_ref().map(|e| e.leader_node_id.to_string()),
                leader_addr: leader.map(|e| e.leader_addr),
                member_node_ids: group.members.iter().map(|n| n.to_string()).collect(),
            });
        self.write(request_id, &response).await
    }

    async fn handle_propose(&mut self, request_id: u64, req: ProposeRequest) -> anyhow::Result<()> {
        let response = self.execute_propose(req).await?;
        self.write(request_id, &response).await?;
        Ok(())
    }

    async fn execute_propose(&self, req: ProposeRequest) -> anyhow::Result<ProposeResponse> {
        let Some(shard_group) = self
            .swim_sender
            .resolve_shard_group(req.resource_key.clone())
            .await?
        else {
            return Ok(ProposeResponse::Error(ProposeError::ShardNotFound));
        };
        let raft_cmd = req.command.clone().into_raft_command(shard_group.members);
        let Some(result) = self.raft_sender.propose(shard_group.id, raft_cmd).await else {
            return Ok(ProposeResponse::Error(ProposeError::ShardNotFound));
        };
        let response = match result {
            Ok(()) => ProposeResponse::Success,
            Err(ProposeError::NotLeader(ref hint)) if !req.forwarded => {
                self.try_forward(hint.clone(), shard_group.id, req).await
            }
            Err(err) => ProposeResponse::Error(err),
        };
        Ok(response)
    }

    async fn try_forward(
        &self,
        leader_hint: Option<NodeId>,
        shard_group_id: ShardGroupId,
        req: ProposeRequest,
    ) -> ProposeResponse {
        if let Some(resp) = self.try_forward_via_hint(&leader_hint, &req).await {
            return resp;
        }
        if let Some(resp) = self.try_forward_via_gossip(shard_group_id, &req).await {
            return resp;
        }
        ProposeResponse::Error(ProposeError::NotLeader(leader_hint))
    }

    async fn try_forward_via_hint(
        &self,
        leader_hint: &Option<NodeId>,
        req: &ProposeRequest,
    ) -> Option<ProposeResponse> {
        let leader_id = leader_hint.as_ref()?;
        let node_addr = self
            .swim_sender
            .resolve_address(leader_id.clone())
            .await
            .inspect_err(|e| tracing::debug!("Resolve address for {} failed: {e}", leader_id))
            .ok()
            .flatten()?;
        Self::forward_to_leader(node_addr.client_addr, req)
            .await
            .inspect_err(|e| {
                tracing::debug!("Forward via hint to {} failed: {e}", node_addr.client_addr)
            })
            .ok()
    }

    async fn try_forward_via_gossip(
        &self,
        shard_group_id: ShardGroupId,
        req: &ProposeRequest,
    ) -> Option<ProposeResponse> {
        let entry = self
            .swim_sender
            .resolve_shard_leader(shard_group_id)
            .await
            .inspect_err(|e| {
                tracing::debug!("Resolve shard leader for {:?} failed: {e}", shard_group_id)
            })
            .ok()
            .flatten()?;
        Self::forward_to_leader(entry.leader_addr.client_addr, req)
            .await
            .inspect_err(|e| {
                tracing::debug!(
                    "Forward via gossip to {} failed: {e}",
                    entry.leader_addr.client_addr
                )
            })
            .ok()
    }

    async fn forward_to_leader(
        addr: SocketAddr,
        req: &ProposeRequest,
    ) -> anyhow::Result<ProposeResponse> {
        // Bound the forward attempt: a TCP connect to an unreachable host can stall
        // indefinitely, which would hold the client connection open for no reason.
        let stream = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            TcpStream::connect(addr),
        )
        .await
        .map_err(|_| anyhow::anyhow!("connect to leader timed out"))??;
        let (read_half, write_half) = stream.into_split();
        let mut writer = ClientRawWriter::new(write_half);
        let mut reader = ClientStreamReader::new(read_half);

        let forwarded_req = ConnectionRequests::Propose(ProposeRequest {
            forwarded: true,
            ..req.clone()
        });
        // request_id = 0: forwarded hop is internal; the response request_id is discarded.
        writer.write(0, &forwarded_req).await?;
        let (_, response) = reader.read_request().await?;
        Ok(response)
    }
}

pub struct ClientRawWriter {
    stream: OwnedWriteHalf,
}

impl ClientRawWriter {
    pub fn new(write_half: OwnedWriteHalf) -> Self {
        Self { stream: write_half }
    }

    pub async fn write<T: bincode::Encode>(
        &mut self,
        request_id: u64,
        data: &T,
    ) -> anyhow::Result<()> {
        let encoded = bincode::encode_to_vec(data, SERDE_CONFIG)?;
        let len = (REQUEST_ID_SIZE + encoded.len()) as u32;
        self.stream.write_all(&len.to_be_bytes()).await.expect("write len failed");
        self.stream
            .write_all(&request_id.to_be_bytes())
            .await
            .expect("write request_id failed");
        self.stream
            .write_all(&encoded)
            .await
            .expect("write encoded failed");
        Ok(())
    }
}

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
    pub async fn read_bytes(&mut self) -> Result<(u64, BytesMut), std::io::Error> {
        loop {
            if let Some(msg) = self.parse_frame()? {
                return Ok(msg);
            }
            self.read_more().await?;
        }
    }

    async fn read_more(&mut self) -> Result<(), std::io::Error> {
        let n = self.stream.read_buf(&mut self.buffer).await?;
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
                format!("Frame too small: len={len}, minimum is {REQUEST_ID_SIZE} (request_id field)"),
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
