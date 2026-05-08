use std::{io::ErrorKind, net::SocketAddr};

use crate::{
    clusters::{
        NodeId,
        raft::actor::RaftSender,
        raft::messages::ProposeError,
        swims::{ShardGroupId, SwimQueryCommand, actor::SwimSender},
    },
    connections::request::{
        ConnectionRequests, ProposeRequest, ProposeResponse, QueryCommand, ShardInfoResponse,
    },
    net::{OwnedReadHalf, OwnedWriteHalf, TcpStream},
};
use anyhow::bail;
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::SERDE_CONFIG;

pub struct ClientStreamWriter {
    pub(crate) stream: OwnedWriteHalf,
}

impl ClientStreamWriter {
    pub(crate) fn new(write_half: OwnedWriteHalf) -> Self {
        Self { stream: write_half }
    }

    // TODO: refactor
    pub(crate) async fn write<T: bincode::Encode>(&mut self, data: &T) -> anyhow::Result<()> {
        let encoded = bincode::encode_to_vec(data, SERDE_CONFIG)?;
        let len = (encoded.len() as u32).to_be_bytes();
        self.stream.write_all(&len).await.expect("write len failed");
        self.stream
            .write_all(&encoded)
            .await
            .expect("write encoded failed");
        Ok(())
    }

    pub(crate) async fn dispatch(
        &mut self,
        swim_sender: SwimSender,
        raft_sender: RaftSender,
        request: ConnectionRequests,
    ) -> anyhow::Result<()> {
        match request {
            ConnectionRequests::Connection(_) => Ok(()),
            ConnectionRequests::Query(q) => self.handle_query(swim_sender, q).await,
            ConnectionRequests::Propose(req) => {
                self.handle_propose(swim_sender, raft_sender, req).await
            }
        }
    }

    pub(crate) async fn handle_query(
        &mut self,
        swim_sender: SwimSender,
        query_type: QueryCommand,
    ) -> anyhow::Result<()> {
        match query_type {
            QueryCommand::GetMembers => self.handle_get_members(swim_sender).await,
            QueryCommand::GetShardInfo { key } => {
                self.handle_get_shard_info(swim_sender, key).await
            }
        }
    }

    async fn handle_get_members(&mut self, swim_sender: SwimSender) -> anyhow::Result<()> {
        let (send, recv) = tokio::sync::oneshot::channel();
        swim_sender
            .send(SwimQueryCommand::GetMembers { reply: send })
            .await?;
        let result = recv.await?;
        self.write(&result).await
    }

    async fn handle_get_shard_info(
        &mut self,
        swim_sender: SwimSender,
        key: Vec<u8>,
    ) -> anyhow::Result<()> {
        let response = swim_sender
            .get_shard_info(key)
            .await?
            .map(|(group, leader)| ShardInfoResponse {
                shard_group_id: group.id.0,
                leader_node_id: leader.as_ref().map(|e| e.leader_node_id.to_string()),
                leader_addr: leader.map(|e| e.leader_addr),
            });
        self.write(&response).await
    }

    pub(crate) async fn handle_propose(
        &mut self,
        swim_sender: SwimSender,
        raft_sender: RaftSender,
        req: ProposeRequest,
    ) -> anyhow::Result<()> {
        let response = Self::execute_propose(&swim_sender, &raft_sender, req).await?;
        self.write(&response).await?;
        Ok(())
    }

    async fn execute_propose(
        swim_sender: &SwimSender,
        raft_sender: &RaftSender,
        req: ProposeRequest,
    ) -> anyhow::Result<ProposeResponse> {
        let Some(shard_group) = swim_sender
            .resolve_shard_group(req.resource_key.clone())
            .await?
        else {
            return Ok(ProposeResponse::Error(ProposeError::ShardNotFound));
        };
        let raft_cmd = req.command.clone().into_raft_command(shard_group.members);
        let Some(result) = raft_sender.propose(shard_group.id, raft_cmd).await else {
            return Ok(ProposeResponse::Error(ProposeError::ShardNotFound));
        };
        let response = match result {
            Ok(()) => ProposeResponse::Success,
            Err(ProposeError::NotLeader(ref hint)) if !req.forwarded => {
                Self::try_forward(swim_sender, hint.clone(), shard_group.id, req).await
            }
            Err(err) => ProposeResponse::Error(err),
        };
        Ok(response)
    }

    async fn try_forward(
        swim_sender: &SwimSender,
        leader_hint: Option<NodeId>,
        shard_group_id: ShardGroupId,
        req: ProposeRequest,
    ) -> ProposeResponse {
        if let Some(resp) = Self::try_forward_via_hint(swim_sender, &leader_hint, &req).await {
            return resp;
        }
        if let Some(resp) = Self::try_forward_via_gossip(swim_sender, shard_group_id, &req).await {
            return resp;
        }
        ProposeResponse::Error(ProposeError::NotLeader(leader_hint))
    }

    async fn try_forward_via_hint(
        swim_sender: &SwimSender,
        leader_hint: &Option<NodeId>,
        req: &ProposeRequest,
    ) -> Option<ProposeResponse> {
        let leader_id = leader_hint.as_ref()?;
        let node_addr = swim_sender
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
        swim_sender: &SwimSender,
        shard_group_id: ShardGroupId,
        req: &ProposeRequest,
    ) -> Option<ProposeResponse> {
        let entry = swim_sender
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
        let stream = TcpStream::connect(addr).await?;
        let (read_half, write_half) = stream.into_split();
        let mut writer = ClientStreamWriter::new(write_half);
        let mut reader = ClientStreamReader::new(read_half);

        let forwarded_req = ConnectionRequests::Propose(ProposeRequest {
            forwarded: true,
            ..req.clone()
        });
        writer.write(&forwarded_req).await?;
        reader.read_request().await
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
    pub async fn read_bytes(&mut self) -> Result<BytesMut, std::io::Error> {
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
}
