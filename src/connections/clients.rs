use std::{io::ErrorKind, net::SocketAddr};

use crate::{
    clusters::{
        NodeAddress, NodeId,
        raft::messages::{MultiRaftActorCommand, ProposeError},
        swims::{ShardGroupId, ShardLeaderEntry, SwimCommand, SwimQueryCommand},
    },
    connections::request::{ConnectionRequests, ProposeRequest, ProposeResponse, QueryCommand},
    net::{OwnedReadHalf, OwnedWriteHalf, TcpStream},
};
use anyhow::bail;
use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc::Sender,
};

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

    pub(crate) async fn handle_query(
        &mut self,
        swim_sender: Sender<SwimCommand>,
        query_type: QueryCommand,
    ) -> anyhow::Result<()> {
        match query_type {
            QueryCommand::GetMembers => {
                let (send, recv) = tokio::sync::oneshot::channel();
                swim_sender
                    .send(SwimQueryCommand::GetMembers { reply: send }.into())
                    .await?;

                let result = recv.await?;
                self.write(&result).await.expect("Failed to write message");
                Ok(())
            }
        }
    }

    pub(crate) async fn handle_propose(
        &mut self,
        swim_sender: Sender<SwimCommand>,
        raft_tx: Sender<MultiRaftActorCommand>,
        req: ProposeRequest,
    ) -> anyhow::Result<()> {
        let (send, recv) = tokio::sync::oneshot::channel();
        swim_sender
            .send(
                SwimQueryCommand::ResolveShardGroup {
                    key: req.resource_key.clone(),
                    reply: send,
                }
                .into(),
            )
            .await?;

        let Some(shard_group) = recv.await? else {
            self.write(&ProposeResponse::Error(ProposeError::ShardNotFound))
                .await?;
            return Ok(());
        };

        let raft_cmd = req.command.clone().into_raft_command(shard_group.members);

        let (send, recv) = tokio::sync::oneshot::channel();
        raft_tx
            .send(MultiRaftActorCommand::Propose {
                shard_group_id: shard_group.id,
                command: raft_cmd,
                reply: send,
            })
            .await?;

        let response = match recv.await? {
            Ok(()) => ProposeResponse::Success,
            Err(ProposeError::NotLeader(ref hint)) if !req.forwarded => {
                Self::try_forward(&swim_sender, hint.clone(), shard_group.id, req).await
            }
            Err(err) => ProposeResponse::Error(err),
        };

        self.write(&response).await?;
        Ok(())
    }

    async fn try_forward(
        swim_sender: &Sender<SwimCommand>,
        leader_hint: Option<NodeId>,
        shard_group_id: ShardGroupId,
        req: ProposeRequest,
    ) -> ProposeResponse {
        // Try 1: use leader hint from Raft → resolve client_addr via SWIM member table
        if let Some(leader_id) = &leader_hint
            && let Some(node_addr) = Self::resolve_address(swim_sender, leader_id.clone()).await
            && let Ok(response) = Self::forward_to_leader(node_addr.client_addr, &req).await
        {
            return response;
        }

        // Try 2: fall back to shard leader gossip (may have different/newer leader info)
        if let Some(entry) = Self::resolve_shard_leader(swim_sender, shard_group_id).await
            && let Ok(response) = Self::forward_to_leader(entry.leader_addr.client_addr, &req).await
        {
            return response;
        }

        ProposeResponse::Error(ProposeError::NotLeader(leader_hint))
    }

    async fn resolve_address(
        swim_sender: &Sender<SwimCommand>,
        node_id: NodeId,
    ) -> Option<NodeAddress> {
        let (send, recv) = tokio::sync::oneshot::channel();
        swim_sender
            .send(
                SwimQueryCommand::ResolveAddress {
                    node_id,
                    reply: send,
                }
                .into(),
            )
            .await
            .ok()?;
        recv.await.ok()?
    }

    async fn resolve_shard_leader(
        swim_sender: &Sender<SwimCommand>,
        shard_group_id: ShardGroupId,
    ) -> Option<ShardLeaderEntry> {
        let (send, recv) = tokio::sync::oneshot::channel();
        swim_sender
            .send(
                SwimQueryCommand::ResolveShardLeader {
                    shard_group_id,
                    reply: send,
                }
                .into(),
            )
            .await
            .ok()?;
        recv.await.ok()?
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
}
