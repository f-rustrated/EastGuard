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
use std::{io::ErrorKind, mem::size_of};

const LEN_PREFIX_SIZE: usize = size_of::<u32>();
const REQUEST_ID_SIZE: usize = size_of::<u64>();

use anyhow::Context;
use tokio::sync::mpsc;

use crate::connections::protocol::{
    AdminRequest, AdminResponse, ClientRequest, ClientResponse, ControlPlaneRequest,
    ControlPlaneResponse, DataPlaneRequest, NodeInfo, NodeState, ShardDetail, TopicStats,
    TopicSummary,
};
use crate::{
    control_plane::{
        SwimNodeState,
        consensus::actor::MutlRaftSender,
        consensus::messages::ProposeError,
        membership::{ShardGroupId, actor::SwimSender},
        metadata::{
            command::{CreateTopic, DeleteTopic, MetadataCommand},
            strategy::StoragePolicy,
        },
    },
    net::{OwnedReadHalf, OwnedWriteHalf},
};
use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::SERDE_CONFIG;

#[allow(dead_code)]
pub struct ClientRawWriter {
    stream: OwnedWriteHalf,
}

#[allow(dead_code)]
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
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(&request_id.to_be_bytes()).await?;
        self.stream.write_all(&encoded).await?;
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

// ── Client handler ─────────────────────────────────────────────────────────

/// Pure request-to-response converter for one persistent client connection.
///
/// Shared across connections (wrappable in `Arc`) — holds no per-connection
/// state. The caller owns the writer channel and passes it into `run`.
///
/// `node_id`, `data_plane_sender`, and `routing_cache` will be added as fields
/// in PRs 4–5 once the corresponding types exist.
#[allow(dead_code)]
#[derive(Clone)]
pub struct ClientHandler {
    swim_sender: SwimSender,
    raft_sender: MutlRaftSender,
}

#[allow(dead_code)]
impl ClientHandler {
    pub fn new(swim_sender: SwimSender, raft_sender: MutlRaftSender) -> Self {
        Self {
            swim_sender,
            raft_sender,
        }
    }

    /// Reads frames in a loop, spawning one handler task per request.
    pub async fn run(
        &self,
        mut reader: ClientStreamReader,
        writer_tx: mpsc::Sender<(u64, ClientResponse)>,
    ) {
        loop {
            match reader.read_request::<ClientRequest>().await {
                Ok((request_id, request)) => {
                    let handler = self.clone();
                    let tx = writer_tx.clone();
                    tokio::spawn(async move {
                        let response = handler.dispatch(request).await;
                        if tx.send((request_id, response)).await.is_err() {
                            tracing::debug!("client writer closed");
                        }
                    });
                }
                Err(e) => {
                    tracing::debug!("client connection closed: {e}");
                    break;
                }
            }
        }
    }

    pub async fn dispatch(&self, request: ClientRequest) -> ClientResponse {
        match request {
            ClientRequest::ControlPlane(cp) => self.handle_control_plane(cp).await,
            ClientRequest::DataPlane(dp) => self.handle_data_plane(dp).await,
            ClientRequest::Admin(admin) => self.handle_admin(admin).await,
        }
    }

    async fn handle_control_plane(&self, request: ControlPlaneRequest) -> ClientResponse {
        let res = match request {
            ControlPlaneRequest::CreateTopic {
                name,
                storage_policy,
            } => self.handle_create_topic(name, storage_policy).await,
            ControlPlaneRequest::DeleteTopic { name } => self.handle_delete_topic(name).await,
            ControlPlaneRequest::ListHostedTopics => self.handle_list_hosted_topics().await,
            ControlPlaneRequest::DescribeTopic { .. } => todo!(),
        };
        match res {
            Ok(resp) => resp,
            Err(e) => {
                tracing::error!("client control plane dispatch error: {e}");
                ClientResponse::ControlPlane(ControlPlaneResponse::InternalError(e.to_string()))
            }
        }
    }

    async fn handle_create_topic(
        &self,
        name: String,
        storage_policy: StoragePolicy,
    ) -> anyhow::Result<ClientResponse> {
        let shard_group = self
            .swim_sender
            .resolve_shard_group(name.as_bytes().to_vec())
            .await?
            .context("shard group not found")?;

        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let cmd = CreateTopic {
            storage_policy,
            name,
            replica_set: shard_group.members,
            created_at,
        };

        Ok(
            match self
                .raft_sender
                .propose(shard_group.id, cmd.into())
                .await
                .context("shard group not found")?
            {
                Ok(()) => ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated),
                Err(ProposeError::NotLeader(_)) => {
                    ClientResponse::ControlPlane(ControlPlaneResponse::InternalError(
                        "not the leader for this shard group — retry on another node".into(),
                    ))
                }
                Err(e) => ClientResponse::ControlPlane(ControlPlaneResponse::InternalError(
                    format!("{e:?}"),
                )),
            },
        )
    }

    async fn handle_delete_topic(&self, name: String) -> anyhow::Result<ClientResponse> {
        let shard_group = self
            .swim_sender
            .resolve_shard_group(name.as_bytes().to_vec())
            .await?
            .context("shard group not found")?;

        let cmd = MetadataCommand::DeleteTopic(DeleteTopic { name });
        Ok(match self.raft_sender.propose(shard_group.id, cmd).await {
            Some(Ok(())) => ClientResponse::ControlPlane(ControlPlaneResponse::TopicDeleted),
            Some(Err(ProposeError::NotLeader(_))) => {
                ClientResponse::ControlPlane(ControlPlaneResponse::InternalError(
                    "not the leader for this shard group — retry on another node".into(),
                ))
            }
            Some(Err(e)) => {
                ClientResponse::ControlPlane(ControlPlaneResponse::InternalError(format!("{e:?}")))
            }
            None => ClientResponse::ControlPlane(ControlPlaneResponse::TopicNotFound),
        })
    }

    async fn handle_list_hosted_topics(&self) -> anyhow::Result<ClientResponse> {
        let topics = self
            .raft_sender
            .get_topics()
            .await
            .into_iter()
            .map(|name| TopicSummary {
                name,
                range_count: 0,
                state: crate::connections::protocol::TopicState::Active,
            })
            .collect();
        Ok(ClientResponse::ControlPlane(
            ControlPlaneResponse::TopicList { topics },
        ))
    }

    async fn handle_data_plane(&self, request: DataPlaneRequest) -> ClientResponse {
        match request {
            DataPlaneRequest::Produce { .. } => todo!(),
            DataPlaneRequest::Fetch { .. } => todo!(),
            DataPlaneRequest::ListOffsets { .. } => todo!(),
        }
    }

    async fn handle_admin(&self, request: AdminRequest) -> ClientResponse {
        let res = match request {
            AdminRequest::DescribeCluster => self.handle_describe_cluster().await,
            AdminRequest::ListHostedTopicsWithStats => {
                self.handle_list_hosted_topics_with_stats().await
            }
            AdminRequest::SplitRange { .. } => todo!(),
            AdminRequest::GetShardInfo { key } => self.handle_get_shard_info(key).await,
            AdminRequest::GetShardLeader { shard_group_id } => {
                self.handle_get_shard_leader(shard_group_id).await
            }
        };
        res.unwrap_or_else(|e| {
            tracing::error!("client admin dispatch error: {e}");
            ClientResponse::Admin(AdminResponse::InternalError(e.to_string()))
        })
    }

    async fn handle_describe_cluster(&self) -> anyhow::Result<ClientResponse> {
        let members = self.swim_sender.get_members().await?;
        let nodes = members
            .into_iter()
            .map(|m| NodeInfo {
                node_id: m.node_id.to_string(),
                addr: m.addr.client_addr,
                state: match m.state {
                    SwimNodeState::Alive => NodeState::Alive,
                    SwimNodeState::Suspect => NodeState::Suspect,
                    SwimNodeState::Dead => NodeState::Dead,
                },
            })
            .collect();
        Ok(ClientResponse::Admin(AdminResponse::ClusterInfo { nodes }))
    }

    async fn handle_list_hosted_topics_with_stats(&self) -> anyhow::Result<ClientResponse> {
        let topics = self
            .raft_sender
            .get_topic_stats()
            .await
            .into_iter()
            .map(|s| TopicStats {
                name: s.name,
                range_count: s.range_count,
                total_bytes: s.total_bytes,
            })
            .collect();
        Ok(ClientResponse::Admin(AdminResponse::TopicStats { topics }))
    }

    async fn handle_get_shard_info(&self, key: Vec<u8>) -> anyhow::Result<ClientResponse> {
        let detail = self
            .swim_sender
            .get_shard_info(key)
            .await?
            .map(|(group, leader)| ShardDetail {
                shard_group_id: group.id.0,
                leader_node_id: leader.as_ref().map(|e| e.leader_node_id.to_string()),
                leader_addr: leader.map(|e| e.leader_addr.client_addr),
                member_node_ids: group.members.iter().map(|n| n.to_string()).collect(),
            });
        Ok(ClientResponse::Admin(AdminResponse::ShardInfo { detail }))
    }

    async fn handle_get_shard_leader(&self, shard_group_id: u64) -> anyhow::Result<ClientResponse> {
        let leader = self
            .raft_sender
            .get_leader(ShardGroupId(shard_group_id))
            .await
            .map(|n| n.to_string());
        Ok(ClientResponse::Admin(AdminResponse::ShardLeader { leader }))
    }
}

/// Writer task for a single client connection.
///
/// Drains `rx` and writes length-prefixed response frames to `write_half`.
/// Exits when the sender side of `rx` is dropped (connection closed).
#[allow(dead_code)]
pub async fn run_client_writer(
    mut write_half: OwnedWriteHalf,
    mut rx: mpsc::Receiver<(u64, ClientResponse)>,
) -> anyhow::Result<()> {
    while let Some((request_id, response)) = rx.recv().await {
        let encoded = bincode::encode_to_vec(&response, SERDE_CONFIG)?;
        let len = (REQUEST_ID_SIZE + encoded.len()) as u32;
        write_half.write_all(&len.to_be_bytes()).await?;
        write_half.write_all(&request_id.to_be_bytes()).await?;
        write_half.write_all(&encoded).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use crate::connections::protocol::{
        AdminRequest, AdminResponse, ClientRequest, ClientResponse, ControlPlaneRequest,
        ControlPlaneResponse, NodeState,
    };
    use crate::control_plane::consensus::actor::MultiRaftActor;
    use crate::control_plane::consensus::messages::MultiRaftActorCommand;
    use crate::control_plane::membership::actor::SwimActor;
    use crate::control_plane::membership::{
        ShardGroup, ShardGroupId, ShardLeaderEntry, SwimActorCommand, QueryCommand,
    };
    use crate::control_plane::metadata::types::TopicStats as MetadataTopicStats;
    use crate::control_plane::{NodeAddress, NodeId, SwimNode, SwimNodeState};

    use super::ClientHandler;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn node_id(s: &str) -> NodeId {
        NodeId::new(s)
    }

    fn test_shard_group() -> ShardGroup {
        ShardGroup {
            id: ShardGroupId(42),
            members: vec![node_id("node-1")],
        }
    }

    fn swim_sender_with(
        handler: impl Fn(SwimActorCommand) + Send + 'static,
    ) -> crate::control_plane::membership::actor::SwimSender {
        let (tx, mut rx) = SwimActor::channel(16);
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                handler(cmd);
            }
        });
        tx
    }

    fn raft_sender_with(
        handler: impl Fn(MultiRaftActorCommand) + Send + 'static,
    ) -> crate::control_plane::consensus::actor::MutlRaftSender {
        let (tx, mut rx) = MultiRaftActor::channel(16);
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                handler(cmd);
            }
        });
        tx
    }

    #[tokio::test]
    async fn create_topic_ok() {
        let swim = swim_sender_with(|cmd| {
            if let SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) = cmd
            {
                let _ = reply.send(Some(test_shard_group()));
            }
        });
        let raft = raft_sender_with(|cmd| {
            if let MultiRaftActorCommand::Propose { reply, .. } = cmd {
                let _ = reply.send(Ok(()));
            }
        });
        let resp = ClientHandler::new(swim, raft)
            .dispatch(ClientRequest::ControlPlane(
                ControlPlaneRequest::CreateTopic {
                    name: "t1".into(),
                    storage_policy: crate::control_plane::metadata::strategy::StoragePolicy {
                        retention_ms: 3_600_000,
                        replication_factor: 1,
                        partition_strategy:
                            crate::control_plane::metadata::strategy::PartitionStrategy::AutoSplit,
                    },
                },
            ))
            .await;
        assert!(
            matches!(
                resp,
                ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated)
            ),
            "expected TopicCreated, got {resp:?}"
        );
    }

    #[tokio::test]
    async fn create_topic_shard_not_found() {
        let swim = swim_sender_with(|cmd| {
            if let SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) = cmd
            {
                let _ = reply.send(None);
            }
        });
        let resp = ClientHandler::new(swim, raft_sender_with(|_| {}))
            .dispatch(ClientRequest::ControlPlane(
                ControlPlaneRequest::CreateTopic {
                    name: "t1".into(),
                    storage_policy: crate::control_plane::metadata::strategy::StoragePolicy {
                        retention_ms: 3_600_000,
                        replication_factor: 1,
                        partition_strategy:
                            crate::control_plane::metadata::strategy::PartitionStrategy::AutoSplit,
                    },
                },
            ))
            .await;
        assert!(
            matches!(
                resp,
                ClientResponse::ControlPlane(ControlPlaneResponse::InternalError(_))
            ),
            "expected InternalError, got {resp:?}"
        );
    }

    #[tokio::test]
    async fn delete_topic_ok() {
        let swim = swim_sender_with(|cmd| {
            if let SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) = cmd
            {
                let _ = reply.send(Some(test_shard_group()));
            }
        });
        let raft = raft_sender_with(|cmd| {
            if let MultiRaftActorCommand::Propose { reply, .. } = cmd {
                let _ = reply.send(Ok(()));
            }
        });
        let resp = ClientHandler::new(swim, raft)
            .dispatch(ClientRequest::ControlPlane(
                ControlPlaneRequest::DeleteTopic { name: "t1".into() },
            ))
            .await;
        assert!(
            matches!(
                resp,
                ClientResponse::ControlPlane(ControlPlaneResponse::TopicDeleted)
            ),
            "expected TopicDeleted, got {resp:?}"
        );
    }

    #[tokio::test]
    async fn list_hosted_topics() {
        let raft = raft_sender_with(|cmd| {
            if let MultiRaftActorCommand::GetTopics { reply } = cmd {
                let _ = reply.send(vec!["alpha".into(), "beta".into()]);
            }
        });
        let resp = ClientHandler::new(swim_sender_with(|_| {}), raft)
            .dispatch(ClientRequest::ControlPlane(
                ControlPlaneRequest::ListHostedTopics,
            ))
            .await;
        let ClientResponse::ControlPlane(ControlPlaneResponse::TopicList { topics }) = resp else {
            panic!("expected TopicList, got {resp:?}");
        };
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0].name, "alpha");
        assert_eq!(topics[1].name, "beta");
    }

    #[tokio::test]
    async fn describe_cluster_maps_node_states() {
        let node_addr = NodeAddress::test(addr(18001), addr(8081));
        let nodes = vec![
            SwimNode {
                node_id: node_id("n1"),
                addr: node_addr,
                state: SwimNodeState::Alive,
                incarnation: 0,
            },
            SwimNode {
                node_id: node_id("n2"),
                addr: node_addr,
                state: SwimNodeState::Dead,
                incarnation: 1,
            },
        ];
        let swim = {
            let nodes = nodes.clone();
            swim_sender_with(move |cmd| {
                if let SwimActorCommand::Query(QueryCommand::GetMembers { reply }) = cmd {
                    let _ = reply.send(nodes.clone());
                }
            })
        };
        let resp = ClientHandler::new(swim, raft_sender_with(|_| {}))
            .dispatch(ClientRequest::Admin(AdminRequest::DescribeCluster))
            .await;
        let ClientResponse::Admin(AdminResponse::ClusterInfo { nodes: info }) = resp else {
            panic!("expected ClusterInfo, got {resp:?}");
        };
        assert_eq!(info.len(), 2);
        assert_eq!(info[0].node_id, "n1");
        assert!(matches!(info[0].state, NodeState::Alive));
        assert!(matches!(info[1].state, NodeState::Dead));
    }

    #[tokio::test]
    async fn get_shard_info_present() {
        let node_addr = NodeAddress::test(addr(18001), addr(8081));
        let group = test_shard_group();
        let leader = ShardLeaderEntry {
            leader_node_id: node_id("n1"),
            leader_addr: node_addr,
            term: 3,
        };
        let swim = {
            let group = group.clone();
            let leader = leader.clone();
            swim_sender_with(move |cmd| {
                if let SwimActorCommand::Query(QueryCommand::GetShardInfo { reply, .. }) = cmd {
                    let _ = reply.send(Some((group.clone(), Some(leader.clone()))));
                }
            })
        };
        let resp = ClientHandler::new(swim, raft_sender_with(|_| {}))
            .dispatch(ClientRequest::Admin(AdminRequest::GetShardInfo {
                key: b"any".to_vec(),
            }))
            .await;
        let ClientResponse::Admin(AdminResponse::ShardInfo { detail: Some(d) }) = resp else {
            panic!("expected ShardInfo with detail, got {resp:?}");
        };
        assert_eq!(d.shard_group_id, 42);
        assert_eq!(d.leader_node_id.as_deref(), Some("n1"));
        assert_eq!(d.leader_addr, Some(addr(8081)));
        assert_eq!(d.member_node_ids, vec!["node-1"]);
    }

    #[tokio::test]
    async fn get_shard_info_absent() {
        let swim = swim_sender_with(|cmd| {
            if let SwimActorCommand::Query(QueryCommand::GetShardInfo { reply, .. }) = cmd {
                let _ = reply.send(None);
            }
        });
        let resp = ClientHandler::new(swim, raft_sender_with(|_| {}))
            .dispatch(ClientRequest::Admin(AdminRequest::GetShardInfo {
                key: b"x".to_vec(),
            }))
            .await;
        assert!(
            matches!(
                resp,
                ClientResponse::Admin(AdminResponse::ShardInfo { detail: None })
            ),
            "expected ShardInfo{{None}}, got {resp:?}"
        );
    }

    #[tokio::test]
    async fn get_shard_leader() {
        let raft = raft_sender_with(|cmd| {
            if let MultiRaftActorCommand::GetLeader { reply, .. } = cmd {
                let _ = reply.send(Some(node_id("n1")));
            }
        });
        let resp = ClientHandler::new(swim_sender_with(|_| {}), raft)
            .dispatch(ClientRequest::Admin(AdminRequest::GetShardLeader {
                shard_group_id: 42,
            }))
            .await;
        let ClientResponse::Admin(AdminResponse::ShardLeader { leader }) = resp else {
            panic!("expected ShardLeader, got {resp:?}");
        };
        assert_eq!(leader.as_deref(), Some("n1"));
    }

    #[tokio::test]
    async fn list_topic_stats() {
        let raft = raft_sender_with(|cmd| {
            if let MultiRaftActorCommand::GetTopicStats { reply } = cmd {
                let _ = reply.send(vec![MetadataTopicStats {
                    name: "t1".into(),
                    range_count: 2,
                    total_bytes: 1024,
                }]);
            }
        });
        let resp = ClientHandler::new(swim_sender_with(|_| {}), raft)
            .dispatch(ClientRequest::Admin(
                AdminRequest::ListHostedTopicsWithStats,
            ))
            .await;
        let ClientResponse::Admin(AdminResponse::TopicStats { topics }) = resp else {
            panic!("expected TopicStats, got {resp:?}");
        };
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "t1");
        assert_eq!(topics[0].range_count, 2);
        assert_eq!(topics[0].total_bytes, 1024);
    }
}
