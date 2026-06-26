use crate::connections::reader::ClientStreamReader;
use crate::connections::writer::ClientRawWriter;
use crate::connections::{protocol::*, run_client_writer};
use crate::control_plane::consensus::raft::errors::ProposalError;
use crate::control_plane::metadata::RangeMeta;
use crate::control_plane::{
    NodeAddress, NodeId, SwimNodeState,
    consensus::actor::MutlRaftSender,
    membership::{
        ShardGroupId,
        actor::{ShardRouting, SwimSender},
    },
    metadata::{
        RangeId, TopicId,
        command::{CreateTopic, DeleteTopic, MetadataCommand},
        strategy::StoragePolicy,
    },
};
use crate::data_plane::EntryPayload;
use crate::data_plane::SegmentKey;
use crate::data_plane::actor::DataPlaneSender;
use crate::data_plane::messages::command::{Produce, ProduceAck};
use crate::data_plane::messages::query::{DataPlaneQuery, Fetch, ListOffsets};
use crate::net::TcpStream;
use anyhow::Context;
use tokio::sync::mpsc;
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

#[derive(Clone)]
pub struct ClientController {
    node_id: NodeId,
    swim_sender: SwimSender,
    raft_sender: MutlRaftSender,
    data_plane_tx: DataPlaneSender,
}

impl ClientController {
    fn new(
        node_id: NodeId,
        swim_sender: SwimSender,
        raft_sender: MutlRaftSender,
        data_plane_tx: DataPlaneSender,
    ) -> Self {
        Self {
            node_id,
            swim_sender,
            raft_sender,
            data_plane_tx,
        }
    }

    /// Reads frames in a loop, spawning one handler task per request.
    pub async fn run(
        &self,
        mut reader: ClientStreamReader,
        writer_tx: mpsc::Sender<(u64, ClientResponse)>,
    ) {
        loop {
            // TODO idempotency based on request id (or separate idempotence key)
            match reader.read_request::<ClientRequest>().await {
                Ok((request_id, request)) => {
                    let response = self.dispatch(request).await;
                    if writer_tx.send((request_id, response)).await.is_err() {
                        tracing::debug!("client writer closed");
                    }
                }
                Err(e) => {
                    tracing::debug!("client connection closed: {e}");
                    break;
                }
            }
        }

        let _ = writer_tx.send((0, ClientResponse::Stop)).await;
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
            ControlPlaneRequest::ListHostedTopics => return self.handle_list_hosted_topics().await,
            ControlPlaneRequest::DescribeTopic { name } => self.handle_describe_topic(name).await,
        };
        match res {
            Ok(resp) => resp,
            Err(e) => {
                tracing::error!("client control plane dispatch error: {e}");
                ClientResponse::ControlPlane(ControlPlaneResponse::InternalError(e.to_string()))
            }
        }
    }

    /// If this node belongs to the topic's owning shard group, answers directly;
    /// otherwise it redirects to a member so the consumer retries against the right
    /// node — no server-side proxying.
    async fn handle_describe_topic(&self, topic_name: String) -> anyhow::Result<ClientResponse> {
        if let ShardRouting::Redirect(member) = self.route(topic_name.as_bytes().to_vec()).await? {
            return Ok(self.control_plane_redirect(member).into());
        }

        let Some(meta) = self.raft_sender.get_topic_metadata(topic_name).await else {
            return Ok(ControlPlaneResponse::TopicNotFound.into());
        };

        let addresses = self.swim_sender.list_all_node_addresses().await?;
        let detail = TopicDetail::from_meta(meta, &addresses);
        Ok(ControlPlaneResponse::TopicDetail(detail).into())
    }

    async fn handle_create_topic(
        &self,
        name: String,
        storage_policy: StoragePolicy,
    ) -> anyhow::Result<ClientResponse> {
        let group = match self.route(name.as_bytes().to_vec()).await? {
            ShardRouting::Local(group) => group,
            ShardRouting::Redirect(member) => {
                return Ok(self.control_plane_redirect(member).into());
            }
        };

        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let cmd = CreateTopic {
            storage_policy,
            name,
            replica_set: group.members,
            created_at,
        };
        self.propose_topic_write(group.id, cmd.into(), ControlPlaneResponse::TopicCreated)
            .await
    }

    async fn handle_delete_topic(&self, topic_name: String) -> anyhow::Result<ClientResponse> {
        let group = match self.route(topic_name.as_bytes().to_vec()).await? {
            ShardRouting::Local(group) => group,
            ShardRouting::Redirect(member) => {
                return Ok(self.control_plane_redirect(member).into());
            }
        };

        let cmd = DeleteTopic { name: topic_name };
        self.propose_topic_write(group.id, cmd.into(), ControlPlaneResponse::TopicDeleted)
            .await
    }

    /// Route a key relative to this node — the single SWIM call behind every
    /// membership-first handler.
    async fn route(&self, key: Vec<u8>) -> anyhow::Result<ShardRouting> {
        self.swim_sender
            .resolve_shard_routing(key, &self.node_id)
            .await
    }

    /// Structural redirect for a control-plane op that isn't local: to the member if
    /// one resolves, else a retriable error (no member's address known here yet).
    fn control_plane_redirect(
        &self,
        member: Option<(NodeId, NodeAddress)>,
    ) -> ControlPlaneResponse {
        match member {
            Some((node_id, addr)) => ControlPlaneResponse::TopicMetadataRedirect {
                owner: NodeAddressInfo {
                    node_id: node_id.to_string(),
                    client_addr: addr.client_addr(),
                },
            },
            None => {
                ControlPlaneResponse::InternalError("no reachable member of the shard group".into())
            }
        }
    }

    /// Propose a control-plane write to the group's Raft leader. `on_ok` on success;
    /// any non-leader / not-ready rejection becomes a retriable `NotRaftLeader` redirect.
    async fn propose_topic_write(
        &self,
        group_id: ShardGroupId,
        cmd: MetadataCommand,
        on_ok: ControlPlaneResponse,
    ) -> anyhow::Result<ClientResponse> {
        let leader = match self
            .raft_sender
            .submit_metadata_command(group_id, cmd)
            .await
        {
            Ok(()) => return Ok(on_ok.into()),
            Err(ProposalError::NotLeader(leader)) => leader,
            Err(ProposalError::ShardNotFound | ProposalError::ShardGroupRemoved) => None,
        };

        let leader_addr = match leader {
            Some(l) => {
                self.swim_sender
                    .resolve_address(l.clone())
                    .await?
                    .map(|a| NodeAddressInfo {
                        node_id: l.to_string(),
                        client_addr: a.client_addr(),
                    })
            }
            None => None,
        };
        Ok(ControlPlaneResponse::NotRaftLeader { leader_addr }.into())
    }

    async fn handle_list_hosted_topics(&self) -> ClientResponse {
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
        ClientResponse::ControlPlane(ControlPlaneResponse::TopicList { topics })
    }

    async fn handle_data_plane(&self, request: ClientDataPlaneRequest) -> ClientResponse {
        let res = match request {
            ClientDataPlaneRequest::Produce(req) => self.handle_produce(req).await,
            ClientDataPlaneRequest::Fetch(req) => self.handle_fetch(req).await,
            ClientDataPlaneRequest::FetchById(req) => self.handle_fetch_by_id(req).await,
            ClientDataPlaneRequest::ListOffsets(req) => self.handle_list_offsets(req).await,
        };
        match res {
            Ok(resp) => resp,
            Err(e) => {
                tracing::error!("client data plane dispatch error: {e}");
                ClientResponse::DataPlane(DataPlaneResponse::InternalError(e.to_string()))
            }
        }
    }

    /// Routes a produce to the active segment's write leader (`replica_set[0]`),
    /// redirecting if this node isn't it. Membership-first (like `handle_describe_topic`)
    /// so we can tell "topic absent" from "not hosted here".
    async fn handle_produce(&self, req: ProduceRequest) -> anyhow::Result<ClientResponse> {
        // Not local (ring unconverged or this node isn't a member) → retriable
        // redirect; the hint is best-effort, absent until SWIM converges.
        if let ShardRouting::Redirect(member) =
            self.route(req.topic_name.as_bytes().to_vec()).await?
        {
            let hint_node = member.map(|(_, addr)| addr.client_addr());
            return Ok(DataPlaneResponse::ShardNotLocal { hint_node }.into());
        }

        let Some(meta) = self.raft_sender.get_topic_metadata(req.topic_name).await else {
            return Ok(DataPlaneResponse::TopicNotFound.into());
        };

        let Some(range) = meta.route_active_range(&req.routing_key) else {
            return Ok(DataPlaneResponse::TopicNotFound.into());
        };

        // Invariant: an active range has exactly one active segment.
        let Some(seg) = range.active_segment.and_then(|id| range.segments.get(&id)) else {
            return Ok(DataPlaneResponse::InternalError(
                "active range has no active segment".into(),
            )
            .into());
        };

        if seg.replica_set.first() != Some(&self.node_id) {
            let leader_addr = match seg.replica_set.first() {
                Some(leader) => self
                    .swim_sender
                    .resolve_address(leader.clone())
                    .await?
                    .map(|a| a.client_addr()),
                None => None,
            };
            return Ok(DataPlaneResponse::NotWriteLeader { leader_addr }.into());
        }

        let segment_key = SegmentKey::new(meta.id, range.range_id, seg.segment_id);
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let cmd = Produce {
            segment_key,
            data: EntryPayload::from(req.data),
            record_count: req.record_count,
            reply: reply_tx,
        };
        let _ = self.data_plane_tx.send(cmd);

        let ack = reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("data plane dropped produce reply"))?;

        Ok(match ack {
            ProduceAck::Ok { entry_id } => DataPlaneResponse::Produced { entry_id }.into(),
            // Metadata named us leader but the assignment hasn't applied yet — retriable.
            ProduceAck::Err(reason) if reason == "not leader" || reason == "segment not found" => {
                DataPlaneResponse::NotWriteLeader { leader_addr: None }.into()
            }
            ProduceAck::Err(reason) => DataPlaneResponse::InternalError(reason).into(),
        })
    }

    /// Consume read path.
    /// 1. Resolves topic_name -> TopicMeta via the metadata RSM (one round trip),
    /// 2. validates the optional keyspace bound,
    /// 3. computes the range's current `RangeProgressSignal` from its lineage
    /// 4. dispatches a `DataPlaneQuery::Fetch` carrying everything the sync data-plane
    ///
    /// state machine needs to answer without any further I/O.
    async fn handle_fetch(&self, req: FetchRequest) -> anyhow::Result<ClientResponse> {
        let Some(meta) = self.raft_sender.get_topic_metadata(req.topic_name).await else {
            return Ok(DataPlaneResponse::TopicNotFound.into());
        };
        let Some(range) = meta.ranges.get(&RangeId(req.range_id)) else {
            return Ok(DataPlaneResponse::TopicNotFound.into());
        };
        if !keyspace_bound_matches_range(&req.keyspace_bound, range) {
            return Ok(DataPlaneResponse::KeyspaceBoundNarrowed.into());
        }
        let progress_signal = RangeProgressSignal::compute_progress_signal(range, &meta);

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let query = DataPlaneQuery::Fetch(Fetch {
            topic_id: meta.id,
            range_id: RangeId(req.range_id),
            entry_id: req.entry_id,
            max_bytes: req.max_bytes,
            progress_signal,
            reply: reply_tx,
        });
        if self.data_plane_tx.send(query).is_err() {
            return Ok(DataPlaneResponse::InternalError("data plane closed".into()).into());
        }
        let result = reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("data plane dropped reply"))?;
        Ok(DataPlaneResponse::from_fetch_result(result).into())
    }

    /// Consume read path addressed by resolved `topic_id` (the client resolved the
    /// name via `DescribeTopic`). Serves straight from this node's local segment
    /// store, so a replica that holds the segment but isn't a metadata peer can
    /// serve it. No proxying: a miss returns `SegmentNotLocal` and the client
    /// retries another replica.
    async fn handle_fetch_by_id(&self, req: FetchByIdRequest) -> anyhow::Result<ClientResponse> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let query = DataPlaneQuery::Fetch(Fetch {
            topic_id: TopicId(req.topic_id),
            range_id: req.range_id,
            entry_id: req.entry_id,
            max_bytes: req.max_bytes,
            // The by-id consumer already has the range lineage from DescribeTopic,
            // so the response's progress signal isn't authoritative here.
            progress_signal: RangeProgressSignal::Active,
            reply: reply_tx,
        });
        if self.data_plane_tx.send(query).is_err() {
            return Ok(DataPlaneResponse::InternalError("data plane closed".into()).into());
        }
        let result = reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("data plane dropped reply"))?;
        Ok(DataPlaneResponse::from_fetch_result(result).into())
    }

    /// Offset-bounds query — used by consumers to bound their read window
    /// for the range's currently-active segment on this node.
    async fn handle_list_offsets(&self, req: ListOffsetsRequest) -> anyhow::Result<ClientResponse> {
        let Some(meta) = self.raft_sender.get_topic_metadata(req.topic_name).await else {
            return Ok(DataPlaneResponse::TopicNotFound.into());
        };
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

        let query: DataPlaneQuery = ListOffsets {
            topic_id: meta.id,
            range_id: req.range_id,
            reply: reply_tx,
        }
        .into();

        let _ = self.data_plane_tx.send(query);

        let list_offset_result = reply_rx.await.context("data plane dropped reply")?;
        let result = DataPlaneResponse::from_list_offset_result(list_offset_result);
        Ok(result.into())
    }

    async fn handle_admin(&self, request: AdminRequest) -> ClientResponse {
        let res = match request {
            AdminRequest::DescribeCluster => self.handle_describe_cluster().await,
            AdminRequest::ListHostedTopicsWithStats => {
                self.handle_list_hosted_topics_with_stats().await
            }

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
                addr: m.addr.client_addr(),
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
                leader_addr: leader.map(|e| e.leader_addr.client_addr()),
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

// ── Pure helpers for the fetch path ────────────────────────────────────────

/// D4 accepts only `None` or a bound matching the range's full keyspace.
/// Anything narrower is rejected with `KeyspaceBoundNarrowed` so the future
/// consumer-group layer can enable server-side narrowing without a wire-format
/// break (see d4_consumer_range_tracking.md §Wire Protocol).
fn keyspace_bound_matches_range(bound: &Option<KeyspaceBound>, range: &RangeMeta) -> bool {
    match bound {
        None => true,
        Some(b) => b.start == range.keyspace_start && b.end == range.keyspace_end,
    }
}

pub async fn handle_client_stream(
    stream: TcpStream,
    node_id: NodeId,
    swim_sender: SwimSender,
    raft_sender: MutlRaftSender,
    data_plane_tx: DataPlaneSender,
) {
    let (read_half, write_half) = stream.into_split();
    let (writer_tx, writer_rx) = mpsc::channel(128);
    let handler = ClientController::new(node_id, swim_sender, raft_sender, data_plane_tx);
    tokio::spawn(run_client_writer(
        ClientRawWriter::new(write_half),
        writer_rx,
    ));
    handler
        .run(ClientStreamReader::new(read_half), writer_tx)
        .await;
}

#[cfg(test)]
mod tests {
    use crate::connections::protocol::{
        AdminRequest, AdminResponse, ClientDataPlaneRequest, ClientRequest, ClientResponse,
        ControlPlaneRequest, ControlPlaneResponse, DataPlaneResponse, NodeState, ProduceRequest,
    };
    use crate::control_plane::consensus::actor::MultiRaftActor;
    use crate::control_plane::consensus::messages::MultiRaftActorCommand;
    use crate::control_plane::membership::actor::SwimActor;
    use crate::control_plane::membership::{
        QueryCommand, ShardGroup, ShardGroupId, ShardLeaderEntry, SwimActorCommand,
    };
    use crate::control_plane::metadata::TopicStats as MetadataTopicStats;
    use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
    use crate::control_plane::metadata::{TopicId, TopicMeta};
    use crate::control_plane::{NodeAddress, NodeId, SwimNode, SwimNodeState};
    use crate::data_plane::actor::DataPlaneSender;
    use crate::data_plane::messages::DataPlaneMessage;
    use crate::data_plane::messages::command::{DataPlaneCommand, ProduceAck};
    use std::net::SocketAddr;

    use super::ClientController;

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

    /// Test stub for the data-plane sender. Existing tests don't exercise the
    /// data plane path, so a dropped-receiver channel is fine — sends would
    /// fail, but the tested code paths never send.
    fn dp_stub() -> DataPlaneSender {
        let (tx, _) = flume::bounded(1);
        DataPlaneSender(tx)
    }

    /// Data-plane stub that acks every produce — for the dispatch-when-leader path.
    fn dp_acking() -> DataPlaneSender {
        let (tx, rx) = flume::bounded::<DataPlaneMessage>(16);
        tokio::spawn(async move {
            while let Ok(msg) = rx.recv_async().await {
                if let DataPlaneMessage::Command(DataPlaneCommand::Produce(p)) = msg {
                    let _ = p.reply.send(ProduceAck::Ok { entry_id: 7 });
                }
            }
        });
        DataPlaneSender(tx)
    }

    fn produce_req() -> ClientRequest {
        ClientRequest::DataPlane(ClientDataPlaneRequest::Produce(ProduceRequest {
            topic_name: "t1".into(),
            routing_key: b"k".to_vec(),
            data: b"hello".to_vec(),
            record_count: 1,
        }))
    }

    /// A single full-keyspace topic whose active segment's write leader is `leader`.
    fn topic_meta(leader: &str) -> TopicMeta {
        TopicMeta::new(
            "t1".into(),
            TopicId(1),
            vec![node_id(leader)],
            0,
            StoragePolicy {
                retention_ms: Some(3_600_000),
                replication_factor: 1,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
        )
    }

    /// Ring can't map the key yet (topology not converged) → retriable
    /// `ShardNotLocal` with no hint, not `TopicNotFound`.
    #[tokio::test]
    async fn produce_redirects_when_ring_unresolved() {
        let swim = swim_sender_with(|cmd| {
            if let SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) = cmd {
                let _ = reply.send(None);
            }
        });
        let resp =
            ClientController::new(node_id("self"), swim, raft_sender_with(|_| {}), dp_stub())
                .dispatch(produce_req())
                .await;
        assert!(
            matches!(
                resp,
                ClientResponse::DataPlane(DataPlaneResponse::ShardNotLocal { hint_node: None })
            ),
            "expected ShardNotLocal{{None}}, got {resp:?}"
        );
    }

    /// Not a member of the topic's shard group → redirect to a member, no metadata
    /// lookup (membership-first).
    #[tokio::test]
    async fn produce_redirects_when_not_member() {
        let owner = node_id("owner");
        let owner_addr = NodeAddress::test(addr(18003), addr(8083));
        let group = ShardGroup {
            id: ShardGroupId(42),
            members: vec![owner.clone()],
        };
        let swim = swim_sender_with(move |cmd| match cmd {
            SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) => {
                let _ = reply.send(Some(group.clone()));
            }
            SwimActorCommand::Query(QueryCommand::ResolveAddress { node_id: q, reply }) => {
                let _ = reply.send((q == owner).then_some(owner_addr));
            }
            _ => {}
        });
        let resp =
            ClientController::new(node_id("self"), swim, raft_sender_with(|_| {}), dp_stub())
                .dispatch(produce_req())
                .await;
        let ClientResponse::DataPlane(DataPlaneResponse::ShardNotLocal { hint_node }) = resp else {
            panic!("expected ShardNotLocal, got {resp:?}");
        };
        assert_eq!(hint_node, Some(addr(8083)));
    }

    /// A member but not the segment's write leader → `NotWriteLeader` carrying the
    /// leader's resolved client address.
    #[tokio::test]
    async fn produce_redirects_to_segment_leader() {
        let me = node_id("self");
        let leader = node_id("leader");
        let leader_addr = NodeAddress::test(addr(18004), addr(8084));
        let group = ShardGroup {
            id: ShardGroupId(1),
            members: vec![me.clone(), leader.clone()],
        };
        let swim = swim_sender_with(move |cmd| match cmd {
            SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) => {
                let _ = reply.send(Some(group.clone()));
            }
            SwimActorCommand::Query(QueryCommand::ResolveAddress { node_id: q, reply }) => {
                let _ = reply.send((q == leader).then_some(leader_addr));
            }
            _ => {}
        });
        let raft = raft_sender_with(|cmd| {
            if let MultiRaftActorCommand::GetTopicMetadata { reply, .. } = cmd {
                let _ = reply.send(Some(topic_meta("leader")));
            }
        });
        let resp = ClientController::new(me, swim, raft, dp_stub())
            .dispatch(produce_req())
            .await;
        let ClientResponse::DataPlane(DataPlaneResponse::NotWriteLeader {
            leader_addr: Some(a),
        }) = resp
        else {
            panic!("expected NotWriteLeader, got {resp:?}");
        };
        assert_eq!(a, addr(8084));
    }

    /// This node is the segment's write leader → dispatch and ack as `Produced`.
    #[tokio::test]
    async fn produce_dispatches_when_segment_leader() {
        let me = node_id("self");
        let group = ShardGroup {
            id: ShardGroupId(1),
            members: vec![me.clone()],
        };
        let swim = swim_sender_with(move |cmd| {
            if let SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) = cmd {
                let _ = reply.send(Some(group.clone()));
            }
        });
        let raft = raft_sender_with(|cmd| {
            if let MultiRaftActorCommand::GetTopicMetadata { reply, .. } = cmd {
                let _ = reply.send(Some(topic_meta("self")));
            }
        });
        let resp = ClientController::new(me, swim, raft, dp_acking())
            .dispatch(produce_req())
            .await;
        let ClientResponse::DataPlane(DataPlaneResponse::Produced { entry_id }) = resp else {
            panic!("expected Produced, got {resp:?}");
        };
        assert_eq!(entry_id, 7);
    }

    /// A control-plane write on a non-member node returns the structural redirect
    /// (`TopicMetadataRedirect`), same as describe.
    #[tokio::test]
    async fn create_topic_redirects_when_not_member() {
        let owner = node_id("owner");
        let owner_addr = NodeAddress::test(addr(18005), addr(8085));
        let group = ShardGroup {
            id: ShardGroupId(9),
            members: vec![owner.clone()],
        };
        let swim = swim_sender_with(move |cmd| match cmd {
            SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) => {
                let _ = reply.send(Some(group.clone()));
            }
            SwimActorCommand::Query(QueryCommand::ResolveAddress { node_id: q, reply }) => {
                let _ = reply.send((q == owner).then_some(owner_addr));
            }
            _ => {}
        });
        let resp =
            ClientController::new(node_id("self"), swim, raft_sender_with(|_| {}), dp_stub())
                .dispatch(ClientRequest::ControlPlane(
                    ControlPlaneRequest::CreateTopic {
                        name: "t1".into(),
                        storage_policy: StoragePolicy {
                            retention_ms: Some(3_600_000),
                            replication_factor: 1,
                            partition_strategy: PartitionStrategy::AutoSplit,
                        },
                    },
                ))
                .await;
        let ClientResponse::ControlPlane(ControlPlaneResponse::TopicMetadataRedirect {
            owner: redirect_owner,
        }) = resp
        else {
            panic!("expected TopicMetadataRedirect, got {resp:?}");
        };
        assert_eq!(redirect_owner.node_id, "owner");
        assert_eq!(redirect_owner.client_addr, addr(8085));
    }

    #[tokio::test]
    async fn create_topic_ok() {
        let swim = swim_sender_with(|cmd| {
            if let SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) = cmd {
                let _ = reply.send(Some(test_shard_group()));
            }
        });
        let raft = raft_sender_with(|cmd| {
            if let MultiRaftActorCommand::ClientProposal { reply, .. } = cmd {
                let _ = reply.send(Ok(()));
            }
        });
        let resp = ClientController::new(node_id("node-1"), swim, raft, dp_stub())
            .dispatch(ClientRequest::ControlPlane(
                ControlPlaneRequest::CreateTopic {
                    name: "t1".into(),
                    storage_policy: crate::control_plane::metadata::strategy::StoragePolicy {
                        retention_ms: Some(3_600_000),
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
            if let SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) = cmd {
                let _ = reply.send(None);
            }
        });
        let resp = ClientController::new(
            node_id("self"),
            swim,
            raft_sender_with(|_| {}),
            dp_stub(),
        )
        .dispatch(ClientRequest::ControlPlane(
            ControlPlaneRequest::CreateTopic {
                name: "t1".into(),
                storage_policy: crate::control_plane::metadata::strategy::StoragePolicy {
                    retention_ms: Some(3_600_000),
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
            if let SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) = cmd {
                let _ = reply.send(Some(test_shard_group()));
            }
        });
        let raft = raft_sender_with(|cmd| {
            if let MultiRaftActorCommand::ClientProposal { reply, .. } = cmd {
                let _ = reply.send(Ok(()));
            }
        });
        let resp = ClientController::new(node_id("node-1"), swim, raft, dp_stub())
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
                let _ = reply.send(Box::new(["alpha".into(), "beta".into()]));
            }
        });
        let resp =
            ClientController::new(node_id("self"), swim_sender_with(|_| {}), raft, dp_stub())
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

    /// When the addressed node is NOT in the topic's owning shard group, the
    /// handler must return a redirect carrying the address of an owner — never
    /// proxy the request. See d4_consumer_range_tracking.md §Bootstrap.
    #[tokio::test]
    async fn describe_topic_redirects_when_not_owner() {
        let owner = node_id("owner");
        let owner_addr = NodeAddress::test(addr(18002), addr(8082));
        let group = ShardGroup {
            id: ShardGroupId(42),
            members: vec![owner.clone()],
        };
        let swim = {
            let group = group.clone();
            let owner = owner.clone();
            swim_sender_with(move |cmd| match cmd {
                SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) => {
                    let _ = reply.send(Some(group.clone()));
                }
                SwimActorCommand::Query(QueryCommand::ResolveAddress { node_id: q, reply }) => {
                    let _ = reply.send((q == owner).then_some(owner_addr));
                }
                _ => {}
            })
        };
        let resp =
            ClientController::new(node_id("self"), swim, raft_sender_with(|_| {}), dp_stub())
                .dispatch(ClientRequest::ControlPlane(
                    ControlPlaneRequest::DescribeTopic {
                        name: "elsewhere".into(),
                    },
                ))
                .await;
        let ClientResponse::ControlPlane(ControlPlaneResponse::TopicMetadataRedirect {
            owner: redirect_owner,
        }) = resp
        else {
            panic!("expected TopicMetadataRedirect, got {resp:?}");
        };
        assert_eq!(redirect_owner.node_id, "owner");
        assert_eq!(redirect_owner.client_addr, addr(8082));
    }

    /// When the addressed node IS in the owning shard group but the topic
    /// does not exist there, return TopicNotFound — only the owner can
    /// authoritatively report absence.
    #[tokio::test]
    async fn describe_topic_not_found_on_owner() {
        let me = node_id("self");
        let group = ShardGroup {
            id: ShardGroupId(7),
            members: vec![me.clone()],
        };
        let swim = swim_sender_with(move |cmd| {
            if let SwimActorCommand::Query(QueryCommand::ResolveShardGroup { reply, .. }) = cmd {
                let _ = reply.send(Some(group.clone()));
            }
        });
        let raft = raft_sender_with(|cmd| {
            if let MultiRaftActorCommand::GetTopicMetadata { reply, .. } = cmd {
                let _ = reply.send(None);
            }
        });
        let resp = ClientController::new(me, swim, raft, dp_stub())
            .dispatch(ClientRequest::ControlPlane(
                ControlPlaneRequest::DescribeTopic {
                    name: "missing".into(),
                },
            ))
            .await;
        assert!(
            matches!(
                resp,
                ClientResponse::ControlPlane(ControlPlaneResponse::TopicNotFound)
            ),
            "expected TopicNotFound, got {resp:?}"
        );
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
        let resp =
            ClientController::new(node_id("self"), swim, raft_sender_with(|_| {}), dp_stub())
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
        let resp =
            ClientController::new(node_id("self"), swim, raft_sender_with(|_| {}), dp_stub())
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
        assert_eq!(d.member_node_ids[0], "node-1");
        assert_eq!(d.member_node_ids.len(), 1);
    }

    #[tokio::test]
    async fn get_shard_info_absent() {
        let swim = swim_sender_with(|cmd| {
            if let SwimActorCommand::Query(QueryCommand::GetShardInfo { reply, .. }) = cmd {
                let _ = reply.send(None);
            }
        });
        let resp =
            ClientController::new(node_id("self"), swim, raft_sender_with(|_| {}), dp_stub())
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
        let resp =
            ClientController::new(node_id("self"), swim_sender_with(|_| {}), raft, dp_stub())
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
                let _ = reply.send(Box::new([MetadataTopicStats {
                    name: "t1".into(),
                    range_count: 2,
                    total_bytes: 1024,
                }]));
            }
        });
        let resp =
            ClientController::new(node_id("self"), swim_sender_with(|_| {}), raft, dp_stub())
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
