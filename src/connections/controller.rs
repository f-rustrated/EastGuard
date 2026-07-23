use crate::connections::reader::ClientStreamReader;
use crate::connections::writer::ClientRawWriter;
use crate::connections::{protocol::*, run_client_writer};
use crate::control_plane::NodeAddressInfo;
use crate::control_plane::consensus::raft::errors::ProposalError;
use crate::control_plane::metadata::{
    OpenProducerSession, RangeMeta, SyncConsumerGroup, SyncConsumerGroupRequest, TopicState,
};
use crate::control_plane::{
    NodeId, SwimNodeState,
    consensus::actor::MutlRaftSender,
    membership::{
        ShardGroupId,
        actor::{ShardRouting, SwimSender},
    },
    metadata::{
        command::{CreateTopic, DeleteTopic, MetadataCommand},
        strategy::StoragePolicy,
    },
};
use crate::data_plane::ProduceError;
use crate::data_plane::SegmentKey;
use crate::data_plane::actor::DataPlaneSender;
use crate::data_plane::auxiliary_states::consumer_offsets::state::StaleEpoch;
use crate::data_plane::messages::command::{
    CommitConsumerOffset, ConsumerOffsetCommitAck, Produce, ProduceAck,
};
use crate::data_plane::messages::query::{
    DataPlaneQuery, Fetch, ListOffsets, ReadConsumerOffset, ReadConsumerOffsetResult,
};
use crate::net::TcpStream;
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
                    let writer_tx = writer_tx.clone();
                    let controller = self.clone();
                    // Spawn a task per request to process them concurrently. This prevents
                    // Head-of-Line (HOL) blocking on the multiplexed connection: slow requests
                    // (e.g. disk reads or consensus proposals) won't block fast hot-cache reads
                    // from other concurrent client threads. Strict partition-level ordering for
                    // sequential clients remains intact because they await responses sequentially.
                    tokio::spawn(async move {
                        let response = controller.dispatch(request).await;
                        if writer_tx.send((request_id, response)).await.is_err() {
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
        use ControlPlaneRequest::*;

        let res = match request {
            CreateTopic {
                name,
                storage_policy,
            } => self.handle_create_topic(name, storage_policy).await,
            DeleteTopic { name } => self.delete_topic(name).await,
            ListHostedTopics => self.list_hosted_topics().await,
            DescribeTopic { name } => self.describe_topic(name).await,
            SyncConsumerGroup(req) => self.sync_consumer_group(req).await,
            OpenProducerSession(req) => self.open_producer_session(req).await,
        };
        res.into()
    }

    async fn open_producer_session(
        &self,
        req: OpenProducerSessionRequest,
    ) -> Result<ClientSuccess, ServerError> {
        let command: OpenProducerSession = req.into_command();

        let group = match self.route(command.topic_name.as_bytes().to_vec()).await? {
            ShardRouting::Local(group) => group,
            ShardRouting::Redirect(member) => {
                return Err(self.control_plane_redirect(member));
            }
        };

        self.propose_topic_write(group.id, command.clone()).await?;

        let topic_meta = self
            .raft_sender
            .get_topic_metadata(command.topic_name)
            .await?;

        let session = topic_meta
            .producer_sessions
            .get(&command.producer_id)
            .copied()
            .ok_or_else(|| {
                ServerError::Internal("committed producer session is unavailable".into())
            })?;

        Ok(ClientSuccess::ProducerSessionOpened(
            ProducerSessionOpened {
                incarnation: session.incarnation,
                expires_at: session.expires_at,
            },
        ))
    }

    async fn sync_consumer_group(
        &self,
        req: SyncConsumerGroupRequest,
    ) -> Result<ClientSuccess, ServerError> {
        let group = match self.route(req.topic_name.as_bytes().to_vec()).await? {
            ShardRouting::Local(group) => group,
            ShardRouting::Redirect(member) => {
                return Err(self.control_plane_redirect(member));
            }
        };

        self.propose_topic_write(group.id, SyncConsumerGroup::new(req.clone()))
            .await?;

        if req.action == ConsumerGroupSyncAction::Leave {
            return Ok(ClientSuccess::ConsumerGroupLeft);
        }

        let Some(assignment) = self
            .raft_sender
            .get_consumer_group_assignment(req.topic_name, req.group_id, req.member_id)
            .await
        else {
            return Err(ServerError::Internal(
                "committed consumer group assignment is unavailable".into(),
            ));
        };
        Ok(ClientSuccess::ConsumerGroupAssignment(
            ConsumerGroupAssignmentResponse {
                generation: assignment.generation,
                ranges: assignment.ranges,
            },
        ))
    }

    /// If this node belongs to the topic's owning shard group, answers directly;
    /// otherwise it redirects to a member so the consumer retries against the right
    /// node — no server-side proxying.
    async fn describe_topic(&self, topic_name: String) -> Result<ClientSuccess, ServerError> {
        if let ShardRouting::Redirect(member) = self.route(topic_name.as_bytes().to_vec()).await? {
            return Err(self.control_plane_redirect(member));
        }

        let topic = self.raft_sender.get_topic_metadata(topic_name).await?;

        let addresses = self.swim_sender.list_all_node_addresses().await?;
        let detail = TopicDetail::from_meta(topic, &addresses);
        Ok(ClientSuccess::TopicDetail(detail))
    }

    async fn handle_create_topic(
        &self,
        name: String,
        storage_policy: StoragePolicy,
    ) -> Result<ClientSuccess, ServerError> {
        let group = match self.route(name.as_bytes().to_vec()).await? {
            ShardRouting::Local(group) => group,
            ShardRouting::Redirect(member) => {
                return Err(self.control_plane_redirect(member));
            }
        };

        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let cmd = CreateTopic {
            storage_policy,
            name,
            replica_set: group.replicas,
            created_at,
        };
        self.propose_topic_write(group.id, cmd).await?;
        Ok(ClientSuccess::TopicCreated)
    }

    async fn delete_topic(&self, topic_name: String) -> Result<ClientSuccess, ServerError> {
        let group = match self.route(topic_name.as_bytes().to_vec()).await? {
            ShardRouting::Local(group) => group,
            ShardRouting::Redirect(member) => {
                return Err(self.control_plane_redirect(member));
            }
        };

        let cmd = DeleteTopic { name: topic_name };
        self.propose_topic_write(group.id, cmd).await?;
        Ok(ClientSuccess::TopicDeleted)
    }

    /// Route a key relative to this node — the single SWIM call behind every
    /// membership-first handler.
    async fn route(&self, key: Vec<u8>) -> Result<ShardRouting, ServerError> {
        self.swim_sender
            .resolve_shard_routing(key, &self.node_id)
            .await
    }

    /// Structural redirect for a control-plane op that isn't local: to the member if
    /// one resolves, else a retriable error (no member's address known here yet).
    fn control_plane_redirect(&self, member: Option<NodeAddressInfo>) -> ServerError {
        match member {
            Some(node_addr) => ServerError::TopicMetadataRedirect { owner: node_addr },
            None => ServerError::Internal("no reachable member of the shard group".into()),
        }
    }

    /// Propose a control-plane write to the group's Raft leader.
    /// Returns `ServerError::NotRaftLeader` when the write must be retried at the leader.
    async fn propose_topic_write(
        &self,
        group_id: ShardGroupId,
        cmd: impl Into<MetadataCommand>,
    ) -> Result<(), ServerError> {
        let Err(err) = self
            .raft_sender
            .submit_metadata_command(group_id, cmd.into())
            .await
        else {
            return Ok(());
        };

        let leader = match err {
            ProposalError::NotLeader(leader) => leader,
            ProposalError::ShardNotFound | ProposalError::ShardGroupRemoved => None,
        };

        let leader_addr = self.resolve_id_to_addr(leader).await;
        Err(ServerError::NotRaftLeader { leader_addr })
    }

    async fn resolve_id_to_addr(&self, node_id: Option<NodeId>) -> Option<NodeAddressInfo> {
        let node_id = node_id?;
        self.swim_sender
            .resolve_address(node_id.clone())
            .await
            .ok()?
            .map(|addr| NodeAddressInfo { node_id, addr })
    }

    async fn list_hosted_topics(&self) -> Result<ClientSuccess, ServerError> {
        let topics = self
            .raft_sender
            .get_topics()
            .await
            .into_iter()
            .map(|name| TopicSummary {
                name,
                range_count: 0,
                state: TopicState::Active,
            })
            .collect();
        Ok(ClientSuccess::TopicList { topics })
    }

    async fn handle_data_plane(&self, request: ClientDataPlaneRequest) -> ClientResponse {
        use ClientDataPlaneRequest::*;

        let res: Result<ClientSuccess, ServerError> = match request {
            Produce(req) => self.produce_entry(req).await,
            Fetch(req) => self.fetch(req).await,
            FetchById(req) => self.fetch_by_id(req).await,
            ListOffsets(req) => self.list_offsets(req).await,
            CommitConsumerOffset(req) => self.handle_commit_consumer_offset(req).await,
            FetchConsumerOffset(req) => self.handle_fetch_consumer_offset(req).await,
        };
        res.into()
    }

    async fn handle_commit_consumer_offset(
        &self,
        req: CommitConsumerOffsetRequest,
    ) -> Result<ClientSuccess, ServerError> {
        let (tx, recv) = tokio::sync::oneshot::channel();
        self.data_plane_tx
            .send_async(CommitConsumerOffset {
                update: req.0,
                reply: tx,
            })
            .await?;

        let res = recv
            .await
            .map_err(|e| ServerError::Internal(e.to_string()))?;

        match res {
            ConsumerOffsetCommitAck::Committed => Ok(ClientSuccess::ConsumerOffsetCommitted),
            ConsumerOffsetCommitAck::NotWriteLeader(leader) => Err(ServerError::NotWriteLeader {
                leader_addr: self.resolve_id_to_addr(leader).await,
            }),
            ConsumerOffsetCommitAck::StaleEpoch(StaleEpoch(sealed_generation)) => {
                Err(ServerError::StaleConsumerGroupEpoch(sealed_generation))
            }
            ConsumerOffsetCommitAck::InternalError(error) => Err(ServerError::Internal(error)),
        }
    }

    async fn handle_fetch_consumer_offset(
        &self,
        req: FetchConsumerOffsetRequest,
    ) -> Result<ClientSuccess, ServerError> {
        let (reply, recv) = tokio::sync::oneshot::channel();
        self.data_plane_tx
            .send_async(ReadConsumerOffset {
                key: req.key,
                generation: req.generation,
                reply,
            })
            .await?;

        let res = recv
            .await
            .map_err(|e| ServerError::Internal(e.to_string()))?;

        match res {
            ReadConsumerOffsetResult::Offset(offset) => Ok(ClientSuccess::ConsumerOffset(offset)),
            ReadConsumerOffsetResult::GenerationMismatch(mismatch) => {
                Err(ServerError::ConsumerOffsetGenerationMismatch(mismatch))
            }
            ReadConsumerOffsetResult::NotLeader(leader) => Err(ServerError::NotWriteLeader {
                leader_addr: self.resolve_id_to_addr(leader).await,
            }),
        }
    }

    /// Routes a produce to the active segment's write leader (`replica_set[0]`),
    /// redirecting if this node isn't it. Membership-first (like `handle_describe_topic`)
    /// so we can tell "topic absent" from "not hosted here".
    async fn produce_entry(&self, req: ProduceRequest) -> Result<ClientSuccess, ServerError> {
        let received_at_ms = crate::now_ms();
        // Not local (ring unconverged or this node isn't a member) → retriable
        // redirect; the hint is best-effort, absent until SWIM converges.
        if let ShardRouting::Redirect(hint_node) =
            self.route(req.topic_name.as_bytes().to_vec()).await?
        {
            return Err(ServerError::ShardNotLocal { hint_node });
        }

        let topic = self.raft_sender.get_topic_metadata(req.topic_name).await?;

        let producer_identity = req
            .producer_identity
            .map(|id| topic.verify_producer_session(id, received_at_ms))
            .transpose()
            .map_err(ServerError::ProduceRejected)?;

        let range = topic.resolve_writable_range(req.range_id, &req.routing_key)?;
        let seg = range.active_write_segment()?;

        if !seg.is_shard_leader(&self.node_id) {
            let leader_addr = self
                .resolve_id_to_addr(seg.replica_set.first().cloned())
                .await;
            return Err(ServerError::NotWriteLeader { leader_addr });
        }

        let segment_key = SegmentKey::new(topic.id, range.range_id, seg.segment_id);
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

        let cmd = Produce {
            segment_key,
            data: req.data,
            record_count: req.record_count,
            received_at_ms,
            producer_identity,
            reply: reply_tx,
        };
        let _ = self.data_plane_tx.send_async(cmd).await;

        let ack = reply_rx
            .await
            .map_err(|_| ServerError::Internal("data plane dropped produce reply".into()))?;

        match ack {
            ProduceAck::Ok(entry_id) => Ok(ClientSuccess::Produced(entry_id)),
            // Metadata named us leader but the assignment hasn't applied yet — retriable.
            ProduceAck::Err(ProduceError::NotLeader | ProduceError::SegmentNotFound) => {
                Err(ServerError::NotWriteLeader { leader_addr: None })
            }
            ProduceAck::Err(error) => Err(ServerError::ProduceRejected(error)),
        }
    }

    /// Consume read path.
    /// 1. Resolves topic_name -> TopicMeta via the metadata RSM (one round trip),
    /// 2. validates the optional keyspace bound,
    /// 3. computes the range's current `RangeProgressSignal` from its lineage
    /// 4. dispatches a `DataPlaneQuery::Fetch` carrying everything the sync data-plane
    ///
    /// state machine needs to answer without any further I/O.
    async fn fetch(&self, req: FetchRequest) -> Result<ClientSuccess, ServerError> {
        let topic = self.raft_sender.get_topic_metadata(req.topic_name).await?;

        let range = topic.get_range(&req.range_id)?;

        if !keyspace_bound_matches_range(&req.keyspace_bound, range) {
            return Err(ServerError::KeyspaceBoundNarrowed);
        }
        let progress_signal = RangeProgressSignal::compute_progress_signal(range, &topic);

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let query = Fetch {
            topic_id: topic.id,
            range_id: req.range_id,
            entry_id: req.entry_id,
            max_bytes: req.max_bytes,
            progress_signal,
            reply: reply_tx,
        };

        self.data_plane_tx.send_async(query).await?;
        let fetched_records = reply_rx
            .await
            .map_err(|_| ServerError::Internal("data plane dropped reply".into()))??;

        Ok(EntryPayload::from_fetched_record(fetched_records))
    }

    /// Consume read path addressed by resolved `topic_id` (the client resolved the
    /// name via `DescribeTopic`). Serves straight from this node's local segment
    /// store, so a replica that holds the segment but isn't a metadata peer can
    /// serve it. No proxying: a miss returns `SegmentNotLocal` and the client
    /// retries another replica.
    async fn fetch_by_id(&self, req: FetchByIdRequest) -> Result<ClientSuccess, ServerError> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let query = Fetch {
            topic_id: req.topic_id,
            range_id: req.range_id,
            entry_id: req.entry_id,
            max_bytes: req.max_bytes,
            // The by-id consumer already has the range lineage from DescribeTopic,
            // so the response's progress signal isn't authoritative here.
            progress_signal: RangeProgressSignal::Active,
            reply: reply_tx,
        };
        self.data_plane_tx.send_async(query).await?;

        let fetched_records = reply_rx
            .await
            .map_err(|_| ServerError::Internal("data plane dropped reply".into()))??;
        Ok(EntryPayload::from_fetched_record(fetched_records))
    }

    /// Offset-bounds query — used by consumers to bound their read window
    /// for the range's currently-active segment on this node.
    async fn list_offsets(&self, req: RangeOffsetRequest) -> Result<ClientSuccess, ServerError> {
        let topic = self.raft_sender.get_topic_metadata(req.topic_name).await?;

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

        let query: DataPlaneQuery = ListOffsets {
            topic_id: topic.id,
            range_id: req.range_id,
            reply: reply_tx,
        }
        .into();

        self.data_plane_tx.send_async(query).await?;

        let range_offset = reply_rx
            .await
            .map_err(|_| ServerError::Internal("data plane dropped reply".into()))??;

        Ok(ClientSuccess::RangeOffset(range_offset))
    }

    async fn handle_admin(&self, request: AdminRequest) -> ClientResponse {
        use AdminRequest::*;

        let res = match request {
            DescribeCluster => self.describe_cluster().await,
            ListHostedTopicsWithStats => self.list_hosted_topics_with_stats().await,

            GetShardInfo { key } => self.get_shard_info(key).await,
            GetShardLeader { shard_group_id } => self.handle_get_shard_leader(shard_group_id).await,
        };
        res.into()
    }

    async fn describe_cluster(&self) -> Result<ClientSuccess, ServerError> {
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
        Ok(ClientSuccess::ClusterInfo { nodes })
    }

    async fn list_hosted_topics_with_stats(&self) -> Result<ClientSuccess, ServerError> {
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
        Ok(ClientSuccess::TopicStats { topics })
    }

    async fn get_shard_info(&self, key: Vec<u8>) -> Result<ClientSuccess, ServerError> {
        let detail = self
            .swim_sender
            .get_shard_info(key)
            .await?
            .map(|(group, leader)| ShardDetail {
                shard_group_id: group.id,
                leader_node_id: leader.as_ref().map(|e| e.leader.node_id.to_string()),
                leader_addr: leader.map(|e| e.leader.client_addr()),
                member_node_ids: group.replicas.iter().map(|n| n.to_string()).collect(),
            });
        Ok(ClientSuccess::ShardInfo { detail })
    }

    async fn handle_get_shard_leader(
        &self,
        shard_group_id: ShardGroupId,
    ) -> Result<ClientSuccess, ServerError> {
        let leader = self
            .raft_sender
            .get_leader(shard_group_id)
            .await
            .map(|n| n.to_string());
        Ok(ClientSuccess::ShardLeader { leader })
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
    use super::*;
    use crate::connections::protocol::{
        AdminRequest, ClientDataPlaneRequest, ClientRequest, ClientResponse, ClientSuccess,
        ControlPlaneRequest, NodeState, ProduceRequest, ServerError,
    };
    use crate::control_plane::consensus::actor::MultiRaftActor;
    use crate::control_plane::consensus::messages::MultiRaftActorCommand;
    use crate::control_plane::membership::actor::SwimActor;
    use crate::control_plane::membership::{
        QueryCommand, ShardGroup, ShardGroupId, ShardLeaderEntry, SwimActorCommand,
    };
    use crate::control_plane::metadata::TopicStats as MetadataTopicStats;
    use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
    use crate::control_plane::metadata::{RangeId, TopicId, TopicMeta};
    use crate::control_plane::{NodeAddress, NodeId, Replicas, SwimNode, SwimNodeState};
    use crate::data_plane::actor::DataPlaneSender;
    use crate::data_plane::messages::DataPlaneMessage;
    use crate::data_plane::messages::command::{DataPlaneCommand, ProduceAck};
    use std::net::SocketAddr;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn node_id(s: &str) -> NodeId {
        NodeId::new(s)
    }

    fn test_shard_group() -> ShardGroup {
        ShardGroup {
            id: ShardGroupId(42),
            replicas: Replicas::new(vec![node_id("node-1")]),
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
                    let _ = p.reply.send(ProduceAck::Ok(7.into()));
                }
            }
        });
        DataPlaneSender(tx)
    }

    fn produce_req() -> ClientRequest {
        ClientRequest::DataPlane(ClientDataPlaneRequest::Produce(ProduceRequest {
            topic_name: "t1".into(),
            range_id: RangeId(0),
            routing_key: b"k".to_vec(),
            data: b"hello".to_vec().into(),
            record_count: 1,
            producer_identity: None,
        }))
    }

    /// A single full-keyspace topic whose active segment's write leader is `leader`.
    fn topic_meta(leader: &str) -> TopicMeta {
        TopicMeta::new(
            "t1".into(),
            TopicId(1),
            Replicas::new(vec![node_id(leader)]),
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
                ClientResponse::Err(ServerError::ShardNotLocal { hint_node: None })
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
            replicas: Replicas::new(vec![owner.clone()]),
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
        let ClientResponse::Err(ServerError::ShardNotLocal { hint_node }) = resp else {
            panic!("expected ShardNotLocal, got {resp:?}");
        };
        assert_eq!(hint_node.unwrap().client_addr(), addr(8083));
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
            replicas: Replicas::new(vec![me.clone(), leader.clone()]),
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
        let ClientResponse::Err(ServerError::NotWriteLeader {
            leader_addr: Some(a),
        }) = resp
        else {
            panic!("expected NotWriteLeader, got {resp:?}");
        };
        assert_eq!(a.client_addr(), addr(8084));
    }

    /// This node is the segment's write leader → dispatch and ack as `Produced`.
    #[tokio::test]
    async fn produce_dispatches_when_segment_leader() {
        let me = node_id("self");
        let group = ShardGroup {
            id: ShardGroupId(1),
            replicas: Replicas::new(vec![me.clone()]),
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
        let ClientResponse::Ok(ClientSuccess::Produced(entry_id)) = resp else {
            panic!("expected Produced, got {resp:?}");
        };
        assert_eq!(entry_id, 7.into());
    }

    /// A control-plane write on a non-member node returns the structural redirect
    /// (`TopicMetadataRedirect`), same as describe.
    #[tokio::test]
    async fn create_topic_redirects_when_not_member() {
        let owner = node_id("owner");
        let owner_addr = NodeAddress::test(addr(18005), addr(8085));
        let group = ShardGroup {
            id: ShardGroupId(9),
            replicas: Replicas::new(vec![owner.clone()]),
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
        let ClientResponse::Err(ServerError::TopicMetadataRedirect {
            owner: redirect_owner,
        }) = resp
        else {
            panic!("expected TopicMetadataRedirect, got {resp:?}");
        };
        assert_eq!(&*redirect_owner.node_id, "owner");
        assert_eq!(redirect_owner.addr.client_addr, addr(8085));
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
                    storage_policy: StoragePolicy {
                        retention_ms: Some(3_600_000),
                        replication_factor: 1,
                        partition_strategy: PartitionStrategy::AutoSplit,
                    },
                },
            ))
            .await;
        assert!(
            matches!(resp, ClientResponse::Ok(ClientSuccess::TopicCreated)),
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
        assert!(
            matches!(resp, ClientResponse::Err(ServerError::Internal(_))),
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
            matches!(resp, ClientResponse::Ok(ClientSuccess::TopicDeleted)),
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
        let ClientResponse::Ok(ClientSuccess::TopicList { topics }) = resp else {
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
            replicas: Replicas::new(vec![owner.clone()]),
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
        let ClientResponse::Err(ServerError::TopicMetadataRedirect {
            owner: redirect_owner,
        }) = resp
        else {
            panic!("expected TopicMetadataRedirect, got {resp:?}");
        };
        assert_eq!(&*redirect_owner.node_id, "owner");
        assert_eq!(redirect_owner.addr.client_addr, addr(8082));
    }

    /// When the addressed node IS in the owning shard group but the topic
    /// does not exist there, return TopicNotFound — only the owner can
    /// authoritatively report absence.
    #[tokio::test]
    async fn describe_topic_not_found_on_owner() {
        let me = node_id("self");
        let group = ShardGroup {
            id: ShardGroupId(7),
            replicas: Replicas::new(vec![me.clone()]),
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
            matches!(resp, ClientResponse::Err(ServerError::TopicNotFound)),
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
        let ClientResponse::Ok(ClientSuccess::ClusterInfo { nodes: info }) = resp else {
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
            leader: NodeAddressInfo::new(node_id("n1"), node_addr),
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
        let ClientResponse::Ok(ClientSuccess::ShardInfo { detail: Some(d) }) = resp else {
            panic!("expected ShardInfo with detail, got {resp:?}");
        };
        assert_eq!(*d.shard_group_id, 42);
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
                ClientResponse::Ok(ClientSuccess::ShardInfo { detail: None })
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
                    shard_group_id: ShardGroupId(42),
                }))
                .await;
        let ClientResponse::Ok(ClientSuccess::ShardLeader { leader }) = resp else {
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
        let ClientResponse::Ok(ClientSuccess::TopicStats { topics }) = resp else {
            panic!("expected TopicStats, got {resp:?}");
        };
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "t1");
        assert_eq!(topics[0].range_count, 2);
        assert_eq!(topics[0].total_bytes, 1024);
    }
}
