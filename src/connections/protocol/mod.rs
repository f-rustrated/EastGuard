//! Wire protocol for client ↔ server traffic.
//!
//! Split into three sub-protocols by audience and routing model:
//!
//! - [`control_plane`] — topic lifecycle and metadata lookups (create / delete /
//!   list / describe). The server resolves the right destination internally;
//!   on the describe path, a non-owner returns a redirect.
//! - [`data_plane`] — produce / fetch / list-offsets. The client routes
//!   directly to the right data node using its local routing cache; on stale
//!   targeting the server returns a redirect error and the client retries.
//! - [`admin`] — operator / debug / integration-test affordances (describe
//!   cluster, force split, shard lookup).
//!
//! All three submodules export their types under `crate::connections::protocol`
//! via glob re-export — call sites import flat names without caring which
//! sub-protocol a type belongs to.

#![allow(dead_code)]

mod admin;
mod control_plane;
mod data_plane;
mod error;

pub use admin::*;
pub use control_plane::*;
pub use data_plane::*;
pub use error::*;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::{
    control_plane::metadata::{EntryId, UpdateConsumerGroupMemberRequest},
    data_plane::{
        auxiliary_states::consumer_offsets::state::ConsumerOffsetPosition,
        messages::query::RangeOffsets,
    },
    impl_from_variant, impl_from_variant_via,
};

// ── Top-level dispatch ─────────────────────────────────────────────────────

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum ClientRequest {
    ControlPlane(ControlPlaneRequest),
    DataPlane(ClientDataPlaneRequest),
    Admin(AdminRequest),
}

impl ClientRequest {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::ControlPlane(request) => match request {
                ControlPlaneRequest::CreateTopic { .. } => "CreateTopic",
                ControlPlaneRequest::DeleteTopic { .. } => "DeleteTopic",
                ControlPlaneRequest::ListHostedTopics => "ListHostedTopics",
                ControlPlaneRequest::DescribeTopic { .. } => "DescribeTopic",
                ControlPlaneRequest::SyncConsumerGroup(_) => "SyncConsumerGroup",
                ControlPlaneRequest::OpenProducerSession(_) => "OpenProducerSession",
            },
            Self::DataPlane(request) => match request {
                ClientDataPlaneRequest::Produce(_) => "Produce",
                ClientDataPlaneRequest::Fetch(_) => "Fetch",
                ClientDataPlaneRequest::FetchById(_) => "FetchById",
                ClientDataPlaneRequest::ListOffsets(_) => "ListOffsets",
                ClientDataPlaneRequest::CommitConsumerOffset(_) => "CommitConsumerOffset",
                ClientDataPlaneRequest::FetchConsumerOffset(_) => "FetchConsumerOffset",
            },
            Self::Admin(request) => match request {
                AdminRequest::DescribeCluster => "DescribeCluster",
                AdminRequest::ListHostedTopicsWithStats => "ListHostedTopicsWithStats",
                AdminRequest::GetShardInfo { .. } => "GetShardInfo",
                AdminRequest::GetShardLeader { .. } => "GetShardLeader",
            },
        }
    }
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub enum ClientResponse {
    Ok(ClientSuccess),
    Err(ServerError),
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum ClientSuccess {
    // Control Plane
    TopicCreated,
    TopicDeleted,
    TopicList {
        topics: Box<[TopicSummary]>,
    },
    TopicDetail(TopicDetail),
    ConsumerGroupAssignment(ConsumerGroupAssignmentResponse),
    ConsumerGroupLeft,
    ProducerSessionOpened(ProducerSessionOpened),

    // Data Plane
    Produced(EntryId),
    Fetched {
        entries: Box<[EntryPayload]>,
        next_entry_id: EntryId,
        progress_signal: RangeProgressSignal,
    },
    RangeOffset(RangeOffsets),
    ConsumerOffsetCommitted,
    ConsumerOffset(Option<ConsumerOffsetPosition>),

    // Admin
    ClusterInfo {
        nodes: Box<[NodeInfo]>,
    },
    TopicStats {
        topics: Box<[TopicStats]>,
    },
    ShardInfo {
        detail: Option<ShardDetail>,
    },
    ShardLeader {
        leader: Option<String>,
    },
}

impl From<Result<ClientSuccess, ServerError>> for ClientResponse {
    fn from(res: Result<ClientSuccess, ServerError>) -> Self {
        match res {
            Ok(ok) => ClientResponse::Ok(ok),
            Err(err) => ClientResponse::Err(err),
        }
    }
}

impl From<ClientSuccess> for ClientResponse {
    fn from(ok: ClientSuccess) -> Self {
        ClientResponse::Ok(ok)
    }
}

impl_from_variant!(
    ClientRequest,
    ControlPlane(ControlPlaneRequest),
    DataPlane(ClientDataPlaneRequest),
    Admin(AdminRequest),
);

impl_from_variant_via!(
    ClientRequest,
    ClientDataPlaneRequest,
    ProduceRequest,
    FetchRequest,
    FetchByIdRequest,
    RangeOffsetRequest,
    CommitConsumerOffsetRequest,
    FetchConsumerOffsetRequest
);

impl_from_variant_via!(
    ClientRequest,
    ControlPlaneRequest,
    UpdateConsumerGroupMemberRequest
);
