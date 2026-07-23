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
    control_plane::metadata::{EntryId, SyncConsumerGroupRequest},
    data_plane::{
        auxiliary_states::consumer_offsets::state::ConsumerOffsetPosition,
        messages::query::{FetchResult, ListOffsetsResult},
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
    Produced {
        entry_id: EntryId,
    },
    Fetched {
        entries: Box<[EntryPayload]>,
        next_entry_id: EntryId,
        progress_signal: RangeProgressSignal,
    },
    RangeOffset {
        start_entry_id: EntryId,
        next_entry_id: EntryId,
    },
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

impl ClientSuccess {
    // TODO refactor - remove ListOffsetsResult and return Result<RangeOffset, ServerError>
    pub(crate) fn from_list_offset_result(value: ListOffsetsResult) -> Result<Self, ServerError> {
        match value {
            ListOffsetsResult::RangeOffsets {
                start_entry_id,
                next_entry_id,
            } => Ok(ClientSuccess::RangeOffset {
                start_entry_id,
                next_entry_id,
            }),
            ListOffsetsResult::SegmentNotLocal => Err(ServerError::SegmentNotLocal),
        }
    }

    // TODO refactor - remove FetchResult and return Result<Fetched, ServerError>
    pub(crate) fn from_fetch_result(result: FetchResult) -> Result<Self, ServerError> {
        match result {
            FetchResult::Records {
                entries,
                next_entry_id,
                progress_signal,
            } => {
                let wire_entries = entries
                    .into_iter()
                    .map(|cached| EntryPayload {
                        entry_id: cached.entry_id,
                        record_count: cached.record_count,
                        data: cached.data.to_vec(),
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice();

                Ok(ClientSuccess::Fetched {
                    entries: wire_entries,
                    next_entry_id,
                    progress_signal,
                })
            }
            FetchResult::SegmentNotLocal => Err(ServerError::SegmentNotLocal),
            FetchResult::EntryIdOutOfRange => Err(ServerError::EntryIdOutOfRange),
            FetchResult::InternalError(s) => Err(ServerError::Internal(s)),
        }
    }
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

impl_from_variant_via!(ClientRequest, ControlPlaneRequest, SyncConsumerGroupRequest);
