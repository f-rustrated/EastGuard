use std::time::Duration;

use tokio::sync::oneshot;

use crate::connections::protocol::ServerError;
use crate::control_plane::Replicas;
use crate::control_plane::consensus::raft::states::security::AclRecord;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::AclResource;
use crate::impl_from_variant;
use crate::security::CertificatePrincipal;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct SecurityRequestId(pub(super) u64);

pub(super) enum SecurityCommand {
    Authorize(Authorize),
    ReadLocalAcl(ReadLocalAcl),
}

pub(super) struct Authorize {
    pub(super) principal: CertificatePrincipal,
    pub(super) resource: AclResource,
    pub(super) reply: oneshot::Sender<Result<(), ServerError>>,
}

pub(super) struct ReadLocalAcl {
    pub(super) resource: AclResource,
    pub(super) reply: oneshot::Sender<Result<AclRecord, ServerError>>,
}

impl_from_variant!(SecurityCommand, Authorize, ReadLocalAcl,);

pub(super) enum SecurityEvent {
    AclFetchRequested(AclFetch),
    AuthorizationResolved(AuthorizationResolved),
}

impl_from_variant!(
    SecurityEvent,
    AclFetchRequested(AclFetch),
    AuthorizationResolved,
);

#[derive(Debug)]
pub(super) struct AuthorizationResolved {
    pub(super) request_id: SecurityRequestId,
    pub(super) authorized: bool,
}

#[derive(Clone, Debug)]
pub(super) struct AclFetch {
    pub(super) key: AclRecordKey,
    pub(super) candidates: Replicas,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct AclRecordKey {
    pub(super) shard_group_id: ShardGroupId,
    pub(super) resource: AclResource,
}

#[derive(Debug)]
pub(super) struct AclFetchCompleted {
    pub(super) key: AclRecordKey,
    pub(super) result: Result<AclRecord, AclUnavailable>,
    pub(super) requested_at: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("ACL record unavailable")]
pub(super) struct AclUnavailable;
