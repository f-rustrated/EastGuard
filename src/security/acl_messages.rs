use std::time::Duration;

use tokio::sync::oneshot;

use crate::connections::protocol::ServerError;
use crate::control_plane::consensus::raft::states::security::AclRecord;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::AclResource;
use crate::impl_from_variant;
use crate::security::CertificatePrincipal;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct AuthorizationRequestId(pub(super) u64);

pub(super) enum AclCommand {
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

impl_from_variant!(AclCommand, Authorize, ReadLocalAcl,);

pub(super) enum AclEvent {
    AclFetchRequested(AclRecordKey),
    AuthorizationResolved(AuthorizationResolved),
}

impl_from_variant!(
    AclEvent,
    AclFetchRequested(AclRecordKey),
    AuthorizationResolved,
);

#[derive(Debug)]
pub(super) struct AuthorizationResolved {
    pub(super) request_id: AuthorizationRequestId,
    pub(super) authorized: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct AclRecordKey {
    pub(super) shard_group_id: ShardGroupId,
    pub(super) resource: AclResource,
}

#[derive(Debug)]
pub(super) struct AclFetchCompleted {
    pub(super) key: AclRecordKey,
    pub(super) result: anyhow::Result<AclRecord>,
    pub(super) requested_at: Duration,
}
