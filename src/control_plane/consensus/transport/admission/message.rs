use tokio::sync::oneshot;

use crate::control_plane::NodeAddressInfo;
use crate::control_plane::consensus::raft::states::security::AdmissionRecord;
use crate::control_plane::membership::ShardGroupId;

pub(super) type AdmissionLookupResult = Result<Option<AdmissionRecord>, AdmissionLookupUnavailable>;
pub(super) type AdmissionLookupReply = oneshot::Sender<AdmissionLookupResult>;

pub(super) struct AdmissionQuery {
    pub(super) fetch: AdmissionTarget,
    pub(super) reply: AdmissionLookupReply,
}

#[derive(Clone, Debug)]
pub(super) struct AdmissionTarget {
    pub(super) key: AdmissionLookupKey,
    pub(super) remote_owner: Option<NodeAddressInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct AdmissionLookupKey {
    pub(super) shard_group_id: ShardGroupId,
    pub(super) node_certificate_principal: Box<str>,
}

pub(super) struct AdmissionLookupCompleted {
    pub(super) key: AdmissionLookupKey,
    pub(super) result: Result<Option<AdmissionRecord>, AdmissionLookupUnavailable>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("admission lookup unavailable")]
pub(crate) struct AdmissionLookupUnavailable;
