use tokio::sync::oneshot;

use crate::control_plane::NodeAddressInfo;
use crate::control_plane::consensus::raft::states::security::AdmissionRecord;
use crate::control_plane::membership::ShardGroupId;
use crate::impl_from_variant;

pub(super) type AdmissionLookupResult = Result<Option<AdmissionRecord>, AdmissionLookupUnavailable>;
pub(super) type AdmissionLookupReply = oneshot::Sender<AdmissionLookupResult>;

pub(super) struct LookupAdmission {
    pub(super) fetch: AdmissionFetch,
    pub(super) reply: AdmissionLookupReply,
}

#[derive(Clone, Debug)]
pub(super) enum AdmissionFetch {
    Local(LocalAdmissionFetch),
    Remote(RemoteAdmissionFetch),
}

#[derive(Clone, Debug)]
pub(super) struct LocalAdmissionFetch(pub(super) AdmissionLookupKey);

#[derive(Clone, Debug)]
pub(super) struct RemoteAdmissionFetch {
    pub(super) key: AdmissionLookupKey,
    pub(super) owner: NodeAddressInfo,
}

impl_from_variant!(
    AdmissionFetch,
    Local(LocalAdmissionFetch),
    Remote(RemoteAdmissionFetch),
);

impl AdmissionFetch {
    pub(super) fn key(&self) -> &AdmissionLookupKey {
        match self {
            Self::Local(fetch) => &fetch.0,
            Self::Remote(fetch) => &fetch.key,
        }
    }
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
