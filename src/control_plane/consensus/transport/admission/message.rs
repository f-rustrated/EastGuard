use tokio::sync::oneshot;

use crate::control_plane::NodeAddressInfo;
use crate::control_plane::consensus::raft::states::security::AdmissionRecord;

use super::super::protocol::AdmissionRecordKey;

pub(super) type AdmissionLookupResult = Result<Option<AdmissionRecord>, AdmissionLookupUnavailable>;
pub(super) type AdmissionLookupReply = oneshot::Sender<AdmissionLookupResult>;

pub(super) struct AdmissionQuery {
    pub(super) fetch: AdmissionTarget,
    pub(super) reply: AdmissionLookupReply,
}

#[derive(Clone, Debug)]
pub(super) struct AdmissionTarget {
    pub(super) key: AdmissionRecordKey,
    pub(super) remote_owner: Option<NodeAddressInfo>,
}

pub(super) struct AdmissionLookupCompleted {
    pub(super) key: AdmissionRecordKey,
    pub(super) result: Result<Option<AdmissionRecord>, AdmissionLookupUnavailable>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("admission lookup unavailable")]
pub(crate) struct AdmissionLookupUnavailable;
