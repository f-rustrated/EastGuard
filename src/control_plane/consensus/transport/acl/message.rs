use tokio::sync::oneshot;

use crate::control_plane::{
    NodeAddressInfo, NodeId, consensus::raft::states::security::AclRecord,
    membership::ShardGroupId, metadata::AclResource,
};

pub(super) struct AclSnapshotFetchRequest {
    pub(super) fetch: AclSnapshotFetch,
    pub(super) reply: oneshot::Sender<Option<AclRecord>>,
}

#[derive(Clone, Debug)]
pub(super) struct AclSnapshotFetch {
    pub(super) node_id: NodeId,
    pub(super) key: AclSnapshotKey,
    pub(super) owner: NodeAddressInfo,
}
impl AclSnapshotFetch {
    pub(crate) fn new(
        node_id: NodeId,
        shard_group_id: ShardGroupId,
        resource: AclResource,
        owner: NodeAddressInfo,
    ) -> Self {
        AclSnapshotFetch {
            node_id,
            key: AclSnapshotKey {
                shard_group_id,
                resource,
            },
            owner,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct AclSnapshotKey {
    pub(super) shard_group_id: ShardGroupId,
    pub(super) resource: AclResource,
}

pub(super) struct AclSnapshotCompleted {
    pub(super) key: AclSnapshotKey,
    pub(super) snapshot: Option<AclRecord>,
}
