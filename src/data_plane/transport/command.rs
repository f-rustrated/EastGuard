use crate::control_plane::NodeId;
use crate::control_plane::membership::ShardGroupId;
use crate::data_plane::messages::command::DataPlaneInterNodeCommand;
use crate::impl_from_variant;

#[derive(Debug)]
pub(crate) struct DataTransportSendToTargets {
    pub targets: Box<[NodeId]>,
    pub message: DataPlaneInterNodeCommand,
}

#[derive(Debug)]
pub(crate) struct DataTransportSendToCoordinator {
    pub shard_group_id: ShardGroupId,
    pub message: DataPlaneInterNodeCommand,
}

#[derive(Debug)]
pub(crate) enum DataTransportCommand {
    SendToTargets(DataTransportSendToTargets),
    SendToCoordinator(DataTransportSendToCoordinator),
    #[allow(dead_code)]
    DisconnectPeer(NodeId),
}

impl_from_variant!(
    DataTransportCommand,
    SendToTargets(DataTransportSendToTargets),
    SendToCoordinator(DataTransportSendToCoordinator),
    DisconnectPeer(NodeId)
);

impl DataTransportCommand {
    pub(crate) fn send_to_targets(
        targets: Vec<NodeId>,
        message: impl Into<DataPlaneInterNodeCommand>,
    ) -> Self {
        Self::SendToTargets(DataTransportSendToTargets {
            targets: targets.into_boxed_slice(),
            message: message.into(),
        })
    }
}
