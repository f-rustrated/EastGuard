use crate::control_plane::NodeId;
use crate::control_plane::membership::ShardGroupId;
use crate::data_plane::messages::command::DataPlanePeerMessage;
use crate::impl_from_variant;

#[derive(Debug)]
pub(crate) struct DataTransportSendToTargets {
    pub targets: Box<[NodeId]>,
    pub message: DataPlanePeerMessage,
}

#[derive(Debug)]
pub(crate) struct DataTransportSendToCoordinator {
    pub shard_group_id: ShardGroupId,
    pub message: DataPlanePeerMessage,
}

#[derive(Debug)]
pub(crate) enum DataTransportCommand {
    SendToTargets(DataTransportSendToTargets),
    SendToCoordinator(DataTransportSendToCoordinator),
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
        targets: impl Into<Box<[NodeId]>>,
        message: impl Into<DataPlanePeerMessage>,
    ) -> Self {
        Self::SendToTargets(DataTransportSendToTargets {
            targets: targets.into(),
            message: message.into(),
        })
    }
}
