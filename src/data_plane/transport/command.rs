use crate::clusters::NodeId;
use crate::clusters::swims::ShardGroupId;
use crate::data_plane::messages::command::DataPlaneInterNodeCommand;

pub(crate) struct DataTransportSendToTargets {
    pub targets: Vec<NodeId>,
    pub message: DataPlaneInterNodeCommand,
}

pub(crate) struct DataTransportSendToCoordinator {
    pub shard_group_id: ShardGroupId,
    pub message: DataPlaneInterNodeCommand,
}

pub(crate) enum DataTransportCommand {
    SendToTargets(DataTransportSendToTargets),
    SendToCoordinator(DataTransportSendToCoordinator),
    #[allow(dead_code)]
    DisconnectPeer(NodeId),
}

impl DataTransportCommand {
    pub(crate) fn send_to_targets(
        targets: Vec<NodeId>,
        message: impl Into<DataPlaneInterNodeCommand>,
    ) -> Self {
        Self::SendToTargets(DataTransportSendToTargets {
            targets,
            message: message.into(),
        })
    }
}
