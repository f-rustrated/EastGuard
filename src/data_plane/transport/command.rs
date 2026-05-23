use crate::clusters::NodeId;
use crate::data_plane::messages::command::DataPlaneInterNodeCommand;

pub(crate) enum DataTransportCommand {
    Send {
        targets: Vec<NodeId>,
        message: DataPlaneInterNodeCommand,
    },
    #[allow(dead_code)]
    DisconnectPeer(NodeId),
}

impl DataTransportCommand {
    pub(crate) fn send(
        targets: Vec<NodeId>,
        message: impl Into<DataPlaneInterNodeCommand>,
    ) -> Self {
        Self::Send {
            targets,
            message: message.into(),
        }
    }
}
