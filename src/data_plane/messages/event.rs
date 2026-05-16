use tokio::sync::oneshot;

use crate::{
    data_plane::{checkpoint::CheckpointJob, timer::DataPlaneTimer},
    schedulers::ticker_message::TimerCommand,
};

use super::command::*;
pub(crate) enum DataPlaneEvent {
    SubmitCheckpoint(CheckpointJob),
    ProduceAck {
        reply: oneshot::Sender<ProduceResult>,
        result: ProduceResult,
    },
    Timer(TimerCommand<DataPlaneTimer>),
}
