use tokio::sync::oneshot;

use crate::{
    data_plane::{checkpoint::CheckpointJob, messages::ProduceAck, timer::DataPlaneTimer},
    schedulers::ticker_message::TimerCommand,
};

pub(crate) enum DataPlaneEvent {
    SubmitCheckpoint(CheckpointJob),
    ProducePending(oneshot::Sender<ProduceAck>),
    WalBatchComplete { lsn: u64 },
    WalBatchFailed(String),
    Timer(TimerCommand<DataPlaneTimer>),
}
