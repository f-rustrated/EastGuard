use tokio::sync::oneshot;

use crate::{
    client::{ClientError, RangeId, consumer::RangeCursor},
    connections::protocol::RangeTransition,
    impl_from_variant,
};

pub(crate) struct PauseRange {
    pub(crate) range_id: RangeId,
    pub(crate) reply: oneshot::Sender<Result<(), ClientError>>,
}

pub(crate) struct ResumeRange {
    pub(crate) range_id: RangeId,
    pub(crate) reply: oneshot::Sender<Result<(), ClientError>>,
}

pub(crate) struct SeekRange {
    pub(crate) range_id: RangeId,
    pub(crate) absolute_offset: u64,
    pub(crate) reply: oneshot::Sender<Result<(), ClientError>>,
}

pub(crate) struct RetryCommitAfterEpochRefresh {
    pub(crate) reply: oneshot::Sender<Result<(), ClientError>>,
}

pub(crate) enum TopicFetchManagerCommand {
    PauseRange(PauseRange),
    ResumeRange(ResumeRange),
    SeekRange(SeekRange),
    RetryCommitAfterEpochRefresh(RetryCommitAfterEpochRefresh),
}

impl_from_variant!(
    TopicFetchManagerCommand,
    PauseRange,
    ResumeRange,
    SeekRange,
    RetryCommitAfterEpochRefresh,
);

pub(crate) struct RangeDrained {
    pub cursor: RangeCursor,
    pub transition: RangeTransition,
}
