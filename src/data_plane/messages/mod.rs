pub(crate) mod command;
pub(crate) mod pending;
pub(crate) mod query;
pub(super) use command::*;

use command::DataPlaneCommand;
use query::DataPlaneQuery;

use crate::{
    data_plane::{
        messages::query::{Fetch, ListOffsets, ReadConsumerOffset},
        timer::DataPlaneTimeoutCallback,
    },
    impl_from_variant, impl_from_variant_via,
};

pub enum DataPlaneMessage {
    Command(DataPlaneCommand),
    Query(DataPlaneQuery),
}

impl_from_variant!(
    DataPlaneMessage,
    Command(DataPlaneCommand),
    Query(DataPlaneQuery)
);

impl_from_variant_via!(
    DataPlaneMessage,
    DataPlaneCommand,
    Produce,
    SegmentCheckpointComplete,
    DataPlaneTimeoutCallback,
    DataPlanePeerMessage,
    CatchUpReadComplete,
    CommitConsumerOffset
);

impl_from_variant_via!(
    DataPlaneMessage,
    DataPlaneQuery,
    Fetch,
    ListOffsets,
    ReadConsumerOffset
);
