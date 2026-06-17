pub(crate) mod command;
pub(crate) mod pending;
pub(crate) mod query;
pub(super) use command::*;

use command::DataPlaneCommand;
use query::DataPlaneQuery;

use crate::{
    data_plane::timer::DataPlaneTimeoutCallback, impl_from_variant, impl_from_variant_via,
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
    CheckpointComplete,
    DataPlaneTimeoutCallback,
    DataPlaneInterNodeCommand,
    CatchUpReadComplete
);
