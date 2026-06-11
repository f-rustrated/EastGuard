use bincode::{Decode, Encode};

use crate::control_plane::NodeId;

#[derive(Debug, PartialEq, Eq, Decode, Encode, thiserror::Error)]
pub enum ProposalError {
    #[error("Non-leader cannot make proposal: {0:?}")]
    NotLeader(Option<NodeId>),
    #[error("Shard not found for proposal")]
    ShardNotFound,
    #[error("Shard group removed")]
    ShardGroupRemoved,
}

#[derive(Debug)]
pub(crate) enum EvictionError {
    NotLeader,
    GroupNotFound,
    LeaderNotInRing,
    WaitingForStability,
    NoStalePeers,
    ConfigChangeInFlight,
    FollowersLagging,
    ProposalRejected, // Only allocate a string if it actually errors
}
