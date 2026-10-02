use crate::control_plane::NodeId;
use crate::control_plane::metadata::error::MetadataError;

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProposalError {
    #[error("Non-leader cannot make proposal: {0:?}")]
    NotLeader(Option<NodeId>),
    #[error("Shard not found for proposal")]
    ShardNotFound,
    #[error("Shard group removed")]
    ShardGroupRemoved,
    #[error("Proposal exceeds the Raft transport entry size limit")]
    EntryTooLarge,
    #[error("Metadata command rejected: {0}")]
    ApplyRejected(MetadataError),
}

#[derive(Debug)]
pub(crate) enum EvictionError {
    NotLeader,
    GroupNotFound,
    LeaderNotInRing,
    WaitingForStability,
    NoStalePeers,
    ConfigChangeInFlight,
    /// A ring member is dead or not yet a voter — the post-removal quorum
    /// cannot be certified. Distinct from `FollowersLagging` (member present
    /// but behind) and from `ConfigChangeInFlight` (log-ordering gate).
    RingMemberNotReady,
    FollowersLagging,
    ProposalRejected, // Only allocate a string if it actually errors
}
