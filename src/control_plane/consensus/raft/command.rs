use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::NodeId;
use crate::control_plane::metadata::ReassignSegment;
use crate::control_plane::metadata::command::{
    CreateTopic, DeleteSegments, DeleteTopic, MergeRange, MetadataCommand, RollSegment, SplitRange,
    SyncConsumerGroup,
};
use crate::{impl_from_variant, impl_from_variant_via};

/// Outer Raft log payload. The peer set is part of the replicated state machine,
/// mutated only when `AddPeer` / `RemovePeer` entries apply — never by direct mutation
/// from gossip events. See `docs/metadata-management/d4_membership_and_shard_reconciliation.md`
/// for the rationale.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum RaftCommand {
    Noop,
    Metadata(MetadataCommand),
    AddPeer(NodeId),
    RemovePeer(NodeId),
}

impl_from_variant!(RaftCommand, Metadata(MetadataCommand));
impl_from_variant_via!(
    RaftCommand,
    MetadataCommand,
    CreateTopic,
    RollSegment,
    SplitRange,
    MergeRange,
    DeleteTopic,
    ReassignSegment,
    DeleteSegments,
    SyncConsumerGroup
);
