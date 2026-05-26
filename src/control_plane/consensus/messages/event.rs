use crate::control_plane::NodeId;
use crate::control_plane::metadata::event::ApplyResult;
use crate::control_plane::consensus::raft::log::LogEntry;
use crate::control_plane::consensus::messages::rpc::OutboundRaftPacket;
use crate::control_plane::consensus::messages::timer::RaftTimer;
use crate::control_plane::membership::ShardGroupId;
use crate::data_plane::SegmentKey;
use crate::impl_from_variant;
use crate::schedulers::ticker_message::TimerCommand;

#[derive(Debug, Clone)]
pub struct LeaderChange {
    pub shard_group_id: ShardGroupId,
    pub leader_node_id: NodeId,
    pub term: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct SealContext {
    pub requester: NodeId,
    pub old_segment_key: SegmentKey,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct MetadataApplied {
    pub shard_group_id: ShardGroupId,
    pub result: ApplyResult,
    pub log_index: u64,
    pub seal_context: Option<SealContext>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum LogMutation {
    Append(LogEntry),
    TruncateFrom(u64),
    // Raft state that MUST survive crashes.
    HardState {
        term: u64,
        voted_for: Option<NodeId>,
    },
}

#[derive(Debug)]
pub enum RaftEvent {
    OutboundRaftPacket(OutboundRaftPacket),
    Timer(TimerCommand<RaftTimer>),
    LeaderChange(LeaderChange),
    DisconnectPeer(NodeId),
    #[allow(dead_code)]
    MetadataApplied(MetadataApplied),
}

impl_from_variant!(RaftEvent, LeaderChange, OutboundRaftPacket, MetadataApplied);
