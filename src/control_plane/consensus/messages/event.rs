use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::rpc::OutboundRaftPacket;
use crate::control_plane::consensus::messages::timer::RaftTimer;
use crate::control_plane::consensus::multi_raft::SealContext;
use crate::control_plane::consensus::raft::log::LogEntry;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::event::ApplyResult;
use crate::data_plane::transport::command::DataTransportCommand;
use crate::impl_from_variant;
use crate::schedulers::ticker_message::TimerCommand;

#[derive(Debug, Clone)]
pub struct LeaderChange {
    pub shard_group_id: ShardGroupId,
    pub leader_node_id: NodeId,
    pub term: u64,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct MetadataCommitted {
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
    MetadataCommitted(MetadataCommitted),
    /// Idempotent re-delivery of assignment messages each heartbeat, so a lost
    /// fire-and-forget send self-heals: active-segment `SegmentAssignment`s and
    /// sealed-segment `CatchUpAssignment`s. The actor forwards them to the data
    /// transport.
    RedriveAssignments(Vec<DataTransportCommand>),
    /// Leader-crash `SealBoundaryQuery` fan-out to a segment's survivors.
    /// A coordinator-initiated data-plane send not tied to a committed entry.
    /// The actor just forwards these to the data transport.
    SealBoundaryQueries(Vec<DataTransportCommand>),
}

impl MetadataCommitted {
    pub fn into_data_transport_cmds(self) -> Vec<DataTransportCommand> {
        let sgid = self.shard_group_id;
        match self.result {
            ApplyResult::TopicCreated(tc) => {
                vec![tc.into_command(sgid)]
            }
            ApplyResult::SegmentRolled(sr) => sr.into_command(self.seal_context, sgid),
            ApplyResult::RangeSplit(rs) => rs.into_command(sgid),
            ApplyResult::RangeMerged(rm) => {
                vec![rm.into_command(sgid)]
            }
            ApplyResult::TopicDeleted | ApplyResult::Noop => vec![],
            ApplyResult::SegmentReassigned(r) => r.into_catch_up_commands(sgid),
        }
    }
}

impl_from_variant!(
    RaftEvent,
    LeaderChange,
    OutboundRaftPacket,
    MetadataCommitted
);
