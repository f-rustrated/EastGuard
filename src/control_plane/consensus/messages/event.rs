use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::rpc::OutboundRaftPacket;
use crate::control_plane::consensus::messages::timer::RaftTimer;
use crate::control_plane::consensus::raft::log::LogEntry;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::event::ApplyResult;
use crate::data_plane::SegmentKey;
use crate::data_plane::messages::command::{SealResponse, SegmentAssignment};
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
#[derive(Debug, Clone)]
pub struct SealContext {
    pub requester: NodeId,
    pub old_segment_key: SegmentKey,
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
}

impl MetadataCommitted {
    pub fn into_data_transport_cmds(self) -> Vec<DataTransportCommand> {
        let sgid = self.shard_group_id;
        match self.result {
            ApplyResult::TopicCreated(tc) => {
                let target = tc.replica_set[0].clone();
                vec![DataTransportCommand::send_to_targets(
                    vec![target],
                    SegmentAssignment {
                        segment_key: SegmentKey::new(tc.topic_id, tc.range_id, tc.segment_id),
                        shard_group_id: sgid,
                        replica_set: tc.replica_set,
                        start_entry_id: 0,
                    },
                )]
            }
            ApplyResult::SegmentRolled(sr) => {
                let target = sr.new_replica_set[0].clone();
                let start = sr.end_entry_id.map_or(0, |id| id + 1);
                let mut cmds = vec![DataTransportCommand::send_to_targets(
                    vec![target],
                    SegmentAssignment {
                        segment_key: SegmentKey::new(sr.topic_id, sr.range_id, sr.new_segment_id),
                        shard_group_id: sgid,
                        replica_set: sr.new_replica_set.clone(),
                        start_entry_id: start,
                    },
                )];
                if let Some(ctx) = self.seal_context {
                    cmds.push(DataTransportCommand::send_to_targets(
                        vec![ctx.requester],
                        SealResponse {
                            old_segment_key: ctx.old_segment_key,
                            new_segment_id: sr.new_segment_id,
                            new_replica_set: sr.new_replica_set,
                        },
                    ));
                }
                cmds
            }
            ApplyResult::RangeSplit(rs) => rs
                .children
                .into_iter()
                .map(|(range_id, segment_id, replica_set)| {
                    let target = replica_set[0].clone();
                    DataTransportCommand::send_to_targets(
                        vec![target],
                        SegmentAssignment {
                            segment_key: SegmentKey::new(rs.topic_id, range_id, segment_id),
                            shard_group_id: sgid,
                            replica_set,
                            start_entry_id: 0,
                        },
                    )
                })
                .collect(),
            ApplyResult::RangeMerged(rm) => {
                let target = rm.replica_set[0].clone();
                vec![DataTransportCommand::send_to_targets(
                    vec![target],
                    SegmentAssignment {
                        segment_key: SegmentKey::new(
                            rm.topic_id,
                            rm.merged_range_id,
                            rm.segment_id,
                        ),
                        shard_group_id: sgid,
                        replica_set: rm.replica_set,
                        start_entry_id: 0,
                    },
                )]
            }
            ApplyResult::TopicDeleted | ApplyResult::Noop => vec![],
        }
    }
}

impl_from_variant!(
    RaftEvent,
    LeaderChange,
    OutboundRaftPacket,
    MetadataCommitted
);
