#![allow(dead_code)]

use bincode::{Decode, Encode};

use crate::clusters::NodeId;
use crate::clusters::swims::ShardGroupId;
use crate::impl_from_variant;
use crate::clusters::raft::log::LogEntry;
use crate::schedulers::timer::TTimer;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum RaftCommand {
    /// Placeholder — real commands (CreateTopic, etc.) will be added later.
    Noop,
    /// Raft-internal membership change: remove a peer from the group.
    /// Applied when committed — modifies the Raft peer set directly.
    RemovePeer(NodeId),
    /// Raft-internal membership change: add a peer to the group.
    /// Applied when committed — inserts into peer set, leader initializes PeerState.
    AddPeer(NodeId),
}

impl RaftCommand {
    pub(crate) fn serialize(&self) -> Vec<u8> {
        match self {
            RaftCommand::Noop => vec![0x00],
            RaftCommand::RemovePeer(_) => vec![0x01],
            RaftCommand::AddPeer(_) => vec![0x02],
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader,
}

// ---------------------------------------------------------------------------
// RPC messages
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Encode, Decode)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub node_id: NodeId,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub node_id: NodeId,
    pub success: bool,
    /// The peer's last log index after applying the entries (used by the leader
    /// to advance `match_index`).
    pub last_log_index: u64,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum RaftRpc {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
}

impl_from_variant!(
    RaftRpc,
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse
);

// ---------------------------------------------------------------------------
// Outbound packet
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct OutboundRaftPacket {
    pub shard_group_id: ShardGroupId,
    /// The intended recipient, identified by NodeId.
    /// The actor/transport layer resolves this to a connection.
    pub target: NodeId,
    pub rpc: RaftRpc,
}

impl OutboundRaftPacket {
    pub(crate) fn new(
        shard_group_id: ShardGroupId,
        target: NodeId,
        rpc: impl Into<RaftRpc>,
    ) -> Self {
        Self {
            shard_group_id,
            target,
            rpc: rpc.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Wire message (TCP framing: [4-byte len][WireRaftMessage bincode])
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Encode, Decode)]
pub struct WireRaftMessage {
    pub shard_group_id: ShardGroupId,
    pub sender: NodeId,
    pub rpc: RaftRpc,
}

// ---------------------------------------------------------------------------
// Timer
// ---------------------------------------------------------------------------
//
// DS-RSM is for metadata management (topic assignments, range ownership) — not
// data-plane traffic. Consistency matters more than heartbeat latency, so we use
// relaxed intervals to reduce per-node timer load.
//
// With 600 nodes × 256 vnodes, each node participates in ~768 shard groups.
// At 1s heartbeat: ~256 leader heartbeat callbacks/sec (~512 outbound RPCs/sec).
// Election timeout at 5s base (5× heartbeat) avoids false elections.
const ELECTION_TIMEOUT_BASE_TICKS: u32 = 50; // 5s base (+ jitter)
const HEARTBEAT_INTERVAL_TICKS: u32 = 10; // 1s

#[derive(Debug)]
pub struct RaftTimer {
    shard_group_id: ShardGroupId,
    kind: RaftTimerKind,
    ticks_remaining: u32,
}

#[derive(Debug)]
pub enum RaftTimerKind {
    Election,
    Heartbeat,
}

#[derive(Debug, Default)]
pub enum RaftTimeoutCallback {
    /// Emitted by Ticker's protocol-period clock. Raft has no protocol-period
    /// concept — the actor discards this variant.
    #[default]
    Ignored,
    ElectionTimeout {
        shard_group_id: ShardGroupId,
    },
    HeartbeatTimeout {
        shard_group_id: ShardGroupId,
    },
}

impl TTimer for RaftTimer {
    type Callback = RaftTimeoutCallback;

    fn tick(&mut self) -> u32 {
        self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        self.ticks_remaining
    }

    fn to_timeout_callback(self, _seq: u32) -> RaftTimeoutCallback {
        match self.kind {
            RaftTimerKind::Election => RaftTimeoutCallback::ElectionTimeout {
                shard_group_id: self.shard_group_id,
            },
            RaftTimerKind::Heartbeat => RaftTimeoutCallback::HeartbeatTimeout {
                shard_group_id: self.shard_group_id,
            },
        }
    }

    #[cfg(test)]
    fn target_node_id(&self) -> Option<NodeId> {
        None
    }
}

impl RaftTimer {
    pub fn election(jitter_ticks: u32, shard_group_id: ShardGroupId) -> Self {
        Self {
            shard_group_id,
            kind: RaftTimerKind::Election,
            ticks_remaining: ELECTION_TIMEOUT_BASE_TICKS + jitter_ticks,
        }
    }

    pub fn heartbeat(shard_group_id: ShardGroupId) -> Self {
        Self {
            shard_group_id,
            kind: RaftTimerKind::Heartbeat,
            ticks_remaining: HEARTBEAT_INTERVAL_TICKS,
        }
    }
}
