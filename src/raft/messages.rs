#![allow(dead_code)]

use crate::clusters::NodeId;
use crate::impl_from_variant;
use crate::schedulers::timer::TTimer;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: RaftCommand,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftCommand {
    /// Placeholder — real commands (CreateTopic, etc.) will be added later.
    Noop,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader,
}

// ---------------------------------------------------------------------------
// RPC messages
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub node_id: NodeId,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub node_id: NodeId,
    pub success: bool,
    /// The peer's last log index after applying the entries (used by the leader
    /// to advance `match_index`).
    pub last_log_index: u64,
}

#[derive(Debug, Clone)]
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
    /// The intended recipient, identified by NodeId.
    /// The actor/transport layer should resolve this to a connection.
    pub target: NodeId,
    pub rpc: RaftRpc,
}

impl OutboundRaftPacket {
    pub(crate) fn new(target: NodeId, rpc: impl Into<RaftRpc>) -> Self {
        Self {
            target,
            rpc: rpc.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Timer
// ---------------------------------------------------------------------------
const ELECTION_TIMEOUT_BASE_TICKS: u32 = 15; // 1.5s base
const HEARTBEAT_INTERVAL_TICKS: u32 = 3; // 300ms

#[derive(Debug)]
pub struct RaftTimer {
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
    #[default]
    ElectionTimeout,
    HeartbeatTimeout,
}

impl TTimer for RaftTimer {
    type Callback = RaftTimeoutCallback;

    fn tick(&mut self) -> u32 {
        self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        self.ticks_remaining
    }

    fn to_timeout_callback(self, _seq: u32) -> RaftTimeoutCallback {
        match self.kind {
            RaftTimerKind::Election => RaftTimeoutCallback::ElectionTimeout,
            RaftTimerKind::Heartbeat => RaftTimeoutCallback::HeartbeatTimeout,
        }
    }

    #[cfg(test)]
    fn target_node_id(&self) -> Option<NodeId> {
        None
    }
}

impl RaftTimer {
    pub fn election(jitter_ticks: u32) -> Self {
        Self {
            kind: RaftTimerKind::Election,
            ticks_remaining: ELECTION_TIMEOUT_BASE_TICKS + jitter_ticks,
        }
    }

    pub fn heartbeat() -> Self {
        Self {
            kind: RaftTimerKind::Heartbeat,
            ticks_remaining: HEARTBEAT_INTERVAL_TICKS,
        }
    }
}
