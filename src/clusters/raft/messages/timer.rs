use crate::clusters::swims::ShardGroupId;
use crate::schedulers::timer::TTimer;

const ELECTION_TIMEOUT_BASE_TICKS: u32 = 50; // 5s base (+ jitter)
const HEARTBEAT_INTERVAL_TICKS: u32 = 10; // 1s
const MERGE_CHECK_INTERVAL_TICKS: u32 = 6_000; // 10 minutes

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
    MergeCheck,
}

#[derive(Debug, Default)]
pub enum RaftTimeoutCallback {
    #[default]
    Ignored,
    ElectionTimeout {
        shard_group_id: ShardGroupId,
    },
    HeartbeatTimeout {
        shard_group_id: ShardGroupId,
    },
    MergeCheckTimeout {
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
            RaftTimerKind::MergeCheck => RaftTimeoutCallback::MergeCheckTimeout {
                shard_group_id: self.shard_group_id,
            },
        }
    }

    #[cfg(test)]
    fn target_node_id(&self) -> Option<crate::clusters::NodeId> {
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

    pub fn merge_check(shard_group_id: ShardGroupId) -> Self {
        Self {
            shard_group_id,
            kind: RaftTimerKind::MergeCheck,
            ticks_remaining: MERGE_CHECK_INTERVAL_TICKS,
        }
    }
}
