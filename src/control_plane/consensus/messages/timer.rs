use crate::control_plane::membership::ShardGroupId;
use crate::schedulers::timer::TTimer;

const ELECTION_TIMEOUT_BASE_TICKS: u32 = 50; // 5s base (+ jitter)
const RAFT_RPC_INTERVAL_TICKS: u32 = 10; // 1s
const MERGE_CHECK_INTERVAL_TICKS: u32 = 6_000; // 10 minutes

#[derive(Debug, Clone)]
pub struct RaftTimer {
    pub(crate) shard_group_id: ShardGroupId,
    kind: RaftTimerKind,
    ticks_remaining: u32,
}

#[derive(Debug, Clone)]
pub enum RaftTimerKind {
    Election,
    Rpc,
    MergeCheck,
}

#[derive(Debug, Default)]
pub enum RaftTimeoutCallback {
    #[default]
    Ignored,
    ElectionTimeout {
        shard_group_id: ShardGroupId,
    },
    RpcTimeout {
        shard_group_id: ShardGroupId,
    },
    MergeCheckTimeout {
        shard_group_id: ShardGroupId,
        now: u64,
    },
}

impl TTimer for RaftTimer {
    type Callback = RaftTimeoutCallback;

    fn tick(&mut self) -> u32 {
        self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        self.ticks_remaining
    }

    fn to_timeout_callback(self, _seq: u64, now: u64) -> RaftTimeoutCallback {
        match self.kind {
            RaftTimerKind::Election => RaftTimeoutCallback::ElectionTimeout {
                shard_group_id: self.shard_group_id,
            },
            RaftTimerKind::Rpc => RaftTimeoutCallback::RpcTimeout {
                shard_group_id: self.shard_group_id,
            },
            RaftTimerKind::MergeCheck => RaftTimeoutCallback::MergeCheckTimeout {
                shard_group_id: self.shard_group_id,
                now,
            },
        }
    }

    #[cfg(test)]
    fn target_node_id(&self) -> Option<crate::control_plane::NodeId> {
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

    pub fn rpc(shard_group_id: ShardGroupId) -> Self {
        Self {
            shard_group_id,
            kind: RaftTimerKind::Rpc,
            ticks_remaining: RAFT_RPC_INTERVAL_TICKS,
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
