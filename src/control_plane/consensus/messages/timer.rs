use crate::control_plane::membership::ShardGroupId;
use crate::schedulers::timer::TTimer;

const ELECTION_TIMEOUT_BASE_TICKS: u32 = 50; // 5s base (+ jitter)
const RAFT_RPC_INTERVAL_TICKS: u32 = 10; // 1s
const MERGE_CHECK_INTERVAL_TICKS: u32 = 6_000; // 10 minutes
/// 30s — frequent enough that a rebalance under a stable leader converges
/// within a minute, slow enough that the O(RF) ring diff per group is noise.
/// Also paces the consecutive-observation ring-stability gate for stale-peer removal
const RING_CHECK_INTERVAL_TICKS: u32 = 300;

#[derive(Debug, Clone)]
pub struct RaftTimer {
    pub(crate) shard_group_id: ShardGroupId,
    kind: RaftTimerKind,
    ticks_remaining: u32,
    epoch: u64,
}

#[derive(Debug, Clone)]
pub enum RaftTimerKind {
    Election,
    Rpc,
    MergeCheck,
    RingCheck,
}

#[derive(Debug, Default)]
pub enum RaftTimeoutCallback {
    #[default]
    Ignored,
    ElectionTimeout {
        shard_group_id: ShardGroupId,
        epoch: u64,
    },
    RpcTimeout {
        shard_group_id: ShardGroupId,
    },
    MergeCheckTimeout {
        shard_group_id: ShardGroupId,
        now: u64,
    },
    RingCheckTimeout {
        shard_group_id: ShardGroupId,
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
                epoch: self.epoch,
            },
            RaftTimerKind::Rpc => RaftTimeoutCallback::RpcTimeout {
                shard_group_id: self.shard_group_id,
            },
            RaftTimerKind::MergeCheck => RaftTimeoutCallback::MergeCheckTimeout {
                shard_group_id: self.shard_group_id,
                now,
            },
            RaftTimerKind::RingCheck => RaftTimeoutCallback::RingCheckTimeout {
                shard_group_id: self.shard_group_id,
            },
        }
    }

    #[cfg(test)]
    fn target_node_id(&self) -> Option<crate::control_plane::NodeId> {
        None
    }
}

impl RaftTimer {
    pub fn election(jitter_ticks: u32, shard_group_id: ShardGroupId, epoch: u64) -> Self {
        Self {
            shard_group_id,
            kind: RaftTimerKind::Election,
            ticks_remaining: ELECTION_TIMEOUT_BASE_TICKS + jitter_ticks,
            epoch,
        }
    }

    pub fn rpc(shard_group_id: ShardGroupId) -> Self {
        Self {
            shard_group_id,
            kind: RaftTimerKind::Rpc,
            ticks_remaining: RAFT_RPC_INTERVAL_TICKS,
            epoch: 0,
        }
    }

    pub fn merge_check(shard_group_id: ShardGroupId) -> Self {
        Self {
            shard_group_id,
            kind: RaftTimerKind::MergeCheck,
            ticks_remaining: MERGE_CHECK_INTERVAL_TICKS,
            epoch: 0,
        }
    }

    pub fn ring_check(shard_group_id: ShardGroupId) -> Self {
        Self {
            shard_group_id,
            kind: RaftTimerKind::RingCheck,
            ticks_remaining: RING_CHECK_INTERVAL_TICKS,
            epoch: 0,
        }
    }
}
