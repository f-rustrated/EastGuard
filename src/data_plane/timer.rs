use std::fmt;

#[cfg(test)]
use crate::clusters::NodeId;
use crate::data_plane::SegmentKey;
use crate::schedulers::timer::TTimer;

// Data plane ticker runs at TICK_PERIOD_10_MS (10ms per tick)
const REPLICATION_TIMEOUT_TICKS: u32 = 100; // 100 * 10ms = 1s
const BATCH_FLUSH_DEADLINE_TICKS: u32 = 1; // 1 * 10ms = 10ms

#[derive(Debug)]
pub struct DataPlaneTimer {
    ticks_remaining: u32,
    callback: DataPlaneTimeoutCallback,
}

impl DataPlaneTimer {
    pub(crate) fn replication_timeout(segment_key: SegmentKey) -> Self {
        Self {
            ticks_remaining: REPLICATION_TIMEOUT_TICKS,
            callback: DataPlaneTimeoutCallback::ReplicationTimeout { segment_key },
        }
    }

    pub(crate) fn batch_flush_deadline() -> Self {
        Self {
            ticks_remaining: BATCH_FLUSH_DEADLINE_TICKS,
            callback: DataPlaneTimeoutCallback::BatchFlushDeadline,
        }
    }
}

#[derive(Debug, Default)]
pub enum DataPlaneTimeoutCallback {
    #[default]
    BatchFlushDeadline,
    ReplicationTimeout {
        segment_key: SegmentKey,
    },
}

impl fmt::Display for DataPlaneTimeoutCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataPlaneTimeoutCallback::BatchFlushDeadline => write!(f, "BatchFlushDeadline"),
            DataPlaneTimeoutCallback::ReplicationTimeout { segment_key } => {
                write!(f, "ReplicationTimeout({segment_key:?})")
            }
        }
    }
}

impl TTimer for DataPlaneTimer {
    type Callback = DataPlaneTimeoutCallback;

    fn tick(&mut self) -> u32 {
        self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        self.ticks_remaining
    }

    fn to_timeout_callback(self, _seq: u64, _now: u64) -> DataPlaneTimeoutCallback {
        self.callback
    }

    #[cfg(test)]
    fn target_node_id(&self) -> Option<NodeId> {
        None
    }
}
