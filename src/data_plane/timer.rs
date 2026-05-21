use std::fmt;

#[cfg(test)]
use crate::clusters::NodeId;
use crate::data_plane::record::SegmentKey;
use crate::schedulers::timer::TTimer;

const REPLICATION_TIMEOUT_TICKS: u32 = 10; // 10 * 100ms = 1s

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
}

#[derive(Debug, Default)]
pub enum DataPlaneTimeoutCallback {
    #[default]
    PeriodicTick,
    ReplicationTimeout {
        segment_key: SegmentKey,
    },
}

impl fmt::Display for DataPlaneTimeoutCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataPlaneTimeoutCallback::PeriodicTick => write!(f, "PeriodicTick"),
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

    fn to_timeout_callback(self, _seq: u32, _now: u64) -> DataPlaneTimeoutCallback {
        self.callback
    }

    #[cfg(test)]
    fn target_node_id(&self) -> Option<NodeId> {
        None
    }
}
