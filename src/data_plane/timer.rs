use std::fmt;

#[cfg(test)]
use crate::clusters::NodeId;
use crate::data_plane::SegmentKey;
use crate::schedulers::timer::TTimer;

const REPLICATION_TIMEOUT_TICKS: u32 = 100; // 100 * 10ms = 1s
const BATCH_FLUSH_DEADLINE_TICKS: u32 = 1; // 1 * 10ms = 10ms

// --- Batch flush timer (10ms deadline) ---

#[derive(Debug)]
pub struct BatchFlushTimer {
    ticks_remaining: u32,
}

impl BatchFlushTimer {
    pub(crate) fn deadline() -> Self {
        Self {
            ticks_remaining: BATCH_FLUSH_DEADLINE_TICKS,
        }
    }
}

#[derive(Debug, Default)]
pub enum BatchFlushCallback {
    #[default]
    Deadline,
}

impl TTimer for BatchFlushTimer {
    type Callback = BatchFlushCallback;

    fn tick(&mut self) -> u32 {
        self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        self.ticks_remaining
    }

    fn to_timeout_callback(self, _seq: u64, _now: u64) -> BatchFlushCallback {
        BatchFlushCallback::Deadline
    }

    #[cfg(test)]
    fn target_node_id(&self) -> Option<NodeId> {
        None
    }
}

// --- Replication timer (1s timeout per segment) ---

#[derive(Debug)]
pub struct ReplicationTimer {
    ticks_remaining: u32,
    segment_key: SegmentKey,
}

impl ReplicationTimer {
    pub(crate) fn timeout(segment_key: SegmentKey) -> Self {
        Self {
            ticks_remaining: REPLICATION_TIMEOUT_TICKS,
            segment_key,
        }
    }
}

#[derive(Debug, Default)]
pub enum ReplicationCallback {
    #[default]
    Noop,
    Timeout {
        seq: u64,
        segment_key: SegmentKey,
    },
}

impl TTimer for ReplicationTimer {
    type Callback = ReplicationCallback;

    fn tick(&mut self) -> u32 {
        self.ticks_remaining = self.ticks_remaining.saturating_sub(1);
        self.ticks_remaining
    }

    fn to_timeout_callback(self, seq: u64, _now: u64) -> ReplicationCallback {
        ReplicationCallback::Timeout {
            seq,
            segment_key: self.segment_key,
        }
    }

    #[cfg(test)]
    fn target_node_id(&self) -> Option<NodeId> {
        None
    }
}

// --- Unified callback for DataPlaneCommand::Timeout ---

#[derive(Debug)]
pub enum DataPlaneTimeoutCallback {
    BatchFlushDeadline,
    ReplicationTimeout { seq: u64, segment_key: SegmentKey },
}

impl From<BatchFlushCallback> for DataPlaneTimeoutCallback {
    fn from(_: BatchFlushCallback) -> Self {
        DataPlaneTimeoutCallback::BatchFlushDeadline
    }
}

impl From<ReplicationCallback> for DataPlaneTimeoutCallback {
    fn from(cb: ReplicationCallback) -> Self {
        match cb {
            ReplicationCallback::Noop => DataPlaneTimeoutCallback::BatchFlushDeadline,
            ReplicationCallback::Timeout { seq, segment_key } => {
                DataPlaneTimeoutCallback::ReplicationTimeout { seq, segment_key }
            }
        }
    }
}

impl fmt::Display for DataPlaneTimeoutCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataPlaneTimeoutCallback::BatchFlushDeadline => write!(f, "BatchFlushDeadline"),
            DataPlaneTimeoutCallback::ReplicationTimeout { seq, segment_key } => {
                write!(f, "ReplicationTimeout(seq={seq}, {segment_key:?})")
            }
        }
    }
}
