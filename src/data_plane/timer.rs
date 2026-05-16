use std::fmt;

#[cfg(test)]
use crate::clusters::NodeId;
use crate::schedulers::timer::TTimer;

#[derive(Debug)]
pub struct DataPlaneTimer {
    ticks_remaining: u32,
}

#[derive(Debug, Default)]
pub enum DataPlaneTimeoutCallback {
    #[default]
    PeriodicTick,
}

impl fmt::Display for DataPlaneTimeoutCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataPlaneTimeoutCallback::PeriodicTick => write!(f, "PeriodicTick"),
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
        DataPlaneTimeoutCallback::PeriodicTick
    }

    #[cfg(test)]
    fn target_node_id(&self) -> Option<NodeId> {
        None
    }
}
