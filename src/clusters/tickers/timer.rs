use crate::clusters::TimeoutEvent;
use std::fmt::Debug;

#[cfg(test)]
use crate::clusters::NodeId;

pub(crate) trait TTimer: Debug + Send + Sync + 'static {
    fn tick(&mut self) -> u32;
    fn to_timeout_event(self: Box<Self>, seq: u32) -> TimeoutEvent;

    #[cfg(test)]
    fn target_node_id(&self) -> NodeId;
}
