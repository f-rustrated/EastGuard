use crate::clusters::{NodeId, TimeoutEvent};
use std::fmt::Debug;

pub(crate) trait TTimer: Debug + Send + Sync + 'static {
    fn tick(&mut self) -> u32;
    fn to_timeout_event(self: Box<Self>, seq: u32) -> TimeoutEvent;

    #[cfg(test)]
    fn target_node_id(&self) -> NodeId;
}
