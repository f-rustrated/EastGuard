use std::fmt::Debug;

#[cfg(test)]
use crate::clusters::NodeId;

pub(crate) trait TTimer: Debug + Send + Sync + 'static {
    type Callback: Default;

    fn tick(&mut self) -> u32;
    fn to_timeout_callback(self, id: u32) -> Self::Callback;

    #[cfg(test)]
    fn target_node_id(&self) -> NodeId;
}
