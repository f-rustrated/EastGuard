pub(crate) mod command;
pub mod error;
pub(crate) mod event;
#[allow(dead_code)]
pub(crate) mod state_machine;
pub mod strategy;
#[allow(dead_code)]
pub(crate) mod types;

use bincode::{Decode, Encode};
#[allow(unused_imports)]
pub(crate) use command::*;
#[allow(unused_imports)]
pub(crate) use event::*;
#[allow(unused_imports)]
pub(crate) use state_machine::*;
#[allow(unused_imports)]
pub(crate) use types::*;

use crate::smart_pointer;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct TopicId(pub(crate) u64);

smart_pointer!(TopicId, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode, PartialOrd, Ord)]
pub struct RangeId(pub(crate) u64);

smart_pointer!(RangeId, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct SegmentId(pub(crate) u64);

smart_pointer!(SegmentId, u64);
