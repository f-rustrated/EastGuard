pub mod error;
#[allow(dead_code)]
pub(crate) mod state_machine;
pub mod strategy;
#[allow(dead_code)]
pub(crate) mod types;

use bincode::{Decode, Encode};
#[allow(unused_imports)]
pub(crate) use state_machine::*;
#[allow(unused_imports)]
pub(crate) use types::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct TopicId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct RangeId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct SegmentId(pub u64);
