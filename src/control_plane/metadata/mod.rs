pub(crate) mod command;
pub mod constants;

pub mod error;
pub(crate) mod event;
pub(crate) mod range;
#[allow(dead_code)]
pub(crate) mod state_machine;
pub mod strategy;
pub(crate) mod topic;

pub(crate) use range::{RangeMeta, RangeState};
pub(crate) use topic::{TopicMeta, TopicState, TopicStats};
#[allow(dead_code)]
pub(crate) mod segment;

use borsh::{BorshDeserialize, BorshSerialize};
#[allow(unused_imports)]
pub(crate) use command::*;
#[allow(unused_imports)]
pub(crate) use event::*;
#[allow(unused_imports)]
pub(crate) use segment::*;
#[allow(unused_imports)]
pub(crate) use state_machine::*;

use crate::smart_pointer;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub struct TopicId(pub(crate) u64);

smart_pointer!(TopicId, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize, PartialOrd, Ord)]
pub struct RangeId(pub(crate) u64);

smart_pointer!(RangeId, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, BorshSerialize, BorshDeserialize)]
pub struct SegmentId(pub(crate) u64);

smart_pointer!(SegmentId, u64);
