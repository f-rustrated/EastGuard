pub(crate) mod command;
pub mod constants;
pub(crate) mod consumer_group;

pub mod error;
pub(crate) mod event;
pub(crate) mod range;

pub mod strategy;
pub(crate) mod topic;

pub(crate) use range::{RangeMeta, RangeState};
pub(crate) use topic::{TopicMeta, TopicState, TopicStats};

pub(crate) mod segment;

use borsh::{BorshDeserialize as Deser, BorshSerialize as Ser};

pub(crate) use command::*;
pub(crate) use consumer_group::{ConsumerGroupAssignment, ConsumerGroupMeta, ConsumerMemberId};

pub(crate) use segment::*;

use crate::impl_new_struct_wrapper;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Ser, Deser)]
pub struct TopicId(pub(crate) u64);

impl_new_struct_wrapper!(TopicId, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ser, Deser, PartialOrd, Ord)]
pub struct RangeId(pub(crate) u64);

impl_new_struct_wrapper!(RangeId, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Ser, Deser)]
pub struct SegmentId(pub(crate) u64);

impl_new_struct_wrapper!(SegmentId, u64);

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Ser, Deser)]
pub struct EntryId(pub u64);

impl_new_struct_wrapper!(EntryId, u64);

impl EntryId {
    pub const MIN: EntryId = EntryId(0);
    pub const MAX: EntryId = EntryId(u64::MAX);

    pub fn saturating_sub(self, rhs: u64) -> Self {
        EntryId(self.0.saturating_sub(rhs))
    }

    pub fn saturating_add(self, rhs: u64) -> Self {
        EntryId(self.0.saturating_add(rhs))
    }
}

impl std::fmt::Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<EntryId> for u64 {
    fn from(val: EntryId) -> Self {
        val.0
    }
}

impl std::ops::Add<u64> for EntryId {
    type Output = Self;
    fn add(self, rhs: u64) -> Self::Output {
        EntryId(self.0 + rhs)
    }
}

impl std::ops::Add<usize> for EntryId {
    type Output = Self;
    fn add(self, rhs: usize) -> Self::Output {
        EntryId(self.0 + rhs as u64)
    }
}

impl std::ops::Add<EntryId> for EntryId {
    type Output = Self;
    fn add(self, rhs: EntryId) -> Self::Output {
        EntryId(self.0 + rhs.0)
    }
}

impl std::ops::AddAssign<u64> for EntryId {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl std::ops::AddAssign<usize> for EntryId {
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs as u64;
    }
}

impl std::ops::Sub<u64> for EntryId {
    type Output = Self;
    fn sub(self, rhs: u64) -> Self::Output {
        EntryId(self.0 - rhs)
    }
}

impl std::ops::Sub<usize> for EntryId {
    type Output = Self;
    fn sub(self, rhs: usize) -> Self::Output {
        EntryId(self.0 - rhs as u64)
    }
}

impl std::ops::Sub<EntryId> for EntryId {
    type Output = Self;
    fn sub(self, rhs: EntryId) -> Self::Output {
        EntryId(self.0 - rhs.0)
    }
}

impl std::ops::SubAssign<u64> for EntryId {
    fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
    }
}

impl std::ops::SubAssign<usize> for EntryId {
    fn sub_assign(&mut self, rhs: usize) {
        self.0 -= rhs as u64;
    }
}

impl std::ops::Add<i32> for EntryId {
    type Output = Self;
    fn add(self, rhs: i32) -> Self::Output {
        if rhs >= 0 {
            EntryId(self.0 + rhs as u64)
        } else {
            EntryId(self.0 - (-rhs) as u64)
        }
    }
}

impl std::ops::Sub<i32> for EntryId {
    type Output = Self;
    fn sub(self, rhs: i32) -> Self::Output {
        if rhs >= 0 {
            EntryId(self.0 - rhs as u64)
        } else {
            EntryId(self.0 + (-rhs) as u64)
        }
    }
}
