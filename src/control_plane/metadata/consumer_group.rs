use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};
use uuid::Uuid;

use crate::{connections::protocol::ConsumerGroupSyncAction, impl_new_struct_wrapper};

use super::RangeId;

pub(crate) type ConsumerMemberId = Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, BorshSerialize, BorshDeserialize)]
pub struct GenerationId(pub u64);
impl_new_struct_wrapper!(GenerationId, u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConsumerGroupAssignment {
    pub(crate) generation: GenerationId,
    pub(crate) ranges: Box<[RangeId]>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct ConsumerGroupMeta {
    pub(crate) generation: GenerationId,
    members: BTreeMap<ConsumerMemberId, u64>, // id : last_seen
    assignments: BTreeMap<RangeId, ConsumerMemberId>,
}

impl ConsumerGroupMeta {
    pub(crate) fn new() -> Self {
        Self {
            generation: GenerationId(0),
            members: BTreeMap::new(),
            assignments: BTreeMap::new(),
        }
    }

    pub(crate) fn sync_member(
        &mut self,
        member_id: ConsumerMemberId,
        action: ConsumerGroupSyncAction,
        observed_at: u64,
        session_timeout_ms: u64,
        active_ranges: &[RangeId],
    ) -> bool {
        let previous_members: Box<[ConsumerMemberId]> = self.members.keys().copied().collect();
        self.members
            .retain(|_, last_seen| observed_at.saturating_sub(*last_seen) <= session_timeout_ms);

        match action {
            ConsumerGroupSyncAction::Heartbeat => {
                self.members.insert(member_id, observed_at);
            }
            ConsumerGroupSyncAction::Leave => {
                self.members.remove(&member_id);
            }
        }

        // If nothing changes, no need to increase generation id
        let membership_changed = previous_members.as_ref()
            != self.members.keys().copied().collect::<Box<[_]>>().as_ref();
        let topology_changed = !self.covers_ranges(active_ranges);
        if !membership_changed && !topology_changed {
            return false;
        }

        self.generation = self.generation.saturating_add(1).into();
        self.reassign(active_ranges);
        true
    }

    pub(crate) fn rebalance_for_ranges(&mut self, active_ranges: &[RangeId]) -> bool {
        if self.covers_ranges(active_ranges) {
            return false;
        }
        self.generation = self.generation.saturating_add(1).into();
        self.reassign(active_ranges);
        true
    }

    pub(crate) fn ranges_for(&self, member_id: ConsumerMemberId) -> Box<[RangeId]> {
        self.assignments
            .iter()
            .filter_map(|(range_id, owner)| (*owner == member_id).then_some(*range_id))
            .collect()
    }

    pub(crate) fn assignment_for(&self, member_id: ConsumerMemberId) -> ConsumerGroupAssignment {
        ConsumerGroupAssignment {
            generation: self.generation,
            ranges: self.ranges_for(member_id),
        }
    }

    fn covers_ranges(&self, active_ranges: &[RangeId]) -> bool {
        self.assignments.len() == active_ranges.len()
            && active_ranges
                .iter()
                .all(|range_id| self.assignments.contains_key(range_id))
    }

    /*
    TODO, currently it's round robin assignment
    To achieve "stickiness," we need to stop clearing the map blindly.
    Instead, calculate a diff: keep existing valid assignments intact, find "orphaned" ranges (where the member left),
    and only distribute those orphaned or brand-new ranges to the available members.
    */
    fn reassign(&mut self, active_ranges: &[RangeId]) {
        self.assignments.clear();
        if self.members.is_empty() {
            return;
        }
        let members: Box<[ConsumerMemberId]> = self.members.keys().copied().collect();
        for (index, range_id) in active_ranges.iter().copied().enumerate() {
            self.assignments
                .insert(range_id, members[index % members.len()]);
        }
    }

    #[cfg(any(test, debug_assertions))]
    pub(crate) fn assert_assignments(&self, active_ranges: &[RangeId]) {
        if self.members.is_empty() {
            assert!(
                self.assignments.is_empty(),
                "consumer group without members has assignments"
            );
            return;
        }
        assert!(
            self.covers_ranges(active_ranges),
            "consumer-group assignments must cover every active range exactly once"
        );
        for owner in self.assignments.values() {
            assert!(
                self.members.contains_key(owner),
                "consumer-group assignment references an inactive member"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_changes_advance_generation_and_assign_once() {
        let mut group = ConsumerGroupMeta::new();
        let ranges = [RangeId(1), RangeId(2), RangeId(3)];
        let a = uuid::Uuid::new_v4();
        let b = uuid::Uuid::new_v4();

        assert!(group.sync_member(a, ConsumerGroupSyncAction::Heartbeat, 1, 10, &ranges));
        assert_eq!(*group.generation, 1);
        assert_eq!(group.ranges_for(a).as_ref(), &ranges);

        assert!(!group.sync_member(a, ConsumerGroupSyncAction::Heartbeat, 2, 10, &ranges));
        assert_eq!(*group.generation, 1);

        assert!(group.sync_member(b, ConsumerGroupSyncAction::Heartbeat, 3, 10, &ranges));
        assert_eq!(*group.generation, 2);
        group.assert_assignments(&ranges);
    }

    #[test]
    fn expired_member_is_fenced_by_a_new_generation() {
        let mut group = ConsumerGroupMeta::new();
        let ranges = [RangeId(1), RangeId(2)];
        let stale = uuid::Uuid::new_v4();
        let live = uuid::Uuid::new_v4();
        group.sync_member(stale, ConsumerGroupSyncAction::Heartbeat, 1, 10, &ranges);
        group.sync_member(live, ConsumerGroupSyncAction::Heartbeat, 2, 10, &ranges);

        assert!(group.sync_member(live, ConsumerGroupSyncAction::Heartbeat, 20, 10, &ranges));
        assert_eq!(*group.generation, 3);
        assert!(group.ranges_for(stale).is_empty());
        assert_eq!(group.ranges_for(live).as_ref(), &ranges);
    }
}
