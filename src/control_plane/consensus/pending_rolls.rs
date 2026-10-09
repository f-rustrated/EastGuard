use std::collections::{BTreeMap, HashMap, HashSet};

use crate::control_plane::NodeId;
use crate::control_plane::membership::ShardGroupId;
use crate::data_plane::SegmentKey;

#[derive(Debug)]
pub(crate) struct RollRequestContext {
    pub(crate) requester: NodeId,
    pub(crate) segment_key: SegmentKey,
}

/// Bookkeeping for in-flight roll proposals.
#[derive(Default)]
pub(super) struct PendingRollTracker {
    by_group: HashMap<ShardGroupId, BTreeMap<u64, RollRequestContext>>,
    by_key: HashSet<SegmentKey>,
}

impl PendingRollTracker {
    pub(super) fn contains(&self, key: &SegmentKey) -> bool {
        self.by_key.contains(key)
    }

    pub(super) fn insert(
        &mut self,
        group_id: ShardGroupId,
        log_index: u64,
        roll: RollRequestContext,
    ) {
        self.by_key.insert(roll.segment_key);
        self.by_group
            .entry(group_id)
            .or_default()
            .insert(log_index, roll);
    }

    pub(super) fn take(
        &mut self,
        group_id: ShardGroupId,
        log_index: u64,
    ) -> Option<RollRequestContext> {
        let group_map = self.by_group.get_mut(&group_id)?;
        let roll = group_map.remove(&log_index)?;

        if group_map.is_empty() {
            self.by_group.remove(&group_id);
        }

        self.by_key.remove(&roll.segment_key);
        Some(roll)
    }

    /// Drop pending roll contexts for `group_id` whose log indices were
    /// truncated by a new leader. Prevents unbounded growth of both indexes.
    pub(super) fn drop_from(&mut self, group_id: ShardGroupId, from_index: u64) {
        if let Some(group_map) = self.by_group.get_mut(&group_id) {
            let stale = group_map.split_off(&from_index);
            for roll in stale.into_values() {
                self.by_key.remove(&roll.segment_key);
            }

            if group_map.is_empty() {
                self.by_group.remove(&group_id);
            }
        }
    }

    /// Drop every pending roll context for `group_id`. Called when a leader
    /// steps down and this replica will no longer dispatch the commit.
    pub(super) fn drop_group(&mut self, group_id: ShardGroupId) {
        if let Some(stale_group) = self.by_group.remove(&group_id) {
            for roll in stale_group.into_values() {
                self.by_key.remove(&roll.segment_key);
            }
        }
    }
}
