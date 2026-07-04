use super::cursor::RangeCursor;
use super::cursor_set::RangeCursorSet;
use crate::client::consumer::fetch::FetchActorCommand;
use crate::connections::protocol::{RangeDetail, RangeTransition};
use crate::control_plane::metadata::{RangeId, RangeState};

use std::collections::HashMap;

/// Unified ownership of per-range state: the consumer's cursor set and the
/// matching stop channels for active fetch actors.
///
/// Every mutation (acquire / revoke / drain) keeps the two in sync so that
/// callers cannot accidentally have a cursor without an actor or vice-versa.
pub(crate) struct RangeOwnership {
    cursors: RangeCursorSet,
    senders: HashMap<RangeId, flume::Sender<FetchActorCommand>>,
}

impl RangeOwnership {
    /// Construct from a pre-built cursor set (pre-populated for independent
    /// consumers, empty for group consumers). Stop channels start empty and
    /// are populated via [`register`] or [`acquire`].
    pub fn new(cursors: RangeCursorSet) -> Self {
        Self {
            cursors,
            senders: HashMap::new(),
        }
    }

    /// Acquire ownership of a new range: add cursor to the set and register
    /// its stop channel atomically.
    /// Used when the rebalancer assigns a range to this consumer.
    pub fn acquire(&mut self, cursor: RangeCursor, stop_tx: flume::Sender<FetchActorCommand>) {
        self.senders.insert(cursor.range_id, stop_tx);
        self.cursors.add_or_update(cursor);
    }

    /// Register a stop channel for a range whose cursor already exists.
    /// Used during independent consumer initialization (cursors are
    /// pre-populated at bootstrap) and after [`drain`] (cursors are added
    /// by `apply_drained`).
    pub fn register(&mut self, range_id: RangeId, stop_tx: flume::Sender<FetchActorCommand>) {
        debug_assert!(
            self.cursors.contains(range_id),
            "register called for range {range_id:?} not in cursor set"
        );
        self.senders.insert(range_id, stop_tx);
    }

    /// Revoke ownership: send Stop, remove cursor and stop channel.
    pub fn revoke(&mut self, range_id: RangeId) {
        if let Some(stop_tx) = self.senders.remove(&range_id) {
            let _ = stop_tx.send(FetchActorCommand::Stop);
        }
        self.cursors.remove(range_id);
    }

    /// Handle a drained range's lineage transition.
    /// Returns `None` if the range was already revoked (late drain event).
    /// Returns `Some(new_cursors)` — cursors that were added to the set by
    /// `apply_drained` and now need fetch actors spawned + [`register`]ed.
    pub fn drain(
        &mut self,
        range_id: RangeId,
        transition: RangeTransition,
    ) -> Option<Box<[RangeCursor]>> {
        // If not owned, it was already revoked — late drain event, safe to ignore.
        self.senders.remove(&range_id)?;
        Some(self.cursors.apply_drained(range_id, transition))
    }

    /// Revoke all owned ranges. Used at shutdown.
    pub fn revoke_all(&mut self) {
        for (_, stop_tx) in self.senders.drain() {
            let _ = stop_tx.send(FetchActorCommand::Stop);
        }
    }

    pub fn owns(&self, range_id: &RangeId) -> bool {
        self.senders.contains_key(range_id)
    }

    pub fn owned_range_ids(&self) -> impl Iterator<Item = &RangeId> {
        self.senders.keys()
    }

    pub fn get_cursor(&self, range_id: RangeId) -> Option<&RangeCursor> {
        self.cursors.get(range_id)
    }

    pub fn iter_cursors(&self) -> std::slice::Iter<'_, RangeCursor> {
        self.cursors.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.cursors.is_empty()
    }

    /// Returns true if this range should NOT be started. Consolidates:
    /// - Active descendant check (children already being fetched)
    /// - Undrained parent check (parent not yet fully consumed)
    /// - Sealed-and-consumed check (range already fully consumed)
    pub fn should_skip_start(
        &self,
        range_id: RangeId,
        ranges: &[RangeDetail],
        saved_offsets: &HashMap<RangeId, u64>,
        resolved_entry_id: u64,
    ) -> bool {
        self.has_active_descendant(range_id, ranges)
            || self.has_undrained_parent(range_id, ranges, saved_offsets)
            || ranges
                .iter()
                .find(|r| r.range_id == range_id)
                .is_some_and(|r| {
                    r.state == RangeState::Sealed && resolved_entry_id > r.end_entry_id()
                })
    }

    /// Returns true if any descendant (split child or merge target) of the
    /// given range is currently owned or has a live cursor.
    pub fn has_active_descendant(&self, range_id: RangeId, ranges: &[RangeDetail]) -> bool {
        let Some(r_meta) = ranges.iter().find(|r| r.range_id == range_id) else {
            return false;
        };

        if let Some(children) = r_meta.split_into {
            if self.owns(&children.0) || self.owns(&children.1) {
                return true;
            }
            if self.cursors.contains(children.0) || self.cursors.contains(children.1) {
                return true;
            }
            if self.has_active_descendant(children.0, ranges)
                || self.has_active_descendant(children.1, ranges)
            {
                return true;
            }
        }

        if let Some(child) = r_meta.merged_into {
            if self.owns(&child) {
                return true;
            }
            if self.cursors.contains(child) {
                return true;
            }
            if self.has_active_descendant(child, ranges) {
                return true;
            }
        }

        false
    }

    /// Returns true if any ancestor (split parent or merge source) of the
    /// given range still needs to be drained before this range can start.
    pub fn has_undrained_parent(
        &self,
        range_id: RangeId,
        ranges: &[RangeDetail],
        saved_offsets: &HashMap<RangeId, u64>,
    ) -> bool {
        for p in ranges.iter() {
            let is_parent = p
                .split_into
                .is_some_and(|children| children.0 == range_id || children.1 == range_id)
                || p.merged_into.is_some_and(|child| child == range_id);

            if !is_parent {
                continue;
            }

            // Parent still has an active fetch actor — not yet drained.
            if self.owns(&p.range_id) {
                return true;
            }

            // Parent is still active (not sealed) — can't have drained.
            if p.state == RangeState::Active {
                return true;
            }

            // Parent is sealed but consumer hasn't consumed past its end.
            if p.state == RangeState::Sealed {
                let parent_resolved = saved_offsets.get(&p.range_id).map(|o| o + 1).unwrap_or(0);

                if parent_resolved <= p.end_entry_id() {
                    return true;
                }
            }

            // Walk further up the lineage tree.
            if self.has_undrained_parent(p.range_id, ranges, saved_offsets) {
                return true;
            }
        }

        false
    }
}
