//! Sealed-segment catch-up re-drive — a `Raft`-owned tracker.
//!
//! A committed `ReassignSegment` dispatches its `CatchUpAssignment` once
//! (fire-and-forget). Seeded at apply and driven by the leader's heartbeat, this
//! tracker re-drives the assignment to each member until it acks, so a lost
//! message can't strand the repair. See `.claude/rules/raft-actor.md` #9.

use std::collections::{HashMap, HashSet};

use crate::control_plane::NodeId;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::event::SegmentReassigned;
use crate::data_plane::SegmentKey;
use crate::data_plane::messages::command::CatchUpAssignment;
use crate::data_plane::transport::command::DataTransportCommand;

/// One in-flight repair: re-drive each member in `pending` until it acks.
/// `replica_set` is the full set (the assignment carries it so the receiver
/// picks its own source).
struct CatchUpRepair {
    start_entry_id: u64,
    sealed_end: u64,
    replica_set: Vec<NodeId>,
    pending: HashSet<NodeId>,
}

/// In-flight sealed-segment repairs (D5), keyed by segment. Seeded at
/// `ReassignSegment` apply; re-driven each heartbeat until acked. Leader-volatile.
#[derive(Default)]
pub(crate) struct CatchUpRepairs {
    repairs: HashMap<SegmentKey, CatchUpRepair>,
}

impl CatchUpRepairs {
    /// Track a just-applied reassignment. One with no committed end carries no
    /// catch-up, so skip it. Re-tracking overwrites — a newer replica set wins.
    pub(crate) fn track(&mut self, r: &SegmentReassigned) {
        let Some(sealed_end) = r.sealed_end else {
            return;
        };
        self.repairs.insert(
            r.segment_key,
            CatchUpRepair {
                start_entry_id: r.start_entry_id,
                sealed_end,
                replica_set: r.new_replica_set.clone(),
                pending: r.new_replica_set.iter().cloned().collect(),
            },
        );
    }

    /// A member confirmed; drop it from `pending` and prune once all confirm.
    pub(crate) fn confirm(&mut self, segment_key: SegmentKey, from: NodeId) {
        let Some(repair) = self.repairs.get_mut(&segment_key) else {
            return;
        };
        repair.pending.remove(&from);
        if repair.pending.is_empty() {
            self.repairs.remove(&segment_key);
        }
    }

    /// One re-drive command per unconfirmed member.
    pub(crate) fn redrives(&self, shard_group_id: ShardGroupId) -> Vec<DataTransportCommand> {
        let mut cmds = Vec::new();
        for (segment_key, repair) in &self.repairs {
            for member in &repair.pending {
                cmds.push(DataTransportCommand::send_to_targets(
                    vec![member.clone()],
                    CatchUpAssignment {
                        segment_key: *segment_key,
                        shard_group_id,
                        start_entry_id: repair.start_entry_id,
                        sealed_end_entry_id: repair.sealed_end,
                        replica_set: repair.replica_set.clone(),
                    },
                ));
            }
        }
        cmds
    }

    /// Clear on step-down (leader-volatile).
    pub(crate) fn clear(&mut self) {
        self.repairs.clear();
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.repairs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
    use crate::data_plane::messages::command::DataPlaneInterNodeCommand;

    fn node(id: &str) -> NodeId {
        NodeId::new(id)
    }
    fn seg() -> SegmentKey {
        SegmentKey::new(TopicId(0), RangeId(0), SegmentId(0))
    }
    fn reassigned(members: &[&str], sealed_end: Option<u64>) -> SegmentReassigned {
        SegmentReassigned {
            segment_key: seg(),
            start_entry_id: 0,
            sealed_end,
            new_replica_set: members.iter().map(|n| node(n)).collect(),
        }
    }
    /// Sorted targets of the re-drive commands.
    fn redrive_targets(repairs: &CatchUpRepairs) -> Vec<NodeId> {
        let mut targets: Vec<NodeId> = repairs
            .redrives(ShardGroupId(1))
            .into_iter()
            .filter_map(|cmd| {
                if let DataTransportCommand::SendToTargets(s) = cmd {
                    Some(s.targets[0].clone())
                } else {
                    None
                }
            })
            .collect();
        targets.sort();
        targets
    }

    #[test]
    fn track_skips_unknown_end() {
        let mut r = CatchUpRepairs::default();
        r.track(&reassigned(&["a", "b"], None));
        assert!(r.is_empty());
    }

    #[test]
    fn redrives_one_assignment_per_member_with_bounds() {
        let mut r = CatchUpRepairs::default();
        r.track(&reassigned(&["a", "b", "c"], Some(100)));
        assert_eq!(redrive_targets(&r), vec![node("a"), node("b"), node("c")]);

        let cmds = r.redrives(ShardGroupId(7));
        let DataTransportCommand::SendToTargets(s) = &cmds[0] else {
            panic!("expected SendToTargets");
        };
        let DataPlaneInterNodeCommand::CatchUpAssignment(a) = &s.message else {
            panic!("expected CatchUpAssignment");
        };
        assert_eq!(a.shard_group_id, ShardGroupId(7));
        assert_eq!(a.sealed_end_entry_id, 100);
        assert_eq!(a.replica_set.len(), 3);
    }

    #[test]
    fn confirm_drops_member_then_prunes() {
        let mut r = CatchUpRepairs::default();
        r.track(&reassigned(&["a", "b"], Some(5)));
        r.confirm(seg(), node("a"));
        assert_eq!(redrive_targets(&r), vec![node("b")]);
        r.confirm(seg(), node("b"));
        assert!(r.is_empty());
    }

    #[test]
    fn track_overwrites_with_a_newer_set() {
        let mut r = CatchUpRepairs::default();
        r.track(&reassigned(&["a", "b"], Some(5)));
        r.confirm(seg(), node("a"));
        // Re-reassignment to a fresh set resets confirmations.
        r.track(&reassigned(&["x", "y"], Some(5)));
        assert_eq!(redrive_targets(&r), vec![node("x"), node("y")]);
    }

    #[test]
    fn clear_drops_all() {
        let mut r = CatchUpRepairs::default();
        r.track(&reassigned(&["a"], Some(5)));
        r.clear();
        assert!(r.is_empty());
    }
}
