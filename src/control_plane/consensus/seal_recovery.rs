//! Leader-crash seal-end recovery — a Raft-independent tracker.
//!
//! When a segment's write leader crashes, the coordinator must recover the
//! committed end before sealing it (see `diagrams/data-plane/leader_crash_seal_boundary.md`).
//! This module owns that bookkeeping as a pure state machine: one [`SealEndGather`]
//! per leader-crashed segment, polling the survivors for their durable extents
//! and sealing at their `min`. It holds no Raft/transport/topology state — it
//! consumes the current leaderless set plus survivor reports, and emits
//! [`SealEndStep`]s for the owner (`MultiRaft`) to execute.

use std::collections::{HashMap, HashSet};

use crate::control_plane::NodeId;
use crate::control_plane::consensus::raft::state::LeaderlessSegments;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::EntryId;
use crate::data_plane::SegmentKey;

/// How many drive passes a gather waits for every survivor to report before
/// giving up and sealing with an unknown end.
pub(crate) const GATHER_ATTEMPTS: u32 = 3;

/// A side effect for the owner to carry out after driving recovery.
pub(crate) enum SealEndStep {
    /// (Re)send a boundary query to these survivors.
    Query {
        shard_group_id: ShardGroupId,
        segment_key: SegmentKey,
        targets: Vec<NodeId>,
    },
    /// Propose the recovery roll: seal `segment_key` at `end`, led by `leader`.
    /// `end = None` is the unknown-end fallback (timeout, or a survivor holds
    /// nothing).
    Seal {
        shard_group_id: ShardGroupId,
        segment_key: SegmentKey,
        end: Option<EntryId>,
        leader: Option<NodeId>,
    },
}

/// One in-flight seal-end recovery: poll the survivors for their durable
/// extents, seal at the `min` (the committed end), let the most-recent lead.
struct SealEndGather {
    shard_group_id: ShardGroupId,
    /// Survivors we await, in replica-set order.
    nodes: Vec<NodeId>,
    /// Durable extents reported so far (`None` = the survivor holds nothing).
    reports: HashMap<NodeId, Option<EntryId>>,
    /// Re-drive budget; the roll falls back to an unknown end when it hits 0.
    attempts_left: u32,
    /// True once the roll is proposed — the gather then waits (ignoring late
    /// reports) to be pruned when the seal applies, so it can't re-propose.
    proposed: bool,
}

impl SealEndGather {
    fn new(shard_group_id: ShardGroupId, nodes: Vec<NodeId>) -> Self {
        Self {
            shard_group_id,
            nodes,
            reports: HashMap::new(),
            attempts_left: GATHER_ATTEMPTS,
            proposed: false,
        }
    }

    fn awaits(&self, node: &NodeId) -> bool {
        self.nodes.contains(node)
    }

    fn record(&mut self, from: NodeId, durable_end: Option<EntryId>) {
        self.reports.insert(from, durable_end);
    }

    fn is_complete(&self) -> bool {
        self.reports.len() >= self.nodes.len()
    }

    /// Survivors that haven't reported yet — the targets to re-query.
    fn pending(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|n| !self.reports.contains_key(*n))
            .cloned()
            .collect()
    }

    /// The committed seal end: the `min` of survivors' durable extents — the
    /// highest offset present on *every* survivor, hence committed (commit
    /// requires all-replica ack). `None` if any survivor holds nothing (nothing
    /// was committed) or none reported.
    fn recovered_end(&self) -> Option<EntryId> {
        if self.reports.is_empty() {
            return None;
        }
        let mut min = None;
        for extent in self.reports.values() {
            let extent = (*extent)?; // a survivor holding nothing ⇒ nothing committed
            min = Some(min.map_or(extent, |m: EntryId| m.min(extent)));
        }
        min
    }

    /// The most-recent survivor — highest durable extent, ties broken by lowest
    /// `NodeId` for determinism — to lead the rolled segment. `None` when no
    /// survivor reported any data.
    fn recency_leader(&self) -> Option<NodeId> {
        let mut ranked: Vec<(EntryId, &NodeId)> = self
            .reports
            .iter()
            .filter_map(|(node, extent)| extent.map(|e| (e, node)))
            .collect();
        ranked.sort_by(|(e1, n1), (e2, n2)| e2.cmp(e1).then_with(|| n1.cmp(n2)));
        ranked.first().map(|(_, node)| (*node).clone())
    }

    /// The seal step for a completed gather.
    fn seal(&self, segment_key: SegmentKey) -> SealEndStep {
        SealEndStep::Seal {
            shard_group_id: self.shard_group_id,
            segment_key,
            end: self.recovered_end(),
            leader: self.recency_leader(),
        }
    }
}

/// Tracks every in-flight seal-end recovery (one per leader-crashed segment).
/// Pure: it decides the next [`SealEndStep`]s and the owner executes them,
/// feeding back survivor reports and the current leaderless set.
#[derive(Default)]
pub(crate) struct SealEndRecovery {
    gathers: HashMap<SegmentKey, SealEndGather>,
}

impl SealEndRecovery {
    /// Start / re-query / expire gathers for `group`'s current leaderless
    /// `segments`. Returns the queries to send and rolls to propose. Callers
    /// prune stale gathers (segment no longer leaderless) *before* this — see
    /// `drop_stale_in_group` / `drop_stale_reconciled`.
    pub(crate) fn advance(
        &mut self,
        group: ShardGroupId,
        segments: LeaderlessSegments,
    ) -> Vec<SealEndStep> {
        let mut steps = Vec::with_capacity(segments.len());
        for (segment_key, survivors) in segments {
            let Some(g) = self.gathers.get_mut(&segment_key) else {
                self.gathers
                    .insert(segment_key, SealEndGather::new(group, survivors.clone()));
                steps.push(SealEndStep::Query {
                    shard_group_id: group,
                    segment_key,
                    targets: survivors,
                });
                continue;
            };

            // Roll in flight — wait for it to apply and be pruned.
            if g.proposed {
                continue;
            }

            if g.attempts_left == 0 {
                g.proposed = true;
                tracing::warn!(
                    "seal-end recovery timed out for {:?}; sealing with unknown end",
                    segment_key
                );
                steps.push(SealEndStep::Seal {
                    shard_group_id: group,
                    segment_key,
                    end: None,
                    leader: None,
                });
                continue;
            }

            g.attempts_left -= 1;
            steps.push(SealEndStep::Query {
                shard_group_id: group,
                segment_key,
                targets: g.pending(),
            });
        }
        steps
    }

    /// Record a survivor's durable extent. On the first time the full set is in,
    /// marks the gather proposed and returns the roll to propose; otherwise
    /// `None` (unknown gather, not awaited, late/duplicate, or still partial).
    pub(crate) fn record(
        &mut self,
        segment_key: SegmentKey,
        from: NodeId,
        durable_end: Option<EntryId>,
    ) -> Option<SealEndStep> {
        let g = self.gathers.get_mut(&segment_key)?;
        if g.proposed || !g.awaits(&from) {
            return None;
        }
        g.record(from, durable_end);
        if !g.is_complete() {
            return None;
        }
        g.proposed = true;
        Some(g.seal(segment_key))
    }

    /// Drop `group`'s gathers whose segment is no longer leaderless.
    pub(crate) fn drop_stale_in_group(
        &mut self,
        group: ShardGroupId,
        leaderless: &LeaderlessSegments,
    ) {
        let leaderless_keys: HashSet<SegmentKey> = leaderless.iter().map(|(k, _)| *k).collect();
        self.gathers.retain(|key, g| {
            // Other groups aren't this call's concern (we only know `group`'s
            // leaderless set) — leave them. For this group, keep a gather only
            // while its segment is still leaderless.
            g.shard_group_id != group || leaderless_keys.contains(key)
        });
    }

    /// Drop gathers whose segment is no longer leaderless, across every group in
    /// `reconciled` — a group reconciled with an empty set drops all its gathers.
    pub(crate) fn drop_stale_reconciled(
        &mut self,
        reconciled: &HashMap<ShardGroupId, LeaderlessSegments>,
    ) {
        self.gathers.retain(|key, g| {
            reconciled
                .get(&g.shard_group_id)
                .is_none_or(|ll| ll.iter().any(|(k, _)| k == key))
        });
    }

    /// Drop all of `group`'s gathers — e.g. on step-down, when we can no longer
    /// finalize them.
    pub(crate) fn drop_group(&mut self, group: ShardGroupId) {
        self.gathers.retain(|_, g| g.shard_group_id != group);
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, key: &SegmentKey) -> bool {
        self.gathers.contains_key(key)
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.gathers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::metadata::{EntryId, RangeId, SegmentId, TopicId};

    fn node(id: &str) -> NodeId {
        NodeId::new(id)
    }

    fn gather_with(reports: &[(&str, Option<u64>)]) -> SealEndGather {
        let nodes = reports.iter().map(|(n, _)| node(n)).collect();
        let mut g = SealEndGather::new(ShardGroupId(0), nodes);
        for (n, end) in reports {
            g.record(node(n), end.map(EntryId));
        }
        g
    }

    #[test]
    fn recovered_end_is_min_of_survivor_extents() {
        assert_eq!(
            gather_with(&[("y", Some(50)), ("z", Some(40))]).recovered_end(),
            Some(EntryId(40))
        );
    }

    #[test]
    fn recovered_end_is_none_when_a_survivor_holds_nothing() {
        // A survivor with nothing ⇒ nothing was committed (commit needs all acks).
        assert_eq!(
            gather_with(&[("y", Some(50)), ("z", None)]).recovered_end(),
            None
        );
        assert_eq!(gather_with(&[]).recovered_end(), None);
    }

    #[test]
    fn recency_leader_is_highest_extent_then_lowest_node() {
        assert_eq!(
            gather_with(&[("y", Some(50)), ("z", Some(40))]).recency_leader(),
            Some(node("y"))
        );
        // Tie on extent → lowest NodeId for determinism.
        assert_eq!(
            gather_with(&[("z", Some(50)), ("y", Some(50))]).recency_leader(),
            Some(node("y"))
        );
        // No survivor holds data → no leader.
        assert_eq!(gather_with(&[("y", None)]).recency_leader(), None);
    }

    #[test]
    fn advance_starts_then_expires_with_unknown_end() {
        let seg = SegmentKey::new(TopicId(0), RangeId(0), SegmentId(0));
        let mut recovery = SealEndRecovery::default();
        let segments = || vec![(seg, vec![node("y"), node("z")])];

        // First pass starts the gather and queries both survivors.
        let steps = recovery.advance(ShardGroupId(1), segments());
        assert!(matches!(
            steps.as_slice(),
            [SealEndStep::Query { targets, .. }] if targets == &[node("y"), node("z")]
        ));
        assert!(recovery.contains(&seg));

        // No reports arrive: re-queries until the budget, then seals unknown-end.
        for _ in 0..GATHER_ATTEMPTS {
            assert!(matches!(
                recovery.advance(ShardGroupId(1), segments()).as_slice(),
                [SealEndStep::Query { .. }]
            ));
        }
        assert!(matches!(
            recovery.advance(ShardGroupId(1), segments()).as_slice(),
            [SealEndStep::Seal { end: None, .. }]
        ));
    }

    #[test]
    fn record_seals_at_min_with_recency_leader_once() {
        let seg = SegmentKey::new(TopicId(0), RangeId(0), SegmentId(0));
        let mut recovery = SealEndRecovery::default();
        recovery.advance(ShardGroupId(1), vec![(seg, vec![node("y"), node("z")])]);

        assert!(recovery.record(seg, node("y"), Some(EntryId(50))).is_none()); // partial
        let step = recovery
            .record(seg, node("z"), Some(EntryId(40)))
            .expect("completes");
        assert!(matches!(
            step,
            SealEndStep::Seal { end: Some(EntryId(40)), leader: Some(l), .. } if l == node("y")
        ));
        // A late/duplicate report after the roll is proposed is ignored.
        assert!(recovery.record(seg, node("y"), Some(EntryId(99))).is_none());
    }

    #[test]
    fn drop_stale_reconciled_drops_segments_no_longer_leaderless() {
        let seg = SegmentKey::new(TopicId(0), RangeId(0), SegmentId(0));
        let mut recovery = SealEndRecovery::default();
        recovery.advance(ShardGroupId(1), vec![(seg, vec![node("y")])]);
        assert!(recovery.contains(&seg));

        // Group 1 reconciled with no leaderless segments → its gather is dropped.
        recovery.drop_stale_reconciled(&HashMap::from([(ShardGroupId(1), vec![])]));
        assert!(recovery.is_empty());
    }
}
