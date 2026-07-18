//! Pending segment-roll requests from the write leader to the coordinator.
//!
//! A leader that can't replicate sends `RequestSegmentRoll` and tracks it until
//! `SegmentRollCommitted` clears it; the data plane re-sends timed-out requests. Owns the
//! dedup + timing bookkeeping; building/sending the request stays in the data
//! plane (it reads the live tracker's committed end).

use std::collections::HashMap;
use std::time::Duration;
// tokio's runtime-aware Instant: real time in prod, virtualized under turmoil so
// the retry timeout fires on the sim clock (the data-plane worker runs on a tokio
// runtime in both — see CLAUDE.md "Testing"). Falls back to real time with no
// runtime, so the sync tests below still work.
use tokio::time::Instant;

use crate::control_plane::NodeId;
use crate::control_plane::metadata::command::SegmentRollIntent;
use crate::data_plane::SegmentKey;

struct PendingSegmentRollRequest {
    sent_at: Instant,
    failed_nodes: Vec<NodeId>,
    intent: SegmentRollIntent,
}

/// Segment-roll requests awaiting `SegmentRollCommitted`, keyed by segment.
#[derive(Default)]
pub(crate) struct PendingSegmentRollRequests {
    requests: HashMap<SegmentKey, PendingSegmentRollRequest>,
}

impl PendingSegmentRollRequests {
    /// Whether a request for `key` is already in flight (the dedup guard).
    pub(crate) fn is_tracked(&self, key: &SegmentKey) -> bool {
        self.requests.contains_key(key)
    }

    /// Record a just-sent request.
    pub(crate) fn track(
        &mut self,
        key: SegmentKey,
        failed_nodes: Vec<NodeId>,
        intent: SegmentRollIntent,
        sent_at: Instant,
    ) {
        self.requests.insert(
            key,
            PendingSegmentRollRequest {
                sent_at,
                failed_nodes,
                intent,
            },
        );
    }

    /// Drop a request once its committed result lands.
    pub(crate) fn remove(&mut self, key: &SegmentKey) {
        self.requests.remove(key);
    }

    /// Timed-out requests to re-send — refreshing their
    /// clock so each fires at most once per `timeout`.
    pub(crate) fn take_due(
        &mut self,
        now: Instant,
        timeout: Duration,
    ) -> Vec<(SegmentKey, Vec<NodeId>, SegmentRollIntent)> {
        let mut due = Vec::new();
        for (key, req) in &mut self.requests {
            if now.duration_since(req.sent_at) >= timeout {
                req.sent_at = now;
                due.push((*key, req.failed_nodes.clone(), req.intent));
            }
        }
        due
    }

    #[cfg(test)]
    pub(crate) fn failed_nodes(&self, key: &SegmentKey) -> Option<&[NodeId]> {
        self.requests.get(key).map(|r| r.failed_nodes.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};

    fn seg() -> SegmentKey {
        SegmentKey::new(TopicId(0), RangeId(0), SegmentId(0))
    }

    #[test]
    fn track_then_clear() {
        let mut p = PendingSegmentRollRequests::default();
        assert!(!p.is_tracked(&seg()));
        p.track(
            seg(),
            vec![],
            SegmentRollIntent::DataPressure,
            Instant::now(),
        );
        assert!(p.is_tracked(&seg()));
        p.remove(&seg());
        assert!(!p.is_tracked(&seg()));
    }

    #[test]
    fn take_due_returns_only_timed_out_then_refreshes() {
        let mut p = PendingSegmentRollRequests::default();
        let t0 = Instant::now();
        let timeout = Duration::from_secs(5);
        p.track(
            seg(),
            vec![NodeId::new("d")],
            SegmentRollIntent::ReplicationFailure,
            t0,
        );

        assert!(p.take_due(t0, timeout).is_empty(), "not yet due");
        let due = p.take_due(t0 + timeout, timeout);
        assert_eq!(
            due,
            vec![(
                seg(),
                vec![NodeId::new("d")],
                SegmentRollIntent::ReplicationFailure,
            )]
        );
        // Refreshed → not due again at the same instant.
        assert!(p.take_due(t0 + timeout, timeout).is_empty());
    }
}
