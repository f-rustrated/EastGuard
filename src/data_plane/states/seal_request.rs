//! Pending seal requests — the leader→coordinator `SealRequest` retry tracker.
//!
//! A leader that can't replicate sends a `SealRequest` and tracks it here until a
//! `SealResponse` clears it; the data plane re-sends ones that time out. Owns the
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
use crate::data_plane::SegmentKey;

struct PendingSealRequest {
    sent_at: Instant,
    failed_nodes: Vec<NodeId>,
}

/// Seal requests awaiting a `SealResponse`, keyed by segment.
#[derive(Default)]
pub(crate) struct PendingSealRequests {
    requests: HashMap<SegmentKey, PendingSealRequest>,
}

impl PendingSealRequests {
    /// Whether a request for `key` is already in flight (the dedup guard).
    pub(crate) fn is_tracked(&self, key: &SegmentKey) -> bool {
        self.requests.contains_key(key)
    }

    /// Record a just-sent request.
    pub(crate) fn track(&mut self, key: SegmentKey, failed_nodes: Vec<NodeId>, now: Instant) {
        self.requests.insert(
            key,
            PendingSealRequest {
                sent_at: now,
                failed_nodes,
            },
        );
    }

    /// Drop a request once its `SealResponse` lands.
    pub(crate) fn clear(&mut self, key: &SegmentKey) {
        self.requests.remove(key);
    }

    /// Timed-out requests to re-send — `(key, failed_nodes)` — refreshing their
    /// clock so each fires at most once per `timeout`.
    pub(crate) fn take_due(
        &mut self,
        now: Instant,
        timeout: Duration,
    ) -> Vec<(SegmentKey, Vec<NodeId>)> {
        let mut due = Vec::new();
        for (key, req) in &mut self.requests {
            if now.duration_since(req.sent_at) >= timeout {
                req.sent_at = now;
                due.push((*key, req.failed_nodes.clone()));
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
        let mut p = PendingSealRequests::default();
        assert!(!p.is_tracked(&seg()));
        p.track(seg(), vec![], Instant::now());
        assert!(p.is_tracked(&seg()));
        p.clear(&seg());
        assert!(!p.is_tracked(&seg()));
    }

    #[test]
    fn take_due_returns_only_timed_out_then_refreshes() {
        let mut p = PendingSealRequests::default();
        let t0 = Instant::now();
        let timeout = Duration::from_secs(5);
        p.track(seg(), vec![NodeId::new("d")], t0);

        assert!(p.take_due(t0, timeout).is_empty(), "not yet due");
        let due = p.take_due(t0 + timeout, timeout);
        assert_eq!(due, vec![(seg(), vec![NodeId::new("d")])]);
        // Refreshed → not due again at the same instant.
        assert!(p.take_due(t0 + timeout, timeout).is_empty());
    }
}
