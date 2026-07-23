use crate::client::Client;
use crate::client::error::ClientError;
use crate::connections::protocol::{OpenProducerSessionRequest, ProducerSessionOpened};
use crate::control_plane::metadata::RangeId;
use crate::data_plane::ProducerAppendIdentity;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

const SESSION_RENEWAL_MARGIN_MS: u64 = 1_000;

pub(super) struct ClientProducerSessionManager {
    session: Mutex<ClientProducerSession>,
    range_sequences: std::sync::Mutex<RangeSequences>,
}

#[derive(Clone, Copy)]
pub(super) struct ClientProducerSession {
    producer_id: Uuid,
    nonce: Uuid,
    generation: u64,
    opened: Option<ProducerSessionOpened>,
}
impl ClientProducerSession {
    fn is_valid(&self, now: u64) -> bool {
        if let Some(opened) = self.opened
            && opened.expires_at > now.saturating_add(SESSION_RENEWAL_MARGIN_MS)
        {
            return true;
        }
        false
    }

    fn expired(&self, now: u64) -> bool {
        self.opened.is_some_and(|opened| opened.expires_at <= now)
    }

    fn provision_open_session_req(&mut self, now: u64, topic: &str) -> OpenProducerSessionRequest {
        if self.expired(now) {
            self.producer_id = Uuid::new_v4();
            self.nonce = Uuid::new_v4();
            self.generation = self.generation.wrapping_add(1);
            self.opened = None;
        }

        let (producer_id, nonce) = (self.producer_id, self.nonce);
        OpenProducerSessionRequest {
            topic_name: topic.to_string(),
            producer_id,
            session_nonce: nonce,
        }
    }
}

#[derive(Default)]
struct RangeSequences {
    sequences: HashMap<(ProducerSessionKey, RangeId), Arc<Mutex<u64>>>,
    topologies: HashMap<ProducerSessionKey, ProducerTopology>,
    /// Highest session generation observed so far (prevents stale generation updates).
    latest_generation: u64,
}

/// Key uniquely identifying a producer session generation.
///
/// Generation increments on session rotation (e.g. lease expiration),
/// ensuring new session sequence counters start at 0 without colliding with older sessions.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct ProducerSessionKey {
    producer_id: Uuid,
    generation: u64,
}

/// Active range topology for a producer session.
struct ProducerTopology {
    /// Since range IDs increase monotonically as ranges split over time,
    /// a higher range ID implies a newer topology snapshot, allowing out-of-order
    /// metadata updates to be ignored.
    highest_range_id: RangeId,
    /// Currently active ranges accepting appends for the topic.
    active_ranges: HashSet<RangeId>,
}

impl ClientProducerSessionManager {
    pub(super) fn new(producer_id: Uuid) -> Self {
        Self {
            session: Mutex::new(ClientProducerSession {
                producer_id,
                nonce: Uuid::new_v4(),
                generation: 0,
                opened: None,
            }),
            range_sequences: std::sync::Mutex::new(RangeSequences::default()),
        }
    }

    pub(super) async fn ensure(
        &self,
        client: &Client,
        topic: &str,
    ) -> Result<ClientProducerSession, ClientError> {
        let mut current = self.session.lock().await;
        let now = crate::now_ms();

        if current.is_valid(now) {
            return Ok(*current);
        }
        let req = current.provision_open_session_req(now, topic);
        let opened = client.open_producer_session(req).await?;

        current.opened = Some(opened);
        Ok(*current)
    }

    pub(super) fn observe_topology(
        &self,
        session: ClientProducerSession,
        active_ranges: impl Iterator<Item = RangeId>,
    ) {
        self.range_sequences
            .lock()
            .expect("producer sequence registry poisoned")
            .observe_topology(session.key(), active_ranges.collect());
    }

    pub(super) fn sequence_for(
        &self,
        session: ClientProducerSession,
        range_id: RangeId,
    ) -> Arc<Mutex<u64>> {
        self.range_sequences
            .lock()
            .expect("producer sequence registry poisoned")
            .sequence_for(session.key(), range_id)
    }

    pub(super) async fn mark_expired(&self, session: ClientProducerSession) {
        let mut current = self.session.lock().await;
        if current.key() == session.key()
            && let Some(opened) = current.opened.as_mut()
        {
            opened.expires_at = 0;
        }
    }
}

impl ClientProducerSession {
    fn key(self) -> ProducerSessionKey {
        ProducerSessionKey {
            producer_id: self.producer_id,
            generation: self.generation,
        }
    }

    pub(super) fn append_identity(self, sequence: u64, digest: u32) -> ProducerAppendIdentity {
        let opened = self
            .opened
            .expect("ensured producer session must be opened");
        ProducerAppendIdentity {
            producer_id: self.producer_id,
            incarnation: opened.incarnation,
            expires_at: opened.expires_at,
            sequence,
            digest,
        }
    }
}

impl RangeSequences {
    /// Update active range topology for a producer session generation and garbage collect retired sequences.
    fn observe_topology(&mut self, session: ProducerSessionKey, active_ranges: HashSet<RangeId>) {
        // Ignore stale topology updates from an older session generation
        if session.generation < self.latest_generation {
            return;
        }
        self.latest_generation = session.generation;

        let Some(highest_range_id) = active_ranges.iter().max().copied() else {
            return;
        };

        // Monotonically advance topology: only update if high_watermark >= current high_watermark
        match self.topologies.get_mut(&session) {
            Some(topology) if highest_range_id >= topology.highest_range_id => {
                topology.highest_range_id = highest_range_id;
                topology.active_ranges = active_ranges;
            }
            Some(_) => {} // Ignore out-of-order stale topology observation
            None => {
                self.topologies.insert(
                    session,
                    ProducerTopology {
                        highest_range_id,
                        active_ranges,
                    },
                );
            }
        }

        // Garbage collection: Keep sequence counters if:
        // 1. The range is active in the current session's topology, OR
        // 2. An in-flight flush task still holds a reference handle to it (Arc::strong_count > 1).
        self.sequences.retain(|(key, range_id), sequence| {
            (*key == session && self.topologies[&session].active_ranges.contains(range_id))
                || Arc::strong_count(sequence) > 1
        });

        // Prune old topologies that no longer have active sequences and aren't the current session
        self.topologies.retain(|key, _| {
            *key == session
                || self
                    .sequences
                    .keys()
                    .any(|(sequence_key, _)| sequence_key == key)
        });
    }

    /// Retrieve or create the sequence counter handle for a (session, range_id) pair.
    fn sequence_for(&mut self, session: ProducerSessionKey, range_id: RangeId) -> Arc<Mutex<u64>> {
        self.sequences
            .entry((session, range_id))
            .or_insert_with(|| Arc::new(Mutex::new(0)))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranges(ids: &[u64]) -> HashSet<RangeId> {
        ids.iter().copied().map(RangeId).collect()
    }

    fn session(generation: u64) -> ProducerSessionKey {
        ProducerSessionKey {
            producer_id: Uuid::new_v4(),
            generation,
        }
    }

    #[test]
    fn stale_topology_does_not_prune_newer_range_sequences() {
        let mut sequences = RangeSequences::default();
        let session = session(0);
        sequences.observe_topology(session, ranges(&[0]));
        sequences.observe_topology(session, ranges(&[1, 2]));
        sequences.sequence_for(session, RangeId(1));

        sequences.observe_topology(session, ranges(&[0]));

        assert!(sequences.sequences.contains_key(&(session, RangeId(1))));
        assert_eq!(sequences.topologies[&session].highest_range_id, RangeId(2));
    }

    #[test]
    fn retired_range_sequence_is_kept_while_a_flush_owns_it() {
        let mut sequences = RangeSequences::default();
        let session = session(0);
        sequences.observe_topology(session, ranges(&[0]));
        let in_flight = sequences.sequence_for(session, RangeId(0));

        sequences.observe_topology(session, ranges(&[1, 2]));
        assert!(sequences.sequences.contains_key(&(session, RangeId(0))));

        drop(in_flight);
        sequences.observe_topology(session, ranges(&[1, 2]));
        assert!(!sequences.sequences.contains_key(&(session, RangeId(0))));
    }

    #[test]
    fn rotated_identity_never_reuses_an_old_in_flight_counter() {
        let mut sequences = RangeSequences::default();
        let old_session = session(0);
        let new_session = session(1);
        sequences.observe_topology(old_session, ranges(&[0]));
        let old_in_flight = sequences.sequence_for(old_session, RangeId(0));

        sequences.observe_topology(new_session, ranges(&[0]));
        let new_sequence = sequences.sequence_for(new_session, RangeId(0));

        assert!(!Arc::ptr_eq(&old_in_flight, &new_sequence));
    }

    #[test]
    fn old_session_cleanup_cannot_prune_new_session_sequences() {
        let mut sequences = RangeSequences::default();
        let old_session = session(0);
        let new_session = session(1);
        sequences.observe_topology(new_session, ranges(&[1]));
        sequences.sequence_for(new_session, RangeId(1));

        sequences.observe_topology(old_session, ranges(&[0]));

        assert!(sequences.sequences.contains_key(&(new_session, RangeId(1))));
    }

    #[test]
    fn failed_expired_session_open_retries_the_same_identity() {
        let mut session = ClientProducerSession {
            producer_id: Uuid::new_v4(),
            nonce: Uuid::new_v4(),
            generation: 0,
            opened: Some(ProducerSessionOpened {
                incarnation: 0,
                expires_at: 10,
            }),
        };

        let first = session.provision_open_session_req(11, "topic");
        let retry = session.provision_open_session_req(12, "topic");

        assert_eq!(first.producer_id, retry.producer_id);
        assert_eq!(first.session_nonce, retry.session_nonce);
        assert_eq!(session.generation, 1);
        assert!(session.opened.is_none());
    }
}
