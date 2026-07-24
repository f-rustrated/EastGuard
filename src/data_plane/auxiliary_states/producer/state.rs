use std::collections::{HashMap, HashSet, VecDeque};

use borsh::{BorshDeserialize, BorshSerialize};
use uuid::Uuid;

use super::{AuthorizedProducerIdentity, ProduceError, ProducerAppendIdentity};
use crate::control_plane::metadata::{EntryId, RangeId, TopicId};
use crate::data_plane::SegmentKey;
use crate::data_plane::auxiliary_states::producer::types::AppendKey;
use crate::impl_new_struct_wrapper;

const RECENT_RESULT_LIMIT: usize = 16;

pub type ProducerKey = (TopicId, RangeId, Uuid);

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct ProducerDeduplicationEntry {
    sequence: u64,
    digest: u32,
    entry_id: EntryId,
}
impl ProducerDeduplicationEntry {
    fn is_exact_retry(&self, digest: u32) -> bool {
        self.digest == digest
    }
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(crate) struct ProducerSession {
    incarnation: u32,
    expires_at: u64,
    frontier: Option<u64>,
    dedup_entries: VecDeque<ProducerDeduplicationEntry>,
}

impl ProducerSession {
    pub(crate) fn new(incarnation: u32, expires_at: u64) -> Self {
        Self {
            incarnation,
            expires_at,
            frontier: None,
            dedup_entries: VecDeque::new(),
        }
    }

    /// Verify an incoming append request against this session's incarnation and sequence history.
    pub(crate) fn verify(
        &mut self,
        identity: &ProducerAppendIdentity,
        allow_session_creation: bool,
    ) -> Result<(), ProduceError> {
        if identity.incarnation < self.incarnation {
            return Err(ProduceError::ProducerFenced);
        }
        if identity.incarnation > self.incarnation {
            if !allow_session_creation {
                return Err(ProduceError::SessionNotInstalled);
            }
            // * frontier and dedup_entries must be reset rather than reserved because:
            // * dedup_entries stores entries indexed by sequence number and if old dedup were kept, sending seq=0 in new incarnation could match previous one,
            // * leading to cross session contamination
            *self = Self::new(identity.incarnation, identity.expires_at);
        }

        let Some(frontier) = self.frontier else {
            // ! If self.frontier is none, no append has been made so the first message in any session must start at 0
            if identity.sequence != 0 {
                return Err(ProduceError::SequenceGap(0));
            }
            return Ok(());
        };

        if identity.sequence <= frontier {
            // * Idempotent retry case
            let Some(entry) = self
                .dedup_entries
                .iter()
                .find(|e| e.sequence == identity.sequence)
            else {
                return Err(ProduceError::DuplicatePositionUnavailable);
            };

            if entry.is_exact_retry(identity.digest) {
                return Err(ProduceError::Duplicate(entry.entry_id));
            }

            // Conflict or payload reuse case
            return Err(ProduceError::RequestIdentityConflict);
        }

        if identity.sequence != frontier + 1 {
            return Err(ProduceError::SequenceGap(frontier + 1));
        }
        Ok(())
    }

    /// Record a committed entry for a sequence number and advance the session frontier.
    pub(crate) fn advance(&mut self, p_id: &ProducerAppendIdentity, entry_id: EntryId) {
        if p_id.incarnation < self.incarnation
            || self.frontier.is_some_and(|seq| p_id.sequence <= seq)
        {
            return;
        }
        self.incarnation = p_id.incarnation;
        self.expires_at = p_id.expires_at;
        self.frontier = Some(p_id.sequence);
        self.dedup_entries.push_back(ProducerDeduplicationEntry {
            sequence: p_id.sequence,
            digest: p_id.digest,
            entry_id,
        });
        while self.dedup_entries.len() > RECENT_RESULT_LIMIT {
            self.dedup_entries.pop_front();
        }
    }
}

/// Crash-durable producer end state. In-flight appends are intentionally not
/// part of this snapshot image.
#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
pub(crate) struct ProducerSessions(HashMap<ProducerKey, ProducerSession>);
impl_new_struct_wrapper!(ProducerSessions,HashMap<ProducerKey, ProducerSession>);

/// Transient producer append coordination. This state is deliberately absent
/// from snapshots and is rebuilt empty after recovery.
#[derive(Debug, Clone, Default)]
pub(crate) struct ProducerTracker {
    sessions: ProducerSessions,
    in_flight_appends: HashSet<AppendKey>,
    last_cleanup_ms: u64,
}

impl ProducerTracker {
    pub(crate) fn from_sessions(sessions: ProducerSessions) -> Self {
        Self {
            sessions,
            ..Self::default()
        }
    }

    pub(crate) fn sessions(&self) -> ProducerSessions {
        self.sessions.clone()
    }

    pub(crate) fn verify(
        &mut self,
        segment: SegmentKey,
        identity: AuthorizedProducerIdentity,
        received_at_ms: u64,
    ) -> Result<(), ProduceError> {
        let (producer_identity, allow_session_creation) = identity.destruct();

        if producer_identity.expires_at < received_at_ms {
            return Err(ProduceError::SessionExpired);
        }

        self.cleanup_expired_sessions(received_at_ms);
        let append_key = AppendKey::new(segment, &producer_identity);
        let producer_key = append_key.producer_key();

        if !allow_session_creation && !self.sessions.contains_key(&producer_key) {
            return Err(ProduceError::SessionNotInstalled);
        }

        let session = self.sessions.entry(producer_key).or_insert_with(|| {
            ProducerSession::new(producer_identity.incarnation, producer_identity.expires_at)
        });

        session.verify(&producer_identity, allow_session_creation)?;

        if !self.in_flight_appends.insert(append_key) {
            return Err(ProduceError::RequestInFlight);
        }
        Ok(())
    }

    pub(crate) fn advance(
        &mut self,
        segment: SegmentKey,
        p_id: ProducerAppendIdentity,
        entry_id: EntryId,
    ) {
        let append_key = AppendKey::new(segment, &p_id);
        self.unstage(&append_key);

        let session = self
            .sessions
            .entry((segment.topic_id, segment.range_id, p_id.producer_id))
            .or_insert_with(|| ProducerSession::new(p_id.incarnation, p_id.expires_at));
        session.advance(&p_id, entry_id);
    }

    pub(crate) fn unstage(&mut self, append_key: &AppendKey) {
        self.in_flight_appends.remove(append_key);
    }

    fn cleanup_expired_sessions(&mut self, now: u64) {
        const CLEANUP_INTERVAL_MS: u64 = 10_000;

        if now.saturating_sub(self.last_cleanup_ms) >= CLEANUP_INTERVAL_MS {
            self.sessions.retain(|_, state| state.expires_at >= now);
            self.last_cleanup_ms = now;
        }
    }

    #[cfg(any(test, debug_assertions))]
    pub(crate) fn assert_invariants(&self) {
        for write_key in &self.in_flight_appends {
            if let Some(state) = self.sessions.get(&write_key.producer_key()) {
                assert!(
                    write_key.incarnation >= state.incarnation,
                    "a fenced producer incarnation remains staged"
                );
            }
        }
    }
}

#[cfg(any(test, debug_assertions))]
impl crate::test_traits::TAssertInvariant for ProducerTracker {
    fn assert_invariants(&self) {
        for state in self.sessions.values() {
            assert!(
                state.dedup_entries.len() <= RECENT_RESULT_LIMIT,
                "producer result window exceeds its fixed bound"
            );
            let mut previous = None;
            for result in &state.dedup_entries {
                assert!(
                    state
                        .frontier
                        .is_some_and(|frontier| result.sequence <= frontier),
                    "producer result is ahead of its frontier"
                );
                assert!(
                    previous.is_none_or(|sequence| sequence < result.sequence),
                    "producer result window is not strictly ordered"
                );
                previous = Some(result.sequence);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::metadata::SegmentId;

    fn verify(
        coordination: &mut ProducerTracker,
        identity: AuthorizedProducerIdentity,
    ) -> Result<(), ProduceError> {
        coordination.verify(key(), identity, 1)
    }

    fn key() -> SegmentKey {
        SegmentKey::new(TopicId(1), RangeId(2), SegmentId(3))
    }

    fn request(
        producer_id: Uuid,
        incarnation: u32,
        sequence: u64,
        digest: u32,
    ) -> ProducerAppendIdentity {
        ProducerAppendIdentity {
            producer_id,
            incarnation,
            expires_at: u64::MAX,
            sequence,
            digest,
        }
    }

    fn verified(req: ProducerAppendIdentity) -> AuthorizedProducerIdentity {
        AuthorizedProducerIdentity::MetadataVerified(req)
    }

    fn existing(req: ProducerAppendIdentity) -> AuthorizedProducerIdentity {
        AuthorizedProducerIdentity::ExistingOnly(req)
    }

    #[test]
    fn committed_retry_returns_original_position_without_advancing() {
        let id = Uuid::new_v4();
        let mut coordination = ProducerTracker::default();
        let first = request(id, 0, 0, 7);

        assert_eq!(verify(&mut coordination, verified(first)), Ok(()));
        coordination.advance(key(), first, EntryId(11));
        assert_eq!(
            verify(&mut coordination, existing(first)),
            Err(ProduceError::Duplicate(EntryId(11)))
        );
        assert_eq!(
            verify(&mut coordination, existing(request(id, 0, 1, 8)),),
            Ok(())
        );
    }

    #[test]
    fn conflict_gap_in_flight_and_fencing_are_explicit() {
        let id = Uuid::new_v4();

        let mut coordination = ProducerTracker::default();
        let first = request(id, 2, 0, 7);
        assert_eq!(verify(&mut coordination, verified(first)), Ok(()));
        assert_eq!(
            verify(&mut coordination, existing(first)),
            Err(ProduceError::RequestInFlight)
        );
        coordination.advance(key(), first, EntryId(4));

        assert_eq!(
            verify(&mut coordination, existing(request(id, 2, 0, 9))),
            Err(ProduceError::RequestIdentityConflict)
        );
        assert_eq!(
            verify(&mut coordination, existing(request(id, 2, 2, 9))),
            Err(ProduceError::SequenceGap(1))
        );
        assert_eq!(
            verify(&mut coordination, verified(request(id, 3, 0, 9))),
            Ok(())
        );
        assert_eq!(
            verify(&mut coordination, existing(request(id, 2, 1, 9))),
            Err(ProduceError::ProducerFenced)
        );
    }

    #[test]
    fn result_window_is_bounded_and_frontier_still_rejects_old_retries() {
        let id = Uuid::new_v4();

        let mut coordination = ProducerTracker::default();
        for sequence in 0..=RECENT_RESULT_LIMIT as u64 {
            let item = request(id, 0, sequence, sequence as u32);
            assert_eq!(verify(&mut coordination, verified(item)), Ok(()));
            coordination.advance(key(), item, EntryId(sequence));
        }
        assert_eq!(
            verify(&mut coordination, existing(request(id, 0, 0, 0))),
            Err(ProduceError::DuplicatePositionUnavailable)
        );
    }

    #[test]
    fn unknown_session_cannot_create_frontier_without_metadata_authority() {
        let id = Uuid::new_v4();

        let mut coordination = ProducerTracker::default();
        assert_eq!(
            verify(&mut coordination, existing(request(id, 0, 0, 7))),
            Err(ProduceError::SessionNotInstalled)
        );
        assert_eq!(
            verify(&mut coordination, verified(request(id, 0, 0, 7))),
            Ok(())
        );
    }

    #[test]
    fn expired_and_aborted_requests_have_distinct_retry_behavior() {
        let id = Uuid::new_v4();

        let mut coordination = ProducerTracker::default();
        let mut expired = request(id, 0, 0, 7);
        expired.expires_at = 0;
        assert_eq!(
            verify(&mut coordination, verified(expired)),
            Err(ProduceError::SessionExpired)
        );

        let live = request(id, 0, 0, 7);
        assert_eq!(verify(&mut coordination, verified(live)), Ok(()));

        let append_key = AppendKey::new(key(), &live);
        coordination.unstage(&append_key);
        assert_eq!(
            verify(&mut coordination, existing(live)),
            Ok(()),
            "an aborted append must not remain permanently in flight"
        );
    }
}
