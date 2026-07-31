use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use crate::control_plane::consensus::{
    raft::states::security::AdmissionRecord,
    transport::admission::{
        AdmissionLookupCompleted, AdmissionLookupReply, AdmissionQuery, AdmissionTarget,
        message::AdmissionLookupUnavailable,
    },
    transport::protocol::AdmissionRecordKey,
};

const MAX_ADMISSION_CACHE_TTL: Duration = Duration::from_secs(60);
pub(super) const MAX_IN_FLIGHT_LOOKUPS: usize = 16;
const MAX_QUEUED_LOOKUPS: usize = 128;
const MAX_WAITERS_PER_LOOKUP: usize = 256;

/// Pure scheduling and cache state for admission lookups.
#[derive(Default)]
pub(super) struct AdmissionLookupState {
    cache: AdmissionCache,
    active: HashSet<AdmissionRecordKey>,
    queued: VecDeque<AdmissionTarget>,
    waiters: HashMap<AdmissionRecordKey, Vec<AdmissionLookupReply>>,
    pending_events: Vec<AdmissionTarget>,
}

impl AdmissionLookupState {
    pub(super) fn lookup(&mut self, request: AdmissionQuery, now: Duration) {
        let AdmissionQuery { fetch, reply } = request;
        let key = fetch.key.clone();

        if let Some(admission) = self.cache.fresh_admission(&key, now) {
            let _ = reply.send(Ok(admission.clone()));
            return;
        }
        if let Some(waiting) = self.waiters.get_mut(&key) {
            if waiting.len() == MAX_WAITERS_PER_LOOKUP {
                tracing::debug!(?key, "admission lookup has too many waiting callers");
                let _ = reply.send(Err(AdmissionLookupUnavailable));
                return;
            }
            waiting.push(reply);
            return;
        }
        if self.active.len() < MAX_IN_FLIGHT_LOOKUPS {
            self.active.insert(key.clone());
            self.waiters.insert(key, vec![reply]);
            self.pending_events.push(fetch);
            return;
        }
        if self.queued.len() < MAX_QUEUED_LOOKUPS {
            self.waiters.insert(key, vec![reply]);
            self.queued.push_back(fetch);
            return;
        }
        tracing::debug!(?key, "admission lookup queue is full");
        let _ = reply.send(Err(AdmissionLookupUnavailable));
    }

    pub(super) fn complete(&mut self, completed: AdmissionLookupCompleted, now: Duration) {
        debug_assert!(self.active.remove(&completed.key));
        if let Ok(admission) = &completed.result {
            self.cache.insert(&completed.key, admission.clone(), now);
        }

        let response = match completed.result {
            Ok(_) => self
                .cache
                .fresh_admission(&completed.key, now)
                .cloned()
                .ok_or(AdmissionLookupUnavailable),
            Err(error) => Err(error),
        };
        if let Some(waiting) = self.waiters.remove(&completed.key) {
            for reply in waiting {
                let _ = reply.send(response.clone());
            }
        } else {
            tracing::debug!("admission lookup completed without waiting callers");
        }
        if let Some(next) = self.queued.pop_front() {
            self.active.insert(next.key.clone());
            self.pending_events.push(next);
        }
    }

    pub(super) fn take_pending(&mut self) -> Vec<AdmissionTarget> {
        std::mem::take(&mut self.pending_events)
    }
}

#[derive(Debug)]
struct AdmissionCacheEntry {
    source_shard_id: crate::control_plane::membership::ShardGroupId,
    admission: Option<AdmissionRecord>,
    expires_at: Duration,
}

#[derive(Debug, Default)]
struct AdmissionCache {
    entries: HashMap<Box<str>, AdmissionCacheEntry>,
}

impl AdmissionCache {
    fn fresh_admission(
        &self,
        key: &AdmissionRecordKey,
        now: Duration,
    ) -> Option<&Option<AdmissionRecord>> {
        let entry = self.entries.get(key.node_certificate_principal.as_ref())?;
        if entry.source_shard_id != key.shard_group_id || entry.expires_at <= now {
            return None;
        }
        Some(&entry.admission)
    }

    /// Keeps the highest revision observed from a shard. An older response
    /// cannot extend the authority window of a newer, possibly expired record.
    fn insert(
        &mut self,
        key: &AdmissionRecordKey,
        admission: Option<AdmissionRecord>,
        now: Duration,
    ) {
        if admission.as_ref().is_some_and(|record| {
            record.node_certificate_principal != key.node_certificate_principal.as_ref()
        }) {
            return;
        }
        let replace = match self.entries.get(key.node_certificate_principal.as_ref()) {
            Some(entry) if entry.source_shard_id == key.shard_group_id => {
                match (&entry.admission, &admission) {
                    (Some(current), Some(incoming)) => current.revision <= incoming.revision,
                    (Some(_), None) => false,
                    (None, Some(_)) | (None, None) => true,
                }
            }
            Some(_) | None => true,
        };
        if replace {
            self.entries.insert(
                key.node_certificate_principal.clone(),
                AdmissionCacheEntry {
                    source_shard_id: key.shard_group_id,
                    admission,
                    expires_at: now + MAX_ADMISSION_CACHE_TTL,
                },
            );
        }
    }
}

#[cfg(test)]
pub mod tests {
    use tokio::sync::oneshot;

    use super::*;
    use crate::control_plane::{
        NodeId, consensus::transport::admission::AdmissionLookupResult, membership::ShardGroupId,
    };

    fn admission(revision: u64) -> AdmissionRecord {
        AdmissionRecord {
            node_certificate_principal: "broker-a".to_string(),
            revision,
            epoch: revision,
            node_id: NodeId::new(format!("broker-a::process-{revision}")),
            process_public_key: vec![revision as u8].into_boxed_slice(),
        }
    }

    fn local_fetch(principal: &str) -> AdmissionTarget {
        AdmissionTarget {
            key: AdmissionRecordKey {
                shard_group_id: ShardGroupId(42),
                node_certificate_principal: principal.into(),
            },
            remote_owner: None,
        }
    }
    fn request(principal: &str) -> (AdmissionQuery, oneshot::Receiver<AdmissionLookupResult>) {
        let (reply, receiver) = oneshot::channel();
        (
            AdmissionQuery {
                fetch: local_fetch(principal),
                reply,
            },
            receiver,
        )
    }

    #[test]
    fn fresh_cache_entry_avoids_another_lookup() {
        let mut state = AdmissionLookupState::default();
        let key = local_fetch("broker-a").key.clone();
        state.cache.insert(&key, Some(admission(3)), Duration::ZERO);
        let (lookup_request, mut reply) = request("broker-a");

        state.lookup(lookup_request, Duration::from_secs(1));

        assert!(state.take_pending().is_empty());
        assert_eq!(reply.try_recv(), Ok(Ok(Some(admission(3)))));
    }

    #[test]
    fn older_revision_cannot_refresh_an_expired_admission() {
        let mut state = AdmissionLookupState::default();
        let key = local_fetch("broker-a").key.clone();
        state.cache.insert(&key, Some(admission(3)), Duration::ZERO);
        let expired = MAX_ADMISSION_CACHE_TTL + Duration::from_millis(1);
        let (lookup_request, mut reply) = request("broker-a");
        state.lookup(lookup_request, expired);
        state.take_pending();

        state.complete(
            AdmissionLookupCompleted {
                key,
                result: Ok(Some(admission(2))),
            },
            expired,
        );

        assert_eq!(reply.try_recv(), Ok(Err(AdmissionLookupUnavailable)));
    }

    #[test]
    fn combines_simultaneous_lookups_for_the_same_admission() {
        let mut state = AdmissionLookupState::default();
        let (first, mut first_reply) = request("broker-a");
        let (second, mut second_reply) = request("broker-a");

        state.lookup(first, Duration::ZERO);
        state.lookup(second, Duration::ZERO);

        let pending = state.take_pending();
        assert_eq!(pending.len(), 1);
        state.complete(
            AdmissionLookupCompleted {
                key: pending[0].key.clone(),
                result: Ok(Some(admission(3))),
            },
            Duration::ZERO,
        );
        assert_eq!(first_reply.try_recv(), Ok(Ok(Some(admission(3)))));
        assert_eq!(second_reply.try_recv(), Ok(Ok(Some(admission(3)))));
    }

    #[test]
    fn unavailable_lookup_is_not_cached() {
        let mut state = AdmissionLookupState::default();
        let (lookup_request, mut reply) = request("broker-a");
        state.lookup(lookup_request, Duration::ZERO);
        let pending = state.take_pending();
        state.complete(
            AdmissionLookupCompleted {
                key: pending[0].key.clone(),
                result: Err(AdmissionLookupUnavailable),
            },
            Duration::ZERO,
        );
        assert_eq!(reply.try_recv(), Ok(Err(AdmissionLookupUnavailable)));

        let (retry, _reply) = request("broker-a");
        state.lookup(retry, Duration::ZERO);
        assert_eq!(state.take_pending().len(), 1);
    }

    #[test]
    fn response_for_another_principal_is_not_cached() {
        let mut state = AdmissionLookupState::default();
        let (lookup_request, mut reply) = request("broker-b");
        state.lookup(lookup_request, Duration::ZERO);
        let pending = state.take_pending();

        state.complete(
            AdmissionLookupCompleted {
                key: pending[0].key.clone(),
                result: Ok(Some(admission(3))),
            },
            Duration::ZERO,
        );

        assert_eq!(reply.try_recv(), Ok(Err(AdmissionLookupUnavailable)));
    }

    #[test]
    fn rejects_a_lookup_when_active_work_and_queue_are_full() {
        let mut state = AdmissionLookupState::default();
        for index in 0..MAX_IN_FLIGHT_LOOKUPS + MAX_QUEUED_LOOKUPS {
            let principal = format!("broker-{index}");
            let (lookup_request, _reply) = request(&principal);
            state.lookup(lookup_request, Duration::ZERO);
        }

        let (rejected, mut reply) = request("broker-overflow");
        state.lookup(rejected, Duration::ZERO);

        assert_eq!(reply.try_recv(), Ok(Err(AdmissionLookupUnavailable)));
    }
}
