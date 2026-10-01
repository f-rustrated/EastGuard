use std::collections::HashMap;
use std::time::Duration;

use crate::control_plane::consensus::raft::states::security::AclRecord;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::AclResource;
use crate::security::{CertificatePrincipal, MAX_SECURITY_ID_BYTES};

#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;

use super::acl_messages::{
    AclEvent, AclFetchCompleted, AclRecordKey, AuthorizationRequestId, AuthorizationResolved,
};

pub(super) const CACHE_TTL: Duration = Duration::from_secs(60);
const MAX_WAITERS_PER_FETCH: usize = 256;
const MAX_CACHE_ENTRIES: usize = 4_096;
const MAX_ACL_CACHE_BYTES: usize = 16 * 1024 * 1024;
const MAX_ACL_PRINCIPALS: usize = 4_096;
const BOXED_STRING_ALLOCATION_OVERHEAD: usize = 16;
// Leaves ample room for Borsh string lengths and the cluster response envelope
// beneath the 8 MiB transport frame ceiling.
const MAX_ACL_CACHE_ENTRY_BYTES: usize = 4 * 1024 * 1024;

/// Pure authorization decisions, cache lifetime, and request coalescing.
/// Takes snapshots and elapsed time as inputs; never touches sockets or channels.
#[derive(Default)]
pub(super) struct AclState {
    entries: HashMap<AclResource, AclCacheEntry>,
    size_bytes: usize,
    waiters: HashMap<AclRecordKey, Vec<(AuthorizationRequestId, CertificatePrincipal)>>,
    pending_events: Vec<AclEvent>,
}

#[derive(Debug)]
struct AclCacheEntry {
    source_shard_id: ShardGroupId,
    revision: u64,
    principals: Box<[Box<str>]>,
    expires_at: Duration,
    size_bytes: usize,
}

impl AclState {
    pub(super) fn authorize(
        &mut self,
        key: AclRecordKey,
        principal: CertificatePrincipal,
        request_id: AuthorizationRequestId,
        now: Duration,
    ) {
        let mut authorized = if !principal.has_valid_length()
            || !key
                .resource
                .has_valid_identifier_length(MAX_SECURITY_ID_BYTES)
        {
            Some(false)
        } else {
            self.cached_authorization(&key, &principal, now)
        };
        if authorized.is_none() {
            let waiters = self.waiters.entry(key.clone()).or_default();
            if waiters.len() < MAX_WAITERS_PER_FETCH {
                waiters.push((request_id, principal));
                if waiters.len() == 1 {
                    self.pending_events.push(key.into());
                }
            } else {
                authorized = Some(false);
            }
        }
        if let Some(authorized) = authorized {
            self.pending_events.push(
                AuthorizationResolved {
                    request_id,
                    authorized,
                }
                .into(),
            );
        }
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    pub(super) fn complete_acl(&mut self, completed: AclFetchCompleted, now: Duration) {
        if let Ok(snapshot) = completed.result
            && snapshot.resource == completed.key.resource
        {
            self.insert(&completed.key, snapshot, completed.requested_at);
        }
        if let Some(waiters) = self.waiters.remove(&completed.key) {
            for (request_id, principal) in waiters {
                let authorized = self
                    .cached_authorization(&completed.key, &principal, now)
                    .unwrap_or(false);
                self.pending_events.push(
                    AuthorizationResolved {
                        request_id,
                        authorized,
                    }
                    .into(),
                );
            }
        }
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
    }

    pub(super) fn take_events(&mut self) -> Vec<AclEvent> {
        let events = std::mem::take(&mut self.pending_events);
        #[cfg(any(test, debug_assertions))]
        self.assert_invariants();
        events
    }

    fn cached_authorization(
        &mut self,
        key: &AclRecordKey,
        principal: &CertificatePrincipal,
        now: Duration,
    ) -> Option<bool> {
        let current = self.entries.get(&key.resource)?;
        if current.source_shard_id != key.shard_group_id || current.expires_at <= now {
            self.remove(&key.resource);
            return None;
        }
        Some(
            current
                .principals
                .binary_search_by(|candidate| candidate.as_ref().cmp(principal.as_ref()))
                .is_ok(),
        )
    }

    fn insert(&mut self, key: &AclRecordKey, snapshot: AclRecord, requested_at: Duration) -> bool {
        if !key
            .resource
            .has_valid_identifier_length(MAX_SECURITY_ID_BYTES)
        {
            return false;
        }
        if let Some(entry) = self.entries.get(&key.resource)
            && entry.source_shard_id == key.shard_group_id
            && entry.revision > snapshot.revision
        {
            return false;
        }
        if snapshot.principals.len() > MAX_ACL_PRINCIPALS
            || snapshot
                .principals
                .iter()
                .any(|principal| principal.is_empty() || principal.len() > MAX_SECURITY_ID_BYTES)
        {
            return false;
        }

        let mut principals: Vec<Box<str>> = snapshot
            .principals
            .into_iter()
            .map(String::into_boxed_str)
            .collect();
        principals.sort_unstable();
        principals.dedup();
        let size_bytes = Self::entry_size(&key.resource, &principals);
        if size_bytes > MAX_ACL_CACHE_ENTRY_BYTES {
            return false;
        }
        self.remove_expired(requested_at);
        self.remove(&key.resource);
        while self.entries.len() >= MAX_CACHE_ENTRIES
            || self.size_bytes.saturating_add(size_bytes) > MAX_ACL_CACHE_BYTES
        {
            let Some(victim) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.expires_at)
                .map(|(resource, _)| resource.clone())
            else {
                return false;
            };
            self.remove(&victim);
        }

        self.size_bytes += size_bytes;
        self.entries.insert(
            key.resource.clone(),
            AclCacheEntry {
                source_shard_id: key.shard_group_id,
                revision: snapshot.revision,
                principals: principals.into_boxed_slice(),
                expires_at: requested_at + CACHE_TTL,
                size_bytes,
            },
        );
        true
    }

    fn entry_size(resource: &AclResource, principals: &[Box<str>]) -> usize {
        std::mem::size_of::<AclCacheEntry>()
            + resource.to_string().len()
            + principals
                .iter()
                .map(|principal| {
                    std::mem::size_of::<Box<str>>()
                        + BOXED_STRING_ALLOCATION_OVERHEAD
                        + principal.len()
                })
                .sum::<usize>()
    }

    fn remove_expired(&mut self, now: Duration) {
        let size_bytes = &mut self.size_bytes;
        self.entries.retain(|_, entry| {
            let keep = entry.expires_at > now;
            if !keep {
                *size_bytes = size_bytes.saturating_sub(entry.size_bytes);
            }
            keep
        });
    }

    fn remove(&mut self, resource: &AclResource) {
        if let Some(entry) = self.entries.remove(resource) {
            self.size_bytes = self.size_bytes.saturating_sub(entry.size_bytes);
        }
    }
}

#[cfg(any(test, debug_assertions))]
impl TAssertInvariant for AclState {
    fn assert_invariants(&self) {
        assert!(self.entries.len() <= MAX_CACHE_ENTRIES);
        assert!(self.size_bytes <= MAX_ACL_CACHE_BYTES);
        assert_eq!(
            self.size_bytes,
            self.entries
                .values()
                .map(|entry| entry.size_bytes)
                .sum::<usize>()
        );
        for (resource, entry) in &self.entries {
            assert!(resource.has_valid_identifier_length(MAX_SECURITY_ID_BYTES));
            assert!(entry.size_bytes <= MAX_ACL_CACHE_ENTRY_BYTES);
            assert!(entry.principals.len() <= MAX_ACL_PRINCIPALS);
            assert_eq!(
                entry.size_bytes,
                Self::entry_size(resource, &entry.principals)
            );
            assert!(entry.principals.iter().all(|principal| {
                !principal.is_empty() && principal.len() <= MAX_SECURITY_ID_BYTES
            }));
            assert!(entry.principals.windows(2).all(|pair| pair[0] < pair[1]));
        }

        let mut request_ids = std::collections::HashSet::new();
        for (key, waiters) in &self.waiters {
            assert!(
                key.resource
                    .has_valid_identifier_length(MAX_SECURITY_ID_BYTES)
            );
            assert!(!waiters.is_empty());
            assert!(waiters.len() <= MAX_WAITERS_PER_FETCH);
            assert!(waiters.iter().all(|(request_id, principal)| {
                principal.has_valid_length() && request_ids.insert(*request_id)
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::metadata::{AclResource, ConsumerGroupResource, TopicId};

    fn acl_key(shard_group_id: u64, topic_id: u64) -> AclRecordKey {
        AclRecordKey {
            shard_group_id: ShardGroupId(shard_group_id),
            resource: AclResource::TopicData(TopicId(topic_id)),
        }
    }

    impl AclState {
        fn authorization_request(
            &mut self,
            request_id: u64,
            key: AclRecordKey,
            principal: &str,
            now: Duration,
        ) {
            self.authorize(
                key,
                CertificatePrincipal::new(principal),
                AuthorizationRequestId(request_id),
                now,
            );
        }
    }

    fn fetch_count(events: &[AclEvent]) -> usize {
        events
            .iter()
            .filter(|event| matches!(event, AclEvent::AclFetchRequested(_)))
            .count()
    }

    fn authorization_results(events: Vec<AclEvent>) -> HashMap<AuthorizationRequestId, bool> {
        events
            .into_iter()
            .filter_map(|event| match event {
                AclEvent::AuthorizationResolved(resolved) => {
                    Some((resolved.request_id, resolved.authorized))
                }
                AclEvent::AclFetchRequested(_) => None,
            })
            .collect()
    }

    impl AclRecordKey {
        fn record(&self, revision: u64, principals: &[&str]) -> AclRecord {
            AclRecord {
                resource: self.resource.clone(),
                revision,
                principals: principals
                    .iter()
                    .map(|principal| (*principal).to_owned())
                    .collect(),
            }
        }
    }

    #[test]
    fn invalid_authorization_requests_are_denied_without_a_fetch() {
        for invalid in [String::new(), "x".repeat(MAX_SECURITY_ID_BYTES + 1)] {
            let mut key = acl_key(42, 7);
            key.resource = AclResource::ConsumerGroup(ConsumerGroupResource {
                topic_id: TopicId(7),
                group_id: invalid.clone(),
            });
            for (request, principal) in [(acl_key(42, 7), invalid.as_str()), (key, "orders")] {
                let mut state = AclState::default();
                state.authorization_request(1, request, principal, Duration::ZERO);
                let events = state.take_events();
                assert_eq!(events.len(), 1);
                assert!(!authorization_results(events)[&AuthorizationRequestId(1)]);
            }
        }
    }

    #[test]
    fn concurrent_acl_checks_share_one_fetch_and_cache_its_result() {
        let mut state = AclState::default();
        let key = acl_key(42, 7);
        state.authorization_request(1, key.clone(), "orders", Duration::ZERO);
        state.authorization_request(2, key.clone(), "payments", Duration::ZERO);
        assert_eq!(fetch_count(&state.take_events()), 1);

        state.complete_acl(
            AclFetchCompleted {
                key: key.clone(),
                result: Ok(key.record(1, &["orders"])),
                requested_at: Duration::ZERO,
            },
            Duration::ZERO,
        );
        let results = authorization_results(state.take_events());
        assert_eq!(results.get(&AuthorizationRequestId(1)), Some(&true));
        assert_eq!(results.get(&AuthorizationRequestId(2)), Some(&false));

        state.authorization_request(3, key, "orders", Duration::from_secs(1));
        let events = state.take_events();
        assert_eq!(fetch_count(&events), 0);
        assert_eq!(
            authorization_results(events).get(&AuthorizationRequestId(3)),
            Some(&true)
        );
    }

    #[test]
    fn acl_failure_denies_without_populating_the_cache() {
        let mut state = AclState::default();
        let key = acl_key(42, 7);
        state.authorization_request(1, key.clone(), "orders", Duration::ZERO);
        state.take_events();
        state.complete_acl(
            AclFetchCompleted {
                key,
                result: Err(anyhow::anyhow!("ACL read failed")),
                requested_at: Duration::ZERO,
            },
            Duration::ZERO,
        );
        assert_eq!(
            authorization_results(state.take_events()).get(&AuthorizationRequestId(1)),
            Some(&false)
        );

        state.authorization_request(2, acl_key(42, 7), "orders", Duration::ZERO);
        assert_eq!(fetch_count(&state.take_events()), 1);
    }

    #[test]
    fn empty_acl_is_a_cacheable_denial() {
        let mut state = AclState::default();
        let key = acl_key(42, 7);
        state.authorization_request(1, key.clone(), "orders", Duration::ZERO);
        state.take_events();
        state.complete_acl(
            AclFetchCompleted {
                key: key.clone(),
                result: Ok(key.record(0, &[])),
                requested_at: Duration::ZERO,
            },
            Duration::ZERO,
        );
        assert_eq!(
            authorization_results(state.take_events()).get(&AuthorizationRequestId(1)),
            Some(&false)
        );

        state.authorization_request(2, key, "orders", Duration::from_secs(1));
        let events = state.take_events();
        assert_eq!(fetch_count(&events), 0);
        assert_eq!(
            authorization_results(events).get(&AuthorizationRequestId(2)),
            Some(&false)
        );
    }

    #[test]
    fn source_shard_change_invalidates_acl_and_prevents_resurrection() {
        let mut state = AclState::default();
        let shard_a = acl_key(1, 7);
        let shard_b = acl_key(2, 7);
        state.authorization_request(1, shard_a.clone(), "orders", Duration::ZERO);
        state.take_events();
        state.complete_acl(
            AclFetchCompleted {
                key: shard_a.clone(),
                result: Ok(shard_a.record(1, &["orders"])),
                requested_at: Duration::ZERO,
            },
            Duration::ZERO,
        );
        assert_eq!(
            authorization_results(state.take_events()).get(&AuthorizationRequestId(1)),
            Some(&true)
        );

        state.authorization_request(2, shard_b.clone(), "orders", Duration::from_secs(1));
        assert_eq!(fetch_count(&state.take_events()), 1);
        state.complete_acl(
            AclFetchCompleted {
                key: shard_b,
                result: Err(anyhow::anyhow!("ACL read failed")),
                requested_at: Duration::from_secs(1),
            },
            Duration::from_secs(1),
        );

        state.take_events();
        state.authorization_request(3, shard_a, "orders", Duration::from_secs(2));
        assert_eq!(fetch_count(&state.take_events()), 1);
    }

    #[test]
    fn mismatched_acl_record_is_denied_and_not_cached() {
        let mut state = AclState::default();
        let requested = acl_key(42, 7);
        let other = acl_key(42, 8);
        state.authorization_request(1, requested.clone(), "orders", Duration::ZERO);
        state.take_events();
        state.complete_acl(
            AclFetchCompleted {
                key: requested,
                result: Ok(other.record(1, &["orders"])),
                requested_at: Duration::ZERO,
            },
            Duration::ZERO,
        );
        assert_eq!(
            authorization_results(state.take_events()).get(&AuthorizationRequestId(1)),
            Some(&false)
        );

        state.authorization_request(2, acl_key(42, 7), "orders", Duration::ZERO);
        assert_eq!(fetch_count(&state.take_events()), 1);
    }

    #[test]
    fn older_acl_revision_cannot_replace_or_renew_a_newer_record() {
        let mut cache = AclState::default();
        let key = acl_key(42, 7);
        assert!(cache.insert(&key, key.record(3, &["orders"]), Duration::ZERO));
        assert!(!cache.insert(&key, key.record(2, &["payments"]), Duration::from_secs(1),));
        assert_eq!(
            cache.cached_authorization(
                &key,
                &CertificatePrincipal::new("orders"),
                CACHE_TTL - Duration::from_nanos(1),
            ),
            Some(true)
        );
        assert_eq!(
            cache.cached_authorization(&key, &CertificatePrincipal::new("orders"), CACHE_TTL,),
            None
        );
    }

    #[test]
    fn acl_deadline_starts_at_read_start_and_expires_exactly() {
        let key = acl_key(42, 7);
        let principal = CertificatePrincipal::new("orders");
        let mut cache = AclState::default();
        assert!(cache.insert(&key, key.record(1, &["orders"]), Duration::ZERO));
        assert_eq!(
            cache.cached_authorization(&key, &principal, CACHE_TTL - Duration::from_nanos(1)),
            Some(true)
        );
        assert_eq!(
            cache.cached_authorization(&key, &principal, CACHE_TTL),
            None
        );

        let mut state = AclState::default();
        state.authorization_request(1, key.clone(), "orders", Duration::ZERO);
        state.take_events();
        state.complete_acl(
            AclFetchCompleted {
                key: key.clone(),
                result: Ok(key.record(1, &["orders"])),
                requested_at: Duration::ZERO,
            },
            CACHE_TTL,
        );
        assert!(!authorization_results(state.take_events())[&AuthorizationRequestId(1)]);
    }

    #[test]
    fn acl_waiter_limit_denies_only_overflow() {
        let mut state = AclState::default();
        let key = acl_key(42, 7);
        for id in 0..=MAX_WAITERS_PER_FETCH as u64 {
            state.authorization_request(id, key.clone(), "orders", Duration::ZERO);
        }
        let events = state.take_events();
        assert_eq!(fetch_count(&events), 1);
        assert!(
            !authorization_results(events)[&AuthorizationRequestId(MAX_WAITERS_PER_FETCH as u64)]
        );
        state.complete_acl(
            AclFetchCompleted {
                key: key.clone(),
                result: Ok(key.record(1, &["orders"])),
                requested_at: Duration::ZERO,
            },
            Duration::ZERO,
        );
        let results = authorization_results(state.take_events());
        assert_eq!(results.len(), MAX_WAITERS_PER_FETCH);
        assert!(results.values().all(|authorized| *authorized));
    }

    #[test]
    fn acl_cache_cardinality_is_bounded() {
        let mut cache = AclState::default();
        for topic_id in 0..=MAX_CACHE_ENTRIES as u64 {
            let key = acl_key(42, topic_id);
            assert!(cache.insert(&key, key.record(0, &[]), Duration::ZERO));
        }

        assert_eq!(cache.entries.len(), MAX_CACHE_ENTRIES);
        assert!(cache.size_bytes <= MAX_ACL_CACHE_BYTES);
    }

    #[test]
    fn oversized_acl_record_is_not_cacheable() {
        let mut cache = AclState::default();
        let key = acl_key(42, 7);
        let oversized = "x".repeat(MAX_ACL_CACHE_ENTRY_BYTES + 1);

        assert!(!cache.insert(
            &key,
            AclRecord {
                resource: key.resource.clone(),
                revision: 1,
                principals: vec![oversized].into_boxed_slice(),
            },
            Duration::ZERO,
        ));
        assert!(cache.entries.is_empty());
    }

    #[test]
    fn acl_with_too_many_short_principals_is_not_cacheable() {
        let mut cache = AclState::default();
        let key = acl_key(42, 7);
        let principals = (0..=MAX_ACL_PRINCIPALS)
            .map(|index| index.to_string())
            .collect::<Vec<_>>()
            .into_boxed_slice();

        assert!(!cache.insert(
            &key,
            AclRecord {
                resource: key.resource.clone(),
                revision: 1,
                principals,
            },
            Duration::ZERO,
        ));
        assert!(cache.entries.is_empty());
    }

    #[test]
    fn acl_with_invalid_principal_is_not_cacheable() {
        let key = acl_key(42, 7);
        for principal in [String::new(), "x".repeat(MAX_SECURITY_ID_BYTES + 1)] {
            let mut cache = AclState::default();
            assert!(!cache.insert(
                &key,
                AclRecord {
                    resource: key.resource.clone(),
                    revision: 1,
                    principals: vec![principal].into_boxed_slice(),
                },
                Duration::ZERO,
            ));
            assert!(cache.entries.is_empty());
        }
    }
}
