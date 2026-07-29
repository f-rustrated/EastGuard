use std::collections::HashMap;
use std::time::Duration;

use tokio::time::Instant;

use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::AclResource;

/// Maximum time an ACL entry may authorize without a fresh read from its
/// owning metadata shard.
pub(crate) const MAX_ACL_CACHE_TTL: Duration = Duration::from_secs(60);

/// One ACL record copied from its owning metadata shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CachedAcl {
    pub(crate) source_shard_id: ShardGroupId,
    pub(crate) revision: u64,
    pub(crate) principals: Box<[Box<str>]>,
}

/// Result of checking one principal against the local ACL cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CachedAuthorization {
    Authorized,
    Denied,
    Miss,
}

#[derive(Debug)]
struct AclCacheEntry {
    acl: CachedAcl,
    expires_at: Instant,
}

/// Short-lived, revision-aware copies of ACL records.
///
/// The cache has no authority of its own. A caller must supply the shard that
/// currently owns the ACL resource; an entry from a previous owner, an expired
/// entry, or no entry at all is a miss and must be refreshed or denied.
#[derive(Debug, Default)]
pub(crate) struct AclCache {
    entries: HashMap<AclResource, AclCacheEntry>,
}

impl AclCache {
    pub(crate) fn authorize(
        &self,
        resource: &AclResource,
        source_shard_id: ShardGroupId,
        principal: &str,
        now: Instant,
    ) -> CachedAuthorization {
        let Some(entry) = self.entries.get(resource) else {
            return CachedAuthorization::Miss;
        };
        if entry.acl.source_shard_id != source_shard_id || entry.expires_at <= now {
            return CachedAuthorization::Miss;
        }
        if entry
            .acl
            .principals
            .iter()
            .any(|candidate| candidate.as_ref() == principal)
        {
            CachedAuthorization::Authorized
        } else {
            CachedAuthorization::Denied
        }
    }

    /// Retains the newest known revision from a shard and caps its authority
    /// window at [`MAX_ACL_CACHE_TTL`]. A record from a newly assigned owner
    /// replaces the old owner's revision, because revisions are shard-local.
    pub(crate) fn insert(&mut self, resource: AclResource, acl: CachedAcl, now: Instant) {
        let replace = match self.entries.get(&resource) {
            Some(entry) if entry.acl.source_shard_id == acl.source_shard_id => {
                entry.acl.revision <= acl.revision
            }
            Some(_) | None => true,
        };
        if replace {
            self.entries.insert(
                resource,
                AclCacheEntry {
                    acl,
                    expires_at: now + MAX_ACL_CACHE_TTL,
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::metadata::TopicId;

    fn resource() -> AclResource {
        AclResource::TopicData(TopicId(7))
    }

    fn acl(source_shard_id: ShardGroupId, revision: u64, principals: &[&str]) -> CachedAcl {
        CachedAcl {
            source_shard_id,
            revision,
            principals: principals
                .iter()
                .map(|principal| (*principal).into())
                .collect(),
        }
    }

    #[test]
    fn authorizes_and_denies_exact_principals_from_a_fresh_entry() {
        let now = Instant::now();
        let mut cache = AclCache::default();
        cache.insert(
            resource(),
            acl(ShardGroupId(3), 4, &["orders-service"]),
            now,
        );

        assert_eq!(
            cache.authorize(&resource(), ShardGroupId(3), "orders-service", now),
            CachedAuthorization::Authorized
        );
        assert_eq!(
            cache.authorize(&resource(), ShardGroupId(3), "billing-service", now),
            CachedAuthorization::Denied
        );
    }

    #[test]
    fn expires_entries_after_the_bounded_ttl() {
        let now = Instant::now();
        let mut cache = AclCache::default();
        cache.insert(
            resource(),
            acl(ShardGroupId(3), 4, &["orders-service"]),
            now,
        );

        assert_eq!(
            cache.authorize(
                &resource(),
                ShardGroupId(3),
                "orders-service",
                now + MAX_ACL_CACHE_TTL + Duration::from_millis(1),
            ),
            CachedAuthorization::Miss
        );
    }

    #[test]
    fn rejects_an_entry_from_a_previous_owner_shard() {
        let now = Instant::now();
        let mut cache = AclCache::default();
        cache.insert(
            resource(),
            acl(ShardGroupId(3), 4, &["orders-service"]),
            now,
        );

        assert_eq!(
            cache.authorize(&resource(), ShardGroupId(4), "orders-service", now),
            CachedAuthorization::Miss
        );
    }

    #[test]
    fn does_not_replace_a_newer_revision_from_the_same_shard() {
        let now = Instant::now();
        let mut cache = AclCache::default();
        cache.insert(
            resource(),
            acl(ShardGroupId(3), 5, &["orders-service"]),
            now,
        );
        cache.insert(
            resource(),
            acl(ShardGroupId(3), 4, &["billing-service"]),
            now,
        );

        assert_eq!(
            cache.authorize(&resource(), ShardGroupId(3), "orders-service", now),
            CachedAuthorization::Authorized
        );
        assert_eq!(
            cache.authorize(&resource(), ShardGroupId(3), "billing-service", now),
            CachedAuthorization::Denied
        );
    }
}
