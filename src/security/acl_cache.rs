use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::connections::protocol::ServerError;
use crate::control_plane::consensus::raft::states::security::AclRecord;
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::AclResource;

/// Maximum time an ACL entry may authorize without a fresh read from its
/// owning metadata shard.
pub(crate) const MAX_ACL_CACHE_TTL: Duration = Duration::from_secs(60);

/// One ACL record copied from its owning metadata shard.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CachedAcl {
    pub(crate) source_shard_id: ShardGroupId,
    pub(crate) revision: u64,
    pub(crate) principals: Box<[Box<str>]>,
}

impl CachedAcl {
    fn from_snapshot(source_shard_id: ShardGroupId, snapshot: AclRecord) -> Self {
        Self {
            source_shard_id,
            revision: snapshot.revision,
            principals: snapshot
                .principals
                .into_iter()
                .map(String::into_boxed_str)
                .collect(),
        }
    }
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
struct AclCache {
    entries: HashMap<AclResource, AclCacheEntry>,
}

impl AclCache {
    /// Returns `None` when there is no current cache entry for this resource.
    fn authorization(
        &self,
        resource: &AclResource,
        source_shard_id: ShardGroupId,
        principal: &str,
        now: Instant,
    ) -> Option<bool> {
        let entry = self.entries.get(resource)?;
        if entry.acl.source_shard_id != source_shard_id || entry.expires_at <= now {
            return None;
        }
        Some(
            entry
                .acl
                .principals
                .iter()
                .any(|candidate| candidate.as_ref() == principal),
        )
    }

    /// Retains the newest known revision from a shard and caps its authority
    /// window at [`MAX_ACL_CACHE_TTL`]. A record from a newly assigned owner
    /// replaces the old owner's revision, because revisions are shard-local.
    fn insert(&mut self, resource: AclResource, acl: CachedAcl, now: Instant) {
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

/// Shareable cache boundary for concurrent client request handlers.
///
/// The lock protects only an in-memory map lookup or replacement.
/// No network or Raft operation is performed while it is held.
/// Use of `RwLock` over lock-free data structure is jusitified because
/// the dominant cost factor won't be write lock but raft snapshot query
#[derive(Debug, Clone, Default)]
pub(crate) struct SharedAclCache(Arc<RwLock<AclCache>>);

impl SharedAclCache {
    /// Authorizes from a fresh cache entry, or lazily refreshes the entry and
    /// checks it again. The refresh future is created only after a cache miss.
    /// A missing snapshot fails closed.
    pub(crate) async fn authorize_or_refresh<F>(
        &self,
        resource: &AclResource,
        source_shard_id: ShardGroupId,
        principal: &str,
        refresh: F,
    ) -> Result<(), ServerError>
    where
        F: AsyncFnOnce() -> Option<AclRecord>,
    {
        if let Some(authorized) = self
            .cached_authorization(resource, source_shard_id, principal)
            .await
        {
            return if authorized {
                Ok(())
            } else {
                Err(ServerError::Unauthorized)
            };
        }

        // Cache Miss Case
        let Some(snapshot) = refresh().await else {
            return Err(ServerError::Unauthorized);
        };
        self.0.write().await.insert(
            resource.clone(),
            CachedAcl::from_snapshot(source_shard_id, snapshot),
            Instant::now(),
        );
        match self
            .cached_authorization(resource, source_shard_id, principal)
            .await
        {
            Some(true) => Ok(()),
            Some(false) | None => Err(ServerError::Unauthorized),
        }
    }

    async fn cached_authorization(
        &self,
        resource: &AclResource,
        source_shard_id: ShardGroupId,
        principal: &str,
    ) -> Option<bool> {
        self.0
            .read()
            .await
            .authorization(resource, source_shard_id, principal, Instant::now())
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

    fn snapshot(resource: AclResource, revision: u64, principals: &[&str]) -> AclRecord {
        AclRecord {
            resource,
            revision,
            principals: principals
                .iter()
                .map(|principal| (*principal).to_owned())
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
            cache.authorization(&resource(), ShardGroupId(3), "orders-service", now),
            Some(true)
        );
        assert_eq!(
            cache.authorization(&resource(), ShardGroupId(3), "billing-service", now),
            Some(false)
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
            cache.authorization(
                &resource(),
                ShardGroupId(3),
                "orders-service",
                now + MAX_ACL_CACHE_TTL + Duration::from_millis(1),
            ),
            None
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
            cache.authorization(&resource(), ShardGroupId(4), "orders-service", now),
            None
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
            cache.authorization(&resource(), ShardGroupId(3), "orders-service", now),
            Some(true)
        );
        assert_eq!(
            cache.authorization(&resource(), ShardGroupId(3), "billing-service", now),
            Some(false)
        );
    }

    #[tokio::test]
    async fn refreshes_a_miss_then_reuses_the_fresh_entry() {
        let cache = SharedAclCache::default();
        let resource = resource();

        assert_eq!(
            cache
                .authorize_or_refresh(&resource, ShardGroupId(3), "orders-service", || async {
                    Some(snapshot(resource.clone(), 1, &["orders-service"]))
                })
                .await,
            Ok(())
        );
        assert_eq!(
            cache
                .authorize_or_refresh(&resource, ShardGroupId(3), "orders-service", || async {
                    panic!("fresh entry must not refresh")
                })
                .await,
            Ok(())
        );
    }

    #[tokio::test]
    async fn missing_snapshot_leaves_a_miss_for_fail_closed_handling() {
        let cache = SharedAclCache::default();

        assert_eq!(
            cache
                .authorize_or_refresh(&resource(), ShardGroupId(3), "orders-service", || async {
                    None
                },)
                .await,
            Err(ServerError::Unauthorized)
        );
    }
}
