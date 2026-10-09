use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use serde::Deserialize;

use crate::config::{Environment, SecurityMode};
use crate::connections::protocol::ServerError;
use crate::control_plane::metadata::{AclResource, TopicId};

use super::{CertificatePrincipal, MAX_SECURITY_ID_BYTES};

const MAX_POLICY_BYTES: usize = 1024 * 1024;

/// Immutable operator policy. Reloading it requires restarting the broker.
#[derive(Clone, Default)]
pub(crate) struct Permissions {
    policy: Arc<Policy>,
    trusted_development: bool,
}

#[derive(Default)]
struct Policy {
    topics: HashMap<String, TopicId>,
    grants: HashMap<AclResource, HashSet<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFile {
    #[serde(deserialize_with = "PolicyFile::unique_map")]
    topics: HashMap<String, u64>,
    #[serde(deserialize_with = "PolicyFile::unique_map")]
    grants: HashMap<String, Box<[String]>>,
}

impl PolicyFile {
    fn unique_map<'de, D, V>(deserializer: D) -> Result<HashMap<String, V>, D::Error>
    where
        D: serde::Deserializer<'de>,
        V: Deserialize<'de>,
    {
        struct UniqueMap<V>(std::marker::PhantomData<V>);
        impl<'de, V: Deserialize<'de>> serde::de::Visitor<'de> for UniqueMap<V> {
            type Value = HashMap<String, V>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a map with unique keys")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut values = HashMap::new();
                while let Some((key, value)) = map.next_entry()? {
                    if values.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate policy key"));
                    }
                }
                Ok(values)
            }
        }
        deserializer.deserialize_map(UniqueMap(std::marker::PhantomData))
    }
}

impl Permissions {
    pub(crate) fn load(env: &Environment) -> Result<Self> {
        if env.security_mode == SecurityMode::TrustedDevelopment {
            ensure!(
                env.permissions_path.is_none(),
                "permissions require secure mode"
            );
            return Ok(Self::trusted_development());
        }
        let path = env
            .permissions_path
            .as_ref()
            .context("secure mode requires --permissions-path")?;
        let mut bytes = Vec::new();
        File::open(path)
            .context("open permissions policy")?
            .take((MAX_POLICY_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        Self::parse(&bytes)
    }

    pub(crate) fn trusted_development() -> Self {
        Self {
            trusted_development: true,
            ..Self::default()
        }
    }

    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() <= MAX_POLICY_BYTES,
            "permissions policy exceeds 1 MiB"
        );
        let file: PolicyFile =
            serde_json::from_slice(bytes).context("invalid permissions policy")?;
        let mut policy = Policy::default();
        let mut ids = HashSet::new();
        for (name, id) in file.topics {
            ensure!(
                !name.is_empty() && name.len() <= MAX_SECURITY_ID_BYTES,
                "invalid policy topic name"
            );
            ensure!(ids.insert(TopicId(id)), "duplicate policy topic ID");
            policy.topics.insert(name, TopicId(id));
        }
        for (key, principals) in file.grants {
            let resource: AclResource = key.parse().map_err(anyhow::Error::msg)?;
            ensure!(
                key == resource.to_string(),
                "noncanonical permission resource"
            );
            let topic = match &resource {
                AclResource::Cluster => None,
                AclResource::TopicAdmin(id) | AclResource::TopicData(id) => Some(id),
                AclResource::ConsumerGroup(group) => Some(&group.topic_id),
            };
            ensure!(
                topic.is_none_or(|id| ids.contains(id)),
                "permission references an unmapped topic ID"
            );
            ensure!(
                resource.has_valid_identifier_length(MAX_SECURITY_ID_BYTES),
                "invalid permission resource"
            );
            ensure!(
                principals
                    .iter()
                    .all(|p| !p.is_empty() && p.len() <= MAX_SECURITY_ID_BYTES),
                "invalid permission principal"
            );
            policy
                .grants
                .insert(resource, principals.into_vec().into_iter().collect());
        }
        Ok(Self {
            policy: Arc::new(policy),
            trusted_development: false,
        })
    }

    pub(crate) fn authorize(
        &self,
        principal: Option<&CertificatePrincipal>,
        resource: AclResource,
    ) -> Result<(), ServerError> {
        if self.trusted_development && principal.is_none() {
            return Ok(());
        }
        if let Some(principal) = principal
            && principal.has_valid_length()
            && resource.has_valid_identifier_length(MAX_SECURITY_ID_BYTES)
            && self
                .policy
                .grants
                .get(&resource)
                .is_some_and(|grants| grants.contains(principal.as_ref()))
        {
            return Ok(());
        }
        Err(ServerError::Unauthorized)
    }

    /// Names must be authorized before consulting routing or returning addresses.
    pub(crate) fn authorize_topic(
        &self,
        principal: Option<&CertificatePrincipal>,
        name: &str,
        resource: impl FnOnce(TopicId) -> AclResource,
    ) -> Result<(), ServerError> {
        if self.trusted_development && principal.is_none() {
            return Ok(());
        }
        let id = self
            .policy
            .topics
            .get(name)
            .ok_or(ServerError::Unauthorized)?;
        self.authorize(principal, resource(*id))
    }

    pub(crate) fn verify_topic(&self, name: &str, id: TopicId) -> Result<(), ServerError> {
        if self.trusted_development || self.policy.topics.get(name) == Some(&id) {
            Ok(())
        } else {
            Err(ServerError::Unauthorized)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_loading_fails_closed_and_changes_only_on_restart() {
        use clap::Parser;
        let mut env =
            Environment::try_parse_from(["eastguard", "--security-mode", "secure"]).unwrap();
        env.permissions_path = None;
        assert!(Permissions::load(&env).is_err());
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("permissions.json");
        env.permissions_path = Some(path.clone());
        assert!(Permissions::load(&env).is_err());
        std::fs::write(&path, br#"{"topics":{},"grants":{"cluster":["operator"]}}"#).unwrap();
        let before = Permissions::load(&env).unwrap();
        let principal = CertificatePrincipal::new("operator");
        std::fs::write(&path, br#"{"topics":{},"grants":{}}"#).unwrap();
        assert_eq!(
            before.authorize(Some(&principal), AclResource::Cluster),
            Ok(())
        );
        assert_eq!(
            Permissions::load(&env)
                .unwrap()
                .authorize(Some(&principal), AclResource::Cluster),
            Err(ServerError::Unauthorized)
        );
        std::fs::write(&path, b"invalid").unwrap();
        assert!(Permissions::load(&env).is_err());
        let file = File::create(&path).unwrap();
        file.set_len((MAX_POLICY_BYTES + 1) as u64).unwrap();
        assert!(Permissions::load(&env).is_err());
    }

    #[test]
    fn policy_is_bounded_exact_and_denies_missing_grants() {
        let permissions =
            Permissions::parse(br#"{"topics":{"orders":7},"grants":{"topic-data/7":["writer"]}}"#)
                .unwrap();
        let writer = CertificatePrincipal::new("writer");
        assert_eq!(
            permissions.authorize_topic(Some(&writer), "orders", AclResource::TopicData),
            Ok(())
        );
        for (principal, resource) in [
            (None, AclResource::TopicData(TopicId(7))),
            (Some(&writer), AclResource::Cluster),
            (Some(&writer), AclResource::TopicAdmin(TopicId(7))),
            (Some(&writer), AclResource::TopicData(TopicId(8))),
        ] {
            assert_eq!(
                permissions.authorize(principal, resource),
                Err(ServerError::Unauthorized)
            );
        }
        assert_eq!(
            permissions.verify_topic("orders", TopicId(8)),
            Err(ServerError::Unauthorized)
        );
        assert_eq!(
            permissions.authorize_topic(Some(&writer), "missing", AclResource::TopicData),
            Err(ServerError::Unauthorized)
        );
        for invalid in [
            br#"{}"#.as_slice(),
            br#"{"topics":{},"grants":{"cluster":["a"],"cluster":["b"]}}"#,
            br#"{"topics":{"a":1,"a":2},"grants":{}}"#,
            br#"{"topics":{},"grants":{"topic-data/7":["writer"]}}"#,
            br#"{"topics":{},"grants":{"cluster":[""]}}"#,
            br#"{"topics":{"a":7,"b":7},"grants":{}}"#,
            br#"{"topics":{},"grants":{"security/cluster":["writer"]}}"#,
        ] {
            assert!(Permissions::parse(invalid).is_err());
        }
        assert!(Permissions::parse(&vec![b' '; MAX_POLICY_BYTES + 1]).is_err());
        assert_eq!(
            Permissions::trusted_development().authorize(None, AclResource::Cluster),
            Ok(())
        );
        assert_eq!(
            Permissions::trusted_development().authorize(Some(&writer), AclResource::Cluster),
            Err(ServerError::Unauthorized)
        );
    }
}
