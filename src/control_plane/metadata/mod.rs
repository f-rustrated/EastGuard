pub(crate) mod command;
pub mod constants;
pub(crate) mod consumer_group;

pub mod error;
pub(crate) mod event;
pub(crate) mod range;

mod producer_sessions;

pub mod strategy;
pub(crate) mod topic;

pub(crate) use range::{RangeMeta, RangeState};
pub(crate) use topic::{TopicMeta, TopicState, TopicStats};

pub(crate) mod segment;

use borsh::{BorshDeserialize as Deser, BorshSerialize as Ser};
use uuid::Uuid;

pub(crate) use command::*;
pub(crate) use consumer_group::{ConsumerGroupAssignment, ConsumerGroupMeta, ConsumerMemberId};

pub(crate) use segment::*;

use crate::{impl_new_struct_wrapper, security::TransportIdentity};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Ser, Deser)]
pub struct TopicId(pub(crate) u64);

impl_new_struct_wrapper!(TopicId, u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Ser, Deser)]
pub enum AclResource {
    Cluster,
    TopicAdmin(TopicId),
    TopicData(TopicId),
    ConsumerGroup(ConsumerGroupResource),
    ProducerSession(ProducerSessionResource),
    SecurityCluster,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Ser, Deser)]
pub struct ConsumerGroupResource {
    pub topic_id: TopicId,
    pub group_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Ser, Deser)]
pub struct ProducerSessionResource {
    pub topic_id: TopicId,
    pub producer_id: Uuid,
}

/// Durable owner of one producer session.
///
/// This is deliberately separate from the connection's transport identity:
/// Raft snapshots retain ownership after the TLS connection disappears, and
/// transport refactors must not change the persisted metadata schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ser, Deser)]
pub enum ProducerSessionOwner {
    CertificatePrincipal(Box<str>),
    TrustedDevelopment,
}

impl ProducerSessionOwner {
    pub(crate) fn from(transport_identity: &TransportIdentity) -> Self {
        match transport_identity {
            TransportIdentity::CertificatePrincipal(principal) => {
                ProducerSessionOwner::CertificatePrincipal(principal.clone())
            }
            TransportIdentity::TrustedDevelopment => ProducerSessionOwner::TrustedDevelopment,
        }
    }
}

impl AclResource {
    pub(crate) fn routing_key(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

impl std::fmt::Display for AclResource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cluster => formatter.write_str("cluster"),
            Self::TopicAdmin(topic_id) => write!(formatter, "topic-admin/{}", topic_id.0),
            Self::TopicData(topic_id) => write!(formatter, "topic-data/{}", topic_id.0),
            Self::ConsumerGroup(resource) => write!(
                formatter,
                "consumer-group/{}/{}",
                resource.topic_id.0, resource.group_id
            ),
            Self::ProducerSession(resource) => write!(
                formatter,
                "producer-session/{}/{}",
                resource.topic_id.0, resource.producer_id
            ),
            Self::SecurityCluster => formatter.write_str("security/cluster"),
        }
    }
}

impl std::str::FromStr for AclResource {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let topic_id = |candidate: &str| {
            candidate
                .parse::<u64>()
                .map(TopicId)
                .map_err(|_| format!("invalid ACL topic ID: {candidate}"))
        };

        if input == "cluster" {
            return Ok(Self::Cluster);
        }
        if input == "security/cluster" {
            return Ok(Self::SecurityCluster);
        }
        if let Some(topic) = input.strip_prefix("topic-admin/") {
            return topic_id(topic).map(Self::TopicAdmin);
        }
        if let Some(topic) = input.strip_prefix("topic-data/") {
            return topic_id(topic).map(Self::TopicData);
        }
        if let Some(consumer_group) = input.strip_prefix("consumer-group/") {
            let (topic, group_id) = consumer_group
                .split_once('/')
                .ok_or_else(|| "consumer-group ACL requires a group ID".to_string())?;
            if group_id.is_empty() {
                return Err("consumer-group ACL requires a non-empty group ID".to_string());
            }
            return Ok(Self::ConsumerGroup(ConsumerGroupResource {
                topic_id: topic_id(topic)?,
                group_id: group_id.to_string(),
            }));
        }
        if let Some(producer_session) = input.strip_prefix("producer-session/") {
            let (topic, producer_id) = producer_session
                .split_once('/')
                .ok_or_else(|| "producer-session ACL requires a producer ID".to_string())?;
            let producer_id = Uuid::parse_str(producer_id)
                .map_err(|_| format!("invalid ACL producer ID: {producer_id}"))?;
            return Ok(Self::ProducerSession(ProducerSessionResource {
                topic_id: topic_id(topic)?,
                producer_id,
            }));
        }

        Err(format!("unknown ACL resource: {input}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ser, Deser, PartialOrd, Ord)]
pub struct RangeId(pub(crate) u64);

impl_new_struct_wrapper!(RangeId, u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Ser, Deser)]
pub struct SegmentId(pub(crate) u64);

impl_new_struct_wrapper!(SegmentId, u64);

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Ser, Deser)]
pub struct EntryId(pub u64);

impl_new_struct_wrapper!(EntryId, u64);

impl EntryId {
    pub const MIN: EntryId = EntryId(0);
    pub const MAX: EntryId = EntryId(u64::MAX);

    pub fn saturating_sub(self, rhs: u64) -> Self {
        EntryId(self.0.saturating_sub(rhs))
    }

    pub fn saturating_add(self, rhs: u64) -> Self {
        EntryId(self.0.saturating_add(rhs))
    }
}

impl std::fmt::Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<EntryId> for u64 {
    fn from(val: EntryId) -> Self {
        val.0
    }
}

impl std::ops::Add<u64> for EntryId {
    type Output = Self;
    fn add(self, rhs: u64) -> Self::Output {
        EntryId(self.0 + rhs)
    }
}

impl std::ops::Add<usize> for EntryId {
    type Output = Self;
    fn add(self, rhs: usize) -> Self::Output {
        EntryId(self.0 + rhs as u64)
    }
}

impl std::ops::Add<EntryId> for EntryId {
    type Output = Self;
    fn add(self, rhs: EntryId) -> Self::Output {
        EntryId(self.0 + rhs.0)
    }
}

impl std::ops::AddAssign<u64> for EntryId {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl std::ops::AddAssign<usize> for EntryId {
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs as u64;
    }
}

impl std::ops::Sub<u64> for EntryId {
    type Output = Self;
    fn sub(self, rhs: u64) -> Self::Output {
        EntryId(self.0 - rhs)
    }
}

impl std::ops::Sub<usize> for EntryId {
    type Output = Self;
    fn sub(self, rhs: usize) -> Self::Output {
        EntryId(self.0 - rhs as u64)
    }
}

impl std::ops::Sub<EntryId> for EntryId {
    type Output = Self;
    fn sub(self, rhs: EntryId) -> Self::Output {
        EntryId(self.0 - rhs.0)
    }
}

impl std::ops::SubAssign<u64> for EntryId {
    fn sub_assign(&mut self, rhs: u64) {
        self.0 -= rhs;
    }
}

impl std::ops::SubAssign<usize> for EntryId {
    fn sub_assign(&mut self, rhs: usize) {
        self.0 -= rhs as u64;
    }
}

impl std::ops::Add<i32> for EntryId {
    type Output = Self;
    fn add(self, rhs: i32) -> Self::Output {
        if rhs >= 0 {
            EntryId(self.0 + rhs as u64)
        } else {
            EntryId(self.0 - (-rhs) as u64)
        }
    }
}

impl std::ops::Sub<i32> for EntryId {
    type Output = Self;
    fn sub(self, rhs: i32) -> Self::Output {
        if rhs >= 0 {
            EntryId(self.0 - rhs as u64)
        } else {
            EntryId(self.0 + (-rhs) as u64)
        }
    }
}

#[cfg(test)]
mod acl_resource_tests {
    use super::*;

    #[test]
    fn resources_round_trip_through_canonical_routing_keys() {
        let producer_id = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();
        let resources = [
            AclResource::Cluster,
            AclResource::TopicAdmin(TopicId(42)),
            AclResource::TopicData(TopicId(42)),
            AclResource::ConsumerGroup(ConsumerGroupResource {
                topic_id: TopicId(42),
                group_id: "billing/readers".to_string(),
            }),
            AclResource::ProducerSession(ProducerSessionResource {
                topic_id: TopicId(42),
                producer_id,
            }),
            AclResource::SecurityCluster,
        ];

        for resource in resources {
            let key = resource.to_string();
            assert_eq!(key.parse::<AclResource>(), Ok(resource.clone()));
            assert_eq!(resource.routing_key(), key.as_bytes());
        }
    }

    #[test]
    fn malformed_resource_keys_are_rejected() {
        for key in [
            "",
            "topic-data/",
            "topic-data/name",
            "topic-data/42/extra",
            "consumer-group/42",
            "consumer-group/42/",
            "producer-session/42/not-a-uuid",
            "unknown/42",
        ] {
            assert!(key.parse::<AclResource>().is_err(), "{key} was accepted");
        }
    }
}
