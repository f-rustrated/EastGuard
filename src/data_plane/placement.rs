use std::collections::BTreeMap;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::control_plane::NodeId;
use crate::control_plane::membership::TopologyReader;
use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::control_plane::metadata::segment::SegmentMetaState;
use crate::control_plane::metadata::{RangeId, SegmentId, SegmentMeta, TopicId};

use super::SegmentKey;
use super::messages::command::DataPlanePeerMessage;

/// Published from applied metadata by its coordinator, never learned from data traffic.
/// This is volatile: a restarted replica must receive committed placement again.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub(crate) struct CommittedPlacement {
    pub segment_key: SegmentKey,
    pub term: u64,
    pub log_index: u64,
    pub segment: SegmentMeta,
    pub consumer_epochs: Box<[(String, GenerationId)]>,
}

impl CommittedPlacement {
    pub(crate) fn is_from_coordinator(&self, from: &NodeId, topology: &TopologyReader) -> bool {
        self.segment_key.segment_id == self.segment.segment_id
            && topology
                .shard_leader(self.segment_key.topic_id.shard_group_id())
                .is_some_and(|leader| leader.leader.node_id == *from && leader.term == self.term)
    }
}

impl DataPlanePeerMessage {
    /// Metadata-coordinator output that needs its committed view delivered first.
    pub(crate) fn coordinated_topics(&self) -> Box<[TopicId]> {
        use DataPlanePeerMessage as M;
        let topic = match self {
            M::PlaceSegment(c) => c.segment_key.topic_id,
            M::SegmentRollCommitted(c) => c.old_segment_key.topic_id,
            M::SegmentMetaSealed(c) => c.segment_key.topic_id,
            M::AssignSegmentCatchUp(c) => c.segment_key.topic_id,
            M::RequestDurableSegmentEnd(c) => c.segment_key.topic_id,
            M::ConsumerGroupEpochSealed(c) => c.key.topic_id,
            M::DeleteSegments(c) => return c.segment_keys.iter().map(|key| key.topic_id).collect(),
            M::CommittedPlacement(_)
            | M::SegmentPlaced(_)
            | M::ReplicateSegmentEntries(_)
            | M::ReplicaEntriesAppended(_)
            | M::ReplicateConsumerOffset(_)
            | M::ConsumerOffsetReplicated(_)
            | M::InstallConsumerOffsetSnapshot(_)
            | M::RequestConsumerOffsetSnapshot(_)
            | M::ConsumerOffsetSnapshotInstalled(_)
            | M::AdvanceReplicaCommit(_)
            | M::RequestSegmentRoll(_)
            | M::RequestCatchUpEntries(_)
            | M::CatchUpEntries(_)
            | M::CatchUpEntriesSent(_)
            | M::SegmentCaughtUp(_)
            | M::DurableSegmentEndReported(_) => return Box::new([]),
        };
        Box::new([topic])
    }
}

/// Local-only command. The transport checks the authenticated coordinator before
/// constructing it, including for self-delivery.
pub(crate) struct InstallCommittedPlacement {
    pub coordinator: NodeId,
    pub placement: CommittedPlacement,
}

#[derive(Default)]
pub(crate) struct DataPlacements {
    entries: BTreeMap<SegmentKey, InstallCommittedPlacement>,
}

impl DataPlacements {
    pub(crate) fn install(&mut self, update: InstallCommittedPlacement) -> bool {
        let key = update.placement.segment_key;
        if let Some(previous) = self.entries.get(&key) {
            let old = &previous.placement;
            let new = &update.placement;
            if new.term < old.term
                || new.log_index < old.log_index
                || (new.log_index == old.log_index
                    && (new.segment != old.segment || new.consumer_epochs != old.consumer_epochs))
            {
                tracing::warn!(?key, "rejected stale or conflicting committed placement");
                return false;
            }
        }
        self.entries.insert(key, update);
        true
    }

    pub(crate) fn get(&self, key: &SegmentKey) -> Option<&CommittedPlacement> {
        self.entries.get(key).map(|entry| &entry.placement)
    }

    fn latest(&self, topic: TopicId, range: RangeId) -> Option<&CommittedPlacement> {
        self.entries
            .range(
                SegmentKey::new(topic, range, SegmentId(0))
                    ..=SegmentKey::new(topic, range, SegmentId(u64::MAX)),
            )
            .next_back()
            .map(|(_, entry)| &entry.placement)
    }

    fn coordinator(&self, key: &SegmentKey, from: &NodeId) -> bool {
        self.entries
            .get(key)
            .is_some_and(|entry| &entry.coordinator == from)
    }

    pub(crate) fn can_catch_up(&self, key: &SegmentKey, peer: &NodeId, local: &NodeId) -> bool {
        self.get(key).is_some_and(|p| {
            p.segment.state == SegmentMetaState::Sealed
                && p.segment.replica_set.contains(peer)
                && p.segment.replica_set.contains(local)
                && peer != local
        })
    }

    pub(crate) fn allows(
        &self,
        from: &NodeId,
        local: &NodeId,
        message: &DataPlanePeerMessage,
    ) -> bool {
        use DataPlanePeerMessage as M;
        use SegmentMetaState as S;
        let member = |key: &SegmentKey| {
            self.get(key)
                .filter(|p| p.segment.replica_set.contains(local))
        };
        let replication = |key: &SegmentKey| {
            member(key).filter(|p| {
                p.segment.state == S::Active
                    && p.segment.replica_set.leader() == Some(from)
                    && from != local
            })
        };
        let acknowledgement = |key: &SegmentKey| {
            member(key).is_some_and(|p| {
                p.segment.state != S::Deleting
                    && p.segment.replica_set.leader() == Some(local)
                    && p.segment.replica_set.contains(from)
                    && from != local
            })
        };
        // Offsets outlive the active tail when a range is sealed by a split/merge.
        let offset_placement = |key: &SegmentKey| {
            member(key).filter(|p| {
                p.segment.state != S::Deleting
                    && self
                        .latest(key.topic_id, key.range_id)
                        .is_some_and(|latest| latest.segment_key == *key)
            })
        };
        match message {
            // Only the transport may turn this into the local installation command.
            M::CommittedPlacement(_) => false,
            M::PlaceSegment(c) => {
                self.coordinator(&c.segment_key, from)
                    && member(&c.segment_key).is_some_and(|p| {
                        p.segment.state == S::Active
                            && p.segment.replica_set == c.replica_set
                            && p.segment.start_entry_id == c.start_entry_id
                            && c.shard_group_id == c.segment_key.topic_id.shard_group_id()
                    })
            }
            M::ReplicateSegmentEntries(c) => replication(&c.segment_key).is_some_and(|p| {
                p.segment.replica_set == c.replicas && c.entry_id >= p.segment.start_entry_id
            }),
            M::ReplicaEntriesAppended(c) => c.from == *from && acknowledgement(&c.segment_key),
            M::AdvanceReplicaCommit(c) => replication(&c.segment_key).is_some(),
            M::ReplicateConsumerOffset(c) => offset_placement(&c.segment_key).is_some_and(|p| {
                p.segment.replica_set.leader() == Some(from)
                    && from != local
                    && p.segment.replica_set == c.replica_set
                    && c.segment_key.placement_key()
                        == (c.update.key.topic_id, c.update.key.range_id)
            }),
            M::ConsumerOffsetReplicated(c) => {
                c.from == *from
                    && self
                        .latest(c.update.key.topic_id, c.update.key.range_id)
                        .is_some_and(|p| acknowledgement(&p.segment_key))
            }
            M::InstallConsumerOffsetSnapshot(c) => {
                offset_placement(&c.segment_key).is_some_and(|p| {
                    p.segment.replica_set == c.replica_set
                        && p.segment.replica_set.contains(from)
                        && (p.segment.replica_set.leader() == Some(from)
                            || p.segment.replica_set.leader() == Some(local))
                        && c.entries.iter().all(|entry| {
                            (entry.key.topic_id, entry.key.range_id)
                                == c.segment_key.placement_key()
                        })
                })
            }
            M::RequestConsumerOffsetSnapshot(c) => {
                c.requester == *from
                    && offset_placement(&c.segment_key).is_some_and(|p| {
                        p.segment.replica_set == c.replicas && p.segment.replica_set.contains(from)
                    })
            }
            M::ConsumerOffsetSnapshotInstalled(c) => {
                c.from == *from
                    && c.leader == *local
                    && acknowledgement(&c.segment_key)
                    && offset_placement(&c.segment_key).is_some()
            }
            M::SegmentRollCommitted(c) => {
                let next = c.old_segment_key.with_segment_id(c.new_segment_id);
                self.coordinator(&c.old_segment_key, from)
                    && self.coordinator(&next, from)
                    && member(&c.old_segment_key).is_some_and(|p| p.segment.state == S::Sealed)
                    && member(&next).is_some_and(|p| {
                        p.segment.state == S::Active
                            && p.segment.replica_set == c.new_replica_set
                            && p.segment.replica_set.leader() == Some(local)
                    })
            }
            M::SegmentMetaSealed(c) => member(&c.segment_key).is_some_and(|p| {
                p.segment.state == S::Sealed
                    && (self.coordinator(&c.segment_key, from)
                        || p.segment.replica_set.leader() == Some(from))
                    && c.committed_entry_id
                        .is_none_or(|end| p.segment.end_entry_id == Some(end))
            }),
            M::AssignSegmentCatchUp(c) => {
                self.coordinator(&c.segment_key, from)
                    && member(&c.segment_key).is_some_and(|p| {
                        p.segment.state == S::Sealed
                            && p.segment.replica_set == c.replica_set
                            && p.segment.start_entry_id == c.start_entry_id
                            && p.segment.end_entry_id == Some(c.sealed_end_entry_id)
                            && c.shard_group_id == c.segment_key.topic_id.shard_group_id()
                    })
            }
            M::RequestCatchUpEntries(c) => {
                c.from == *from && self.can_catch_up(&c.segment_key, from, local)
            }
            M::CatchUpEntries(c) => self.can_catch_up(&c.segment_key, from, local),
            M::CatchUpEntriesSent(c) => self.can_catch_up(&c.segment_key, from, local),
            M::RequestDurableSegmentEnd(c) => {
                c.coordinator == *from
                    && self.coordinator(&c.segment_key, from)
                    && member(&c.segment_key).is_some_and(|p| p.segment.state != S::Deleting)
            }
            M::DeleteSegments(c) => {
                !c.segment_keys.is_empty()
                    && c.segment_keys.iter().all(|key| {
                        self.coordinator(key, from)
                            && member(key).is_some_and(|p| p.segment.state == S::Deleting)
                    })
            }
            M::ConsumerGroupEpochSealed(c) => self
                .latest(c.key.topic_id, c.key.range_id)
                .is_some_and(|p| {
                    self.coordinator(&p.segment_key, from)
                        && p.segment.replica_set.contains(local)
                        && p.consumer_epochs.iter().any(|(group, generation)| {
                            group == &c.key.group_id && *generation == c.generation
                        })
                }),
            // The owning Raft actor checks its committed data set. This node need
            // not itself store segment bytes (nor have received a placement).
            M::RequestSegmentRoll(c) => c.from == *from,
            M::SegmentPlaced(c) => c.from == *from,
            M::SegmentCaughtUp(c) => c.from == *from,
            M::DurableSegmentEndReported(c) => c.from == *from,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::metadata::EntryId;
    use crate::control_plane::{
        Replicas,
        membership::{ShardGroupId, Topology, TopologyConfig},
    };
    use crate::data_plane::messages::command::*;

    fn node(id: &str) -> NodeId {
        NodeId::new(id)
    }

    fn placement() -> CommittedPlacement {
        CommittedPlacement {
            segment_key: SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0)),
            term: 2,
            log_index: 7,
            segment: SegmentMeta::new(
                SegmentId(0),
                Replicas::new(vec![node("leader"), node("follower"), node("other")]),
                EntryId(0),
                0,
            ),
            consumer_epochs: Box::new([]),
        }
    }

    impl DataPlacements {
        fn install_test(&mut self, placement: CommittedPlacement) -> bool {
            self.install(InstallCommittedPlacement {
                coordinator: node("coordinator"),
                placement,
            })
        }
    }

    #[test]
    fn missing_stale_and_forged_data_authority_is_rejected() {
        let mut store = DataPlacements::default();
        let mut committed = placement();
        let key = committed.segment_key;
        let append: DataPlanePeerMessage = ReplicateSegmentEntries {
            segment_key: key,
            replicas: committed.segment.replica_set.clone(),
            data: vec![1].into(),
            record_count: 1,
            entry_id: EntryId(0),
            producer_identity: None,
        }
        .into();
        assert!(!store.allows(&node("leader"), &node("follower"), &append));
        assert!(store.install_test(committed.clone()));
        assert!(store.allows(&node("leader"), &node("follower"), &append));
        assert!(!store.allows(&node("other"), &node("follower"), &append));
        assert!(!store.allows(&node("leader"), &node("outsider"), &append));
        let ack: DataPlanePeerMessage = ReplicaEntriesAppended {
            segment_key: key,
            entry_id: EntryId(0),
            from: node("follower"),
        }
        .into();
        assert!(store.allows(&node("follower"), &node("leader"), &ack));
        assert!(!store.allows(&node("other"), &node("leader"), &ack));
        assert!(!store.allows(&node("follower"), &node("other"), &ack));
        let old = committed.clone();
        committed.log_index += 1;
        committed.segment.state = SegmentMetaState::Sealed;
        committed.segment.end_entry_id = Some(EntryId(2));
        committed.segment.replica_set = Replicas::new(vec![node("leader"), node("other")]);
        assert!(store.install_test(committed.clone()));
        assert!(!store.install_test(old));
        assert!(!store.allows(&node("leader"), &node("follower"), &append));
        assert!(!store.allows(&node("follower"), &node("leader"), &ack));
        let catch_up: DataPlanePeerMessage = RequestCatchUpEntries {
            segment_key: key,
            from: node("other"),
            local_end: None,
        }
        .into();
        assert!(store.allows(&node("other"), &node("leader"), &catch_up));
        assert!(!store.allows(&node("follower"), &node("leader"), &catch_up));
        assert!(!store.allows(&node("other"), &node("follower"), &catch_up));
        let delete: DataPlanePeerMessage = DeleteSegments {
            segment_keys: Box::new([key]),
        }
        .into();
        assert!(!store.allows(&node("coordinator"), &node("leader"), &delete));
        committed.log_index += 1;
        committed.segment.state = SegmentMetaState::Deleting;
        assert!(store.install_test(committed));
        assert!(store.allows(&node("coordinator"), &node("leader"), &delete));
        assert!(!store.allows(&node("leader"), &node("other"), &delete));
        assert!(!store.allows(&node("other"), &node("leader"), &catch_up));
    }

    #[test]
    fn embedded_sender_and_requester_must_match_the_connection() {
        let mut store = DataPlacements::default();
        let committed = placement();
        let key = committed.segment_key;
        store.install_test(committed.clone());
        let messages: Box<[DataPlanePeerMessage]> = Box::new([
            RequestSegmentRoll {
                from: node("leader"),
                segment_key: key,
                failed_nodes: vec![],
                end_entry_id: EntryId(0),
                intent: crate::control_plane::metadata::SegmentRollIntent::IdleMaintenance,
            }
            .into(),
            SegmentPlaced {
                segment_key: key,
                shard_group_id: ShardGroupId(0),
                from: node("leader"),
            }
            .into(),
            SegmentCaughtUp {
                segment_key: key,
                shard_group_id: ShardGroupId(0),
                from: node("leader"),
            }
            .into(),
            DurableSegmentEndReported {
                segment_key: key,
                from: node("leader"),
                durable_end: None,
            }
            .into(),
            RequestConsumerOffsetSnapshot {
                segment_key: key,
                replicas: committed.segment.replica_set,
                requester: node("leader"),
            }
            .into(),
            RequestDurableSegmentEnd {
                segment_key: key,
                coordinator: node("coordinator"),
            }
            .into(),
            ConsumerOffsetSnapshotInstalled {
                segment_key: key,
                from: node("follower"),
                leader: node("leader"),
            }
            .into(),
        ]);
        for message in &messages {
            assert!(
                !store.allows(&node("outsider"), &node("leader"), message),
                "{message:?}"
            );
        }
    }

    #[test]
    fn only_the_known_coordinator_can_publish_placement() {
        use crate::control_plane::membership::messages::dissemination_buffer::ShardLeaderInfo;
        let mut topology = Topology::new(
            [node("coordinator")],
            TopologyConfig {
                vnodes_per_pnode: 1,
                replication_factor: 1,
            },
        );
        let committed = placement();
        let info = ShardLeaderInfo {
            shard_group_id: ShardGroupId(0),
            leader_node_id: node("coordinator"),
            term: 2,
            leader_addr: crate::control_plane::NodeAddress::new(
                "127.0.0.1:1".parse().unwrap(),
                "127.0.0.1:2".parse().unwrap(),
                "127.0.0.1:3".parse().unwrap(),
            ),
        };
        let (_, missing) = topology.clone().channel();
        assert!(!committed.is_from_coordinator(&node("coordinator"), &missing));
        topology.update_shard_leader(&info);
        let (_, reader) = topology.channel();
        assert!(committed.is_from_coordinator(&node("coordinator"), &reader));
        assert!(!committed.is_from_coordinator(&node("leader"), &reader));
        let mut stale = committed;
        stale.term = 1;
        assert!(!stale.is_from_coordinator(&node("coordinator"), &reader));
    }
}
