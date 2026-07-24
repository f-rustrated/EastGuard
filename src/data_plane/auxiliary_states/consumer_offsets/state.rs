use std::cmp::Ordering;
use std::collections::HashMap;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::client::RangeId;
use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::control_plane::metadata::{EntryId, TopicId};
use crate::data_plane::SegmentKey;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, BorshSerialize, BorshDeserialize)]
pub(crate) struct ConsumerOffsetKey {
    pub(crate) topic_id: TopicId,
    pub(crate) range_id: RangeId,
    pub(crate) group_id: String,
}

impl ConsumerOffsetKey {
    pub(crate) fn placement_key(&self) -> (TopicId, RangeId) {
        (self.topic_id, self.range_id)
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ConsumerOffsetPosition {
    pub entry_id: EntryId,
    pub batch_offset: u64,
    pub absolute_offset: u64,
}

impl Ord for ConsumerOffsetPosition {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.entry_id, self.batch_offset).cmp(&(other.entry_id, other.batch_offset))
    }
}

impl PartialOrd for ConsumerOffsetPosition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EpochSeal {
    pub key: ConsumerOffsetKey,
    pub generation: GenerationId,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct StaleEpoch(pub GenerationId); // sealed gid

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ConsumerOffsetUpdate {
    pub key: ConsumerOffsetKey,
    pub generation: GenerationId,
    pub position: ConsumerOffsetPosition,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ConsumerOffsetEntry {
    pub key: ConsumerOffsetKey,
    pub generation: GenerationId,
    pub position: Option<ConsumerOffsetPosition>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(crate) enum OffsetRecord {
    EpochSeal(EpochSeal),
    OffsetCommit(ConsumerOffsetUpdate),
    BootstrapEntry(ConsumerOffsetEntry),
    PlacementInstalled(SegmentKey),
}

/// "Durable" consumer-group state. Live mutations are persisted by the shared
/// data-plane WAL; this type is only the in-memory cache and its asynchronous
/// WAL-reclamation snapshot.
#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
pub(crate) struct ConsumerOffsets {
    epochs: HashMap<ConsumerOffsetKey, GenerationId>,
    offsets: HashMap<ConsumerOffsetKey, ConsumerOffsetPosition>,
    installed_placements: HashMap<(TopicId, RangeId), SegmentKey>,
}

impl ConsumerOffsets {
    pub(crate) fn generation(&self, key: &ConsumerOffsetKey) -> GenerationId {
        self.epochs.get(key).copied().unwrap_or(GenerationId(0))
    }

    pub(crate) fn apply(&mut self, record: OffsetRecord) {
        match record {
            OffsetRecord::EpochSeal(EpochSeal { generation, key }) => {
                if generation > self.generation(&key) {
                    self.epochs.insert(key, generation);
                }
            }
            OffsetRecord::OffsetCommit(ConsumerOffsetUpdate {
                key,
                generation,
                position,
            }) => {
                if generation != self.generation(&key) {
                    return;
                }
                self.offsets
                    .entry(key)
                    .and_modify(|current| {
                        if position > *current {
                            *current = position;
                        }
                    })
                    .or_insert(position);
            }
            OffsetRecord::BootstrapEntry(snapshot) => {
                self.epochs
                    .entry(snapshot.key.clone())
                    .and_modify(|generation| *generation = (*generation).max(snapshot.generation))
                    .or_insert(snapshot.generation);
                if let Some(position) = snapshot.position {
                    self.offsets
                        .entry(snapshot.key)
                        .and_modify(|current| *current = (*current).max(position))
                        .or_insert(position);
                }
            }
            OffsetRecord::PlacementInstalled(segment_key) => {
                self.installed_placements
                    .entry((segment_key.topic_id, segment_key.range_id))
                    .and_modify(|current| {
                        if segment_key.segment_id > current.segment_id {
                            *current = segment_key;
                        }
                    })
                    .or_insert(segment_key);
            }
        }
    }

    pub(crate) fn offset(&self, key: &ConsumerOffsetKey) -> Option<ConsumerOffsetPosition> {
        self.offsets.get(key).copied()
    }

    pub(crate) fn has_installed_placement(&self, segment_key: &SegmentKey) -> bool {
        self.installed_placements.get(&segment_key.placement_key()) == Some(segment_key)
    }

    pub(crate) fn can_graduate_to(&self, segment_key: &SegmentKey) -> bool {
        let Some(installed) = self.installed_placements.get(&segment_key.placement_key()) else {
            return false;
        };

        installed.segment_id.checked_add(1) == Some(segment_key.segment_id.0)
    }

    pub(crate) fn can_source_snapshot_for(&self, segment_key: &SegmentKey) -> bool {
        self.has_installed_placement(segment_key) || self.can_graduate_to(segment_key)
    }

    pub(crate) fn snapshot_range(
        &self,
        topic_id: TopicId,
        range_id: RangeId,
    ) -> Box<[ConsumerOffsetEntry]> {
        self.epochs
            .iter()
            .filter(|(key, _)| key.topic_id == topic_id && key.range_id == range_id)
            .map(|(key, generation)| ConsumerOffsetEntry {
                key: key.clone(),
                generation: *generation,
                position: self.offsets.get(key).copied(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        client::RangeId,
        control_plane::metadata::{SegmentId, TopicId},
    };

    fn key() -> ConsumerOffsetKey {
        ConsumerOffsetKey {
            topic_id: TopicId(1),
            range_id: RangeId(2),
            group_id: "g".into(),
        }
    }

    #[test]
    fn placement_readiness_and_graduation_are_distinct() {
        let current = SegmentKey::new(TopicId(1), RangeId(2), SegmentId(4));
        let mut ledger = ConsumerOffsets::default();
        ledger.apply(OffsetRecord::PlacementInstalled(current));

        assert!(ledger.has_installed_placement(&current));
        assert!(!ledger.can_graduate_to(&current));
        assert!(ledger.can_graduate_to(&current.with_segment_id(SegmentId(5))));
        assert!(!ledger.can_graduate_to(&current.with_segment_id(SegmentId(6))));
    }

    #[test]
    fn offsets_advance_monotonically() {
        let position = ConsumerOffsetPosition {
            entry_id: EntryId(4),
            batch_offset: 2,
            absolute_offset: 9,
        };
        let mut ledger = ConsumerOffsets::default();
        ledger.apply(OffsetRecord::EpochSeal(EpochSeal {
            key: key(),
            generation: GenerationId(3),
        }));
        ledger.apply(OffsetRecord::OffsetCommit(ConsumerOffsetUpdate {
            key: key(),
            generation: GenerationId(3),
            position,
        }));
        assert_eq!(*ledger.generation(&key()), 3);
        assert_eq!(ledger.offset(&key()), Some(position));
    }
}
