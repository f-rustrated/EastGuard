use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::client::RangeId;
use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::control_plane::metadata::{EntryId, TopicId};
use crate::data_plane::SegmentKey;

const SNAPSHOT_FILE: &str = "consumer-offsets.snapshot";
const SNAPSHOT_TEMP_FILE: &str = "consumer-offsets.snapshot.tmp";

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
pub struct ConsumerOffsetSnapshot {
    pub key: ConsumerOffsetKey,
    pub generation: GenerationId,
    pub position: Option<ConsumerOffsetPosition>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub(crate) enum OffsetRecord {
    EpochSeal(EpochSeal),
    OffsetCommit(ConsumerOffsetUpdate),
    BootstrapEntry(ConsumerOffsetSnapshot),
    PlacementReady(SegmentKey),
}

/// "Durable" consumer-group state. Live mutations are persisted by the shared
/// data-plane WAL; this type is only the in-memory cache and its asynchronous
/// WAL-reclamation snapshot.
#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
pub(crate) struct OffsetLedger {
    epochs: HashMap<ConsumerOffsetKey, GenerationId>,
    offsets: HashMap<ConsumerOffsetKey, ConsumerOffsetPosition>,
    ready_placements: HashMap<(TopicId, RangeId), SegmentKey>,
}

impl OffsetLedger {
    pub(crate) fn load_snapshot(data_dir: &Path) -> io::Result<Self> {
        let path = data_dir.join(SNAPSHOT_FILE);
        match fs::read(path) {
            Ok(bytes) => Self::try_from_slice(&bytes).map_err(io::Error::other),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn write_snapshot(&self, data_dir: &Path) -> io::Result<()> {
        fs::create_dir_all(data_dir)?;
        let bytes = borsh::to_vec(self).map_err(io::Error::other)?;
        let temporary = data_dir.join(SNAPSHOT_TEMP_FILE);
        let final_path = data_dir.join(SNAPSHOT_FILE);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(temporary, final_path)?;
        File::open(data_dir)?.sync_all()
    }

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
            OffsetRecord::PlacementReady(segment_key) => {
                self.ready_placements
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

    pub(crate) fn is_placement_ready(&self, segment_key: &SegmentKey) -> bool {
        self.ready_placements
            .get(&(segment_key.topic_id, segment_key.range_id))
            == Some(segment_key)
    }

    pub(crate) fn snapshot_range(
        &self,
        topic_id: TopicId,
        range_id: RangeId,
    ) -> Box<[ConsumerOffsetSnapshot]> {
        self.epochs
            .iter()
            .filter(|(key, _)| key.topic_id == topic_id && key.range_id == range_id)
            .map(|(key, generation)| ConsumerOffsetSnapshot {
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
    use crate::{client::RangeId, control_plane::metadata::TopicId};

    fn key() -> ConsumerOffsetKey {
        ConsumerOffsetKey {
            topic_id: TopicId(1),
            range_id: RangeId(2),
            group_id: "g".into(),
        }
    }

    #[test]
    fn snapshot_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let position = ConsumerOffsetPosition {
            entry_id: EntryId(4),
            batch_offset: 2,
            absolute_offset: 9,
        };
        let mut ledger = OffsetLedger::default();
        ledger.apply(OffsetRecord::EpochSeal(EpochSeal {
            key: key(),
            generation: GenerationId(3),
        }));
        ledger.apply(OffsetRecord::OffsetCommit(ConsumerOffsetUpdate {
            key: key(),
            generation: GenerationId(3),
            position,
        }));
        ledger.write_snapshot(dir.path()).unwrap();

        let recovered = OffsetLedger::load_snapshot(dir.path()).unwrap();
        assert_eq!(*recovered.generation(&key()), 3);
        assert_eq!(recovered.offset(&key()), Some(position));
    }
}
