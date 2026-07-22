use std::collections::HashMap;
use std::path::PathBuf;

use super::segment_scan::RecoveredSegments;
use crate::control_plane::metadata::EntryId;
use crate::data_plane::SegmentKey;
use crate::data_plane::auxiliary_states::consumer_offsets::state::AuxiliaryState;

/// What recovery verified for one on-disk segment: its `start_offset` (the filename base,
/// needed to locate the file) and `verified_end` (the highest entry id a CRC-complete batch
/// put on disk).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InventoryEntry {
    pub(crate) start_entry: EntryId,
    pub(crate) verified_end: EntryId,
    pub(crate) next_entry_id: EntryId,
}

/// Built only by [`LocalInventory::from_recovered`]; maps each locally-held segment to what
/// recovery verified for it.
#[derive(Debug)]
pub(crate) struct LocalInventory(HashMap<SegmentKey, InventoryEntry>);

impl LocalInventory {
    pub(crate) fn from_recovered(recovered: &RecoveredSegments) -> Self {
        Self(
            recovered
                .inventory()
                .map(|(key, start_offset, verified_end, next_entry_id)| {
                    (
                        key,
                        InventoryEntry {
                            start_entry: start_offset,
                            verified_end,
                            next_entry_id,
                        },
                    )
                })
                .collect(),
        )
    }

    /// Highest verified entry id held for `key`, or `None` if the segment holds
    /// no verified data locally.
    pub(crate) fn get(&self, key: &SegmentKey) -> Option<EntryId> {
        self.0.get(key).map(|e| e.verified_end)
    }

    /// `(segment, start_offset)` for every recovered segment — the orphan-GC sweep's
    /// candidate set. Each is a stray until the cluster (re)assigns it here.
    pub(crate) fn orphan_candidates(&self) -> impl Iterator<Item = (SegmentKey, EntryId)> + '_ {
        self.0.iter().map(|(key, e)| (*key, e.start_entry))
    }

    /// No recovered segments remain — orphan GC is done and need not re-arm.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Drop a segment from the inventory — either reused (now a live registered segment) or
    /// deleted as a stray. Once dropped it is no longer an orphan candidate.
    pub(crate) fn remove(&mut self, key: &SegmentKey) {
        self.0.remove(key);
    }
}

// [`RecoveryOutput`] bundles [`LocalInventory`] with the data dir and is what `DataPlane::new`
// will require: recovery-before-serving stops being a convention and becomes a type, since the serving side cannot be built without one.
#[derive(Debug)]
pub(crate) struct RecoveryOutput {
    pub(crate) inventory: LocalInventory,
    pub(crate) data_dir: PathBuf,
    pub(crate) offsets: AuxiliaryState,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};

    fn seg(topic: u64, segment: u64) -> SegmentKey {
        SegmentKey::new(TopicId(topic), RangeId(0), SegmentId(segment))
    }

    #[test]
    fn inventory_carries_start_offset_and_verified_end() {
        let a = seg(1, 0);
        let b = seg(2, 0);
        let mut recovered = RecoveredSegments::default();
        recovered.advance(a, 5.into()); // from-nothing segment, base 5 / cursor 5
        recovered.advance(b, 0.into());
        recovered.advance(b, 1.into()); // base 0 / cursor 1

        let inv = LocalInventory::from_recovered(&recovered);
        // `get` exposes the verified end (unchanged semantics).
        assert_eq!(inv.get(&a), Some(5.into()));
        assert_eq!(inv.get(&b), Some(1.into()));
        // orphan candidates expose the start_offset (the filename base).
        let candidates: HashMap<SegmentKey, EntryId> = inv.orphan_candidates().collect();
        assert_eq!(candidates.get(&a), Some(&5.into()));
        assert_eq!(candidates.get(&b), Some(&0.into()));
    }

    #[test]
    fn empty_recovery_yields_an_empty_inventory() {
        // The empty-disk path commit 14 constructs through.
        let inv = LocalInventory::from_recovered(&RecoveredSegments::default());
        assert_eq!(inv.orphan_candidates().count(), 0);
    }

    #[test]
    fn remove_drops_the_candidate() {
        let a = seg(1, 0);
        let mut recovered = RecoveredSegments::default();
        recovered.advance(a, 0.into());
        let mut inv = LocalInventory::from_recovered(&recovered);
        assert_eq!(inv.get(&a), Some(0.into()));
        inv.remove(&a);
        assert_eq!(inv.get(&a), None);
        assert_eq!(inv.orphan_candidates().count(), 0);
    }

    #[test]
    fn recovery_output_bundles_inventory_and_data_dir() {
        let inv = LocalInventory::from_recovered(&RecoveredSegments::default());
        let output = RecoveryOutput {
            inventory: inv,
            data_dir: PathBuf::from("/var/lib/eastguard/data"),
            offsets: AuxiliaryState::default(),
        };
        assert_eq!(output.inventory.orphan_candidates().count(), 0);
        assert_eq!(output.data_dir, PathBuf::from("/var/lib/eastguard/data"));
    }
}
