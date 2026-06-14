use std::collections::HashMap;
use std::path::PathBuf;

use super::segment_scan::RecoveredSegments;
use crate::data_plane::SegmentKey;

/// Built only by [`LocalInventory::from_recovered`];
/// maps each locally-held segment to the highest entry id a CRC-complete batch put on disk
#[derive(Debug)]
pub(crate) struct LocalInventory(HashMap<SegmentKey, u64>);

impl LocalInventory {
    pub(crate) fn from_recovered(recovered: &RecoveredSegments) -> Self {
        Self(recovered.cursors().collect())
    }
}

// [`RecoveryOutput`] bundles [`LocalInventory`] with the data dir and is what `DataPlane::new`
// will require: recovery-before-serving stops being a convention and becomes a type, since the serving side cannot be built without one.
#[derive(Debug)]
pub(crate) struct RecoveryOutput {
    pub(crate) inventory: LocalInventory,
    pub(crate) data_dir: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};

    fn seg(topic: u64, segment: u64) -> SegmentKey {
        SegmentKey::new(TopicId(topic), RangeId(0), SegmentId(segment))
    }

    #[test]
    fn inventory_is_exactly_the_post_replay_cursors() {
        let a = seg(1, 0);
        let b = seg(2, 0);
        let mut recovered = RecoveredSegments::default();
        recovered.advance(a, 5); // from-nothing segment, base/cursor 5
        recovered.advance(b, 0);
        recovered.advance(b, 1); // cursor 1

        let inv = LocalInventory::from_recovered(&recovered);
        let cursors: HashMap<SegmentKey, u64> = recovered.cursors().collect();
        assert_eq!(inv.0, cursors);
        assert_eq!(inv.0.get(&a), Some(&5));
        assert_eq!(inv.0.get(&b), Some(&1));
    }

    #[test]
    fn empty_recovery_yields_an_empty_inventory() {
        // The empty-disk path commit 14 constructs through.
        let inv = LocalInventory::from_recovered(&RecoveredSegments::default());
        assert!(inv.0.is_empty());
    }

    #[test]
    fn recovery_output_bundles_inventory_and_data_dir() {
        let inv = LocalInventory::from_recovered(&RecoveredSegments::default());
        let output = RecoveryOutput {
            inventory: inv,
            data_dir: PathBuf::from("/var/lib/eastguard/data"),
        };
        assert!(output.inventory.0.is_empty());
        assert_eq!(output.data_dir, PathBuf::from("/var/lib/eastguard/data"));
    }
}
