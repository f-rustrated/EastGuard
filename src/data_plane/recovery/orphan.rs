//! Orphan GC for crash recovery (D5 commit 26).
//!
//! A restarted node has a fresh NodeId, so no replica set names its recovered segments —
//! every one is an orphan candidate *at recovery time*. Deletion is lazy and
//! **cluster-confirmed**: orphan status is classified at recovery but acted on much later,
//! and in between the coordinator may have (re)assigned the segment back to this node — the
//! recovery "lottery". So before deleting, the GC asks the coordinator (which owns the
//! replica set) whether it still names this node — an `OrphanCheck` carrying this node's id,
//! answered yes/no. "Named" spares the segment: its on-disk copy is reused by the catch-up.
//! Only an unnamed segment is deleted — destruction is cluster-confirmed and last.
//!
//! See `diagrams/data-plane/d5_crash_recovery.md` § "Orphaned Data Cleanup".

use std::io;
use std::path::Path;

use crate::data_plane::SegmentKey;

/// A recovered sealed segment no replica set currently names this node for. `start_offset`
/// is its base entry id (in the filename), needed to locate the file for deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrphanCandidate {
    pub(crate) segment_key: SegmentKey,
    pub(crate) start_offset: u64,
}

impl OrphanCandidate {
    pub(crate) fn new(segment_key: SegmentKey, start_offset: u64) -> Self {
        Self {
            segment_key,
            start_offset,
        }
    }

    /// Delete this orphan's segment file. Precondition (the GC handler's): the coordinator
    /// has just answered that no replica set names this node for the segment — destruction
    /// is cluster-confirmed and last. Returns the key so the caller can drop its inventory
    /// entry. Idempotent: an already-absent file is success.
    pub(crate) fn delete(&self, data_dir: &Path) -> io::Result<SegmentKey> {
        let path = self.segment_key.file_path(data_dir, crate::control_plane::metadata::EntryId(self.start_offset));
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(self.segment_key),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(self.segment_key),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
    use std::fs;

    fn seg() -> SegmentKey {
        SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0))
    }

    #[test]
    fn delete_removes_the_segment_file() {
        let dir = tempfile::tempdir().unwrap();
        let candidate = OrphanCandidate {
            segment_key: seg(),
            start_offset: 0,
        };
        let path = candidate.segment_key.file_path(dir.path(), 0);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"segment data").unwrap();
        assert!(path.exists());

        let deleted = candidate.delete(dir.path()).unwrap();

        assert_eq!(deleted, seg());
        assert!(!path.exists(), "segment file must be gone after delete");
    }

    #[test]
    fn delete_is_idempotent_when_file_absent() {
        let dir = tempfile::tempdir().unwrap();
        let candidate = OrphanCandidate {
            segment_key: seg(),
            start_offset: 0,
        };
        // No file on disk: deleting is a no-op success (idempotent re-runs).
        assert_eq!(candidate.delete(dir.path()).unwrap(), seg());
    }
}
