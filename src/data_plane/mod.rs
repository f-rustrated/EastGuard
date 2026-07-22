use borsh::{BorshDeserialize, BorshSerialize};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::control_plane::metadata::{EntryId, RangeId, SegmentId, TopicId};
use crate::impl_new_struct_wrapper;

pub(crate) mod actor;
pub(crate) mod checkpoint;
pub(crate) mod cold_read;
pub(crate) mod consumer_offset_management;
pub(crate) mod messages;
pub(crate) mod producer_ledger;
pub(crate) mod recovery;
pub(crate) mod segment_writer;
pub(crate) mod sparse_index;
pub(crate) mod state;
pub(crate) mod states;
pub(crate) mod timer;
pub(crate) mod transport;
pub(crate) mod wal;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, BorshSerialize, BorshDeserialize,
)]
pub struct SegmentKey {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub segment_id: SegmentId,
}

/// Stable identity of one producer append. The range is supplied by the
/// enclosing `SegmentKey`, so it is deliberately not duplicated here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub struct ProducerAppendIdentity {
    pub producer_id: Uuid,
    pub incarnation: u32,
    pub expires_at: u64,
    pub sequence: u64,
    pub digest: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, thiserror::Error)]
pub enum ProduceError {
    #[error("not the write leader")]
    NotLeader,
    #[error("segment not found")]
    SegmentNotFound,
    #[error("producer incarnation was fenced")]
    ProducerFenced,
    #[error("producer session expired or is unknown")]
    SessionExpired,
    #[error("sequence was reused with a different payload")]
    RequestIdentityConflict,
    #[error("duplicate position is outside the retained result window")]
    DuplicatePositionUnavailable,
    #[error("sequence gap; expected {0}")]
    SequenceGap(u64),
    #[error("the same producer request is already in flight")]
    RequestInFlight,
    #[error("internal produce failure: {0}")]
    Internal(String),
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct EntryPayload(Bytes);

impl_new_struct_wrapper!(EntryPayload, Bytes);

impl From<Vec<u8>> for EntryPayload {
    fn from(v: Vec<u8>) -> Self {
        Self(Bytes::from(v))
    }
}

impl SegmentKey {
    pub fn new(topic_id: TopicId, range_id: RangeId, segment_id: SegmentId) -> Self {
        Self {
            topic_id,
            range_id,
            segment_id,
        }
    }

    pub(crate) fn placement_key(&self) -> (TopicId, RangeId) {
        (self.topic_id, self.range_id)
    }

    /// Path to this segment's file. The filename encodes the segment's
    /// `start_entry` (its first entry id) so the file is self-describing for
    /// crash recovery — discovery derives the base entry id from the name
    /// alone, no metadata lookup. `start_entry` is immutable, so the name is
    /// stable for the segment's life (no rename on seal).
    pub fn file_path(&self, data_dir: &Path, start_entry: impl Into<EntryId>) -> PathBuf {
        data_dir
            .join(self.topic_id.to_string())
            .join(self.range_id.to_string())
            .join(format!("{}-{}.seg", *self.segment_id, *start_entry.into()))
    }

    pub fn with_segment_id(&self, segment_id: SegmentId) -> Self {
        Self {
            topic_id: self.topic_id,
            range_id: self.range_id,
            segment_id,
        }
    }
}

/// Parses a segment filename (`{segment_id}-{start_offset}.seg`) into its
/// parts. Returns `Err` for names that don't match — recovery discovery skips
/// foreign files. Inverse of the filename produced by [`SegmentKey::file_path`].
pub(crate) fn parse_segment_file(path: &Path) -> std::io::Result<(SegmentId, EntryId)> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid file path for segment",
            )
        })?;

    let invalid_format_err = || {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Invalid segment file format",
        )
    };
    let stem = file_name
        .strip_suffix(".seg")
        .ok_or_else(invalid_format_err)?;

    let (segment_id, start_offset) = stem.split_once('-').ok_or_else(invalid_format_err)?;

    let offset_u64: u64 = start_offset.parse().map_err(|_| invalid_format_err())?;
    Ok((
        SegmentId(segment_id.parse().map_err(|_| invalid_format_err())?),
        EntryId(offset_u64),
    ))
}
