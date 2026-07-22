use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::oneshot;

use super::SegmentKey;
use super::actor::DataPlaneSender;
use super::sparse_index::SparseIndex;
use super::states::segment::cache::CachedEntry;
use super::wal::{WalRecord, WalRecordType};
use crate::connections::protocol::RangeProgressSignal;
use crate::control_plane::NodeId;
use crate::control_plane::metadata::EntryId;
use crate::data_plane::messages::command::CatchUpReadComplete;
use crate::data_plane::messages::query::FetchResult;

pub const DEFAULT_POOL_SIZE: usize = 4;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ColdReadError {
    #[error("failed to open segment file: {0}")]
    FileOpen(std::io::Error),
    #[error("failed to read segment file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to decode wal record: {0}")]
    Decode(std::io::Error),
}

pub(crate) struct ColdReadRequest {
    pub(crate) segment_key: SegmentKey,
    pub(crate) segment_file_path: PathBuf,
    /// First entry id wanted (inclusive).
    pub(crate) start_entry_offset: EntryId,
    /// Last entry id stored in this sealed segment (inclusive). Reads never
    /// cross past this even if the file has a trailing partial write.
    pub(crate) end_entry_id: EntryId,
    pub(crate) max_bytes: u64,
    pub(crate) reply: ColdReadReply,
}

/// Where a finished cold read is delivered.
pub(crate) enum ColdReadReply {
    Consumer {
        reply: oneshot::Sender<FetchResult>,
        progress_signal: RangeProgressSignal,
    },
    CatchUp(CatchUpReadReply),
}

/// Routing context for a catch-up source read (the [`ColdReadReply::CatchUp`] arm).
pub(crate) struct CatchUpReadReply {
    /// The node that requested the catch-up — chunks are addressed here.
    pub(crate) requester: NodeId,
    /// Echoed into `CatchUpReadComplete` so the worker can re-arm the next read
    /// and decide when the sealed end is reached.
    pub(crate) start_offset: EntryId,
    pub(crate) sealed_end: EntryId,
    pub(crate) mailbox: DataPlaneSender,
}

/// Records read off a sealed segment file, already paired with their entry ids
/// and per-entry `record_count` (restored from the on-disk `WalRecord`).
pub(crate) struct ColdReadRecords {
    entries: Vec<Arc<CachedEntry>>,
    next_offset: EntryId,
}

pub(crate) struct ColdReadPool;

impl ColdReadPool {
    pub(crate) fn spawn(
        pool_size: usize,
        sparse_index: Arc<dyn SparseIndex>,
    ) -> flume::Sender<ColdReadRequest> {
        let (tx, rx) = flume::bounded::<ColdReadRequest>(256);

        for _ in 0..pool_size {
            let rx = rx.clone();
            let index = Arc::clone(&sparse_index);

            #[cfg(not(test))]
            std::thread::Builder::new()
                .name("cold-read".into())
                .spawn(move || {
                    while let Ok(req) = rx.recv() {
                        Self::handle_request(req, &*index);
                    }
                })
                .expect("failed to spawn cold-read thread");

            // e2e (turmoil) spawns the pool from inside the sim runtime — run as a
            // task so cold reads share the sim's cooperative scheduling. Sync
            // state-machine tests have no runtime — fall back to a thread.
            #[cfg(test)]
            if tokio::runtime::Handle::try_current().is_ok() {
                tokio::spawn(async move {
                    while let Ok(req) = rx.recv_async().await {
                        Self::handle_request(req, &*index);
                    }
                });
            } else {
                std::thread::Builder::new()
                    .name("cold-read".into())
                    .spawn(move || {
                        while let Ok(req) = rx.recv() {
                            Self::handle_request(req, &*index);
                        }
                    })
                    .expect("failed to spawn cold-read thread");
            }
        }

        tx
    }

    fn handle_request(req: ColdReadRequest, sparse_index: &dyn SparseIndex) {
        println!(
            "[DEBUG COLD READ] handle_request received for {:?} offset {} path {:?}",
            req.segment_key, req.start_entry_offset, req.segment_file_path
        );
        let result = Self::process_request(&req, sparse_index);
        let ColdReadRequest {
            segment_key, reply, ..
        } = req;

        match result {
            Ok(records) => {
                println!(
                    "[DEBUG COLD READ] handle_request SUCCESS for {:?} records {}",
                    segment_key,
                    records.entries.len()
                );
                match reply {
                    ColdReadReply::Consumer {
                        reply,
                        progress_signal,
                    } => {
                        let _ = reply.send(FetchResult::Records {
                            entries: records.entries,
                            next_entry_id: records.next_offset,
                            progress_signal,
                        });
                    }
                    ColdReadReply::CatchUp(cu) => {
                        let done = CatchUpReadComplete {
                            requester: cu.requester,
                            segment_key,
                            start_offset: cu.start_offset,
                            sealed_end: cu.sealed_end,
                            entries: records.entries,
                            next_offset: records.next_offset,
                        };

                        let _ = cu.mailbox.send(done);
                    }
                }
            }

            Err(e) => {
                println!(
                    "[DEBUG COLD READ] handle_request FAILED for {:?}: {:?}",
                    segment_key, e
                );
                match reply {
                    ColdReadReply::Consumer { reply, .. } => {
                        tracing::warn!("cold read failed for {segment_key:?}: {e}");
                        let err_msg = format!("cold read failed: {e}");
                        let _ = reply.send(FetchResult::InternalError(err_msg));
                    }
                    ColdReadReply::CatchUp(_) => {
                        // Source-side read failed; nothing durable changed. The
                        // replacement times out and the coordinator re-drives, so just drop with a log.
                        tracing::warn!("catch-up source read failed for {segment_key:?}: {e}");
                    }
                }
            }
        }
    }

    fn process_request(
        req: &ColdReadRequest,
        sparse_index: &dyn SparseIndex,
    ) -> Result<ColdReadRecords, ColdReadError> {
        let (anchor_id, byte_position) =
            sparse_index.seek_index(req.segment_key, *req.start_entry_offset);

        let file = File::open(&req.segment_file_path).map_err(ColdReadError::FileOpen)?;
        let file_len = file.metadata()?.len();

        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(byte_position))?;

        let mut entries: Vec<Arc<CachedEntry>> = Vec::new();
        let mut current_entry_id = EntryId(anchor_id);
        let mut next_offset = req.start_entry_offset;
        let mut bytes_read = 0u64;

        loop {
            let pos_before = reader.stream_position()?;
            if pos_before >= file_len {
                break;
            }

            let record = match WalRecord::decode_from(&mut reader) {
                Ok(r) => r,
                Err(e) => {
                    // Adjust this check based on how `decode_from` signals EOF
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                    // Bubble up real corruption/IO errors
                    return Err(ColdReadError::Decode(e));
                }
            };

            match record.record_type {
                // `BatchEnd` is a per-entry separator in segment files, not the
                // end of the stream — keep going.
                WalRecordType::BatchEnd => continue,
                WalRecordType::ConsumerOffset => {
                    return Err(ColdReadError::Decode(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "consumer offset record in segment file",
                    )));
                }
                WalRecordType::Data => {
                    let entry_id = current_entry_id;
                    current_entry_id += 1u64;

                    if entry_id < req.start_entry_offset {
                        continue;
                    }
                    if entry_id > req.end_entry_id {
                        break;
                    }

                    let payload_len = record.payload.len() as u64;

                    // Ensure we always return at least one entry even if it exceeds max_bytes
                    if !entries.is_empty() && bytes_read + payload_len > req.max_bytes {
                        break;
                    }

                    bytes_read += payload_len;
                    next_offset = entry_id + 1u64;
                    entries.push(Arc::new(CachedEntry {
                        data: record.payload.into(),
                        record_count: record.record_count,
                        entry_id,
                        // lsn is a live-WAL concept; cold reads serve durable
                        // data, so it carries no meaningful LSN.
                        lsn: 0,
                        producer_append_id: None,
                    }));

                    if bytes_read >= req.max_bytes {
                        break;
                    }
                }
            }
        }

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = reader.get_ref().as_raw_fd();
            // Calculate the ACTUAL bytes consumed from the file for the kernel
            if let Ok(end_position) = reader.stream_position() {
                let actual_file_bytes_read = end_position.saturating_sub(byte_position);

                unsafe {
                    libc::posix_fadvise(
                        fd,
                        byte_position as i64,
                        actual_file_bytes_read as i64,
                        libc::POSIX_FADV_DONTNEED,
                    );
                }
            }
        }

        Ok(ColdReadRecords {
            entries,
            next_offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::metadata::{RangeId, SegmentId, TopicId};
    use crate::data_plane::sparse_index::SparseEntry;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::io::Write;

    /// Test double for the sparse index: maps each entry id to its byte offset
    /// in the segment file, mirroring the per-entry anchors checkpoint writes.
    struct FakeIndex {
        positions: HashMap<u64, u64>,
    }
    impl SparseIndex for FakeIndex {
        fn put_batch(&self, _entries: Vec<SparseEntry>) -> std::io::Result<()> {
            Ok(())
        }
        fn delete_segment_entries(&self, _segment_key: SegmentKey) -> std::io::Result<()> {
            Ok(())
        }
        fn seek_index(&self, _segment_key: SegmentKey, target_offset: u64) -> (u64, u64) {
            // Nearest anchor at or below target (seek_for_prev semantics),
            // returned as (anchor_id, byte_position).
            self.positions
                .iter()
                .filter(|(o, _)| **o <= target_offset)
                .max_by_key(|(o, _)| **o)
                .map(|(o, p)| (*o, *p))
                .unwrap_or((target_offset, 0))
        }
    }

    fn key() -> SegmentKey {
        SegmentKey::new(TopicId(1), RangeId(0), SegmentId(0))
    }

    /// Write entries to a segment file the same way `checkpoint.rs` does: a
    /// `Data` record per entry, each followed by a `BatchEnd` separator. Returns
    /// the byte offset of each entry id for the fake index.
    fn write_segment(path: &std::path::Path, entries: &[(&[u8], u32)]) -> HashMap<u64, u64> {
        let mut buf = Vec::new();
        let mut positions = HashMap::new();
        for (i, (payload, rc)) in entries.iter().enumerate() {
            positions.insert(i as u64, buf.len() as u64);
            WalRecord::data(Bytes::copy_from_slice(payload), *rc)
                .encode_to(&mut buf)
                .unwrap();
            WalRecord::batch_end().encode_to(&mut buf).unwrap();
        }
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(&buf).unwrap();
        positions
    }

    fn request(
        path: std::path::PathBuf,
        start_offset: u64,
        end_entry_id: u64,
        max_bytes: u64,
    ) -> ColdReadRequest {
        let (reply, _rx) = oneshot::channel();
        ColdReadRequest {
            segment_key: key(),
            segment_file_path: path,
            start_entry_offset: EntryId(start_offset),
            end_entry_id: EntryId(end_entry_id),
            max_bytes,
            reply: ColdReadReply::Consumer {
                reply,
                progress_signal: RangeProgressSignal::Active,
            },
        }
    }

    #[test]
    fn forward_scans_from_a_sparse_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.log");
        let positions = write_segment(
            &path,
            &[(b"e0", 1), (b"e1", 1), (b"e2", 1), (b"e3", 1), (b"e4", 1)],
        );
        // Sparse index: only ids 0 and 3 are anchored.
        let positions: HashMap<u64, u64> = positions
            .into_iter()
            .filter(|(o, _)| *o == 0 || *o == 3)
            .collect();
        let idx = FakeIndex { positions };

        // Reading id 4 lands on the id-3 anchor and scans forward past id 3.
        let req = request(path, 4, 4, 1 << 20);
        let out = ColdReadPool::process_request(&req, &idx).unwrap();

        assert_eq!(out.entries.len(), 1);
        assert_eq!(out.entries[0].entry_id, EntryId(4));
        assert_eq!(out.entries[0].data.to_vec(), b"e4".to_vec());
        assert_eq!(out.next_offset, EntryId(5));
    }

    #[test]
    fn reads_all_entries_with_record_counts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.log");
        let positions = write_segment(&path, &[(b"aaa", 1), (b"bbbb", 3), (b"cc", 2)]);

        let idx = FakeIndex { positions };
        let req = request(path, 0, 2, 1 << 20);
        let out = ColdReadPool::process_request(&req, &idx).unwrap();

        assert_eq!(out.entries.len(), 3, "BatchEnd must not end the stream");
        assert_eq!(out.next_offset, EntryId(3));
        let got: Vec<(u64, u32, Vec<u8>)> = out
            .entries
            .iter()
            .map(|e| (*e.entry_id, e.record_count, e.data.to_vec()))
            .collect();
        assert_eq!(
            got,
            vec![
                (0, 1, b"aaa".to_vec()),
                (1, 3, b"bbbb".to_vec()),
                (2, 2, b"cc".to_vec()),
            ]
        );
    }

    #[test]
    fn max_bytes_returns_at_least_one_then_caps() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.log");
        let positions = write_segment(&path, &[(b"aaa", 1), (b"bbbb", 3)]);

        let idx = FakeIndex { positions };
        let req = request(path, 0, 1, 1); // 1 byte cap
        let out = ColdReadPool::process_request(&req, &idx).unwrap();

        assert_eq!(out.entries.len(), 1, "always include at least one entry");
        assert_eq!(out.entries[0].entry_id, EntryId(0));
        assert_eq!(out.next_offset, EntryId(1));
    }

    #[test]
    fn end_entry_id_bounds_the_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.log");
        let positions = write_segment(&path, &[(b"a", 1), (b"b", 1), (b"c", 1)]);

        let idx = FakeIndex { positions };
        let req = request(path, 0, 1, 1 << 20); // stop after id 1
        let out = ColdReadPool::process_request(&req, &idx).unwrap();

        assert_eq!(out.entries.len(), 2);
        assert_eq!(out.entries.last().unwrap().entry_id, EntryId(1));
        assert_eq!(out.next_offset, EntryId(2));
    }

    #[test]
    fn starts_at_requested_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seg.log");
        let positions = write_segment(&path, &[(b"a", 1), (b"b", 2), (b"c", 3)]);

        let idx = FakeIndex { positions };
        let req = request(path, 1, 2, 1 << 20);
        let out = ColdReadPool::process_request(&req, &idx).unwrap();

        let got: Vec<(u64, u32)> = out
            .entries
            .iter()
            .map(|e| (*e.entry_id, e.record_count))
            .collect();
        assert_eq!(got, vec![(1, 2), (2, 3)]);
        assert_eq!(out.next_offset, EntryId(3));
    }
}
