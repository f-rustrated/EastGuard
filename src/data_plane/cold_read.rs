use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{Receiver, Sender};
use tokio::sync::oneshot;

use super::SegmentKey;
use super::wal::{WalRecord, WalRecordType};
use super::sparse_index::SparseIndex;

const DEFAULT_POOL_SIZE: usize = 4;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ColdReadError {
    #[error("failed to open segment file: {0}")]
    FileOpen(std::io::Error),
    #[error("failed to read segment file: {0}")]
    Io(#[from] std::io::Error),
}

pub(crate) struct ColdReadRequest {
    pub(crate) segment_key: SegmentKey,
    pub(crate) segment_file_path: PathBuf,
    pub(crate) start_offset: u64,
    pub(crate) max_bytes: u64,
    pub(crate) respond_tx: oneshot::Sender<Result<ColdReadRecords, ColdReadError>>,
}

pub(crate) struct ColdReadRecords {
    pub(crate) records: Vec<bytes::Bytes>,
    pub(crate) next_offset: u64,
}

pub(crate) struct ColdReadPool;

impl ColdReadPool {
    pub(crate) fn spawn(
        pool_size: usize,
        sparse_index: Arc<dyn SparseIndex>,
    ) -> Sender<ColdReadRequest> {
        let (tx, rx) = crossbeam_channel::bounded::<ColdReadRequest>(256);

        for i in 0..pool_size {
            let rx = rx.clone();
            let index = Arc::clone(&sparse_index);
            thread::Builder::new()
                .name(format!("cold-read-{i}"))
                .spawn(move || {
                    Self::worker_loop(rx, &*index);
                })
                .expect("failed to spawn cold-read thread");
        }

        tx
    }

    pub(crate) fn spawn_default(sparse_index: Arc<dyn SparseIndex>) -> Sender<ColdReadRequest> {
        Self::spawn(DEFAULT_POOL_SIZE, sparse_index)
    }

    fn worker_loop(mailbox: Receiver<ColdReadRequest>, sparse_index: &dyn SparseIndex) {
        while let Ok(req) = mailbox.recv() {
            let result = Self::process_request(&req, sparse_index);
            let _ = req.respond_tx.send(result);
        }
    }

    fn process_request(
        req: &ColdReadRequest,
        sparse_index: &dyn SparseIndex,
    ) -> Result<ColdReadRecords, ColdReadError> {
        let byte_position = sparse_index.seek_index(req.segment_key, req.start_offset);

        let file = File::open(&req.segment_file_path).map_err(ColdReadError::FileOpen)?;
        let file_len = file.metadata()?.len();

        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(byte_position))?;

        let mut records = Vec::new();
        let mut current_offset = req.start_offset;
        let mut bytes_read = 0u64;

        loop {
            if bytes_read >= req.max_bytes {
                break;
            }

            let pos_before = reader.stream_position()?;
            if pos_before >= file_len {
                break;
            }

            let Ok(record) = WalRecord::decode_from(&mut reader) else {
                break;
            };

            let pos_after = reader.stream_position()?;
            bytes_read += pos_after - pos_before;

            match record.record_type {
                WalRecordType::BatchEnd => break,
                WalRecordType::Data => {
                    current_offset += 1;
                    records.push(record.payload);
                }
            }
        }

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = reader.get_ref().as_raw_fd();
            unsafe {
                libc::posix_fadvise(
                    fd,
                    byte_position as i64,
                    bytes_read as i64,
                    libc::POSIX_FADV_DONTNEED,
                );
            }
        }

        Ok(ColdReadRecords {
            records,
            next_offset: current_offset,
        })
    }
}
