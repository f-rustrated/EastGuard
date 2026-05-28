use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{Receiver, Sender};

use crate::data_plane::states::segment::cache::SegmentRingBuffer;

use super::SegmentKey;
use super::messages::*;
use super::sparse_index::{SparseEntry, SparseIndex};
use super::wal::WalRecord;
pub struct CheckpointWorker;

impl CheckpointWorker {
    pub(crate) fn spawn(
        sparse_index: Arc<dyn SparseIndex>,
        mailbox: Receiver<Box<[CheckpointJob]>>,
        data_plane_tx: Sender<DataPlaneCommand>,
    ) {
        thread::Builder::new()
            .name("checkpoint-worker".into())
            .spawn(move || {
                while let Ok(batch) = mailbox.recv() {
                    for job in batch {
                        if let Err(e) =
                            Self::process_job(sparse_index.as_ref(), &job, &data_plane_tx)
                        {
                            tracing::error!(
                                "Checkpoint failed for {:?}: {e}",
                                job.segment_key
                            );
                        }
                    }
                }
            })
            .expect("failed to spawn checkpoint thread");
    }

    fn process_job(
        sparse_index: &dyn SparseIndex,
        job: &CheckpointJob,
        data_plane_tx: &Sender<DataPlaneCommand>,
    ) -> std::io::Result<()> {
        let checkpoint = job.cache.drain_for_checkpoint();
        if checkpoint.is_empty() {
            return Ok(());
        }

        let mut byte_position = fs::metadata(&job.segment_file_path)
            .map(|m| m.len())
            .unwrap_or(0);

        let mut writer = job.get_writer()?;
        let mut index_entries = Vec::with_capacity(checkpoint.batches.len());

        for entry in &checkpoint.batches {
            let entry_start_position = byte_position;

            let record = WalRecord::data((*entry.data).clone());
            record.encode_to(&mut writer)?;
            byte_position += record.encoded_size() as u64;

            let end_record = WalRecord::batch_end();
            end_record.encode_to(&mut writer)?;
            byte_position += end_record.encoded_size() as u64;

            index_entries.push(SparseEntry::new(
                job.segment_key,
                entry.entry_id,
                entry_start_position.to_be_bytes(),
            ));
        }

        writer.flush()?;
        writer.get_ref().sync_all()?;

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fd = writer.get_ref().as_raw_fd();
            unsafe {
                libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED);
            }
        }

        sparse_index.put_batch(index_entries)?;
        job.cache.advance_eviction_frontier(checkpoint.new_frontier);

        let _ = data_plane_tx.send(DataPlaneCommand::CheckpointComplete(CheckpointComplete {
            segment_key: job.segment_key,
            checkpointed_lsn: checkpoint.last_lsn(),
        }));

        Ok(())
    }
}

pub struct CheckpointJob {
    pub segment_key: SegmentKey,
    pub cache: Arc<SegmentRingBuffer>,
    pub segment_file_path: PathBuf,
}
impl CheckpointJob {
    fn get_writer(&self) -> Result<BufWriter<File>, std::io::Error> {
        if let Some(parent) = self.segment_file_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.segment_file_path)?;
        Ok(BufWriter::new(file))
    }
}
