use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::Receiver;

use crate::data_plane::actor::DataPlaneSender;
use crate::data_plane::states::segment::cache::SegmentRingBuffer;
use crate::data_plane::wal::WalRecord;

use super::SegmentKey;
use super::messages::command::DataPlaneCommand;
use super::messages::*;
use super::segment_writer::SegmentAppender;
use super::sparse_index::{SparseEntry, SparseIndex};
pub struct CheckpointWorker;

impl CheckpointWorker {
    pub(crate) fn spawn(
        sparse_index: Arc<dyn SparseIndex>,
        mailbox: Receiver<Box<[CheckpointTask]>>,
        data_plane_tx: DataPlaneSender,
    ) {
        thread::Builder::new()
            .name("checkpoint-worker".into())
            .spawn(move || {
                while let Ok(batch) = mailbox.recv() {
                    for task in batch {
                        match task {
                            CheckpointTask::Checkpoint(job) => {
                                if let Err(e) =
                                    Self::process_job(sparse_index.as_ref(), &job, &data_plane_tx)
                                {
                                    tracing::error!(
                                        "Checkpoint failed for {:?}: {e}",
                                        job.segment_key
                                    );
                                }
                            }
                            CheckpointTask::PutAnchors(entries) => {
                                // Catch-up receive's anchors (replacement side):
                                // the worker is the sole runtime sparse-index writer.
                                if let Err(e) = sparse_index.put_batch(entries.into_vec()) {
                                    tracing::error!("Catch-up anchor index write failed: {e}");
                                }
                            }
                        }
                    }
                }
            })
            .expect("failed to spawn checkpoint thread");
    }

    fn process_job(
        sparse_index: &dyn SparseIndex,
        job: &CheckpointJob,
        data_plane_tx: &DataPlaneSender,
    ) -> std::io::Result<()> {
        let checkpoint = job.cache.drain_for_checkpoint();
        if checkpoint.is_empty() {
            return Ok(());
        }

        let mut appender = SegmentAppender::open_append(job.segment_key, &job.segment_file_path)?;
        let mut index_entries = Vec::with_capacity(checkpoint.entries.len());
        for entry in &checkpoint.entries {
            if let Some(anchor) = appender.append_entry(
                entry.entry_id,
                WalRecord::data((*entry.data).clone(), entry.record_count),
            )? {
                index_entries.push(anchor);
            }
        }
        appender.flush_sync_and_release()?;

        sparse_index.put_batch(index_entries)?;
        job.cache.advance_eviction_frontier(checkpoint.new_frontier);

        let completion: DataPlaneCommand = CheckpointComplete {
            segment_key: job.segment_key,
            checkpointed_lsn: checkpoint.last_lsn(),
        }
        .into();
        let _ = data_plane_tx.send(DataPlaneMessage::Command(completion));

        Ok(())
    }
}

/// A unit of work for the checkpoint worker — the sole runtime writer of segment
/// files and the sparse index.
pub(crate) enum CheckpointTask {
    /// Drain a segment's cache into its file and index the new anchors.
    Checkpoint(CheckpointJob),
    /// Persist the sparse-index anchors for a segment the catch-up receive wrote
    /// directly (replacement side); the worker just `put_batch`es them.
    PutAnchors(Box<[SparseEntry]>),
}

pub struct CheckpointJob {
    pub segment_key: SegmentKey,
    pub cache: Arc<SegmentRingBuffer>,
    pub segment_file_path: PathBuf,
}
