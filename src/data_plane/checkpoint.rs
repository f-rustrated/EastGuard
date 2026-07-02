use std::path::PathBuf;
use std::sync::Arc;

use flume::Receiver;

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
        // Prod: a dedicated OS thread (blocking segment-file + index writes stay off
        // the main runtime). Test: a task on turmoil's runtime so it shares the sim's
        // cooperative scheduling — a real OS thread runs in real wall-time outside
        // turmoil (CLAUDE.md "Testing").
        #[cfg(not(test))]
        std::thread::Builder::new()
            .name("checkpoint-worker".into())
            .spawn(move || {
                while let Ok(batch) = mailbox.recv() {
                    Self::process_batch(batch, sparse_index.as_ref(), &data_plane_tx);
                }
            })
            .expect("failed to spawn checkpoint thread");

        #[cfg(test)]
        tokio::spawn(async move {
            while let Ok(batch) = mailbox.recv_async().await {
                Self::process_batch(batch, sparse_index.as_ref(), &data_plane_tx);
            }
        });
    }

    fn process_batch(
        batch: Box<[CheckpointTask]>,
        sparse_index: &dyn SparseIndex,
        data_plane_tx: &DataPlaneSender,
    ) {
        for task in batch {
            match task {
                CheckpointTask::Checkpoint(job) => {
                    if let Err(e) = Self::process_job(sparse_index, &job, data_plane_tx) {
                        tracing::error!("Checkpoint failed for {:?}: {e}", job.segment_key);
                    }
                }
                CheckpointTask::PutAnchors(entries) => {
                    // Catch-up receive's anchors (replacement side)
                    if let Err(e) = sparse_index.put_batch(entries.into_vec()) {
                        tracing::error!("Catch-up anchor index write failed: {e}");
                    }
                }
                CheckpointTask::DeleteSegmentIndex(segment_key) => {
                    // Retention (D7): the segment's file was reclaimed; drop its index.
                    if let Err(e) = sparse_index.delete_segment_entries(segment_key) {
                        tracing::warn!("retention index delete failed for {segment_key:?}: {e}");
                    }
                }
            }
        }
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

        println!(
            "[DEBUG CHECKPOINT] process_job starting for {:?} at path {:?}",
            job.segment_key, job.segment_file_path
        );
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
        // We do NOT advance the eviction frontier here. We defer it to the DataPlane thread
        // via CheckpointComplete to prevent an invariant race condition where the frontier 
        // advances before checkpoint_lsn is updated.

        println!(
            "[DEBUG CHECKPOINT] process_job completed successfully for {:?}",
            job.segment_key
        );
        let completion: DataPlaneCommand = CheckpointComplete {
            segment_key: job.segment_key,
            checkpointed_lsn: checkpoint.last_lsn(),
            new_frontier: checkpoint.new_frontier,
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
    /// Retention (D7): remove a reclaimed segment's sparse-index entries.
    DeleteSegmentIndex(SegmentKey),
}

pub struct CheckpointJob {
    pub segment_key: SegmentKey,
    pub cache: Arc<SegmentRingBuffer>,
    pub segment_file_path: PathBuf,
}
