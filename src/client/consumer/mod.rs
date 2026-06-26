use arc_swap::ArcSwap;
use std::sync::Arc;

use crate::client::consumer::fetch::ConsumerContext;
use crate::client::consumer::manager::{CursorDrained, run_cursor_manager};
use crate::client::{Client, ClientError};
use crate::connections::protocol::{
    ClientDataPlaneRequest, ClientResponse, DataPlaneResponse, FetchByIdRequest,
    RangeOffsetRequest, SegmentDetail, TopicDetail,
};
use crate::control_plane::metadata::RangeId;
pub(crate) mod bootstrap;
pub(crate) mod cursor;
pub(crate) mod cursor_set;
mod fetch;
pub(crate) mod manager;
pub(crate) mod parked_merges;
mod record;
pub use bootstrap::{KeyInterest, StartPolicy};
pub(crate) use cursor::RangeCursor;
pub(crate) use cursor_set::RangeCursorSet;
pub use record::ConsumerRecord;

/// A thread-safe, internally synchronized consumer client for a single topic.
/// Cheap to clone and share across tasks.
#[derive(Clone)]
pub struct Consumer {
    //  The  `ctx`  is completely private and not exposed to the user, but it serves a critical role:
    // it is the ownership anchor(reference count) for the consumer's context.
    ctx: Arc<ConsumerContext>,
    record_rx: flume::Receiver<Result<ConsumerRecord, ClientError>>,
}

impl Consumer {
    /// Create and bootstrap a new consumer for the specified topic.
    pub async fn new(
        client: Arc<Client>,
        topic: String,
        interest: KeyInterest,
        start_policy: StartPolicy,
    ) -> Result<Self, ClientError> {
        let detail = client.resolve_topic(&topic).await?;
        let mut cursors = RangeCursorSet::bootstrap(&detail, interest, start_policy);

        let (record_tx, record_rx) = flume::unbounded();
        let (cursor_tx, cursor_rx) = flume::bounded(100);

        let ctx = Arc::new(ConsumerContext {
            client: client.clone(),
            topic: topic.clone(),
            topic_id: detail.topic_id,
            metadata: ArcSwap::from_pointee(detail),
            cursor_tx,
        });

        if matches!(start_policy, StartPolicy::Latest) {
            for cursor in cursors.cursors_mut() {
                match ctx.fetch_range_offsets(cursor.range_id).await {
                    Ok((_, committed_id)) => {
                        cursor.next_entry_id = committed_id;
                    }
                    Err(_) => {
                        // Keep the bootstrap offset if we fail to fetch
                    }
                }
            }
        }

        // Spawn the asynchronous manager loop for RangeCursorSet
        tokio::spawn(run_cursor_manager(
            cursors,
            cursor_rx,
            Arc::downgrade(&ctx),
            record_tx,
        ));

        Ok(Self { ctx, record_rx })
    }

    /// Retrieve the next record from the topic.
    /// Returns `Ok(Some(record))` when a record is available, and `Ok(None)` when the topic has
    /// been fully consumed (i.e. all lineage paths have been drained to their ends and no active
    /// cursors remain).
    pub async fn next_record(&self) -> Result<Option<ConsumerRecord>, ClientError> {
        match self.record_rx.recv_async().await {
            Ok(Ok(rec)) => Ok(Some(rec)),
            Ok(Err(e)) => Err(e),
            Err(_) => {
                // All fetch tasks have terminated and the channel is closed
                Ok(None)
            }
        }
    }
}
