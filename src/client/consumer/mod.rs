use arc_swap::ArcSwap;
use std::sync::Arc;

use crate::client::consumer::fetch::{Inner, LookupResult, compute_progress_signal};
use crate::client::consumer::manager::{CursorEvent, run_cursor_manager};
use crate::client::{Client, ClientError};
use crate::connections::protocol::{
    ClientDataPlaneRequest, ClientResponse, DataPlaneResponse, FetchByIdRequest,
    ListOffsetsRequest, SegmentDetail, TopicDetail,
};
use crate::control_plane::metadata::RangeId;

pub(crate) mod bootstrap;
pub(crate) mod cursor;
pub(crate) mod cursor_set;
pub(crate) mod parked_merges;

mod fetch;
pub(crate) mod manager;
mod record;

pub use bootstrap::{KeyInterest, StartPolicy};
pub(crate) use cursor::RangeCursor;
pub(crate) use cursor_set::RangeCursorSet;
pub use record::ConsumerRecord;

/// A thread-safe, internally synchronized consumer client for a single topic.
/// Cheap to clone and share across tasks.
#[derive(Clone)]
pub struct Consumer {
    inner: Arc<Inner>,
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
        let cursors = RangeCursorSet::bootstrap(&detail, interest, start_policy);

        let (record_tx, record_rx) = flume::unbounded();
        let (cursor_tx, cursor_rx) = flume::unbounded();

        let inner = Arc::new(Inner {
            client,
            topic,
            topic_id: detail.topic_id,
            metadata: ArcSwap::from_pointee(detail),
            cursor_tx,
        });

        // Arc::downgrade to pass a Weak pointer so the rx channel cleanly disconnects
        // and shuts down the manager when the Consumer is dropped.
        tokio::spawn(run_cursor_manager(
            cursors,
            cursor_rx,
            Arc::downgrade(&inner),
            record_tx,
        ));

        Ok(Self { inner, record_rx })
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
