use arc_swap::ArcSwap;
use dashmap::DashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::client::consumer::bootstrap::CursorBootstrap;
use crate::client::consumer::fetch::ConsumerContext;
use crate::client::consumer::group::{ConsumerGroup, OffsetCommitPayload, SYSTEM_TOPIC_OFFSETS};
use crate::client::consumer::manager::{CursorDrained, run_cursor_manager};
use crate::client::{Client, ClientError, Producer, ProducerConfig};
use crate::connections::protocol::{
    ClientDataPlaneRequest, ClientResponse, DataPlaneResponse, FetchByIdRequest,
    RangeOffsetRequest, SegmentDetail, TopicDetail,
};
use crate::control_plane::metadata::{RangeId, RangeState};
pub(crate) mod bootstrap;
pub(crate) mod cursor;
pub(crate) mod cursor_set;
mod fetch;
pub(crate) mod group;
pub(crate) mod manager;
pub(crate) mod parked_merges;
mod record;
pub use bootstrap::{KeyInterest, StartPolicy};
pub(crate) use cursor::RangeCursor;
pub(crate) use cursor_set::RangeCursorSet;
pub use record::ConsumerRecord;

#[derive(Clone)]
pub struct Consumer {
    ctx: Arc<ConsumerContext>,
    record_rx: flume::Receiver<Result<ConsumerRecord, ClientError>>,
    group: Option<Arc<ConsumerGroup>>,
}

impl Consumer {
    pub async fn new(
        client: Arc<Client>,
        topic: String,
        interest: KeyInterest,
        start_policy: StartPolicy,
        group_id: impl Into<Option<String>>,
    ) -> Result<Self, ClientError> {
        let group_id = group_id.into();
        let detail = client.resolve_topic(&topic).await?;
        let cursors = CursorBootstrap::build(&detail, interest, start_policy);

        let mut consumer_group = None;

        // Consolidate all group-specific logic into a single block
        if let Some(gid) = &group_id {
            let group = Arc::new(ConsumerGroup::new(client.clone(), gid.to_string())?);
            group.bootstrap().await?;
            consumer_group = Some(group);
        }

        let (record_tx, record_rx) = flume::unbounded();
        let (cursor_tx, cursor_rx) = flume::bounded(100);

        let ctx = Arc::new(ConsumerContext {
            client,
            topic,
            topic_id: detail.topic_id,
            metadata: ArcSwap::from_pointee(detail),
            cursor_tx,
        });

        Self::spawn_manager(
            cursors,
            cursor_rx,
            &ctx,
            record_tx,
            consumer_group.clone(),
            start_policy,
        );

        Ok(Self {
            ctx,
            record_rx,
            group: consumer_group,
        })
    }

    fn spawn_manager(
        cursors: RangeCursorSet,
        cursor_rx: flume::Receiver<CursorDrained>,
        ctx: &Arc<ConsumerContext>,
        record_tx: flume::Sender<Result<ConsumerRecord, ClientError>>,
        consumer_group: Option<Arc<ConsumerGroup>>,
        start_policy: StartPolicy,
    ) {
        tokio::spawn(run_cursor_manager(
            cursors,
            cursor_rx,
            Arc::downgrade(ctx),
            record_tx,
            consumer_group,
            start_policy,
        ));
    }

    pub async fn commit(&self) -> Result<(), ClientError> {
        if let Some(group) = &self.group {
            group.commit(&self.ctx.topic).await?;
        }
        Ok(())
    }

    /// Retrieve the next record from the topic.
    /// Returns `Ok(Some(record))` when a record is available, and `Ok(None)` when the topic has
    /// been fully consumed (i.e. all lineage paths have been drained to their ends and no active
    /// cursors remain).
    pub async fn next_record(&self) -> Result<Option<ConsumerRecord>, ClientError> {
        loop {
            let Ok(res) = self.record_rx.recv_async().await else {
                return Ok(None);
            };

            let rec = res?;

            if let Some(group) = &self.group {
                if !group.is_range_owned(rec.range_id) {
                    continue;
                }
                group.record_offset(rec.range_id, rec.offset);
            }
            return Ok(Some(rec));
        }
    }
}
