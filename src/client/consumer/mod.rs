use arc_swap::ArcSwap;
use dashmap::DashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::client::consumer::bootstrap::build_cursors;
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
pub(crate) mod ownership;
pub use bootstrap::{KeyInterest, StartPolicy};
pub(crate) use cursor::RangeCursor;
pub(crate) use cursor_set::RangeCursorSet;

/// A record returned to the consumer application.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub range_id: RangeId,
    pub offset: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl ConsumerRecord {
    pub fn key_match<'a>(&'a self, key: impl Iterator<Item = &'a u8>) -> bool {
        self.key.iter().eq(key)
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub start_policy: StartPolicy,
    pub group_id: Option<String>,
    pub auto_commit_interval_ms: u64,
}

impl ConsumerConfig {
    pub fn new(start_policy: StartPolicy) -> Self {
        Self {
            start_policy,
            group_id: None,
            auto_commit_interval_ms: 5000, // Default to 1 second for groups
        }
    }
}

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
        config: ConsumerConfig,
    ) -> Result<Self, ClientError> {
        let detail = client.resolve_topic(&topic).await?;
        let mut cursors = build_cursors(&detail, interest, config.start_policy);

        let mut consumer_group = None;

        // Consolidate all group-specific logic into a single block
        if let Some(gid) = config.group_id.clone() {
            let group = Arc::new(ConsumerGroup::new(
                client.clone(),
                gid.to_string(),
                topic.clone(),
            )?);
            group.bootstrap().await?;
            consumer_group = Some(group);
            // If in a consumer group, initialize with empty cursors;
            // assignments will be dynamically resolved and started by the rebalancer.
            cursors = RangeCursorSet::new(Vec::new());
        }

        if matches!(config.start_policy, StartPolicy::Latest) {
            for cursor in cursors.cursors_mut() {
                let has_saved_offset = consumer_group.is_some() && cursor.next_entry_id > 0;
                if !has_saved_offset
                    && let Ok((_, committed_entry_id)) =
                        client.fetch_range_entry_ids(&topic, cursor.range_id).await
                {
                    cursor.next_entry_id = committed_entry_id;
                }
            }
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
            config,
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
        config: ConsumerConfig,
    ) {
        tokio::spawn(run_cursor_manager(
            cursors,
            cursor_rx,
            Arc::downgrade(ctx),
            record_tx,
            consumer_group,
            config,
        ));
    }

    pub async fn commit(&self) -> Result<(), ClientError> {
        if let Some(group) = &self.group {
            group.commit().await?;
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
                if !group.is_responsible_for(rec.range_id) {
                    continue;
                }
                group.record_offset(rec.range_id, rec.offset);
            }
            return Ok(Some(rec));
        }
    }
}
