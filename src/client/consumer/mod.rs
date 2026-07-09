use arc_swap::ArcSwap;
use dashmap::DashMap;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

use crate::client::consumer::group::{
    ConsumerGroup, ConsumerPosition, OffsetCommitPayload, SYSTEM_TOPIC_OFFSETS,
};
use crate::client::consumer::range_fetcher::ConsumerContext;
use crate::client::consumer::topic_fetch_manager::{
    RangeDrained, TopicFetchManagerCommand, TopicFetchManagerState, run_topic_fetch_manager,
};
use crate::client::{Client, ClientError, Producer, ProducerConfig};
use crate::connections::protocol::{
    ClientDataPlaneRequest, ClientResponse, DataPlaneResponse, FetchByIdRequest,
    RangeOffsetRequest, SegmentDetail, TopicDetail,
};
use crate::control_plane::metadata::{EntryId, RangeId, RangeState};

pub(crate) mod cursor;
pub(crate) mod group;
pub(crate) mod range_fetcher;
pub(crate) mod topic_fetch_manager;

pub(crate) use cursor::RangeCursor;
pub(crate) use cursor::RangeCursorSet;
pub use cursor::{KeyInterest, StartPolicy};

/// A record returned to the consumer application.
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub range_id: RangeId,
    pub position: ConsumerPosition,
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
    /// The policy (Earliest/Latest) used to start consumption on newly assigned ranges.
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
    consumer_rx: flume::Receiver<Result<ConsumerRecord, ClientError>>,
    command_tx: flume::Sender<TopicFetchManagerCommand>,
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
        let mut cursors = RangeCursorSet::build_cursors(&detail, interest, config.start_policy);

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
            for cursor in cursors.iter_mut() {
                let (_, tail_entry_id) = client
                    .fetch_range_entry_ids(&topic, cursor.range_id)
                    .await?;
                cursor.next_entry_id = tail_entry_id;
            }
        }

        let (record_tx, consumer_rx) = flume::unbounded();
        let (cursor_tx, cursor_rx) = flume::bounded(100);
        let (command_tx, command_rx) = flume::bounded(100);

        let ctx = Arc::new(ConsumerContext {
            client,
            topic,
            topic_id: detail.topic_id,
            metadata: ArcSwap::from_pointee(detail),
            cursor_tx,
        });

        let topic_fetch_manager =
            TopicFetchManagerState::new(cursors, consumer_group.clone(), config, record_tx);

        if !topic_fetch_manager.should_exit() {
            Self::spawn_manager(topic_fetch_manager, cursor_rx, command_rx, &ctx);
        }

        Ok(Self {
            ctx,
            consumer_rx,
            command_tx,
            group: consumer_group,
        })
    }

    fn spawn_manager(
        topic_fetch_manager: TopicFetchManagerState,
        drain_event_rx: flume::Receiver<RangeDrained>,
        command_rx: flume::Receiver<TopicFetchManagerCommand>,
        ctx: &Arc<ConsumerContext>,
    ) {
        tokio::spawn(run_topic_fetch_manager(
            topic_fetch_manager,
            drain_event_rx,
            command_rx,
            Arc::downgrade(ctx),
        ));
    }

    pub async fn commit(&self) -> Result<(), ClientError> {
        if let Some(group) = &self.group {
            group.commit().await?;
        }
        Ok(())
    }

    pub async fn pause_range(&self, range_id: RangeId) -> Result<(), ClientError> {
        const OPERATION: &str = "pause";
        let (reply, response) = tokio::sync::oneshot::channel();
        self.command_tx
            .send_async(TopicFetchManagerCommand::Pause { range_id, reply })
            .await
            .map_err(|_| Self::control_error(OPERATION, range_id, "manager is not running"))?;
        response
            .await
            .map_err(|_| Self::control_error(OPERATION, range_id, "manager dropped the reply"))?
    }

    pub async fn resume_range(&self, range_id: RangeId) -> Result<(), ClientError> {
        const OPERATION: &str = "resume";
        let (reply, response) = tokio::sync::oneshot::channel();
        self.command_tx
            .send_async(TopicFetchManagerCommand::Resume { range_id, reply })
            .await
            .map_err(|_| Self::control_error(OPERATION, range_id, "manager is not running"))?;
        response
            .await
            .map_err(|_| Self::control_error(OPERATION, range_id, "manager dropped the reply"))?
    }

    pub async fn seek_range(
        &self,
        range_id: RangeId,
        absolute_offset: u64,
    ) -> Result<(), ClientError> {
        const OPERATION: &str = "seek";
        let (reply, response) = tokio::sync::oneshot::channel();
        self.command_tx
            .send_async(TopicFetchManagerCommand::Seek {
                range_id,
                absolute_offset,
                reply,
            })
            .await
            .map_err(|_| Self::control_error(OPERATION, range_id, "manager is not running"))?;
        response
            .await
            .map_err(|_| Self::control_error(OPERATION, range_id, "manager dropped the reply"))?
    }

    fn control_error(
        operation: &'static str,
        range_id: RangeId,
        reason: impl Into<String>,
    ) -> ClientError {
        ClientError::ConsumerControl {
            operation,
            range_id: *range_id,
            reason: reason.into(),
        }
    }

    /// Retrieve the next record from the topic.
    /// Returns `Ok(Some(record))` when a record is available, and `Ok(None)` when the topic has
    /// been fully consumed (i.e. all lineage paths have been drained to their ends and no active
    /// cursors remain).
    pub async fn next_record(&self) -> Result<Option<ConsumerRecord>, ClientError> {
        loop {
            let Ok(res) = self.consumer_rx.recv_async().await else {
                tracing::info!("Topic fully consumed");
                return Ok(None);
            };

            let rec = res?;

            if let Some(group) = &self.group {
                if !group.is_responsible_for(rec.range_id) {
                    continue;
                }
                group.record_offset(rec.range_id, rec.position);
            }
            return Ok(Some(rec));
        }
    }
}
