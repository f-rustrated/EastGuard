use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use uuid::Uuid;

use crate::client::{Client, ClientError};
use crate::connections::protocol::{ClientResponse, ConsumerGroupSyncAction, ControlPlaneResponse};
use crate::control_plane::metadata::consumer_group::GenerationId;
use crate::control_plane::metadata::{EntryId, RangeId, SyncConsumerGroupRequest, TopicId};
use crate::data_plane::consumer_offset_management::ledger::{
    ConsumerOffsetKey, ConsumerOffsetPosition, ConsumerOffsetUpdate,
};

#[derive(Debug, Clone, Copy)]
pub struct OffsetTracker {
    pub delivered: ConsumerOffsetPosition,
    pub committable: Option<ConsumerOffsetPosition>,
    pub committed: Option<ConsumerOffsetPosition>,
}

impl OffsetTracker {
    pub fn new_delivered(position: ConsumerOffsetPosition) -> Self {
        Self {
            delivered: position,
            committable: None,
            committed: None,
        }
    }

    fn commit_position(&self) -> Option<ConsumerOffsetPosition> {
        self.committable.filter(|committable| {
            self.committed
                .is_none_or(|committed| *committable > committed)
        })
    }
}

pub struct ConsumerGroup {
    pub(crate) owned_ranges: ArcSwap<HashSet<RangeId>>,
    pub(crate) offsets: DashMap<RangeId, OffsetTracker>,

    topic: String,
    group_id: String,
    topic_id: TopicId,
    consumer_id: Uuid,
    client: Arc<Client>,
    delivery_fenced: AtomicBool,
    generation: AtomicU64,
    commit_lock: tokio::sync::Mutex<()>,
}

impl ConsumerGroup {
    pub async fn new(
        client: Arc<Client>,
        group_id: String,
        topic: String,
        topic_id: TopicId,
    ) -> Result<Self, ClientError> {
        let group = Self {
            group_id,
            topic,
            topic_id,
            consumer_id: Uuid::new_v4(),
            client,
            owned_ranges: ArcSwap::from_pointee(HashSet::new()),
            offsets: DashMap::new(),
            delivery_fenced: AtomicBool::new(true),
            generation: AtomicU64::new(0),
            commit_lock: tokio::sync::Mutex::new(()),
        };
        let ranges = group.request_assignment().await?;
        let _ = group.install_effective_ownership(ranges);
        Ok(group)
    }

    fn consumer_offset_key(&self, range_id: RangeId) -> ConsumerOffsetKey {
        ConsumerOffsetKey {
            topic_id: self.topic_id,
            range_id,
            group_id: self.group_id.clone(),
        }
    }

    pub(crate) fn is_responsible_for(&self, range_id: RangeId) -> bool {
        !self.delivery_fenced.load(AtomicOrdering::Acquire)
            && self.owned_ranges.load().contains(&range_id)
    }

    pub(crate) fn record_delivery(
        &self,
        range_id: RangeId,
        delivered: ConsumerOffsetPosition,
    ) -> Result<(), ClientError> {
        if !self.is_responsible_for(range_id) {
            return Err(ClientError::on_ack(
                range_id,
                "range is not currently owned by this consumer",
            ));
        }
        self.offsets
            .entry(range_id)
            .and_modify(|tracker| {
                if delivered > tracker.delivered {
                    tracker.delivered = delivered;
                }
            })
            .or_insert_with(|| OffsetTracker::new_delivered(delivered));
        Ok(())
    }

    pub(crate) fn ack_offset(
        &self,
        range_id: RangeId,
        processed: ConsumerOffsetPosition,
    ) -> Result<(), ClientError> {
        if !self.is_responsible_for(range_id) {
            return Err(ClientError::on_ack(
                range_id,
                "range is not currently owned by this consumer",
            ));
        }
        let Some(mut tracker) = self.offsets.get_mut(&range_id) else {
            return Err(ClientError::on_ack(
                range_id,
                "record was not delivered by this consumer",
            ));
        };
        if processed > tracker.delivered {
            return Err(ClientError::on_ack(
                range_id,
                "position is ahead of the highest delivered record",
            ));
        }
        if tracker
            .committable
            .is_none_or(|committable| processed > committable)
        {
            tracker.committable = Some(processed);
        }
        Ok(())
    }

    pub async fn commit(&self) -> Result<(), ClientError> {
        self.commit_owned(&self.owned_ranges.load_full()).await
    }

    pub(crate) async fn commit_owned(&self, owned: &HashSet<RangeId>) -> Result<(), ClientError> {
        let _guard = self.commit_lock.lock().await;
        let generation = GenerationId(self.generation.load(AtomicOrdering::Acquire));

        let futures = self
            .offsets
            .iter()
            .filter_map(|entry| {
                let range_id = *entry.key();
                // Only process if it's owned and has a commit position
                if owned.contains(&range_id) {
                    return entry.commit_position().map(|pos| (range_id, pos));
                }
                None
            })
            .map(|(range_id, position)| async move {
                self.client
                    .commit_consumer_offset(
                        &self.topic,
                        ConsumerOffsetUpdate {
                            key: self.consumer_offset_key(range_id),
                            generation,
                            position,
                        },
                    )
                    .await?;

                // Update the committed position in-memory if successful
                if let Some(mut entry) = self.offsets.get_mut(&range_id)
                    && entry.committed.is_none_or(|committed| position > committed)
                {
                    entry.committed = Some(position);
                }
                Ok(())
            });
        futures::future::try_join_all(futures).await?;
        Ok(())
    }

    pub(crate) async fn request_assignment(&self) -> Result<HashSet<RangeId>, ClientError> {
        let request = SyncConsumerGroupRequest {
            topic_name: self.topic.to_string(),
            group_id: self.group_id.to_string(),
            member_id: self.consumer_id,
            action: ConsumerGroupSyncAction::Heartbeat,
        };

        let assignment = match self
            .client
            .call(self.client.next_known_node(), request)
            .await?
            .response
        {
            ClientResponse::ControlPlane(ControlPlaneResponse::ConsumerGroupAssignment(
                assignment,
            )) => assignment,
            _ => return Err(ClientError::UnexpectedResponse),
        };

        self.generation
            .store(*assignment.generation, AtomicOrdering::Release);
        Ok(assignment.ranges.into_vec().into_iter().collect())
    }

    pub(crate) fn install_effective_ownership(&self, ranges: HashSet<RangeId>) -> Box<[RangeId]> {
        let previous = self.owned_ranges.load_full();

        let to_drop = previous.difference(&ranges).copied().collect();

        //  Commit the new state
        let ranges = Arc::new(ranges);
        self.owned_ranges.store(ranges.clone());

        // Clean up dropped ranges
        // self.offsets.retain(|range, _| ranges.contains(range));
        for range in &to_drop {
            self.offsets.remove(range);
        }

        // Release the fence
        self.delivery_fenced.store(false, AtomicOrdering::Release);

        to_drop
    }

    pub(crate) fn fence_delivery(&self) {
        self.delivery_fenced.store(true, AtomicOrdering::Release);
    }

    pub(crate) fn clear_effective_ownership(&self) {
        self.owned_ranges.store(Arc::new(HashSet::new()));
    }

    pub(crate) async fn fetch_saved_offsets(
        &self,
        ranges: Box<[RangeId]>,
    ) -> Result<std::collections::HashMap<RangeId, ConsumerOffsetPosition>, ClientError> {
        // A generation is the distributed snapshot boundary. Holding the same
        // lock used by commits prevents this member from advancing any requested
        // checkpoint while range leaders independently serve that boundary.
        let _guard = self.commit_lock.lock().await;
        let generation = GenerationId(self.generation.load(AtomicOrdering::Acquire));
        let consumer_offset_keys = ranges
            .into_iter()
            .map(|r| self.consumer_offset_key(r))
            .collect();

        self.client
            .fetch_consumer_offsets(&self.topic, consumer_offset_keys, generation)
            .await
    }
}

impl Drop for ConsumerGroup {
    fn drop(&mut self) {
        let client = self.client.clone();
        let req = SyncConsumerGroupRequest {
            topic_name: self.topic.clone(),
            group_id: self.group_id.clone(),
            member_id: self.consumer_id,
            action: ConsumerGroupSyncAction::Leave,
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let served = client.call(client.next_known_node(), req).await;
                tracing::debug!(?served);
            });
        }
    }
}
