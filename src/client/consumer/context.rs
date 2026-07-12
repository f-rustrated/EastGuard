//! Shared context and metadata management for the topic consumer.
//!
//! # Reference Lifecycle (Strong vs. Weak)
//!
//! To prevent resource leaks and enable clean shutdown, references to `ConsumerContext`
//! are shared using a mix of strong (`Arc`) and weak (`Weak`) pointers:
//!
//! 1. **`Consumer` (Owner/Handle)**: Holds a strong `Arc<ConsumerContext>`. Dropping the
//!    `Consumer` signals that the client is done consuming the topic.
//! 2. **`RangeFetchActor` (Active Fetchers)**: Holds a strong `Arc<ConsumerContext>` to safely
//!    perform socket I/O, fetch range metadata, and dispatch messages back to the consumer.
//! 3. **`TopicFetchManager` (Background Coordinator)**: Holds a `Weak<ConsumerContext>`.
//!
//! ### Shutdown Flow:
//! When the user drops the `Consumer`, the strong reference count of `ConsumerContext` drops.
//! In the next iteration of the background fetch manager's loop, `weak_ctx.upgrade()` will fail
//! (returning `None`). The fetch manager breaks its event loop, calls `.abort_all()` on the running
//! actors, and exits. This ensures that the background task terminates gracefully when the parent
//! `Consumer` is dropped, avoiding background thread leaks.

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::client::consumer::topic_fetch_manager::RangeDrained;
use crate::client::redirect::Served;
use crate::client::{Client, ClientError};
use crate::connections::protocol::{
    ClientDataPlaneRequest, FetchByIdRequest, RangeDetail, RangeProgressSignal, RangeTransition,
    SegmentDetail, TopicDetail,
};
use crate::control_plane::metadata::{EntryId, RangeId, RangeState, TopicId};

pub(crate) enum RangeLookupResult {
    Found(SegmentDetail),
    FellBehind {
        first_start: EntryId,
    },
    NeedRefresh,
    RangeSealedAndDrained {
        progress_signal: RangeProgressSignal,
    },
}

pub(crate) struct ConsumerContext {
    pub(crate) client: Arc<Client>,
    pub(crate) topic: String,
    pub(crate) topic_id: TopicId,
    pub(crate) metadata: ArcSwap<TopicDetail>,
    pub(crate) cursor_tx: flume::Sender<RangeDrained>,
}

impl ConsumerContext {
    /// Execute a single raw FetchById request.
    pub(crate) async fn fetch(
        &self,
        segment: &SegmentDetail,
        range_id: RangeId,
        entry_id: EntryId,
    ) -> Result<Served, ClientError> {
        let addr = segment
            .pick_replica()
            .ok_or(ClientError::UnexpectedResponse)?;
        self.fetch_from(addr, range_id, entry_id).await
    }

    pub(crate) async fn fetch_from_active_leader(
        &self,
        segment: &SegmentDetail,
        range_id: RangeId,
        entry_id: EntryId,
    ) -> Result<Served, ClientError> {
        let addr = segment
            .replica_set
            .first() // First being data leader
            .map(|replica| replica.client_addr)
            .ok_or(ClientError::UnexpectedResponse)?;
        self.fetch_from(addr, range_id, entry_id).await
    }

    async fn fetch_from(
        &self,
        addr: std::net::SocketAddr,
        range_id: RangeId,
        entry_id: EntryId,
    ) -> Result<Served, ClientError> {
        let req = FetchByIdRequest {
            topic_id: self.topic_id,
            range_id,
            entry_id,
            max_bytes: 1024 * 1024,
        };
        self.client
            .call(addr, ClientDataPlaneRequest::FetchById(req))
            .await
    }

    /// Refresh the cached metadata snapshot by resolving the topic against the cluster.
    pub(crate) async fn refresh_metadata(&self) -> Result<(), ClientError> {
        if let Ok(Ok(detail)) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            self.client.resolve_topic(&self.topic),
        )
        .await
        {
            self.metadata.store(Arc::new(detail));
        }
        Ok(())
    }

    /// Pure synchronous metadata lookup to isolate the non-Send arc_swap::Guard.
    pub(crate) fn lookup_range(
        &self,
        range_id: RangeId,
        next_entry_id: EntryId,
    ) -> RangeLookupResult {
        let meta = self.metadata.load();
        let Some(r) = meta.ranges.iter().find(|r| r.range_id == range_id) else {
            return RangeLookupResult::NeedRefresh;
        };

        // Try to find the segment directly
        if let Some(segment) = r.find_segment_for_offset(next_entry_id) {
            return RangeLookupResult::Found(segment.clone());
        }

        // If sealed, check if we've drained it completely
        if r.state != RangeState::Active {
            let range_end_entry = r.end_entry_id();

            if next_entry_id > range_end_entry {
                if let Some(progress_signal) = compute_progress_signal(&meta, r) {
                    return RangeLookupResult::RangeSealedAndDrained { progress_signal };
                } else {
                    return RangeLookupResult::NeedRefresh;
                }
            }
        }

        // (Common Fallback) Check if we fell behind the oldest available data,
        // otherwise signal that a metadata refresh is needed to discover the new segment
        if let Some(start_entry) = r.first_segment_start_offset()
            && next_entry_id < start_entry
        {
            return RangeLookupResult::FellBehind {
                first_start: start_entry,
            };
        }

        RangeLookupResult::NeedRefresh
    }

    pub(crate) fn all_ranges(&self) -> Vec<RangeId> {
        self.metadata
            .load()
            .ranges
            .iter()
            .map(|r| r.range_id)
            .collect::<Vec<_>>()
    }
}

pub(crate) fn compute_progress_signal(
    detail: &TopicDetail,
    range: &RangeDetail,
) -> Option<RangeProgressSignal> {
    let end_entry_id = range.end_entry_id();

    if let Some((left_range_id, right_range_id)) = range.split_into {
        Some(RangeProgressSignal::Sealed {
            end_entry_id,
            transition: RangeTransition::Split {
                left_range_id,
                right_range_id,
                split_point: range.keyspace_end.clone(),
            },
        })
    } else if let Some(merged_range_id) = range.merged_into {
        let merged_range = detail
            .ranges
            .iter()
            .find(|r| r.range_id == merged_range_id)?;
        let (m_left, m_right) = merged_range.merged_from?;

        Some(RangeProgressSignal::Sealed {
            end_entry_id,
            transition: RangeTransition::Merged {
                merged_range_id,
                merged_from: [m_left, m_right],
            },
        })
    } else {
        None
    }
}
