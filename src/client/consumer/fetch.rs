use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use super::ConsumerRecord;
use crate::client::consumer::manager::CursorDrained;
use crate::client::redirect::Served;
use crate::client::{Client, ClientError, CompressionCodec};
use crate::connections::protocol::{
    ClientDataPlaneRequest, ClientResponse, DataPlaneResponse, FetchByIdRequest, RangeDetail,
    RangeOffsetRequest, RangeProgressSignal, RangeTransition, SegmentDetail, TopicDetail,
};
use crate::control_plane::metadata::{RangeId, RangeState};

enum RangeLookupResult {
    Found(SegmentDetail),
    FellBehind {
        first_start: u64,
    },
    NeedRefresh,
    RangeSealedAndDrained {
        progress_signal: RangeProgressSignal,
    },
}
/// The asynchronous fetch loop for a single active range cursor.
pub(crate) async fn run_fetch_actor(
    range_id: RangeId,
    next_entry_id: u64,
    ctx: Arc<ConsumerContext>,
    record_tx: flume::Sender<Result<ConsumerRecord, ClientError>>,
) {
    let mut actor = FetchActor {
        range_id,
        next_entry_id,
        ctx,
        record_tx,
    };
    actor.run().await;
}

struct FetchActor {
    range_id: RangeId,
    next_entry_id: u64,
    ctx: Arc<ConsumerContext>,
    record_tx: flume::Sender<Result<ConsumerRecord, ClientError>>,
}

impl FetchActor {
    async fn run(&mut self) {
        loop {
            match self.step().await {
                Ok(true) => continue, // Keep looping
                Ok(false) => return,  // Graceful shutdown (e.g., drained)
                Err(e) => {
                    let _ = self.record_tx.send(Err(e));
                    return; // Fatal error shutdown
                }
            }
        }
    }

    /// Performs a single loop iteration. Returns `Ok(true)` to continue,
    /// `Ok(false)` for graceful shutdown, or `Err(ClientError)` to abort.
    async fn step(&mut self) -> Result<bool, ClientError> {
        // Check if the consumer is dropped (receiver disconnected) to avoid leaking tasks.
        if self.record_tx.is_disconnected() {
            return Ok(false);
        }

        // Read metadata snapshot.
        let segment = match self.ctx.lookup_range(self.range_id, self.next_entry_id) {
            RangeLookupResult::FellBehind { first_start } => {
                self.next_entry_id = first_start;
                return Ok(true);
            }
            RangeLookupResult::NeedRefresh => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                self.ctx.refresh_metadata().await?;
                return Ok(true);
            }
            RangeLookupResult::RangeSealedAndDrained { progress_signal } => {
                if let RangeProgressSignal::Sealed { transition, .. } = progress_signal {
                    let _ = self.ctx.cursor_tx.send(CursorDrained {
                        range_id: self.range_id,
                        transition,
                    });
                }
                return Ok(false);
            }
            RangeLookupResult::Found(segment) => segment,
        };

        let served = self
            .ctx
            .fetch(&segment, self.range_id, self.next_entry_id)
            .await?;

        let ClientResponse::DataPlane(dp_response) = served.response else {
            return Err(ClientError::UnexpectedResponse);
        };

        self.handle_data_plane_response(dp_response).await
    }

    async fn handle_data_plane_response(
        &mut self,
        dp_response: DataPlaneResponse,
    ) -> Result<bool, ClientError> {
        match dp_response {
            DataPlaneResponse::Fetched {
                entries,
                next_entry_id,
                progress_signal,
            } => {
                if entries.is_empty() {
                    // ! short-polling backoff mechanism to prevent the consumer from unintentionally DDoS-ing the data plane server
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }

                for entry in entries.into_iter() {
                    let records = CompressionCodec::decode_payload(&entry.data, entry.record_count)
                        .map_err(|e| {
                            eprintln!("Failed to decompress entry payload: {}", e);
                            ClientError::UnexpectedResponse
                        })?;

                    for (i, rec) in records.iter().enumerate() {
                        let consumer_rec = ConsumerRecord {
                            topic: self.ctx.topic.clone(),
                            range_id: self.range_id,
                            offset: entry.entry_id + i as u64,
                            key: rec.key.clone(),
                            value: rec.value.clone(),
                        };

                        // TODO commit offset management?
                        if self.record_tx.send(Ok(consumer_rec)).is_err() {
                            return Ok(false); // Disconnected
                        }
                    }
                }

                // Data plane returns Fetched response with progress_signal,
                // If the range is permanently sealed, it attaches Sealed signal.
                // end_entry_id suggest hard stop point.
                //
                // next_entry_id is offset of the next record the consumer needs to fetch.
                // So the end_entry_id could be 999 while next_entry_id  is only  501, suggesting that you still have more to fetch in this range
                if let RangeProgressSignal::Sealed {
                    end_entry_id,
                    transition,
                } = progress_signal
                    && next_entry_id > end_entry_id
                {
                    let _ = self.ctx.cursor_tx.send(CursorDrained {
                        range_id: self.range_id,
                        transition,
                    });
                    return Ok(false);
                }

                self.next_entry_id = next_entry_id;
                Ok(true)
            }
            DataPlaneResponse::EntryIdOutOfRange => {
                let prev_entry_id = self.next_entry_id;
                let (start_id, _) = self
                    .ctx
                    .client
                    .fetch_range_entry_ids(&self.ctx.topic, self.range_id)
                    .await?;

                if self.next_entry_id < start_id {
                    self.next_entry_id = start_id;
                }
                if self.next_entry_id == prev_entry_id {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    self.ctx.refresh_metadata().await?;
                }
                Ok(true)
            }

            dp if dp.is_routing_error() => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                self.ctx.refresh_metadata().await?;
                Ok(true)
            }
            _ => Err(ClientError::UnexpectedResponse),
        }
    }
}

pub(crate) struct ConsumerContext {
    pub(crate) client: Arc<Client>,
    pub(crate) topic: String,
    pub(crate) topic_id: u64,
    pub(crate) metadata: ArcSwap<TopicDetail>,
    pub(crate) cursor_tx: flume::Sender<CursorDrained>,
}

impl ConsumerContext {
    /// Execute a single raw FetchById request.
    async fn fetch(
        &self,
        segment: &SegmentDetail,
        range_id: RangeId,
        offset: u64,
    ) -> Result<Served, ClientError> {
        let addr = segment
            .pick_replica()
            .ok_or(ClientError::UnexpectedResponse)?;
        let req = FetchByIdRequest {
            topic_id: self.topic_id,
            range_id,
            entry_id: offset,
            max_bytes: 1024 * 1024,
        };
        self.client
            .call(addr, ClientDataPlaneRequest::FetchById(req))
            .await
    }

    /// Resolve a replica address for the given range, preferring the active segment's
    /// replica, falling back to the last sealed segment's replica
    pub(crate) fn pick_replica_for_range(&self, range_id: RangeId) -> Option<std::net::SocketAddr> {
        self.metadata
            .load()
            .ranges
            .iter()
            .find(|r| r.range_id == range_id)
            // Try the active segment, fallback to the last sealed segment
            .and_then(|r| {
                r.active_segment
                    .as_ref()
                    .or_else(|| r.sealed_segments.last())
            })
            // If we found either, try to pick a replica from it
            .and_then(|seg| seg.pick_replica())
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
    fn lookup_range(&self, range_id: RangeId, next_entry_id: u64) -> RangeLookupResult {
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
            let range_end_offset = r.end_entry_id();

            if next_entry_id > range_end_offset {
                if let Some(progress_signal) = compute_progress_signal(&meta, r) {
                    return RangeLookupResult::RangeSealedAndDrained { progress_signal };
                } else {
                    return RangeLookupResult::NeedRefresh;
                }
            }
        }

        // (Common Fallback) Check if we fell behind the oldest available data,
        // otherwise signal that a metadata refresh is needed to discover the new segment
        if let Some(first_start) = r.first_segment_start_offset()
            && next_entry_id < first_start
        {
            return RangeLookupResult::FellBehind { first_start };
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

pub fn compute_progress_signal(
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
