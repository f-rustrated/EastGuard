use std::sync::Arc;
use std::time::Duration;

use super::{ConsumerContext, ConsumerRecord, RangeCursor};
use crate::client::consumer::context::RangeLookupResult;

use crate::client::consumer::topic_fetch_manager::RangeDrained;
use crate::client::redirect::Served;
use crate::client::{ClientError, CompressionCodec};
use crate::connections::protocol::{
    ClientResponse, DataPlaneResponse, RangeProgressSignal, RangeTransition, SegmentDetail,
};
use crate::control_plane::metadata::{EntryId, RangeId};
use crate::data_plane::offset_ledger::ConsumerOffsetPosition;

const EMPTY_FETCHES_BEFORE_REFRESH: u8 = 20;

pub(crate) enum RangeFetchActorCommand {
    Stop,
    Pause {
        reply: tokio::sync::oneshot::Sender<()>,
    },
    Resume {
        reply: tokio::sync::oneshot::Sender<()>,
    },
    Seek {
        absolute_offset: u64,
        reply: tokio::sync::oneshot::Sender<()>,
    },
}

pub(crate) struct RangeFetchActor {
    cursor: RangeCursor,
    ctx: Arc<ConsumerContext>,
    record_tx: flume::Sender<Result<ConsumerRecord, ClientError>>,
    consecutive_empty_fetches: u8,
    group_fetch: bool,
}

impl RangeFetchActor {
    pub(crate) fn new(
        cursor: RangeCursor,
        ctx: Arc<ConsumerContext>,
        record_tx: flume::Sender<Result<ConsumerRecord, ClientError>>,
        group_fetch: bool,
    ) -> Self {
        Self {
            cursor,
            ctx,
            record_tx,
            consecutive_empty_fetches: 0,
            group_fetch,
        }
    }
    pub(crate) async fn run(mut self, rx: flume::Receiver<RangeFetchActorCommand>) {
        let mut paused = false;
        loop {
            // Check if the consumer is dropped (receiver disconnected) to avoid leaking tasks.
            if self.record_tx.is_disconnected() {
                return;
            }

            tokio::select! {
                cmd = rx.recv_async() => {
                    match cmd {
                        Ok(RangeFetchActorCommand::Stop) | Err(_) => {
                            return; // Graceful shutdown
                        }
                        Ok(RangeFetchActorCommand::Pause { reply }) => {
                            paused = true;
                            let _ = reply.send(());
                        }
                        Ok(RangeFetchActorCommand::Resume { reply }) => {
                            paused = false;
                            let _ = reply.send(());
                        }
                        Ok(RangeFetchActorCommand::Seek {
                            absolute_offset,
                            reply,
                        }) => {
                            self.cursor.seek_to_absolute_offset(absolute_offset);
                            self.ctx.refresh_metadata().await.ok();
                            let _ = reply.send(());
                        }
                    }
                }
                res = self.step(), if !paused => {
                    match res {
                        Ok(true) => continue, // Keep looping
                        Ok(false) => return,  // Graceful shutdown (e.g., drained)
                        Err(e) => {
                            let _ = self.record_tx.send(Err(e));
                            return; // Fatal error shutdown
                        }
                    }
                }
            }
        }
    }

    /// Performs a single loop iteration. Returns `Ok(true)` to continue,
    /// `Ok(false)` for graceful shutdown, or `Err(ClientError)` to abort.
    async fn step(&mut self) -> Result<bool, ClientError> {
        // Read metadata snapshot.
        let segment = match self
            .ctx
            .lookup_range(self.cursor.range_id, self.cursor.next_entry_id)
        {
            RangeLookupResult::FellBehind { first_start } => {
                self.cursor.next_entry_id = first_start;
                self.cursor.skip_batch_offsets_below = None;
                return Ok(true);
            }
            RangeLookupResult::NeedRefresh => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                self.ctx.refresh_metadata().await?;
                return Ok(true);
            }
            RangeLookupResult::RangeSealedAndDrained { progress_signal } => {
                if let RangeProgressSignal::Sealed { transition, .. } = progress_signal {
                    let _ = self.ctx.cursor_tx.send(RangeDrained {
                        cursor: self.cursor.clone(),
                        transition,
                    });
                }
                return Ok(false);
            }
            RangeLookupResult::Found(segment) => segment,
        };

        let served = if segment.end_entry_id.is_none() && self.group_fetch {
            self.ctx
                .fetch_from_active_leader(&segment, self.cursor.range_id, self.cursor.next_entry_id)
                .await?
        } else {
            self.ctx
                .fetch(&segment, self.cursor.range_id, self.cursor.next_entry_id)
                .await?
        };

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
                    if self.group_fetch {
                        self.consecutive_empty_fetches =
                            self.consecutive_empty_fetches.saturating_add(1);
                        if self.consecutive_empty_fetches >= EMPTY_FETCHES_BEFORE_REFRESH {
                            self.ctx.refresh_metadata().await?;
                            self.consecutive_empty_fetches = 0;
                        }
                    }
                } else {
                    self.consecutive_empty_fetches = 0;
                }

                for entry in entries.into_iter() {
                    if self.should_skip_entry_by_absolute_offset(entry.record_count) {
                        continue;
                    }

                    let records = CompressionCodec::decode_payload(&entry.data, entry.record_count)
                        .map_err(|e| {
                            tracing::error!("Failed to decompress entry payload: {}", e);
                            ClientError::UnexpectedResponse
                        })?;

                    let skip_batch_offsets_below = if entry.entry_id == self.cursor.next_entry_id {
                        self.cursor.skip_batch_offsets_below.take()
                    } else {
                        None
                    };

                    for (i, (key, value)) in records.into_iter().enumerate() {
                        if skip_batch_offsets_below.is_some_and(|skip| i as u64 <= skip) {
                            continue;
                        }

                        let absolute_offset = self.cursor.next_absolute_offset;
                        self.cursor.next_absolute_offset =
                            self.cursor.next_absolute_offset.saturating_add(1);

                        if self
                            .cursor
                            .skip_absolute_offsets_below
                            .is_some_and(|target| absolute_offset < target)
                        {
                            continue;
                        }
                        self.cursor.skip_absolute_offsets_below = None;

                        let consumer_rec = ConsumerRecord {
                            topic: self.ctx.topic.clone(),
                            range_id: self.cursor.range_id,
                            position: ConsumerOffsetPosition {
                                batch_offset: i as u64,
                                entry_id: entry.entry_id,
                                absolute_offset,
                            },
                            key,
                            value,
                        };

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
                    self.cursor.next_entry_id = next_entry_id;
                    let _ = self.ctx.cursor_tx.send(RangeDrained {
                        cursor: self.cursor.clone(),
                        transition,
                    });
                    return Ok(false);
                }

                self.cursor.next_entry_id = next_entry_id;
                Ok(true)
            }
            DataPlaneResponse::EntryIdOutOfRange => {
                let prev_entry_id = self.cursor.next_entry_id;
                let (start_id, _) = self
                    .ctx
                    .client
                    .fetch_range_entry_ids(&self.ctx.topic, self.cursor.range_id)
                    .await?;

                if self.cursor.next_entry_id < start_id {
                    self.cursor.next_entry_id = start_id;
                }
                if self.cursor.next_entry_id == prev_entry_id {
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

    fn should_skip_entry_by_absolute_offset(&mut self, record_count: u32) -> bool {
        let Some(target) = self.cursor.skip_absolute_offsets_below else {
            return false;
        };

        let next_entry_absolute_offset = self
            .cursor
            .next_absolute_offset
            .saturating_add(u64::from(record_count));

        if next_entry_absolute_offset <= target {
            self.cursor.next_absolute_offset = next_entry_absolute_offset;
            return true;
        }
        false
    }
}
