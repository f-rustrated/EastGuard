use super::RangeCursorSet;
use crate::client::ClientError;
use crate::client::consumer::fetch::run_fetch_actor;
use crate::client::consumer::{ConsumerRecord, Inner};
use crate::connections::protocol::RangeTransition;
use crate::control_plane::metadata::RangeId;

pub(crate) enum CursorEvent {
    /// Sent when a fetch loop hits the end of a sealed segment and is fully drained.
    /// The fetch loop will terminate immediately after sending this.
    Drained {
        range_id: RangeId,
        transition: RangeTransition,
    },
}

/// The dedicated actor task that owns and mutates the RangeCursorSet without locks.
pub(crate) async fn run_cursor_manager(
    mut cursors: RangeCursorSet,
    rx: flume::Receiver<CursorEvent>,
    weak_inner: std::sync::Weak<Inner>,
    record_tx: flume::Sender<Result<ConsumerRecord, ClientError>>,
) {
    // If the cursor set is empty from the start, disconnect immediately and exit.
    if cursors.is_empty() {
        return;
    }

    // Spawn the initial fetch loops.
    if let Some(inner) = weak_inner.upgrade() {
        for cursor in cursors.cursors() {
            let inner_clone = inner.clone();
            tokio::spawn(run_fetch_actor(
                cursor.range_id,
                cursor.next_entry_id,
                inner_clone,
                record_tx.clone(),
            ));
        }
    }

    while let Ok(event) = rx.recv_async().await {
        match event {
            CursorEvent::Drained {
                range_id,
                transition,
            } => {
                let added = cursors.apply_drained(range_id, transition);

                if let Some(inner) = weak_inner.upgrade() {
                    for new_cursor in added.iter() {
                        let inner_clone = inner.clone();
                        tokio::spawn(run_fetch_actor(
                            new_cursor.range_id,
                            new_cursor.next_entry_id,
                            inner_clone,
                            record_tx.clone(),
                        ));
                    }
                }

                // If all cursors have been drained and no active cursors remain, disconnect the channel and exit
                if cursors.is_empty() {
                    break;
                }
            }
        }
    }
}
