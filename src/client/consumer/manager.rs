use super::RangeCursorSet;
use crate::client::ClientError;
use crate::client::consumer::fetch::run_fetch_actor;
use crate::client::consumer::{ConsumerContext, ConsumerRecord};
use crate::connections::protocol::RangeTransition;
use crate::control_plane::metadata::RangeId;

/// Sent when a fetch loop hits the end of a sealed segment and is fully drained.
/// The fetch loop will terminate immediately after sending this.
pub(crate) struct CursorDrained {
    pub range_id: RangeId,
    pub transition: RangeTransition,
}

/// The dedicated actor task that owns and mutates the RangeCursorSet without locks.
pub(crate) async fn run_cursor_manager(
    mut cursors: RangeCursorSet,
    rx: flume::Receiver<CursorDrained>,
    weak_ctx: std::sync::Weak<ConsumerContext>,
    record_tx: flume::Sender<Result<ConsumerRecord, ClientError>>,
) {
    // If the cursor set is empty from the start, disconnect immediately and exit.
    if cursors.is_empty() {
        return;
    }

    // Spawn the initial fetch loops.
    if let Some(ctx) = weak_ctx.upgrade() {
        for cursor in cursors.cursors() {
            let ctx_clone = ctx.clone();
            tokio::spawn(run_fetch_actor(
                cursor.range_id,
                cursor.next_entry_id,
                ctx_clone,
                record_tx.clone(),
            ));
        }
    }

    while let Ok(event) = rx.recv_async().await {
        let added = cursors.apply_drained(event.range_id, event.transition);

        if let Some(ctx) = weak_ctx.upgrade() {
            for new_cursor in added.iter() {
                let ctx_clone = ctx.clone();
                tokio::spawn(run_fetch_actor(
                    new_cursor.range_id,
                    new_cursor.next_entry_id,
                    ctx_clone,
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
