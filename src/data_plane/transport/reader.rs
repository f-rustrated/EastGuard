use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

use crate::control_plane::NodeId;
use crate::data_plane::actor::DataPlaneSender;
use crate::data_plane::messages::command::{DataPlaneCommand, DataPlanePeerMessage};
use crate::net::OwnedReadHalf;

const NODE_ID_FRAME_MAX: usize = 1024;
const DATA_FRAME_MAX: usize = 64 * 1024 * 1024;

pub(super) struct DataReader(pub OwnedReadHalf);

impl DataReader {
    async fn read_frame<T: borsh::BorshDeserialize>(&mut self, max: usize) -> anyhow::Result<T> {
        let len = self.0.read_u32().await? as usize;
        anyhow::ensure!(len <= max, "frame too large: {len} bytes (max {max})");
        let mut buf = vec![0u8; len];
        self.0.read_exact(&mut buf).await?;
        let val = borsh::from_slice::<T>(&buf)?;
        Ok(val)
    }

    pub(crate) async fn read_node_id(&mut self) -> anyhow::Result<NodeId> {
        self.read_frame(NODE_ID_FRAME_MAX).await
    }

    pub(crate) async fn run(
        mut self,
        data_plane_tx: DataPlaneSender,
        peer: NodeId,
        disconnect_tx: mpsc::Sender<NodeId>,
    ) {
        loop {
            match self
                .read_frame::<DataPlanePeerMessage>(DATA_FRAME_MAX)
                .await
            {
                Ok(msg) => {
                    let _ = data_plane_tx.send(DataPlaneCommand::DataPlanePeerMessage(msg));
                }
                Err(e) => {
                    tracing::debug!("DataReader connection closed: {e}");
                    break;
                }
            }
        }

        // Read inbound frames until the connection closes, then signal the
        // transport to evict `peer`'s cached writer. Without this, a connection
        // that dies (peer reset, or the loser of a simultaneous-connect race) would
        // leave a half-open writer behind — `write_message` keeps succeeding into
        // the local TCP buffer and bytes silently vanish, so retries never recover.
        // Eviction forces the next send to reconnect; a spurious eviction only
        // costs one reconnect, so it's always safe.
        let _ = disconnect_tx.send(peer).await;
    }
}
