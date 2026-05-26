use tokio::io::AsyncReadExt;

use crate::control_plane::{BINCODE_CONFIG, NodeId};
use crate::data_plane::messages::command::{DataPlaneCommand, DataPlaneInterNodeCommand};
use crate::net::OwnedReadHalf;

const NODE_ID_FRAME_MAX: usize = 1024;
const DATA_FRAME_MAX: usize = 64 * 1024 * 1024;

pub(super) struct DataReader(pub OwnedReadHalf);

impl DataReader {
    async fn read_frame<T: bincode::Decode<()>>(&mut self, max: usize) -> anyhow::Result<T> {
        let len = self.0.read_u32().await? as usize;
        anyhow::ensure!(len <= max, "frame too large: {len} bytes (max {max})");
        let mut buf = vec![0u8; len];
        self.0.read_exact(&mut buf).await?;
        let (val, _) = bincode::decode_from_slice::<T, _>(&buf, BINCODE_CONFIG)?;
        Ok(val)
    }

    pub(crate) async fn read_node_id(&mut self) -> anyhow::Result<NodeId> {
        self.read_frame(NODE_ID_FRAME_MAX).await
    }

    pub(crate) async fn run(mut self, data_plane_tx: crossbeam_channel::Sender<DataPlaneCommand>) {
        loop {
            match self
                .read_frame::<DataPlaneInterNodeCommand>(DATA_FRAME_MAX)
                .await
            {
                Ok(msg) => {
                    let _ = data_plane_tx.send(DataPlaneCommand::InterNode(msg));
                }
                Err(e) => {
                    tracing::debug!("DataReader connection closed: {e}");
                    break;
                }
            }
        }
    }
}
