use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::control_plane::consensus::messages::InboundRaftRpc;
use crate::control_plane::consensus::messages::WireRaftMessage;
use crate::control_plane::NodeId;
use crate::net::OwnedReadHalf;
use tokio::io::AsyncReadExt;

pub(super) struct RaftRpcListener(pub(super) OwnedReadHalf);

impl RaftRpcListener {
    pub(super) async fn read_node_id(&mut self) -> anyhow::Result<NodeId> {
        let len = self.0.read_u32().await? as usize;
        anyhow::ensure!(len <= 1024, "NodeId frame too large: {len} bytes");
        let mut buf = vec![0u8; len];
        self.0.read_exact(&mut buf).await?;
        let id = borsh::from_slice::<NodeId>(&buf)?;
        Ok(id)
    }

    pub(super) async fn read_message(&mut self) -> anyhow::Result<WireRaftMessage> {
        let len = self.0.read_u32().await? as usize;
        anyhow::ensure!(
            len <= 4 * 1024 * 1024,
            "Raft message frame too large: {len} bytes"
        );
        let mut buf = vec![0u8; len];
        self.0.read_exact(&mut buf).await?;
        let msg = borsh::from_slice::<WireRaftMessage>(&buf)?;
        Ok(msg)
    }

    pub(super) async fn run(mut self, tx: MutlRaftSender) {
        loop {
            match self.read_message().await {
                Ok(msg) => {
                    let _ = tx
                        .send(InboundRaftRpc {
                            shard_group_id: msg.shard_group_id,
                            from: msg.sender,
                            rpc: msg.rpc,
                        })
                        .await;
                }
                Err(e) => {
                    tracing::debug!("RaftReader connection closed: {e}");
                    break;
                }
            }
        }
    }
}
