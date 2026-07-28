use crate::control_plane::NodeId;
use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::control_plane::consensus::messages::InboundRaftRpc;
use crate::control_plane::consensus::messages::WireRaftMessage;
use crate::net::NodeReadHalf;
use crate::security::NodeTransportIdentity;
use tokio::io::AsyncReadExt;

pub(super) struct RaftRpcListener {
    read_half: NodeReadHalf,
    transport_identity: NodeTransportIdentity,
}

impl RaftRpcListener {
    pub(super) fn new(
        read_half: impl Into<NodeReadHalf>,
        transport_identity: NodeTransportIdentity,
    ) -> Self {
        Self {
            read_half: read_half.into(),
            transport_identity,
        }
    }

    pub(super) async fn read_node_id(&mut self) -> anyhow::Result<NodeId> {
        let len = self.read_half.read_u32().await? as usize;
        anyhow::ensure!(len <= 1024, "NodeId frame too large: {len} bytes");
        let mut buf = vec![0u8; len];
        self.read_half.read_exact(&mut buf).await?;
        let id = borsh::from_slice::<NodeId>(&buf)?;
        Ok(id)
    }

    pub(super) async fn read_message(&mut self) -> anyhow::Result<WireRaftMessage> {
        let len = self.read_half.read_u32().await? as usize;
        anyhow::ensure!(
            len <= 4 * 1024 * 1024,
            "Raft message frame too large: {len} bytes"
        );
        let mut buf = vec![0u8; len];
        self.read_half.read_exact(&mut buf).await?;
        let msg = borsh::from_slice::<WireRaftMessage>(&buf)?;
        Ok(msg)
    }

    #[tracing::instrument(
        level = "trace",
        skip_all,
        fields(peer = %peer, transport_identity = ?self.transport_identity)
    )]
    pub(super) async fn run(mut self, tx: MutlRaftSender, peer: NodeId) {
        loop {
            match self.read_message().await {
                Ok(msg) => {
                    if msg.sender != peer {
                        tracing::warn!(
                            transport_peer = %peer,
                            claimed_sender = %msg.sender,
                            "rejected Raft message whose sender differs from the connection peer",
                        );
                        break;
                    }
                    let _ = tx
                        .send(InboundRaftRpc {
                            shard_group_id: msg.shard_group_id,
                            from: peer.clone(),
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
