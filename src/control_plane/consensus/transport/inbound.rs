use crate::control_plane::NodeId;
use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::control_plane::consensus::messages::InboundRaftRpc;
use crate::control_plane::consensus::messages::WireRaftMessage;
use crate::net::TransportReadHalf;
use crate::security::TransportIdentity;
use borsh::BorshDeserialize;
use tokio::io::AsyncReadExt;

use super::protocol::{AclSnapshotResponse, InitialClusterMessage};

pub(super) struct ClusterMessageReader {
    read_half: TransportReadHalf,
    transport_identity: TransportIdentity,
}

impl ClusterMessageReader {
    pub(super) fn new(
        read_half: impl Into<TransportReadHalf>,
        transport_identity: TransportIdentity,
    ) -> Self {
        Self {
            read_half: read_half.into(),
            transport_identity,
        }
    }

    pub(super) async fn read_initial_message(&mut self) -> anyhow::Result<InitialClusterMessage> {
        self.read_frame(4 * 1024 * 1024, "initial cluster message")
            .await
    }

    pub(super) async fn read_raft_message(&mut self) -> anyhow::Result<WireRaftMessage> {
        self.read_frame(4 * 1024 * 1024, "Raft message").await
    }

    pub(super) async fn read_acl_snapshot_response(
        &mut self,
    ) -> anyhow::Result<AclSnapshotResponse> {
        self.read_frame(4 * 1024 * 1024, "ACL snapshot response")
            .await
    }

    async fn read_frame<T: BorshDeserialize>(
        &mut self,
        maximum_size: usize,
        frame_name: &str,
    ) -> anyhow::Result<T> {
        let len = self.read_half.read_u32().await? as usize;
        anyhow::ensure!(
            len <= maximum_size,
            "{frame_name} frame too large: {len} bytes"
        );
        let mut buf = vec![0u8; len];
        self.read_half.read_exact(&mut buf).await?;
        Ok(borsh::from_slice(&buf)?)
    }

    #[tracing::instrument(
        level = "trace",
        skip_all,
        fields(peer = %peer, transport_identity = ?self.transport_identity)
    )]
    pub(super) async fn run(mut self, tx: MutlRaftSender, peer: NodeId) {
        loop {
            match self.read_raft_message().await {
                Ok(message) => {
                    if message.sender != peer {
                        tracing::warn!(
                            transport_peer = %peer,
                            claimed_sender = %message.sender,
                            "rejected Raft message whose sender differs from the connection peer",
                        );
                        break;
                    }
                    let _ = tx
                        .send(InboundRaftRpc {
                            shard_group_id: message.shard_group_id,
                            peer_id: peer.clone(),
                            rpc: message.rpc,
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
