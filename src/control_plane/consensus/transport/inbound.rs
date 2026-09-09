use crate::control_plane::NodeId;
use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::control_plane::consensus::messages::InboundRaftRpc;
use crate::control_plane::consensus::messages::WireRaftMessage;
use crate::net::{TcpStream, TransportReadHalf, TransportTcpStream, TransportWriteHalf};
use crate::security::{SecurityHandle, node_certificate_principal};
use borsh::BorshDeserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::protocol::{
    AclSnapshotRequest, AclSnapshotResponse, ClusterRequest, MAX_CLUSTER_FRAME_SIZE, encode_frame,
};

pub(super) struct ClusterMessageReader {
    read_half: TransportReadHalf,
}

impl ClusterMessageReader {
    pub(super) fn new(read_half: impl Into<TransportReadHalf>) -> Self {
        Self {
            read_half: read_half.into(),
        }
    }

    pub(super) async fn read_frame<T: BorshDeserialize>(
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
        fields(peer = %peer)
    )]
    pub(super) async fn run(mut self, tx: MutlRaftSender, peer: NodeId) {
        loop {
            match self
                .read_frame::<WireRaftMessage>(MAX_CLUSTER_FRAME_SIZE, "Raft message")
                .await
            {
                Ok(message) => {
                    if message.peer_id != peer {
                        tracing::warn!(
                            transport_peer = %peer,
                            claimed_sender = %message.peer_id,
                            "rejected Raft message whose sender differs from the connection peer",
                        );
                        break;
                    }
                    let command = InboundRaftRpc {
                        shard_group_id: message.shard_group_id,
                        peer_id: peer.clone(),
                        rpc: message.rpc,
                    };
                    if tx.send(command).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::debug!("RaftReader connection closed: {e}");
                    break;
                }
            }
        }
    }
}

/// A Raft connection ready to enter the dispatcher writer map.
pub(super) struct AcceptedRaftConnection {
    pub(super) initial_message: WireRaftMessage,
    pub(super) reader: ClusterMessageReader,
    pub(super) writer: TransportWriteHalf,
}

/// Authenticates a cluster stream and handles its first request.
///
/// ACL reads finish here. Only a verified Raft stream is
/// returned to the persistent connection dispatcher.
pub(super) async fn accept_cluster_connection(
    stream: TcpStream,
    security: SecurityHandle,
) -> anyhow::Result<Option<AcceptedRaftConnection>> {
    let mut stream = TransportTcpStream::accept(
        stream,
        security.node_transport(),
        node_certificate_principal,
        super::CLUSTER_HANDSHAKE_TIMEOUT,
    )
    .await?;

    let peer = if security.node_transport().is_secure() {
        Some(
            security
                .node_transport()
                .exchange_node_identity(&mut stream, security.local_node_id())
                .await?,
        )
    } else {
        None
    };
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = ClusterMessageReader::new(read_half);
    let request = reader
        .read_frame::<ClusterRequest>(MAX_CLUSTER_FRAME_SIZE, "cluster request")
        .await?;

    match request {
        ClusterRequest::Raft(message) => {
            if let Some(peer) = peer {
                anyhow::ensure!(
                    message.peer_id == peer,
                    "cluster requester differs from authenticated node"
                );
            }
            Ok(Some(AcceptedRaftConnection {
                initial_message: message,
                reader,
                writer: write_half,
            }))
        }
        ClusterRequest::AclSnapshot(request) => {
            handle_acl_snapshot(request, &security, &mut write_half).await?;
            Ok(None)
        }
    }
}

async fn handle_acl_snapshot(
    request: AclSnapshotRequest,
    security: &SecurityHandle,
    write_half: &mut TransportWriteHalf,
) -> anyhow::Result<()> {
    let snapshot = security.read_local_acl(request.resource).await?;
    write_half
        .write_all(&encode_frame(&AclSnapshotResponse { snapshot })?)
        .await?;
    write_half.flush().await?;
    Ok(())
}
