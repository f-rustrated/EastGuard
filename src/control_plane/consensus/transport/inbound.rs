use crate::control_plane::NodeId;
use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::control_plane::consensus::messages::InboundRaftRpc;
use crate::control_plane::consensus::messages::WireRaftMessage;
use crate::net::{TcpStream, TransportReadHalf, TransportTcpStream, TransportWriteHalf};
use crate::security::{NodeTransportSecurity, node_certificate_principal};
use borsh::BorshDeserialize;
use tokio::io::AsyncReadExt;

use super::protocol::MAX_CLUSTER_FRAME_SIZE;

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
/// Only a verified Raft stream reaches the persistent connection dispatcher.
pub(super) async fn accept_cluster_connection(
    stream: TcpStream,
    local_node_id: &NodeId,
    node_transport: &NodeTransportSecurity,
) -> anyhow::Result<AcceptedRaftConnection> {
    let mut stream = TransportTcpStream::accept(
        stream,
        node_transport,
        node_certificate_principal,
        super::CLUSTER_HANDSHAKE_TIMEOUT,
    )
    .await?;

    let peer = if node_transport.is_secure() {
        Some(
            node_transport
                .exchange_node_identity(&mut stream, local_node_id)
                .await?,
        )
    } else {
        None
    };
    let (read_half, write_half) = stream.into_split();
    let mut reader = ClusterMessageReader::new(read_half);
    let request = reader
        .read_frame::<WireRaftMessage>(MAX_CLUSTER_FRAME_SIZE, "cluster request")
        .await?;

    if let Some(peer) = peer {
        anyhow::ensure!(
            request.peer_id == peer,
            "cluster requester differs from authenticated node"
        );
    }
    Ok(AcceptedRaftConnection {
        initial_message: request,
        reader,
        writer: write_half,
    })
}
