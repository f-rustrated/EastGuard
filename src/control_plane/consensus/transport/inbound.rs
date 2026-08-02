use crate::control_plane::NodeId;
use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::control_plane::consensus::messages::InboundRaftRpc;
use crate::control_plane::consensus::messages::WireRaftMessage;
use crate::net::{TcpStream, TransportReadHalf, TransportTcpStream, TransportWriteHalf};
use crate::security::{
    AdmissionProof, CertificatePrincipal, SecurityHandle, node_certificate_principal,
};
use borsh::BorshDeserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::protocol::{
    AclSnapshotResponse, AdmissionLookupResponse, ClusterRequest, InitialClusterMessage,
    encode_frame,
};

pub(crate) struct ClusterMessageReader {
    read_half: TransportReadHalf,
    certificate_principal: Option<CertificatePrincipal>,
}

impl ClusterMessageReader {
    pub(crate) fn new(
        read_half: impl Into<TransportReadHalf>,
        certificate_principal: Option<CertificatePrincipal>,
    ) -> Self {
        Self {
            read_half: read_half.into(),
            certificate_principal,
        }
    }

    pub(crate) async fn read_frame<T: BorshDeserialize>(
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
        fields(peer = %peer, certificate_principal = ?self.certificate_principal)
    )]
    pub(super) async fn run(mut self, tx: MutlRaftSender, peer: NodeId) {
        loop {
            match self
                .read_frame::<WireRaftMessage>(4 * 1024 * 1024, "Raft message")
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

/// A Raft connection ready to enter the dispatcher writer map.
pub(super) struct AcceptedRaftConnection {
    pub(super) peer_id: NodeId,
    pub(super) initial_message: WireRaftMessage,
    pub(super) reader: ClusterMessageReader,
    pub(super) writer: TransportWriteHalf,
}

impl AcceptedRaftConnection {
    fn new(
        initial_message: WireRaftMessage,
        reader: ClusterMessageReader,
        writer: TransportWriteHalf,
    ) -> Self {
        Self {
            peer_id: initial_message.peer_id.clone(),
            initial_message,
            reader,
            writer,
        }
    }
}

/// Authenticates a cluster stream and handles its first request.
///
/// Admission lookups and ACL reads finish here. Only a verified Raft stream is
/// returned to the persistent connection dispatcher.
pub(super) async fn accept_cluster_connection(
    stream: TcpStream,
    security: SecurityHandle,
) -> anyhow::Result<Option<AcceptedRaftConnection>> {
    let stream = TransportTcpStream::accept(
        stream,
        security.node_transport(),
        node_certificate_principal,
    )
    .await?;

    let certificate_principal = stream.peer_principal();
    let tls_session_binding = if certificate_principal.is_some() {
        Some(stream.admission_binding()?)
    } else {
        None
    };
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = ClusterMessageReader::new(read_half, certificate_principal.clone());
    let InitialClusterMessage {
        admission_proof,
        request,
    } = reader
        .read_frame::<InitialClusterMessage>(4 * 1024 * 1024, "initial cluster message")
        .await?;

    let tls_session_binding = tls_session_binding
        .as_ref()
        .map(|binding| binding.as_slice());

    match request {
        ClusterRequest::AdmissionLookup(request) => {
            anyhow::ensure!(
                admission_proof.is_none(),
                "admission lookup carried a process proof"
            );
            let admission = security.read_admission(request).await?;
            write_half
                .write_all(&encode_frame(&AdmissionLookupResponse { admission })?)
                .await?;
            Ok(None)
        }
        ClusterRequest::Raft(message) => {
            verify_requester(
                certificate_principal.as_ref(),
                admission_proof.as_ref(),
                &message.peer_id,
                tls_session_binding,
                &security,
                &mut write_half,
            )
            .await?;
            Ok(Some(AcceptedRaftConnection::new(
                message, reader, write_half,
            )))
        }
        ClusterRequest::AclSnapshot(snapshot_req) => {
            verify_requester(
                certificate_principal.as_ref(),
                admission_proof.as_ref(),
                &snapshot_req.requester_node_id,
                tls_session_binding,
                &security,
                &mut write_half,
            )
            .await?;
            let snapshot = security.read_acl(snapshot_req).await?;
            write_half
                .write_all(&encode_frame(&AclSnapshotResponse { snapshot })?)
                .await?;
            Ok(None)
        }
    }
}

async fn verify_requester(
    certificate_principal: Option<&CertificatePrincipal>,
    admission_proof: Option<&AdmissionProof>,
    requester_node_id: &NodeId,
    tls_session_binding: Option<&[u8]>,
    security: &SecurityHandle,
    write_half: &mut TransportWriteHalf,
) -> anyhow::Result<()> {
    let Some(principal) = certificate_principal else {
        anyhow::ensure!(
            admission_proof.is_none(),
            "trusted-development connection carried an admission proof"
        );
        return Ok(());
    };
    let Some(proof) = admission_proof else {
        anyhow::bail!("secure cluster connection omitted process admission");
    };
    let Some(tls_session_binding) = tls_session_binding else {
        anyhow::bail!("secure connection has no TLS session binding");
    };
    let admission = security.lookup_admission(principal).await?;
    let peer_id = proof.verify_admission(&admission, principal, tls_session_binding)?;
    anyhow::ensure!(
        requester_node_id == &peer_id,
        "cluster requester differs from admitted process"
    );
    write_half
        .write_all(&encode_frame(
            &security.create_admission_proof(tls_session_binding)?,
        )?)
        .await?;
    Ok(())
}
