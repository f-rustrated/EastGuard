mod message;
mod state;
use state::*;

use std::time::Duration;

use anyhow::{Context, Result as AnyResult};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::control_plane::consensus::raft::states::security::AdmissionRecord;
use crate::control_plane::membership::actor::ShardRouting;
use crate::net::TransportTcpStream;
use crate::security::NodeTransportSecurity;

use message::*;

use super::inbound::ClusterMessageReader;
use super::protocol::{AdmissionLookupRequest, InitialClusterMessage, encode_frame};

const ADMISSION_FETCH_TIMEOUT: Duration = Duration::from_secs(3);
const ADMISSION_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);

const ADMISSION_LOOKUP_MAILBOX_CAPACITY: usize = 256;

/// Bounded admission-record reader used while cluster connections are admitted.
///
/// One actor owns the short-lived cache and combines simultaneous lookups for
/// the same principal. Local records come from this node's multi-Raft actor;
/// remote records use the limited admission-only connection.
pub(crate) struct AdmissionLookupActor;

impl AdmissionLookupActor {
    pub(crate) fn spawn(
        raft_tx: MutlRaftSender,
        security: NodeTransportSecurity,
    ) -> AdmissionLookupSender {
        let (sender, mailbox) = mpsc::channel(ADMISSION_LOOKUP_MAILBOX_CAPACITY);
        tokio::spawn(Self::run(raft_tx, security, mailbox));
        AdmissionLookupSender(sender)
    }

    async fn run(
        raft_tx: MutlRaftSender,
        security: NodeTransportSecurity,
        mut mailbox: mpsc::Receiver<LookupAdmission>,
    ) {
        let (completed_tx, mut completed_rx) =
            mpsc::channel::<AdmissionLookupCompleted>(MAX_IN_FLIGHT_LOOKUPS);
        let started_at = tokio::time::Instant::now();
        let mut state = AdmissionLookupState::default();
        let mut requests = Vec::with_capacity(64);

        loop {
            tokio::select! {
                count = mailbox.recv_many(&mut requests, 64) => {
                    if count == 0 {
                        break;
                    }
                    let now = started_at.elapsed();
                    for request in requests.drain(..) {
                        state.lookup(request, now);
                    }
                }
                Some(completed) = completed_rx.recv() => {
                    state.complete(completed, started_at.elapsed());
                }
            }

            for fetch in state.take_pending() {
                tokio::spawn(Self::fetch(
                    raft_tx.clone(),
                    security.clone(),
                    fetch,
                    completed_tx.clone(),
                ));
            }
        }
    }

    async fn fetch(
        raft_tx: MutlRaftSender,
        security: NodeTransportSecurity,
        fetch: AdmissionFetch,
        completed_tx: mpsc::Sender<AdmissionLookupCompleted>,
    ) {
        let key = fetch.key().clone();
        let res = match tokio::time::timeout(
            ADMISSION_FETCH_TIMEOUT,
            Self::fetch_inner(raft_tx, security, fetch),
        )
        .await
        {
            Ok(Ok(admission)) => Ok(admission),
            Ok(Err(error)) => {
                tracing::debug!(?key, "admission lookup failed: {error}");
                Err(AdmissionLookupUnavailable)
            }
            Err(_) => {
                tracing::debug!(?key, "admission lookup timed out");
                Err(AdmissionLookupUnavailable)
            }
        };

        let _ = completed_tx
            .send(AdmissionLookupCompleted { key, result: res })
            .await;
    }

    async fn fetch_inner(
        raft_tx: MutlRaftSender,
        security: NodeTransportSecurity,
        fetch: AdmissionFetch,
    ) -> AnyResult<Option<AdmissionRecord>> {
        match fetch {
            AdmissionFetch::Local(LocalAdmissionFetch(key)) => Ok(raft_tx
                .get_admission(key.shard_group_id, key.node_certificate_principal)
                .await?),
            AdmissionFetch::Remote(fetch) => {
                let stream =
                    TransportTcpStream::connect_node(fetch.owner.cluster_addr(), &security).await?;
                let transport_identity = stream.peer_identity();
                let (read_half, mut write_half) = stream.into_split();
                write_half
                    .write_all(&encode_frame(&InitialClusterMessage::AdmissionLookup(
                        AdmissionLookupRequest {
                            shard_group_id: fetch.key.shard_group_id,
                            node_certificate_principal: fetch.key.node_certificate_principal,
                        },
                    ))?)
                    .await
                    .context("write initial admission lookup request")?;

                let mut reader = ClusterMessageReader::new(read_half, transport_identity);
                Ok(reader.read_admission_lookup_response().await?.admission)
            }
        }
    }
}
/// Sends admission-record requests to [`AdmissionLookupActor`].
#[derive(Clone)]
pub(crate) struct AdmissionLookupSender(mpsc::Sender<LookupAdmission>);

impl AdmissionLookupSender {
    pub(crate) async fn lookup(
        &self,
        routing: ShardRouting,
        node_certificate_principal: Box<str>,
    ) -> Result<Option<AdmissionRecord>, AdmissionLookupUnavailable> {
        let fetch = match routing {
            ShardRouting::Local(group) => LocalAdmissionFetch(AdmissionLookupKey {
                shard_group_id: group.id,
                node_certificate_principal,
            })
            .into(),
            ShardRouting::Redirect(Some(remote)) => {
                let Some(owner) = remote.member else {
                    return Err(AdmissionLookupUnavailable);
                };
                RemoteAdmissionFetch {
                    key: AdmissionLookupKey {
                        shard_group_id: remote.group_id,
                        node_certificate_principal,
                    },
                    owner,
                }
                .into()
            }
            ShardRouting::Redirect(None) => return Err(AdmissionLookupUnavailable),
        };
        let (reply, recv) = oneshot::channel();
        self.0
            .try_send(LookupAdmission { fetch, reply })
            .map_err(|e| {
                tracing::debug!("admission lookup actor unavailable {}", e);
                AdmissionLookupUnavailable
            })?;

        match tokio::time::timeout(ADMISSION_REQUEST_TIMEOUT, recv).await {
            Ok(Ok(result)) => result,
            err => {
                tracing::debug!("admission lookup actor unavailable {:?}", err);
                Err(AdmissionLookupUnavailable)
            }
        }
    }
}
