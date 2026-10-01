use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::config::SecurityMode;
use crate::connections::protocol::ServerError;
use crate::control_plane::NodeId;
use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::control_plane::consensus::raft::states::security::AclRecord;
use crate::control_plane::membership::TopologyReader;
use crate::control_plane::membership::actor::SwimSender;
use crate::control_plane::metadata::AclResource;

use super::acl_messages::{
    AclCommand, AclEvent, AclFetchCompleted, AclRecordKey, AuthorizationRequestId, Authorize,
    ReadLocalAcl,
};
use super::acl_reader::AclReader;
use super::acl_state::AclState;
use super::{CertificatePrincipal, MAX_SECURITY_ID_BYTES, NodeTransportSecurity};

const MAILBOX_CAPACITY: usize = 256;
const MAX_ACTIVE_READS: usize = 16;
const AUTHORIZE_REQUEST_TIMEOUT: Duration = Duration::from_secs(22);
const LOCAL_READ_REQUEST_TIMEOUT: Duration = Duration::from_secs(6);

// Local reads reply directly; authorization reads return cache updates.
type AclRead = BoxFuture<'static, Option<AclFetchCompleted>>;

/// ACL requests only: no TLS credentials or connection identity.
#[derive(Clone)]
pub(crate) struct AclHandle {
    mode: SecurityMode,
    mailbox: mpsc::Sender<AclCommand>,
}

impl AclHandle {
    pub(crate) async fn authorize(
        &self,
        certificate_principal: Option<&CertificatePrincipal>,
        resource: AclResource,
    ) -> Result<(), ServerError> {
        let principal = match (self.mode, certificate_principal) {
            (SecurityMode::TrustedDevelopment, None) => return Ok(()),
            (SecurityMode::Secure, None) => return Err(ServerError::Unauthorized),
            (_, Some(principal)) => principal.clone(),
        };
        if !principal.has_valid_length()
            || !resource.has_valid_identifier_length(MAX_SECURITY_ID_BYTES)
        {
            return Err(ServerError::Unauthorized);
        }
        let (reply, receive) = oneshot::channel();
        if let Err(error) = self.send(Authorize {
            principal,
            resource,
            reply,
        }) {
            tracing::debug!("security authorization unavailable: {error}");
            return Err(ServerError::Unauthorized);
        }
        match tokio::time::timeout(AUTHORIZE_REQUEST_TIMEOUT, receive).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                tracing::debug!("ACL actor stopped before authorizing: {error}");
                Err(ServerError::Unauthorized)
            }
            Err(_) => {
                tracing::debug!("security authorization timed out");
                Err(ServerError::Unauthorized)
            }
        }
    }

    pub(crate) async fn read_local_acl(
        &self,
        resource: AclResource,
    ) -> Result<AclRecord, ServerError> {
        if !resource.has_valid_identifier_length(MAX_SECURITY_ID_BYTES) {
            return Err(ServerError::Internal(
                "ACL resource exceeds the security key limit".into(),
            ));
        }
        let (reply, receive) = oneshot::channel();
        self.send(ReadLocalAcl { resource, reply })
            .map_err(|error| ServerError::Internal(error.to_string()))?;
        tokio::time::timeout(LOCAL_READ_REQUEST_TIMEOUT, receive)
            .await
            .map_err(|_| ServerError::Internal("ACL read timed out".into()))?
            .map_err(|error| ServerError::Internal(error.to_string()))?
    }

    fn send(&self, command: impl Into<AclCommand>) -> Result<()> {
        self.mailbox
            .try_send(command.into())
            .context("ACL actor unavailable")
    }
}

/// Owns the ACL mailbox, read concurrency, and pending replies for one broker.
/// Access decisions belong to `AclState`; routing and I/O belong to `AclReader`.
pub(crate) struct AclActor {
    reader: AclReader,
    state: AclState,
    next_request_id: u64,
    authorization_replies:
        HashMap<AuthorizationRequestId, oneshot::Sender<Result<(), ServerError>>>,
    started_at: Instant,
}

impl AclActor {
    pub(crate) fn spawn(
        local_node_id: NodeId,
        swim_tx: SwimSender,
        raft_tx: MutlRaftSender,
        topology: TopologyReader,
        node_transport: NodeTransportSecurity,
    ) -> AclHandle {
        let (mailbox_tx, mailbox) = mpsc::channel(MAILBOX_CAPACITY);
        let started_at = Instant::now();
        let handle = AclHandle {
            mode: match node_transport {
                NodeTransportSecurity::Secure(_) => SecurityMode::Secure,
                NodeTransportSecurity::TrustedDevelopment => SecurityMode::TrustedDevelopment,
            },
            mailbox: mailbox_tx,
        };
        let actor = Self {
            reader: AclReader::new(local_node_id, swim_tx, raft_tx, topology, node_transport),
            state: AclState::default(),
            next_request_id: 0,
            authorization_replies: HashMap::new(),
            started_at,
        };
        tokio::spawn(actor.run(mailbox));
        handle
    }

    async fn run(mut self, mut mailbox: mpsc::Receiver<AclCommand>) {
        let mut commands = Vec::with_capacity(64);
        let mut reads = FuturesUnordered::<AclRead>::new();

        loop {
            self.flush(&mut reads);
            tokio::select! {
                count = mailbox.recv_many(&mut commands, 64) => {
                    if count == 0 {
                        break;
                    }
                    for command in commands.drain(..) {
                        match command {
                            AclCommand::Authorize(command) => self.authorize(command),
                            AclCommand::ReadLocalAcl(command) => {
                                self.schedule_local_acl_read(command, &mut reads);
                            }
                        }
                    }
                }
                Some(completion) = reads.next() => {
                    if let Some(completed) = completion {
                        self.state.complete_acl(completed, self.started_at.elapsed());
                    }
                }
            }
        }
    }

    fn next_request_id(&mut self) -> AuthorizationRequestId {
        let request_id = AuthorizationRequestId(self.next_request_id);
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .expect("authorization request ID exhausted");
        request_id
    }

    fn authorize(&mut self, command: Authorize) {
        match self.reader.resolve(command.resource) {
            Ok(key) => {
                let request_id = self.next_request_id();
                self.authorization_replies.insert(request_id, command.reply);
                self.state.authorize(
                    key,
                    command.principal,
                    request_id,
                    self.started_at.elapsed(),
                );
            }
            Err(error) => {
                tracing::debug!("ACL record routing failed: {error}");
                let _ = command.reply.send(Err(ServerError::Unauthorized));
            }
        }
    }

    fn schedule_local_acl_read(
        &self,
        command: ReadLocalAcl,
        reads: &mut FuturesUnordered<AclRead>,
    ) {
        if reads.len() >= MAX_ACTIVE_READS {
            let _ = command
                .reply
                .send(Err(ServerError::ShardNotLocal { hint_node: None }));
            return;
        }

        let reader = self.reader.clone();
        reads.push(Box::pin(async move {
            let result = reader.read_local_acl(command.resource).await;
            let _ = command.reply.send(result);
            None
        }));
    }

    fn flush(&mut self, reads: &mut FuturesUnordered<AclRead>) {
        loop {
            let events = self.state.take_events();
            if events.is_empty() {
                return;
            }
            for event in events {
                match event {
                    AclEvent::AuthorizationResolved(resolved) => {
                        if let Some(reply) = self.authorization_replies.remove(&resolved.request_id)
                        {
                            let result = if resolved.authorized {
                                Ok(())
                            } else {
                                Err(ServerError::Unauthorized)
                            };
                            let _ = reply.send(result);
                        }
                    }
                    AclEvent::AclFetchRequested(key) => {
                        if reads.len() < MAX_ACTIVE_READS {
                            reads.push(self.acl_fetch_future(key));
                        } else {
                            tracing::debug!("security record read limit reached");
                            let now = self.started_at.elapsed();
                            self.state.complete_acl(
                                AclFetchCompleted {
                                    key,
                                    result: Err(anyhow::anyhow!("ACL read limit reached")),
                                    requested_at: now,
                                },
                                now,
                            );
                        }
                    }
                }
            }
        }
    }

    fn acl_fetch_future(&self, key: AclRecordKey) -> AclRead {
        let reader = self.reader.clone();
        let started_at = self.started_at;
        Box::pin(async move {
            let requested_at = started_at.elapsed();
            let result = reader.fetch_acl(&key).await.inspect_err(|error| {
                tracing::debug!(?key, "ACL snapshot fetch failed: {error}");
            });
            Some(AclFetchCompleted {
                key,
                result,
                requested_at,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::consensus::actor::MultiRaftActor;
    use crate::control_plane::consensus::messages::MultiRaftActorCommand;
    use crate::control_plane::membership::actor::SwimActor;
    use crate::control_plane::membership::{Topology, TopologyConfig};

    #[tokio::test]
    async fn only_trusted_development_allows_a_missing_principal() {
        for mode in [SecurityMode::Secure, SecurityMode::TrustedDevelopment] {
            let (mailbox, receiver) = mpsc::channel(1);
            let handle = AclHandle { mode, mailbox };
            let expected = match mode {
                SecurityMode::Secure => Err(ServerError::Unauthorized),
                SecurityMode::TrustedDevelopment => Ok(()),
            };
            assert_eq!(handle.authorize(None, AclResource::Cluster).await, expected);

            // A supplied principal must never bypass ACLs, even in development.
            drop(receiver);
            assert_eq!(
                handle
                    .authorize(
                        Some(&CertificatePrincipal::new("client")),
                        AclResource::Cluster
                    )
                    .await,
                Err(ServerError::Unauthorized),
            );
        }
    }

    #[tokio::test]
    async fn local_acl_reply_rechecks_ownership_after_reading() {
        for move_owner in [false, true] {
            let local = NodeId::new("node-1");
            let (swim_tx, _swim_mailbox) = SwimActor::channel(1);
            let (raft_tx, mut raft_mailbox) = MultiRaftActor::channel(1);
            let (publisher, topology) = Topology::new(
                [local.clone()],
                TopologyConfig {
                    vnodes_per_pnode: 1,
                    replication_factor: 1,
                },
            )
            .channel();
            let handle = AclActor::spawn(
                local,
                swim_tx,
                raft_tx,
                topology,
                NodeTransportSecurity::TrustedDevelopment,
            );
            let read =
                tokio::spawn(async move { handle.read_local_acl(AclResource::Cluster).await });
            let Some(MultiRaftActorCommand::GetAclSnapshot(query)) = raft_mailbox.recv().await
            else {
                panic!("expected a local ACL snapshot query");
            };
            if move_owner {
                publisher.store(std::sync::Arc::new(Topology::new(
                    [NodeId::new("node-2")],
                    TopologyConfig {
                        vnodes_per_pnode: 1,
                        replication_factor: 1,
                    },
                )));
            }
            let record = AclRecord {
                resource: query.resource,
                revision: 7,
                principals: Box::new([]),
            };
            query.reply.send(Ok(record.clone())).unwrap();

            let result = read.await.unwrap();
            if move_owner {
                assert_eq!(result, Err(ServerError::ShardNotLocal { hint_node: None }));
            } else {
                assert_eq!(result, Ok(record));
            }
        }
    }

    #[tokio::test]
    async fn dropping_the_last_handle_stops_the_actor() {
        let local_node_id = NodeId::new("node-1");
        let (swim_tx, _swim_mailbox) = SwimActor::channel(1);
        let (raft_tx, _raft_mailbox) = MultiRaftActor::channel(1);
        let topology = Topology::new(
            [local_node_id.clone()],
            TopologyConfig {
                vnodes_per_pnode: 1,
                replication_factor: 1,
            },
        )
        .channel()
        .1;
        let handle = AclActor::spawn(
            local_node_id,
            swim_tx,
            raft_tx,
            topology,
            NodeTransportSecurity::TrustedDevelopment,
        );
        let mailbox = handle.mailbox.downgrade();

        drop(handle);

        tokio::time::timeout(Duration::from_secs(1), async {
            while mailbox.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ACL actor retained its own mailbox sender");
    }
}
