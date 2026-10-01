use std::time::Duration;

use anyhow::{Context, Result};
use tokio::time::Instant;

use crate::connections::protocol::ServerError;
use crate::control_plane::consensus::actor::MutlRaftSender;
use crate::control_plane::consensus::raft::states::security::AclRecord;
use crate::control_plane::consensus::transport::OutboundClusterConnection;
use crate::control_plane::membership::TopologyReader;
use crate::control_plane::membership::actor::SwimSender;
use crate::control_plane::metadata::AclResource;
use crate::control_plane::{NodeId, Replicas};
use crate::net::TransportTcpStream;

use super::acl_messages::AclRecordKey;
use super::{MAX_SECURITY_ID_BYTES, NodeTransportSecurity};

const LOCAL_ACL_READ_TIMEOUT: Duration = Duration::from_secs(5);
// Includes address resolution, mTLS, and the target's quorum-backed ACL read.
const REMOTE_ACL_FETCH_TIMEOUT: Duration = Duration::from_secs(20);

/// Resolves ACL ownership and reads from local Raft or a remote owner.
/// Every successful read is rechecked against the current topology before returning.
#[derive(Clone)]
pub(super) struct AclReader {
    local_node_id: NodeId,
    topology: TopologyReader,
    swim_tx: SwimSender,
    raft_tx: MutlRaftSender,
    node_transport: NodeTransportSecurity,
}

impl AclReader {
    pub(super) fn new(
        local_node_id: NodeId,
        swim_tx: SwimSender,
        raft_tx: MutlRaftSender,
        topology: TopologyReader,
        node_transport: NodeTransportSecurity,
    ) -> Self {
        Self {
            local_node_id,
            topology,
            swim_tx,
            raft_tx,
            node_transport,
        }
    }

    pub(super) fn resolve(&self, resource: AclResource) -> Result<AclRecordKey> {
        anyhow::ensure!(
            resource.has_valid_identifier_length(MAX_SECURITY_ID_BYTES),
            "ACL resource exceeds the security key limit"
        );
        let group = self
            .topology
            .shard_group_for(&resource.routing_key())
            .context("security shard is unresolved")?;
        Ok(AclRecordKey {
            shard_group_id: group.id,
            resource,
        })
    }

    // Replica selection belongs to the read, not to cached authorization decisions.
    fn candidates(&self, key: &AclRecordKey) -> Result<Replicas> {
        let group = self
            .topology
            .shard_group_for(&key.resource.routing_key())
            .context("security shard is unresolved")?;
        anyhow::ensure!(
            group.id == key.shard_group_id,
            "ACL ownership changed before read"
        );
        let preferred = self
            .topology
            .shard_leader(group.id)
            .map(|leader| leader.leader.node_id)
            .filter(|leader| group.replicas.contains(leader))
            .or_else(|| {
                group
                    .replicas
                    .contains(&self.local_node_id)
                    .then(|| self.local_node_id.clone())
            });
        let mut candidates = group.replicas;
        if let Some(position) = preferred
            .as_ref()
            .and_then(|preferred| candidates.iter().position(|node| node == preferred))
        {
            candidates.change_leader(position);
        }
        Ok(candidates)
    }

    fn is_local_acl(&self, key: &AclRecordKey) -> bool {
        self.topology
            .shard_group_for(&key.resource.routing_key())
            .is_some_and(|group| {
                group.id == key.shard_group_id && group.replicas.contains(&self.local_node_id)
            })
    }

    pub(super) async fn read_local_acl(
        &self,
        resource: AclResource,
    ) -> Result<AclRecord, ServerError> {
        let key = self
            .resolve(resource)
            .map_err(|_| ServerError::ShardNotLocal { hint_node: None })?;
        if !self.is_local_acl(&key) {
            return Err(ServerError::ShardNotLocal { hint_node: None });
        }
        let result = tokio::time::timeout(
            LOCAL_ACL_READ_TIMEOUT,
            self.raft_tx
                .get_acl_snapshot(key.shard_group_id, key.resource.clone()),
        )
        .await
        .map_err(|_| ServerError::Internal("ACL read timed out".into()))?;
        if !self.is_local_acl(&key) {
            return Err(ServerError::ShardNotLocal { hint_node: None });
        }
        result
    }

    pub(super) async fn fetch_acl(&self, key: &AclRecordKey) -> Result<AclRecord> {
        let mut last_error = None;
        let deadline = Instant::now() + REMOTE_ACL_FETCH_TIMEOUT;
        let candidates = self.candidates(key)?;
        for (index, candidate) in candidates.iter().enumerate() {
            let remaining = u32::try_from(candidates.len() - index).unwrap_or(u32::MAX);
            let budget = deadline.saturating_duration_since(Instant::now()) / remaining;
            match self.fetch_acl_from(key, candidate, budget).await {
                Ok(acl) => {
                    anyhow::ensure!(
                        self.resolve(key.resource.clone())? == *key,
                        "ACL ownership changed during read"
                    );
                    return Ok(acl);
                }
                Err(error) => {
                    tracing::debug!(%candidate, "security ACL candidate failed: {error}");
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("security shard has no candidates")))
    }

    async fn fetch_acl_from(
        &self,
        key: &AclRecordKey,
        candidate: &NodeId,
        budget: Duration,
    ) -> Result<AclRecord> {
        tokio::time::timeout(budget, async {
            if candidate == &self.local_node_id {
                return Ok(self
                    .raft_tx
                    .get_acl_snapshot(key.shard_group_id, key.resource.clone())
                    .await?);
            }
            let address = self
                .swim_tx
                .resolve_address(candidate.clone())
                .await?
                .with_context(|| format!("security shard candidate {candidate} has no address"))?;
            let stream =
                TransportTcpStream::connect_node(address.cluster_addr(), &self.node_transport)
                    .await?;
            let mut connection = OutboundClusterConnection::new(
                stream,
                candidate,
                &self.local_node_id,
                &self.node_transport,
            )
            .await?;
            connection.read_acl_snapshot(key.resource.clone()).await
        })
        .await
        .context("security shard candidate timed out")?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane::NodeAddress;
    use crate::control_plane::consensus::actor::MultiRaftActor;
    use crate::control_plane::consensus::messages::MultiRaftActorCommand;
    use crate::control_plane::membership::actor::SwimActor;
    use crate::control_plane::membership::messages::dissemination_buffer::ShardLeaderInfo;
    use crate::control_plane::membership::{Topology, TopologyConfig};
    use crate::control_plane::metadata::{ConsumerGroupResource, TopicId};

    fn acl_reader(local_node_id: NodeId, topology: TopologyReader) -> AclReader {
        AclReader::new(
            local_node_id,
            SwimActor::channel(1).0,
            MultiRaftActor::channel(1).0,
            topology,
            NodeTransportSecurity::TrustedDevelopment,
        )
    }

    #[test]
    fn stalled_candidate_leaves_time_for_fallback() -> turmoil::Result {
        let mut sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(25))
            .build();
        sim.client("node", async {
            let local = NodeId::new("node-1");
            let stalled = NodeId::new("stalled-peer");
            let mut topology = Topology::new(
                [local.clone(), stalled.clone()],
                TopologyConfig {
                    vnodes_per_pnode: 1,
                    replication_factor: 2,
                },
            );
            topology.update_shard_leader(&ShardLeaderInfo {
                shard_group_id: topology
                    .shard_group_for(&AclResource::Cluster.routing_key())
                    .unwrap()
                    .id,
                leader_node_id: stalled,
                leader_addr: NodeAddress::test(
                    "127.0.0.1:9000".parse().unwrap(),
                    "127.0.0.1:9001".parse().unwrap(),
                ),
                term: 1,
            });
            let mut reader = acl_reader(local.clone(), topology.channel().1);
            let (swim_tx, _stalled_mailbox) = SwimActor::channel(1);
            reader.swim_tx = swim_tx;
            let (raft_tx, mut raft_mailbox) = MultiRaftActor::channel(1);
            reader.raft_tx = raft_tx;
            let responder = tokio::spawn(async move {
                let Some(MultiRaftActorCommand::GetAclSnapshot(query)) = raft_mailbox.recv().await
                else {
                    panic!("expected fallback to the local ACL reader");
                };
                let _ = query.reply.send(Ok(AclRecord {
                    resource: query.resource,
                    revision: 7,
                    principals: Box::new([]),
                }));
            });
            let key = reader.resolve(AclResource::Cluster)?;
            let started_at = Instant::now();

            assert_eq!(reader.fetch_acl(&key).await?.revision, 7);
            assert!(started_at.elapsed() >= REMOTE_ACL_FETCH_TIMEOUT / 2);
            assert!(started_at.elapsed() < REMOTE_ACL_FETCH_TIMEOUT);
            responder.await?;
            Ok(())
        });
        sim.run()
    }

    #[test]
    fn known_shard_leader_is_the_first_bounded_read_candidate() {
        let local = NodeId::new("node-1");
        let leader = NodeId::new("node-2");
        let resource = AclResource::Cluster;
        let key = resource.routing_key();
        let mut topology = Topology::new(
            [local.clone(), leader.clone()],
            TopologyConfig {
                vnodes_per_pnode: 1,
                replication_factor: 2,
            },
        );
        let group_id = topology.shard_group_for(&key).unwrap().id;
        topology.update_shard_leader(&ShardLeaderInfo {
            shard_group_id: group_id,
            leader_node_id: leader.clone(),
            leader_addr: NodeAddress::test(
                "127.0.0.1:9000".parse().unwrap(),
                "127.0.0.1:9001".parse().unwrap(),
            ),
            term: 1,
        });
        let reader = acl_reader(local.clone(), topology.channel().1);

        let candidates = reader
            .candidates(&reader.resolve(resource).unwrap())
            .unwrap();
        assert_eq!(candidates.first(), Some(&leader));
        assert_eq!(candidates.len(), 2);
        assert!(candidates.contains(&local));
    }

    #[test]
    fn stale_shard_leader_outside_the_replica_set_is_ignored() {
        let local = NodeId::new("node-1");
        let remote = NodeId::new("node-2");
        let resource = AclResource::Cluster;
        let key = resource.routing_key();
        let mut topology = Topology::new(
            [local.clone(), remote.clone()],
            TopologyConfig {
                vnodes_per_pnode: 1,
                replication_factor: 2,
            },
        );
        let group = topology.shard_group_for(&key).unwrap().clone();
        topology.update_shard_leader(&ShardLeaderInfo {
            shard_group_id: group.id,
            leader_node_id: NodeId::new("removed-node"),
            leader_addr: NodeAddress::test(
                "127.0.0.1:9000".parse().unwrap(),
                "127.0.0.1:9001".parse().unwrap(),
            ),
            term: 1,
        });
        let reader = acl_reader(local.clone(), topology.channel().1);

        let candidates = reader
            .candidates(&reader.resolve(resource).unwrap())
            .unwrap();
        assert_eq!(candidates.first(), Some(&local));
        assert_eq!(candidates.len(), 2);
        assert!(candidates.contains(&remote));
    }

    #[test]
    fn oversized_security_keys_are_rejected_before_routing() {
        let local = NodeId::new("node-1");
        let topology = Topology::new(
            [local.clone()],
            TopologyConfig {
                vnodes_per_pnode: 1,
                replication_factor: 1,
            },
        );
        let reader = acl_reader(local, topology.channel().1);

        assert!(
            reader
                .resolve(AclResource::ConsumerGroup(ConsumerGroupResource {
                    topic_id: TopicId(1),
                    group_id: "x".repeat(super::super::MAX_SECURITY_ID_BYTES + 1),
                }))
                .is_err()
        );
    }

    #[tokio::test]
    async fn acl_route_change_invalidates_an_inflight_read() {
        let resource = AclResource::Cluster;
        let routing_key = resource.routing_key();
        let old_topology = Topology::new(
            [NodeId::new("node-1")],
            TopologyConfig {
                vnodes_per_pnode: 1,
                replication_factor: 1,
            },
        );
        let (topology_publisher, topology) = old_topology.channel();
        let mut reader = acl_reader(NodeId::new("node-1"), topology);
        let key = reader.resolve(resource).unwrap();
        let (raft_tx, mut mailbox) = MultiRaftActor::channel(1);
        reader.raft_tx = raft_tx;
        let read = {
            let reader = reader.clone();
            let key = key.clone();
            tokio::spawn(async move { reader.fetch_acl(&key).await })
        };
        let Some(MultiRaftActorCommand::GetAclSnapshot(query)) = mailbox.recv().await else {
            panic!("expected an ACL snapshot query");
        };

        let new_topology = Topology::new(
            [NodeId::new("node-2")],
            TopologyConfig {
                vnodes_per_pnode: 1,
                replication_factor: 1,
            },
        );
        assert_ne!(
            key.shard_group_id,
            new_topology.shard_group_for(&routing_key).unwrap().id
        );
        topology_publisher.store(std::sync::Arc::new(new_topology));

        // A queued read must also reject an owner change before starting any I/O.
        assert_eq!(
            reader.fetch_acl(&key).await.unwrap_err().to_string(),
            "ACL ownership changed before read",
        );
        assert!(mailbox.try_recv().is_err());

        query
            .reply
            .send(Ok(AclRecord {
                resource: query.resource,
                revision: 1,
                principals: Box::new([]),
            }))
            .unwrap();
        assert_eq!(
            read.await.unwrap().unwrap_err().to_string(),
            "ACL ownership changed during read",
        );
    }
}
