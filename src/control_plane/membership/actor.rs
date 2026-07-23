use super::*;
use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::channels::BatchSender;
use crate::client::ServerError;
use crate::control_plane::NodeAddress;
use crate::control_plane::NodeAddressInfo;
use crate::control_plane::NodeId;
use crate::control_plane::SwimNode;
use crate::control_plane::consensus::messages::MultiRaftActorCommand;
use crate::control_plane::membership::swim::Swim;
use crate::control_plane::membership::topology::Topology;
use crate::schedulers::actor::spawn_scheduling_actor;
use crate::schedulers::ticker::{PROBE_INTERVAL_TICKS, TICK_PERIOD_100_MS};
use crate::schedulers::ticker_message::{SchedulerSender, TickerCommand};

#[cfg(any(test, debug_assertions))]
use crate::test_traits::TAssertInvariant;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
// ==========================================
// PROTOCOL LAYER (SWIM Actor)
// ==========================================

pub struct SwimActor {
    state: Swim,
    transport_tx: BatchSender<OutboundPacket>,
    scheduler_tx: SchedulerSender<SwimTimer>,
    raft_tx: mpsc::Sender<MultiRaftActorCommand>,

    /// Writer half of the shared topology snapshot. SwimActor is the sole
    /// writer; readers (MultiRaftActor, etc.) hold a `TopologyReader`.
    topology_pub: Arc<ArcSwap<Topology>>,

    packets: Vec<OutboundPacket>,
    timer_cmds: Vec<TickerCommand<SwimTimer>>,
}

impl SwimActor {
    pub fn channel(buffer: usize) -> (SwimSender, mpsc::Receiver<SwimActorCommand>) {
        let (swim_sender, swim_mailbox) = mpsc::channel(buffer);
        (SwimSender(swim_sender), swim_mailbox)
    }

    /// Spawn the SwimActor along with its dedicated scheduler. Returns the
    /// `SwimSender` for use as a callback target by other actors.
    pub fn spawn(
        sender: SwimSender,
        mailbox: mpsc::Receiver<SwimActorCommand>,
        state: Swim,
        transport_tx: impl Into<BatchSender<OutboundPacket>>,
        raft_tx: mpsc::Sender<MultiRaftActorCommand>,
        topology_pub: Arc<ArcSwap<Topology>>,
    ) {
        let scheduler_tx = spawn_scheduling_actor(
            sender.into(),
            64,
            TICK_PERIOD_100_MS,
            Some(PROBE_INTERVAL_TICKS),
        );
        tokio::spawn(Self::run(
            mailbox,
            state,
            transport_tx.into(),
            scheduler_tx,
            raft_tx,
            topology_pub,
        ));
    }

    pub(crate) async fn run(
        mut mailbox: mpsc::Receiver<SwimActorCommand>,
        state: Swim,
        transport_tx: BatchSender<OutboundPacket>,
        scheduler_tx: SchedulerSender<SwimTimer>,
        raft_tx: mpsc::Sender<MultiRaftActorCommand>,
        topology_pub: Arc<ArcSwap<Topology>>,
    ) {
        let mut actor = Self {
            state,
            transport_tx,
            scheduler_tx,
            raft_tx,
            topology_pub,
            packets: Vec::new(),
            timer_cmds: Vec::new(),
        };

        tracing::info!("[{}] SwimActor started.", actor.state.node_id);

        let mut buf = Vec::with_capacity(64);
        loop {
            actor.flush().await;

            if mailbox.recv_many(&mut buf, 64).await == 0 {
                break;
            }
            for event in buf.drain(..) {
                match event {
                    SwimActorCommand::Command(cmd) => actor.state.dispatch_command(cmd),
                    SwimActorCommand::Query(q) => actor.state.handle_query(q),
                }
            }

            // Publish a fresh snapshot if anything in this batch mutated topology.
            // Batched per iteration so several mutations in one mailbox drain.
            if actor.state.topology.take_dirty() {
                actor
                    .topology_pub
                    .store(Arc::new(actor.state.topology.clone()));
            }

            #[cfg(any(test, debug_assertions))]
            actor.state.assert_invariants();
        }
    }

    async fn flush(&mut self) {
        for event in self.state.take_events() {
            match event {
                SwimEvent::Packet(pkt) => self.packets.push(pkt),
                SwimEvent::Timer(cmd) => self.timer_cmds.push(cmd.into()),
                SwimEvent::MembershipEvent(m) => {
                    if let Some(cmd) = m.into_raft_command(&self.state.node_id) {
                        let _ = self.raft_tx.send(cmd).await;
                    }
                }
            }
        }
        tokio::join!(
            self.transport_tx
                .send_batch(std::mem::take(&mut self.packets).into_boxed_slice()),
            self.scheduler_tx
                .send_batch(std::mem::take(&mut self.timer_cmds).into_boxed_slice()),
        );
    }
}

/// Where a key routes relative to a node, resolved against the SWIM ring.
pub(crate) enum ShardRouting {
    /// This node hosts the key's shard group — proceed locally. Carries the group
    /// for callers that need its members/id (control-plane writes).
    Local(ShardGroup),
    /// Not local; redirect the client to a member. `None` until the ring/addresses
    /// converge here — still retriable.
    Redirect(Option<NodeAddressInfo>),
}

#[derive(Clone, Debug)]
pub struct SwimSender(mpsc::Sender<SwimActorCommand>);
impl SwimSender {
    #[inline]
    pub(crate) async fn send(
        &self,
        cmd: impl Into<SwimActorCommand>,
    ) -> Result<(), SendError<SwimActorCommand>> {
        self.0.send(cmd.into()).await
    }

    pub(crate) async fn resolve_shard_group(
        &self,
        resource_key: Vec<u8>,
    ) -> Result<Option<ShardGroup>, ServerError> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(QueryCommand::ResolveShardGroup {
            key: resource_key,
            reply: send,
        })
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;

        recv.await
            .map_err(|err| ServerError::Internal(err.to_string()))
    }

    pub(crate) async fn get_shard_info(
        &self,
        key: Vec<u8>,
    ) -> Result<Option<(ShardGroup, Option<ShardLeaderEntry>)>, ServerError> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(QueryCommand::GetShardInfo { key, reply: send })
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))?;
        recv.await
            .map_err(|err| ServerError::Internal(err.to_string()))
    }

    pub(crate) async fn get_members(&self) -> Result<Vec<SwimNode>, ServerError> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(QueryCommand::GetMembers { reply: send })
            .await
            .map_err(|err| ServerError::Internal(err.to_string()))?;

        recv.await
            .map_err(|err| ServerError::Internal(err.to_string()))
    }

    pub(crate) async fn resolve_address(
        &self,
        node_id: NodeId,
    ) -> Result<Option<NodeAddress>, ServerError> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(QueryCommand::ResolveAddress {
            node_id,
            reply: send,
        })
        .await
        .map_err(|err| ServerError::Internal(err.to_string()))?;

        recv.await
            .map_err(|err| ServerError::Internal(err.to_string()))
    }

    pub(crate) async fn resolve_any(
        &self,
        members: &[NodeId],
    ) -> Result<Option<NodeAddressInfo>, ServerError> {
        for member in members {
            if let Some(addr) = self.resolve_address(member.clone()).await? {
                return Ok(Some(NodeAddressInfo {
                    node_id: member.clone(),
                    addr,
                }));
            }
        }
        Ok(None)
    }

    /// Route a key relative to `node_id`: `Local` if it hosts the key's shard group,
    /// else a `Redirect` to a resolvable member (no hint until the ring converges).
    pub(crate) async fn resolve_shard_routing(
        &self,
        key: Vec<u8>,
        node_id: &NodeId,
    ) -> Result<ShardRouting, ServerError> {
        let Some(group) = self.resolve_shard_group(key).await? else {
            return Ok(ShardRouting::Redirect(None));
        };
        if group.replicas.contains(node_id) {
            return Ok(ShardRouting::Local(group));
        }
        let member = self.resolve_any(&group.replicas).await?;
        Ok(ShardRouting::Redirect(member))
    }

    pub(crate) async fn list_all_node_addresses(
        &self,
    ) -> Result<HashMap<NodeId, NodeAddress>, ServerError> {
        Ok(self
            .get_members()
            .await?
            .into_iter()
            .map(|m| (m.node_id, m.addr))
            .collect())
    }
}

impl From<SwimSender> for mpsc::Sender<SwimActorCommand> {
    fn from(value: SwimSender) -> Self {
        value.0
    }
}
