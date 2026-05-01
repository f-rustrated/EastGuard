use super::*;

use crate::clusters::NodeAddress;
use crate::clusters::NodeId;
use crate::clusters::raft::messages::MultiRaftActorCommand;
use crate::clusters::raft::messages::MultiRaftCommand;
use crate::clusters::swims::swim::Swim;
use crate::schedulers::ticker_message::TickerCommand;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;

// ==========================================
// PROTOCOL LAYER (SWIM Actor)
// ==========================================

pub struct SwimActor;

impl SwimActor {
    pub fn channel(buffer: usize) -> (SwimSender, mpsc::Receiver<SwimActorCommand>) {
        let (swim_sender, swim_mailbox) = mpsc::channel(buffer);
        (SwimSender(swim_sender), swim_mailbox)
    }
    pub async fn run(
        mut mailbox: mpsc::Receiver<SwimActorCommand>,
        mut state: Swim,
        transport_tx: mpsc::Sender<OutboundPacket>,
        scheduler_tx: mpsc::Sender<TickerCommand<SwimTimer>>,
        raft_tx: mpsc::Sender<MultiRaftActorCommand>,
    ) {
        tracing::info!("[{}] SwimActor started.", state.node_id);
        Self::flush(&mut state, &transport_tx, &scheduler_tx, &raft_tx).await;

        let mut buf = Vec::with_capacity(64);
        loop {
            if mailbox.recv_many(&mut buf, 64).await == 0 {
                break;
            }
            for event in buf.drain(..) {
                match event {
                    SwimActorCommand::Protocol(cmd) => match cmd {
                        SwimCommand::PacketReceived { src, packet } => state.step(src, packet),
                        SwimCommand::Timeout(event) => state.handle_timeout(event),
                        SwimCommand::AnnounceShardLeader(event) => state.announce_shard_leader(
                            event.shard_group_id,
                            event.leader_node_id,
                            event.term,
                        ),
                    },
                    SwimActorCommand::Query(q) => Self::handle_query(&state, q),
                }
            }
            Self::flush(&mut state, &transport_tx, &scheduler_tx, &raft_tx).await;
        }
    }

    fn handle_query(state: &Swim, command: SwimQueryCommand) {
        match command {
            SwimQueryCommand::GetMembers { reply } => {
                let _ = reply.send(state.get_members());
            }
            SwimQueryCommand::ResolveAddress { node_id, reply } => {
                let _ = reply.send(state.resolve_address(&node_id));
            }
            SwimQueryCommand::ResolveShardGroup { key, reply } => {
                let _ = reply.send(state.topology.shard_group_for(&key).cloned());
            }
            SwimQueryCommand::ResolveShardLeader {
                shard_group_id,
                reply,
            } => {
                let _ = reply.send(state.topology.shard_leader(shard_group_id).cloned());
            }
            SwimQueryCommand::GetShardInfo { key, reply } => {
                let result = state.topology.shard_group_for(&key).cloned().map(|group| {
                    let leader = state.topology.shard_leader(group.id).cloned();
                    (group, leader)
                });
                let _ = reply.send(result);
            }
        }
    }

    async fn flush(
        state: &mut Swim,
        transport_tx: &mpsc::Sender<OutboundPacket>,
        scheduler_tx: &mpsc::Sender<TickerCommand<SwimTimer>>,
        raft_tx: &mpsc::Sender<MultiRaftActorCommand>,
    ) {
        for event in state.take_events() {
            match event {
                SwimEvent::Packet(pkt) => {
                    let _ = transport_tx.send(pkt).await;
                }
                SwimEvent::Timer(cmd) => {
                    let _ = scheduler_tx.send(cmd.into()).await;
                }
                SwimEvent::Membership(m) => {
                    if let Some(cmd) = Self::to_raft_command(state, m) {
                        let _ = raft_tx.send(cmd).await;
                    }
                }
            }
        }
    }

    /// Translate a membership event into a MultiRaftActor command.
    /// Returns `None` for self-events and nodes with no shard groups.
    fn to_raft_command(state: &Swim, event: MembershipEvent) -> Option<MultiRaftActorCommand> {
        if *event.node_id() == state.node_id {
            return None;
        }

        match event {
            MembershipEvent::NodeDead { node_id } => Some(
                MultiRaftCommand::HandleNodeDeath {
                    dead_node_id: node_id,
                }
                .into(),
            ),
            MembershipEvent::NodeAlive { node_id, .. } => {
                let affected_groups: Vec<_> = state
                    .topology
                    .shard_groups_for_node(&node_id)
                    .into_iter()
                    .cloned()
                    .collect();
                if affected_groups.is_empty() {
                    return None;
                }
                Some(
                    MultiRaftCommand::HandleNodeJoin {
                        new_node_id: node_id,
                        affected_groups,
                    }
                    .into(),
                )
            }
        }
    }
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

    pub(crate) async fn resolve_shard_group(&self, resource_key: Vec<u8>) -> Option<ShardGroup> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(SwimQueryCommand::ResolveShardGroup {
            key: resource_key,
            reply: send,
        })
        .await
        .ok()?;
        recv.await.ok()?
    }
    pub(crate) async fn resolve_shard_leader(
        &self,
        shard_group_id: ShardGroupId,
    ) -> Option<ShardLeaderEntry> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(SwimQueryCommand::ResolveShardLeader {
            shard_group_id,
            reply: send,
        })
        .await
        .ok()?;
        recv.await.ok()?
    }

    pub(crate) async fn get_shard_info(
        &self,
        key: Vec<u8>,
    ) -> Option<(ShardGroup, Option<ShardLeaderEntry>)> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(SwimQueryCommand::GetShardInfo { key, reply: send })
            .await
            .ok()?;
        recv.await.ok()?
    }

    pub(crate) async fn resolve_address(&self, node_id: NodeId) -> Option<NodeAddress> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(SwimQueryCommand::ResolveAddress {
            node_id,
            reply: send,
        })
        .await
        .ok()?;
        recv.await.ok()?
    }
}

impl From<SwimSender> for mpsc::Sender<SwimActorCommand> {
    fn from(value: SwimSender) -> Self {
        value.0
    }
}
