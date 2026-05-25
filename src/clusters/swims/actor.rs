use super::*;

use crate::clusters::NodeAddress;
use crate::clusters::NodeId;
use crate::clusters::raft::messages::MultiRaftActorCommand;
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
        transport_tx: mpsc::Sender<Box<[OutboundPacket]>>,
        scheduler_tx: mpsc::Sender<Box<[TickerCommand<SwimTimer>]>>,
        raft_tx: mpsc::Sender<MultiRaftActorCommand>,
    ) {
        tracing::info!("[{}] SwimActor started.", state.node_id);

        let mut buf = Vec::with_capacity(64);
        loop {
            Self::flush_events(&mut state, &transport_tx, &scheduler_tx, &raft_tx).await;

            if mailbox.recv_many(&mut buf, 64).await == 0 {
                break;
            }
            for event in buf.drain(..) {
                state.dispatch(event);
            }
        }
    }

    async fn flush_events(
        state: &mut Swim,
        transport_tx: &mpsc::Sender<Box<[OutboundPacket]>>,
        scheduler_tx: &mpsc::Sender<Box<[TickerCommand<SwimTimer>]>>,
        raft_tx: &mpsc::Sender<MultiRaftActorCommand>,
    ) {
        let mut packets = Vec::new();
        let mut timer_cmds = Vec::new();
        for event in state.take_events() {
            match event {
                SwimEvent::Packet(pkt) => packets.push(pkt),
                SwimEvent::Timer(cmd) => timer_cmds.push(cmd.into()),
                SwimEvent::Membership(m) => {
                    if let Some(cmd) = m.into_raft_command(&state.node_id, &state.topology) {
                        let _ = raft_tx.send(cmd).await;
                    }
                }
            }
        }
        if !packets.is_empty() {
            let _ = transport_tx.send(packets.into_boxed_slice()).await;
        }
        if !timer_cmds.is_empty() {
            let _ = scheduler_tx.send(timer_cmds.into_boxed_slice()).await;
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

    pub(crate) async fn resolve_shard_group(
        &self,
        resource_key: Vec<u8>,
    ) -> anyhow::Result<Option<ShardGroup>> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(SwimQueryCommand::ResolveShardGroup {
            key: resource_key,
            reply: send,
        })
        .await?;
        Ok(recv.await?)
    }

    pub(crate) async fn resolve_shard_leader(
        &self,
        shard_group_id: ShardGroupId,
    ) -> anyhow::Result<Option<ShardLeaderEntry>> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(SwimQueryCommand::ResolveShardLeader {
            shard_group_id,
            reply: send,
        })
        .await?;
        Ok(recv.await?)
    }

    pub(crate) async fn get_shard_info(
        &self,
        key: Vec<u8>,
    ) -> anyhow::Result<Option<(ShardGroup, Option<ShardLeaderEntry>)>> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(SwimQueryCommand::GetShardInfo { key, reply: send })
            .await?;
        Ok(recv.await?)
    }

    pub(crate) async fn resolve_address(
        &self,
        node_id: NodeId,
    ) -> anyhow::Result<Option<NodeAddress>> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(SwimQueryCommand::ResolveAddress {
            node_id,
            reply: send,
        })
        .await?;
        Ok(recv.await?)
    }
}

impl From<SwimSender> for mpsc::Sender<SwimActorCommand> {
    fn from(value: SwimSender) -> Self {
        value.0
    }
}
