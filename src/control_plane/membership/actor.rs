use super::*;

use crate::channels::BatchSender;
use crate::control_plane::NodeAddress;
use crate::control_plane::NodeId;
use crate::control_plane::SwimNode;
use crate::control_plane::consensus::messages::MultiRaftActorCommand;
use crate::control_plane::membership::swim::Swim;
use crate::schedulers::actor::spawn_scheduling_actor;
use crate::schedulers::ticker::{PROBE_INTERVAL_TICKS, TICK_PERIOD_100_MS};
use crate::schedulers::ticker_message::{SchedulerSender, TickerCommand};

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
        ));
    }

    pub(crate) async fn run(
        mut mailbox: mpsc::Receiver<SwimActorCommand>,
        state: Swim,
        transport_tx: BatchSender<OutboundPacket>,
        scheduler_tx: SchedulerSender<SwimTimer>,
        raft_tx: mpsc::Sender<MultiRaftActorCommand>,
    ) {
        let mut actor = Self {
            state,
            transport_tx: transport_tx.into(),
            scheduler_tx,
            raft_tx,
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
                actor.state.dispatch(event);
            }
        }
    }

    async fn flush(&mut self) {
        for event in self.state.take_events() {
            match event {
                SwimEvent::Packet(pkt) => self.packets.push(pkt),
                SwimEvent::Timer(cmd) => self.timer_cmds.push(cmd.into()),
                SwimEvent::MembershipEvent(m) => {
                    if let Some(cmd) =
                        m.into_raft_command(&self.state.node_id, &self.state.topology)
                    {
                        let _ = self.raft_tx.send(cmd).await;
                    }
                }
            }
        }
        tokio::join!(
            self.transport_tx
                .send_batch(std::mem::take(&mut self.packets)),
            self.scheduler_tx
                .send_batch(std::mem::take(&mut self.timer_cmds)),
        );
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

    pub(crate) async fn get_shard_info(
        &self,
        key: Vec<u8>,
    ) -> anyhow::Result<Option<(ShardGroup, Option<ShardLeaderEntry>)>> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(SwimQueryCommand::GetShardInfo { key, reply: send })
            .await?;
        Ok(recv.await?)
    }

    pub(crate) async fn get_members(&self) -> anyhow::Result<Vec<SwimNode>> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.send(SwimQueryCommand::GetMembers { reply: send })
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
}

impl From<SwimSender> for mpsc::Sender<SwimActorCommand> {
    fn from(value: SwimSender) -> Self {
        value.0
    }
}
