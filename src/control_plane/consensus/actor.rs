#![allow(dead_code)]

use std::collections::HashMap;

use crate::channels::BatchSender;
use crate::control_plane::NodeId;
use crate::control_plane::consensus::messages::*;
use crate::control_plane::consensus::multi_raft::MultiRaft;
use crate::control_plane::consensus::raft::storage::RaftStorage;
use crate::control_plane::membership::actor::SwimSender;
use crate::control_plane::membership::{ShardGroupId, SwimCommand, TopologyReader};
use crate::control_plane::metadata::MetadataCommand;
use crate::data_plane::transport::command::DataTransportCommand;
use crate::schedulers::actor::spawn_scheduling_actor;
use crate::schedulers::ticker::{PROBE_INTERVAL_TICKS, TICK_PERIOD_100_MS};
use crate::schedulers::ticker_message::{SchedulerSender, TickerCommand};

use tokio::sync::mpsc;

pub struct MultiRaftActor {
    store: MultiRaft,
    transport_tx: BatchSender<RaftTransportCommand>,
    scheduler_tx: SchedulerSender<RaftTimer>,
    swim_tx: SwimSender,
    data_transport_tx: BatchSender<DataTransportCommand>,

    packets_by_target: HashMap<NodeId, Vec<OutboundRaftPacket>>,
    timer_cmds: Vec<TickerCommand<RaftTimer>>,
    transport_cmds: Vec<RaftTransportCommand>,
    data_transport_cmds: Vec<DataTransportCommand>,
}

impl MultiRaftActor {
    pub fn channel(buffer: usize) -> (MutlRaftSender, mpsc::Receiver<MultiRaftActorCommand>) {
        let (tx, rx) = mpsc::channel(buffer);
        (MutlRaftSender(tx), rx)
    }

    /// Spawn the MultiRaftActor along with its dedicated scheduler. The scheduler
    /// capacity is sized for `vnodes_per_node`: each vnode can have both an
    /// election and heartbeat timer active, so capacity must comfortably exceed
    /// `vnodes_per_node * 2`; `* 16` gives ample headroom.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        sender: MutlRaftSender,
        mailbox: mpsc::Receiver<MultiRaftActorCommand>,
        node_id: NodeId,
        election_jitter_seed: u64,
        storage: Box<dyn RaftStorage>,
        vnodes_per_node: usize,
        transport_tx: impl Into<BatchSender<RaftTransportCommand>>,
        swim_tx: SwimSender,
        data_transport_tx: impl Into<BatchSender<DataTransportCommand>>,
        topology: TopologyReader,
    ) {
        let scheduler_tx = spawn_scheduling_actor::<RaftTimer, MultiRaftActorCommand>(
            sender.into(),
            vnodes_per_node * 16,
            TICK_PERIOD_100_MS,
            Some(PROBE_INTERVAL_TICKS),
        );
        tokio::spawn(Self::run(
            node_id,
            election_jitter_seed,
            storage,
            mailbox,
            transport_tx.into(),
            scheduler_tx,
            swim_tx,
            data_transport_tx.into(),
            topology,
        ));
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn run(
        node_id: NodeId,
        election_jitter_seed: u64,
        storage: Box<dyn RaftStorage>,
        mut mailbox: mpsc::Receiver<MultiRaftActorCommand>,
        transport_tx: BatchSender<RaftTransportCommand>,
        scheduler_tx: SchedulerSender<RaftTimer>,
        swim_tx: SwimSender,
        data_transport_tx: BatchSender<DataTransportCommand>,
        topology: TopologyReader,
    ) {
        let mut actor = Self {
            store: MultiRaft::new(node_id, election_jitter_seed, storage, topology),
            transport_tx,
            scheduler_tx,
            swim_tx,
            data_transport_tx,
            packets_by_target: HashMap::new(),
            timer_cmds: Vec::new(),
            transport_cmds: Vec::new(),
            data_transport_cmds: Vec::new(),
        };
        let mut buf = Vec::with_capacity(64);

        loop {
            if mailbox.recv_many(&mut buf, 64).await == 0 {
                break;
            }
            for cmd in buf.drain(..) {
                actor.store.process(cmd);
            }
            actor.flush().await;
        }
    }

    async fn flush(&mut self) {
        // Reconciliation proposals created during route_event re-enter
        // store as new dirty groups; loop until store yields no further events.
        // Bounded to prevent runaway in case of a feedback bug.
        // ! The MAX_FLUSH_ROUNDS = 8 bound is purely defensive
        // ! there's no scenario in the current code that should produce a feedback chain longer than 2 or 3.
        const MAX_FLUSH_ROUNDS: u32 = 8;
        for _ in 0..MAX_FLUSH_ROUNDS {
            let events = self.store.flush();
            if events.is_empty() {
                break;
            }
            for event in events {
                self.route_event(event).await;
            }
        }
        for packets in self.packets_by_target.drain() {
            self.transport_cmds
                .push(RaftTransportCommand::Send(packets.1));
        }

        tokio::join!(
            self.transport_tx
                .send_batch(std::mem::take(&mut self.transport_cmds)),
            self.scheduler_tx
                .send_batch(std::mem::take(&mut self.timer_cmds)),
            self.data_transport_tx
                .send_batch(std::mem::take(&mut self.data_transport_cmds)),
        );

        self.store.fire_deferred();
    }

    async fn route_event(&mut self, event: RaftEvent) {
        match event {
            RaftEvent::OutboundRaftPacket(pkt) => {
                self.packets_by_target
                    .entry(pkt.target.clone())
                    .or_default()
                    .push(pkt);
            }
            RaftEvent::Timer(cmd) => self.timer_cmds.push(cmd.into()),
            RaftEvent::LeaderChange(lc) => {
                // On becoming leader, run both halves of takeover reconciliation:
                // - replace any peer that SWIM no longer considers alive
                // - roll any active segment whose replica set still names a non-live node
                if lc.leader_node_id == *self.store.node_id() {
                    self.store.reconcile_on_leadership_change(lc.shard_group_id);
                }
                let _ = self
                    .swim_tx
                    .send(SwimCommand::AnnounceShardLeader(lc))
                    .await;
            }
            RaftEvent::DisconnectPeer(node_id) => {
                self.transport_cmds
                    .push(RaftTransportCommand::DisconnectPeer(node_id));
            }
            RaftEvent::MetadataCommitted(committed) => {
                self.data_transport_cmds
                    .extend(committed.into_data_transport_cmds());
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct MutlRaftSender(mpsc::Sender<MultiRaftActorCommand>);

impl MutlRaftSender {
    pub(crate) async fn propose(
        &self,
        shard_group_id: ShardGroupId,
        command: MetadataCommand,
    ) -> Option<Result<(), ProposeError>> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.0
            .send(MultiRaftActorCommand::Propose {
                propose: RaftPropose {
                    shard_group_id,
                    command,
                },
                reply: send,
            })
            .await
            .ok()?;
        recv.await.ok()
    }

    pub(crate) async fn get_leader(&self, group_id: ShardGroupId) -> Option<NodeId> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.0
            .send(MultiRaftActorCommand::GetLeader {
                group_id,
                reply: send,
            })
            .await
            .ok()?;
        recv.await.ok().flatten()
    }

    pub(crate) async fn get_peers(&self, group_id: ShardGroupId) -> Vec<NodeId> {
        let (send, recv) = tokio::sync::oneshot::channel();
        if self
            .0
            .send(MultiRaftActorCommand::GetPeers {
                group_id,
                reply: send,
            })
            .await
            .is_err()
        {
            return vec![];
        }
        recv.await.unwrap_or_default()
    }

    pub(crate) async fn get_topics(&self) -> Vec<String> {
        let (send, recv) = tokio::sync::oneshot::channel();
        if self
            .0
            .send(MultiRaftActorCommand::GetTopics { reply: send })
            .await
            .is_err()
        {
            return vec![];
        }
        recv.await.unwrap_or_default()
    }

    pub(crate) async fn get_topic_stats(&self) -> Vec<crate::control_plane::metadata::TopicStats> {
        let (send, recv) = tokio::sync::oneshot::channel();
        if self
            .0
            .send(MultiRaftActorCommand::GetTopicStats { reply: send })
            .await
            .is_err()
        {
            return vec![];
        }
        recv.await.unwrap_or_default()
    }

    pub(crate) async fn send(
        &self,
        cmd: impl Into<MultiRaftActorCommand>,
    ) -> Result<(), mpsc::error::SendError<MultiRaftActorCommand>> {
        self.0.send(cmd.into()).await
    }

    pub(crate) fn try_send(
        &self,
        cmd: impl Into<MultiRaftActorCommand>,
    ) -> Result<(), mpsc::error::TrySendError<MultiRaftActorCommand>> {
        self.0.try_send(cmd.into())
    }
}

impl From<MutlRaftSender> for mpsc::Sender<MultiRaftActorCommand> {
    fn from(value: MutlRaftSender) -> Self {
        value.0
    }
}
