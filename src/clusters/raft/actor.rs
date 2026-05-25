#![allow(dead_code)]

use std::collections::HashMap;

use crate::clusters::NodeId;
use crate::clusters::raft::messages::*;
use crate::clusters::raft::multi_raft::MultiRaft;
use crate::clusters::raft::storage::RaftStorage;
use crate::clusters::swims::ShardGroupId;
use crate::clusters::swims::SwimCommand;
use crate::clusters::swims::actor::SwimSender;
use crate::schedulers::ticker_message::TickerCommand;

use tokio::sync::mpsc;

/// Async boundary — receives commands from mailbox, delegates to `MultiRaft`.
pub struct MultiRaftActor;

impl MultiRaftActor {
    pub fn channel(buffer: usize) -> (RaftSender, mpsc::Receiver<MultiRaftActorCommand>) {
        let (tx, rx) = mpsc::channel(buffer);
        (RaftSender(tx), rx)
    }

    pub async fn run(
        node_id: NodeId,
        election_jitter_seed: u64,
        storage: Box<dyn RaftStorage>,
        mut mailbox: mpsc::Receiver<MultiRaftActorCommand>,
        transport_tx: mpsc::Sender<Box<[RaftTransportCommand]>>,
        scheduler_tx: mpsc::Sender<Box<[TickerCommand<RaftTimer>]>>,
        swim_tx: SwimSender,
    ) {
        let mut store = MultiRaft::new(node_id, election_jitter_seed, storage);
        let mut buf = Vec::with_capacity(64);

        loop {
            if mailbox.recv_many(&mut buf, 64).await == 0 {
                break;
            }
            for cmd in buf.drain(..) {
                store.process(cmd);
            }

            Self::flush_events(&mut store, &transport_tx, &scheduler_tx, &swim_tx).await;
            store.fire_deferred();
        }
    }

    async fn flush_events(
        store: &mut MultiRaft,
        transport_tx: &mpsc::Sender<Box<[RaftTransportCommand]>>,
        scheduler_tx: &mpsc::Sender<Box<[TickerCommand<RaftTimer>]>>,
        swim_tx: &SwimSender,
    ) {
        let mut packets_by_target: HashMap<NodeId, Vec<OutboundRaftPacket>> = HashMap::new();
        let mut timer_cmds = Vec::new();
        let mut transport_cmds = Vec::new();
        for event in store.flush() {
            match event {
                RaftEvent::OutboundRaftPacket(pkt) => {
                    packets_by_target
                        .entry(pkt.target.clone())
                        .or_default()
                        .push(pkt);
                }
                RaftEvent::Timer(cmd) => timer_cmds.push(cmd.into()),
                RaftEvent::LeaderChange(lc) => {
                    let _ = swim_tx.send(SwimCommand::AnnounceShardLeader(lc)).await;
                }
                RaftEvent::DisconnectPeer(node_id) => {
                    transport_cmds.push(RaftTransportCommand::DisconnectPeer(node_id));
                }
                RaftEvent::MetadataApplied { .. } => {
                    // D3: Dispatched in Batch 2 (MultiRaft event dispatch)
                }
            }
        }
        for packets in packets_by_target.into_values() {
            transport_cmds.push(RaftTransportCommand::Send(packets));
        }
        if !transport_cmds.is_empty() {
            let _ = transport_tx
                .send(transport_cmds.into_boxed_slice())
                .await;
        }
        if !timer_cmds.is_empty() {
            let _ = scheduler_tx
                .send(timer_cmds.into_boxed_slice())
                .await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct RaftSender(mpsc::Sender<MultiRaftActorCommand>);

impl RaftSender {
    pub(crate) async fn propose(
        &self,
        shard_group_id: ShardGroupId,
        command: RaftCommand,
    ) -> Option<Result<(), ProposeError>> {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.0
            .send(MultiRaftActorCommand::Propose {
                shard_group_id,
                command,
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

    pub(crate) async fn get_topics(&self) -> Vec<String> {
        let (send, recv) = tokio::sync::oneshot::channel();
        if self.0
            .send(MultiRaftActorCommand::GetTopics { reply: send })
            .await
            .is_err()
        {
            return vec![];
        }
        recv.await.unwrap_or_default()
    }

    pub(crate) async fn get_topic_stats(&self) -> Vec<crate::clusters::metadata::TopicStats> {
        let (send, recv) = tokio::sync::oneshot::channel();
        if self.0
            .send(MultiRaftActorCommand::GetTopicStats { reply: send })
            .await
            .is_err()
        {
            return vec![];
        }
        recv.await.unwrap_or_default()
    }
}

impl From<RaftSender> for mpsc::Sender<MultiRaftActorCommand> {
    fn from(value: RaftSender) -> Self {
        value.0
    }
}
