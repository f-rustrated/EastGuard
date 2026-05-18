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
        storage: Box<dyn RaftStorage>,
        mut mailbox: mpsc::Receiver<MultiRaftActorCommand>,
        transport_tx: mpsc::Sender<RaftTransportCommand>,
        scheduler_tx: mpsc::Sender<TickerCommand<RaftTimer>>,
        swim_tx: SwimSender,
    ) {
        let mut store = MultiRaft::new(node_id, storage);
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
        transport_tx: &mpsc::Sender<RaftTransportCommand>,
        scheduler_tx: &mpsc::Sender<TickerCommand<RaftTimer>>,
        swim_tx: &SwimSender,
    ) {
        let mut packets_by_target: HashMap<NodeId, Vec<OutboundRaftPacket>> = HashMap::new();
        for event in store.flush() {
            Self::route_event(
                event,
                &mut packets_by_target,
                transport_tx,
                scheduler_tx,
                swim_tx,
            )
            .await;
        }
        Self::send_batched_packets(packets_by_target, transport_tx).await;
    }

    async fn route_event(
        event: RaftEvent,
        packets: &mut HashMap<NodeId, Vec<OutboundRaftPacket>>,
        transport_tx: &mpsc::Sender<RaftTransportCommand>,
        scheduler_tx: &mpsc::Sender<TickerCommand<RaftTimer>>,
        swim_tx: &SwimSender,
    ) {
        match event {
            RaftEvent::OutboundRaftPacket(pkt) => {
                packets.entry(pkt.target.clone()).or_default().push(pkt);
            }
            RaftEvent::Timer(cmd) => {
                let _ = scheduler_tx.send(cmd.into()).await;
            }
            RaftEvent::LeaderChange(lc) => {
                let _ = swim_tx.send(SwimCommand::AnnounceShardLeader(lc)).await;
            }
            RaftEvent::DisconnectPeer(node_id) => {
                let _ = transport_tx
                    .send(RaftTransportCommand::DisconnectPeer(node_id))
                    .await;
            }
        }
    }

    async fn send_batched_packets(
        packets_by_target: HashMap<NodeId, Vec<OutboundRaftPacket>>,
        transport_tx: &mpsc::Sender<RaftTransportCommand>,
    ) {
        for packets in packets_by_target.into_values() {
            let _ = transport_tx.send(RaftTransportCommand::Send(packets)).await;
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
}

impl From<RaftSender> for mpsc::Sender<MultiRaftActorCommand> {
    fn from(value: RaftSender) -> Self {
        value.0
    }
}
