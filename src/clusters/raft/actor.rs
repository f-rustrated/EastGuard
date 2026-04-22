#![allow(dead_code)]

use std::collections::HashMap;

use crate::clusters::NodeId;
use crate::storage::Db;
use crate::clusters::raft::messages::*;
use crate::clusters::raft::multi_raft::MultiRaft;
use crate::clusters::swims::SwimCommand;
use crate::schedulers::ticker_message::TickerCommand;

use tokio::sync::mpsc;

/// Async boundary — receives commands from mailbox, delegates to `MultiRaft`.
pub struct MultiRaftActor;

impl MultiRaftActor {
    pub async fn run(
        node_id: NodeId,
        db: Db,
        mut mailbox: mpsc::Receiver<MultiRaftActorCommand>,
        transport_tx: mpsc::Sender<RaftTransportCommand>,
        scheduler_tx: mpsc::Sender<TickerCommand<RaftTimer>>,
        swim_tx: mpsc::Sender<SwimCommand>,
    ) {
        let mut store = MultiRaft::new(node_id, db);
        let mut buf = Vec::with_capacity(64);
        let mut deferred: Vec<Box<dyn FnOnce() + Send>> = Vec::with_capacity(64);

        loop {
            if mailbox.recv_many(&mut buf, 64).await == 0 {
                break;
            }

            for cmd in buf.drain(..) {
                let (store_cmd, on_reply) = cmd.split();
                let reply = store.handle_command(store_cmd);
                if let Some(cb) = on_reply {
                    deferred.push(Box::new(move || cb(reply)));
                }
            }

            let mut packets_by_target: HashMap<NodeId, Vec<OutboundRaftPacket>> = HashMap::new();

            for event in store.flush() {
                match event {
                    RaftEvent::OutboundRaftPacket(pkt) => {
                        packets_by_target
                            .entry(pkt.target.clone())
                            .or_default()
                            .push(pkt);
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

            for (_, packets) in packets_by_target {
                let _ = transport_tx.send(RaftTransportCommand::Send(packets)).await;
            }

            for cb in deferred.drain(..) {
                cb();
            }
        }
    }
}
