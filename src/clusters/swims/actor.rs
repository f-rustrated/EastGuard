use super::*;

use crate::clusters::{JoinConfig, NodeId};
use crate::clusters::swims::swim::Swim;
use crate::clusters::swims::topology::Topology;
use crate::clusters::swims::topology::TopologyConfig;
use crate::schedulers::ticker_message::TickerCommand;

use std::net::SocketAddr;
use tokio::sync::mpsc;

// ==========================================
// PROTOCOL LAYER (SWIM Actor)
// ==========================================

pub struct SwimActor {
    mailbox: mpsc::Receiver<SwimCommand>,
    transport_tx: mpsc::Sender<OutboundPacket>,
    scheduler_tx: mpsc::Sender<TickerCommand<SwimTimer>>,
    state: Swim,
}

impl SwimActor {
    pub fn new(
        local_addr: SocketAddr,
        node_id: NodeId,
        mailbox: mpsc::Receiver<SwimCommand>,
        transport_tx: mpsc::Sender<OutboundPacket>,
        ticker_tx: mpsc::Sender<TickerCommand<SwimTimer>>,
        vnodes_per_node: u64,
        join_config: JoinConfig,
    ) -> Self {
        let topology = Topology::new(
            Default::default(),
            TopologyConfig {
                vnodes_per_pnode: vnodes_per_node,
            },
        );
        let state = Swim::new(node_id, local_addr, join_config, topology);

        Self {
            mailbox,
            transport_tx,
            state,
            scheduler_tx: ticker_tx,
        }
    }

    pub async fn run(mut self) {
        println!("SwimActor {} started.", self.state.node_id);

        while let Some(event) = self.mailbox.recv().await {
            self.handle_actor_event(event).await;
        }
    }

    async fn handle_actor_event(&mut self, event: SwimCommand) {
        match event {
            SwimCommand::InitiateJoin => {
                self.state.initiate_join();
            }

            SwimCommand::PacketReceived { src, packet } => {
                self.state.step(src, packet);
            }

            SwimCommand::Timeout(tick_event) => {
                self.state.handle_timeout(tick_event);
            }
        }

        self.flush_outbound_commands().await;
    }

    async fn flush_outbound_commands(&mut self) {
        let timer_commands = self.state.take_timer_commands();
        let outbound_packets = self.state.take_outbound();
        tokio::join!(
            async {
                for cmd in timer_commands {
                    let _ = self.scheduler_tx.send(cmd.into()).await;
                }
            },
            async {
                for pkt in outbound_packets {
                    let _ = self.transport_tx.send(pkt).await;
                }
            }
        );
    }

    #[cfg(test)]
    pub(crate) fn topology(&self) -> &Topology {
        &self.state.topology
    }

    #[cfg(test)]
    pub async fn process_event_for_test(&mut self, event: SwimCommand) {
        match event {
            SwimCommand::InitiateJoin => {
                self.state.initiate_join();
            }
            SwimCommand::PacketReceived { src, packet } => {
                self.state.step(src, packet);
                // Discard timer commands — topology tests don't need timing.
                self.flush_outbound_commands().await;
            }
            SwimCommand::Timeout(_tick_event) => {}
        }
    }
}
