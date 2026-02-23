use crate::clusters::swims::swim::Swim;
use crate::clusters::swims::topology::Topology;
use crate::clusters::swims::topology::TopologyConfig;
use crate::clusters::types::ticker_message::TickerCommand;
use crate::clusters::{NodeId, OutboundPacket, SwimCommand, TickEvent};

use std::net::SocketAddr;
use tokio::sync::mpsc;

// ==========================================
// PROTOCOL LAYER (SWIM Actor)
// ==========================================

pub struct SwimActor {
    mailbox: mpsc::Receiver<SwimCommand>,
    transport_tx: mpsc::Sender<OutboundPacket>,
    ticker_tx: mpsc::Sender<TickerCommand>,
    state: Swim,
}

impl SwimActor {
    pub fn new(
        local_addr: SocketAddr,
        node_id: NodeId,
        mailbox: mpsc::Receiver<SwimCommand>,
        transport_tx: mpsc::Sender<OutboundPacket>,
        ticker_tx: mpsc::Sender<TickerCommand>,
        vnodes_per_node: u64,
    ) -> Self {
        let topology = Topology::new(
            Default::default(),
            TopologyConfig {
                vnodes_per_pnode: vnodes_per_node,
            },
        );
        let state = Swim::new(node_id, local_addr, topology);

        Self {
            mailbox,
            transport_tx,
            state,
            ticker_tx,
        }
    }

    pub async fn run(mut self) {
        println!("SwimActor started.");

        while let Some(event) = self.mailbox.recv().await {
            self.handle_actor_event(event).await;
        }
    }

    async fn handle_tick_event(&mut self, event: TickEvent) {
        match event {
            TickEvent::ProtocolPeriodElapsed => self.state.start_probe(),
            TickEvent::DirectProbeTimedOut {
                seq,
                target_node_id,
            } => self.state.start_indirect_probe(target_node_id, seq),
            TickEvent::IndirectProbeTimedOut { target_node_id, .. } => {
                self.state.try_mark_suspect(target_node_id);
            }
            TickEvent::SuspectTimedOut { node_id } => {
                self.state.try_mark_dead(node_id);
            }
        }
    }

    async fn handle_actor_event(&mut self, event: SwimCommand) {
        match event {
            SwimCommand::PacketReceived { src, packet } => {
                self.state.step(src, packet);
            }

            SwimCommand::Tick(tick_event) => {
                self.handle_tick_event(tick_event).await;
            }
        }

        self.flush_ticker_commands().await;
        self.flush_outbound().await;
    }

    async fn flush_ticker_commands(&mut self) {
        for cmd in self.state.take_timer_commands() {
            let _ = self.ticker_tx.send(cmd.into()).await;
        }
    }
    async fn flush_outbound(&mut self) {
        for pkt in self.state.take_outbound() {
            let _ = self.transport_tx.send(pkt).await;
        }
    }

    #[cfg(test)]
    pub(crate) fn topology(&self) -> &Topology {
        &self.state.topology
    }

    #[cfg(test)]
    pub async fn process_event_for_test(&mut self, event: SwimCommand) {
        match event {
            SwimCommand::PacketReceived { src, packet } => {
                self.state.step(src, packet);
                // Discard timer commands — topology tests don't need timing.
                self.state.take_timer_commands();
                self.flush_outbound().await;
            }
            SwimCommand::Tick(tick_event) => {}
        }
    }
}
