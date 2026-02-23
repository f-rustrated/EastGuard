use super::swim_protocol::SwimProtocol;
use super::*;
use crate::clusters::swim_ticker::TickerCommand;
use crate::clusters::topology::Topology;
use std::net::SocketAddr;
use tokio::sync::mpsc;

// ==========================================
// PROTOCOL LAYER (SWIM Actor)
// ==========================================

pub struct SwimActor {
    mailbox: mpsc::Receiver<ActorEvent>,
    outbound: mpsc::Sender<OutboundPacket>,
    scheduler_sender: mpsc::Sender<TickerCommand>,
    state: SwimProtocol,
}

impl SwimActor {
    pub fn new(
        local_addr: SocketAddr,
        node_id: NodeId,
        mailbox: mpsc::Receiver<ActorEvent>,
        outbound: mpsc::Sender<OutboundPacket>,
        topology: Topology,
        scheduler_sender: mpsc::Sender<TickerCommand>,
    ) -> Self {
        let mut state = SwimProtocol::new(node_id, local_addr, topology);
        state.init_self();
        Self {
            mailbox,
            outbound,
            state,
            scheduler_sender,
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
            TickEvent::ProtocolPeriodElapsed => self.state.on_protocol_period(),
            TickEvent::DirectProbeTimedOut {
                seq,
                target_node_id,
            } => self.state.on_direct_probe_timeout(seq, target_node_id),
            TickEvent::IndirectProbeTimedOut { target_node_id, .. } => {
                self.state.on_indirect_probe_timeout(target_node_id)
            }
            TickEvent::SuspectTimedOut { node_id } => self.state.on_suspect_timeout(node_id),
        }
        self.apply_timer_commands().await;
        self.flush_outbound().await;
    }

    async fn handle_actor_event(&mut self, event: ActorEvent) {
        match event {
            ActorEvent::PacketReceived { src, packet } => {
                self.state.step(src, packet);
                self.apply_timer_commands().await;
                self.flush_outbound().await;
            }

            ActorEvent::Tick(tick_event) => {
                self.handle_tick_event(tick_event).await;
            }
        }
    }

    async fn apply_timer_commands(&mut self) {
        for cmd in self.state.take_timer_commands() {
            let _ = self.scheduler_sender.send(cmd.into()).await;
        }
    }

    async fn flush_outbound(&mut self) {
        for pkt in self.state.take_outbound() {
            let _ = self.outbound.send(pkt).await;
        }
    }

    #[cfg(test)]
    pub(crate) fn topology(&self) -> &Topology {
        &self.state.topology
    }

    #[cfg(test)]
    pub async fn process_event_for_test(&mut self, event: ActorEvent) {
        match event {
            ActorEvent::PacketReceived { src, packet } => {
                self.state.step(src, packet);
                // Discard timer commands — topology tests don't need timing.
                self.state.take_timer_commands();
                self.flush_outbound().await;
            }
            ActorEvent::Tick(tick_event) => {}
        }
    }
}
