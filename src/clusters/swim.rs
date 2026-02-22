use super::swim_state_machine::SwimStateMachine;
use super::*;
use crate::clusters::topology::Topology;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

/// One real-time tick drives one logical tick in SwimStateMachine.
/// PROTOCOL_PERIOD_TICKS (10) × TICK_PERIOD (100 ms) = 1 s per probe round.
const TICK_PERIOD: Duration = Duration::from_millis(100);

// ==========================================
// PROTOCOL LAYER (SWIM Actor)
// ==========================================

pub struct SwimActor {
    mailbox: mpsc::Receiver<ActorEvent>,
    outbound: mpsc::Sender<OutboundPacket>,
    state: SwimStateMachine,
}

impl SwimActor {
    pub fn new(
        local_addr: SocketAddr,
        node_id: NodeId,
        mailbox: mpsc::Receiver<ActorEvent>,
        outbound: mpsc::Sender<OutboundPacket>,
        topology: Topology,
    ) -> Self {
        let mut state = SwimStateMachine::new(node_id, local_addr, topology);
        state.init_self();
        Self { mailbox, outbound, state }
    }

    pub async fn run(mut self) {
        let mut ticker = time::interval(TICK_PERIOD);
        println!("SwimActor started.");

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.handle_tick().await;
                }
                Some(event) = self.mailbox.recv() => {
                    self.handle_event(event).await;
                }
            }
        }
    }

    async fn handle_event(&mut self, event: ActorEvent) {
        match event {
            ActorEvent::PacketReceived { src, packet } => {
                self.state.step(src, packet);
                self.flush_outbound().await;
            }

            ActorEvent::ProtocolTick => {
                // Test hook: advance one logical tick without waiting for the real interval.
                self.handle_tick().await;
            }
        }
    }

    async fn handle_tick(&mut self) {
        self.state.tick();
        self.flush_outbound().await;
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
        self.handle_event(event).await;
    }
}