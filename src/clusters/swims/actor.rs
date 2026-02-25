use crate::clusters::ProbePhase;
use crate::clusters::SwimTimeOutSchedule;
use crate::clusters::swims::swim::Swim;
use crate::clusters::swims::topology::Topology;
use crate::clusters::swims::topology::TopologyConfig;

use crate::clusters::{NodeId, OutboundPacket, SwimCommand, SwimTimeOutCallback};
use crate::schedulers::ticker_message::TickerCommand;

use std::net::SocketAddr;
use tokio::sync::mpsc;

// ==========================================
// PROTOCOL LAYER (SWIM Actor)
// ==========================================

pub struct SwimActor {
    mailbox: mpsc::Receiver<SwimCommand>,
    transport_tx: mpsc::Sender<OutboundPacket>,
    ticker_tx: mpsc::Sender<TickerCommand<SwimTimeOutSchedule>>,
    state: Swim,
}

impl SwimActor {
    pub fn new(
        local_addr: SocketAddr,
        node_id: NodeId,
        mailbox: mpsc::Receiver<SwimCommand>,
        transport_tx: mpsc::Sender<OutboundPacket>,
        ticker_tx: mpsc::Sender<TickerCommand<SwimTimeOutSchedule>>,
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

    async fn handle_tick_event(&mut self, event: SwimTimeOutCallback) {
        match event {
            SwimTimeOutCallback::ProtocolPeriodElapsed => self.state.start_probe(),
            SwimTimeOutCallback::TimedOut {
                seq,
                target_node_id,
                phase,
            } => match phase {
                ProbePhase::Direct => self.state.start_indirect_probe(target_node_id, seq),
                ProbePhase::Indirect => self.state.try_mark_suspect(target_node_id),
                ProbePhase::Suspect => self.state.try_mark_dead(target_node_id),
            },
        }
    }

    async fn handle_actor_event(&mut self, event: SwimCommand) {
        match event {
            SwimCommand::PacketReceived { src, packet } => {
                self.state.step(src, packet);
            }

            SwimCommand::Timeout(tick_event) => {
                self.handle_tick_event(tick_event).await;
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
                    let _ = self.ticker_tx.send(cmd.into()).await;
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
            SwimCommand::PacketReceived { src, packet } => {
                self.state.step(src, packet);
                // Discard timer commands — topology tests don't need timing.
                self.flush_outbound_commands().await;
            }
            SwimCommand::Timeout(tick_event) => {}
        }
    }
}
