pub(crate) mod actor;
pub(crate) mod peer_discovery;

mod livenode_tracker;
pub(crate) mod messages;

pub(crate) mod swim;
mod topology;

use livenode_tracker::*;
pub(super) use messages::dissemination_buffer::*;
pub(crate) use messages::*;

pub(crate) use topology::*;

#[cfg(test)]
pub mod common {
    use std::net::SocketAddr;

    use crate::{
        clusters::{
            NodeAddress, NodeId,
            swims::{
                OutboundPacket, SwimEvent, SwimPacket, SwimTimer, Topology, TopologyConfig,
                swim::Swim,
            },
        },
        schedulers::ticker::Ticker,
    };

    pub fn make_protocol(local_id: &str, local_port: u16) -> Swim {
        let peer_addr: SocketAddr = format!("127.0.0.1:{}", local_port).parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{}", local_port + 1000).parse().unwrap();
        let topology = Topology::new(
            std::iter::empty(),
            TopologyConfig {
                vnodes_per_pnode: 256,
                replication_factor: 3,
            },
        );
        Swim::new(
            NodeId::new(local_id),
            NodeAddress {
                cluster_addr: peer_addr,
                client_addr,
            },
            topology,
            0,
        )
    }

    /// Test harness that coordinates SwimProtocol + SwimTicker, mirroring
    /// what SwimActor does in production.
    pub struct TestHarness<T> {
        pub protocol: Swim,
        pub ticker: Ticker<T>,
    }

    impl TestHarness<SwimTimer> {
        pub fn new(local_id: &str, local_port: u16) -> Self {
            Self {
                protocol: make_protocol(local_id, local_port),
                ticker: Ticker::new(),
            }
        }

        pub fn tick(&mut self) {
            let events = self.ticker.advance_clock(0);
            for event in events {
                self.protocol.handle_timeout(event);
                self.apply_timer_commands();
            }
        }

        pub fn step(&mut self, src: SocketAddr, packet: SwimPacket) {
            self.protocol.step(src, packet);
            self.apply_timer_commands();
        }

        pub fn apply_timer_commands(&mut self) {
            for event in self.protocol.take_events() {
                match event {
                    SwimEvent::Timer(cmd) => self.ticker.apply(cmd),
                    other => self.protocol.re_buffer(other),
                }
            }
        }

        pub fn tick_n_collect(&mut self, n: u32) -> Vec<OutboundPacket> {
            let mut all = vec![];
            for _ in 0..n {
                self.tick();
                all.extend(self.protocol.take_packets());
            }
            all
        }

        pub fn tick_until<T>(
            &mut self,
            max_ticks: u32,
            mut f: impl FnMut(&TestHarness<SwimTimer>) -> Option<T>,
        ) -> T {
            for _ in 0..max_ticks {
                self.tick();
                if let Some(v) = f(self) {
                    return v;
                }
            }
            panic!("condition not met after {max_ticks} ticks");
        }
    }
}
