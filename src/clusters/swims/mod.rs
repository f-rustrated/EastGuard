pub(crate) mod actor;
pub(crate) mod peer_discovery;

mod gossip_buffer;
mod livenode_tracker;
mod messages;
pub(crate) mod swim;
mod topology;

use gossip_buffer::*;
use livenode_tracker::*;
pub(crate) use messages::*;

pub(crate) use topology::*;

#[cfg(test)]
pub mod common {
    use std::{collections::HashMap, net::SocketAddr};

    use crate::{
        clusters::{
            NodeId,
            swims::{OutboundPacket, SwimPacket, SwimTimer, Topology, TopologyConfig, swim::Swim},
        },
        schedulers::ticker::Ticker,
    };

    pub fn make_protocol(local_id: &str, local_port: u16) -> Swim {
        let addr: SocketAddr = format!("127.0.0.1:{}", local_port).parse().unwrap();
        let topology = Topology::new(
            HashMap::new(),
            TopologyConfig {
                vnodes_per_pnode: 256,
            },
        );
        Swim::new(NodeId::new(local_id), addr, topology)
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
            let events = self.ticker.advance_clock();
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
            for cmd in self.protocol.take_timer_commands() {
                self.ticker.apply(cmd);
            }
        }

        pub fn tick_n_collect(&mut self, n: u32) -> Vec<OutboundPacket> {
            let mut all = vec![];
            for _ in 0..n {
                self.tick();
                all.extend(self.protocol.take_outbound());
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
