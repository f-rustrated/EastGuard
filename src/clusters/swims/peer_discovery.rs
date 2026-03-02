use crate::clusters::swims::swim::Swim;

use std::net::SocketAddr;

pub(crate) struct Bootstrapper {
    bootstrap_servers: Vec<JoinAttempt>,
    swim: Swim,
}

impl Bootstrapper {
    pub fn new(bootstrap_servers: Vec<JoinAttempt>, swim: Swim) -> Self {
        Self {
            bootstrap_servers,
            swim,
        }
    }

    pub(crate) fn bootstrap(mut self) -> Swim {
        for attempt in self
            .bootstrap_servers
            .into_iter()
            .filter(|t| t.seed_addr != self.swim.local_addr)
            .collect::<Vec<_>>()
        {
            tracing::info!(
                "[{}] → Sending join Ping to {} ({} attempt(s) left)",
                self.swim.node_id,
                attempt.seed_addr,
                attempt.remaining_attempts
            );
            self.swim.handle_join(attempt);
        }
        self.swim
    }
}

#[derive(Debug)]
pub(crate) struct JoinAttempt {
    pub(crate) seed_addr: SocketAddr,
    pub(crate) ticks_for_wait: u32,
    pub(crate) backoff_ticks: u32,
    pub(crate) multiplier: u32,
    pub(crate) max_attempts: u32,
    pub(crate) remaining_attempts: u32,
}
impl JoinAttempt {
    pub(crate) fn deduct_remaining_attempt(&mut self) {
        self.remaining_attempts = self.remaining_attempts.saturating_sub(1)
    }

    pub(crate) fn reset_next_ticks_for_wait(&mut self) {
        let attempt = self.max_attempts - self.remaining_attempts;
        self.ticks_for_wait = self.backoff_ticks * self.multiplier.pow(attempt);
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct JoinConfig {
    pub(crate) seed_addrs: Vec<SocketAddr>,
    pub(crate) ticks_for_wait: u32,
    pub(crate) backoff_ticks: u32,
    pub(crate) multiplier: u32,
    pub(crate) max_attempts: u32,
}
#[cfg(test)]
impl JoinConfig {
    pub(crate) fn tries(&self) -> Vec<JoinAttempt> {
        self.seed_addrs
            .iter()
            .map(|addr| JoinAttempt {
                seed_addr: *addr,
                ticks_for_wait: self.ticks_for_wait,
                backoff_ticks: self.backoff_ticks,
                multiplier: self.multiplier,
                max_attempts: self.max_attempts,
                remaining_attempts: self.max_attempts,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clusters::NodeId;
    use crate::clusters::swims::messages::*;
    use crate::clusters::swims::topology::{Topology, TopologyConfig};
    use crate::schedulers::ticker::Ticker;
    use std::collections::HashMap;
    use std::net::SocketAddr;

    fn make_protocol(local_id: &str, local_port: u16) -> Swim {
        let addr: SocketAddr = format!("127.0.0.1:{}", local_port).parse().unwrap();
        let topology = Topology::new(
            HashMap::new(),
            TopologyConfig {
                vnodes_per_pnode: 256,
            },
        );
        Swim::new(NodeId::new(local_id), addr, topology)
    }

    struct TestHarness {
        protocol: Swim,
        ticker: Ticker<SwimTimer>,
    }

    impl TestHarness {
        fn tick(&mut self) {
            let events = self.ticker.advance_clock();
            for event in events {
                self.protocol.handle_timeout(event);
                self.apply_timer_commands();
            }
        }

        fn apply_timer_commands(&mut self) {
            for cmd in self.protocol.take_timer_commands() {
                self.ticker.apply(cmd);
            }
        }
    }

    /// Bootstraps the harness's protocol via `Bootstrapper`, then applies timer commands.

    fn tick_n_collect(h: &mut TestHarness, n: u32) -> Vec<OutboundPacket> {
        let mut all = vec![];
        for _ in 0..n {
            h.tick();
            all.extend(h.protocol.take_outbound());
        }
        all
    }

    fn count_join_pings(packets: &[OutboundPacket], target: SocketAddr) -> usize {
        packets
            .iter()
            .filter(|p| p.target == target && matches!(p.packet(), SwimPacket::Ping { .. }))
            .count()
    }

    // -----------------------------------------------------------------------
    // No seed nodes
    // -----------------------------------------------------------------------

    #[test]
    fn no_seeds_does_nothing() {
        let config = JoinConfig {
            seed_addrs: vec![],
            ticks_for_wait: 1,
            backoff_ticks: 10,
            multiplier: 2,
            max_attempts: 3,
        };
        let mut swim = {
            let swim = make_protocol("node-local", 8000);
            let join_config: &JoinConfig = &config;
            Bootstrapper::new(join_config.tries(), swim).bootstrap()
        };

        assert!(swim.take_outbound().is_empty());
        assert!(swim.take_timer_commands().is_empty());
    }

    #[test]
    fn self_addr_in_seeds_excluded() {
        let local: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let config = JoinConfig {
            seed_addrs: vec![local],
            ticks_for_wait: 1,
            backoff_ticks: 10,
            multiplier: 2,
            max_attempts: 3,
        };
        let mut swim = {
            let swim = make_protocol("node-local", 8000);
            let join_config: &JoinConfig = &config;
            Bootstrapper::new(join_config.tries(), swim).bootstrap()
        };

        assert!(swim.take_outbound().is_empty());
        assert!(swim.take_timer_commands().is_empty());
    }

    // -----------------------------------------------------------------------
    // delay = 0 — Ping sent immediately, retry timer with left_attempts = max - 1
    // -----------------------------------------------------------------------

    #[test]
    fn delay_zero_sends_ping_immediately() {
        let config = JoinConfig {
            seed_addrs: vec!["127.0.0.1:9000".parse().unwrap()],
            ticks_for_wait: 0,
            backoff_ticks: 10,
            multiplier: 2,
            max_attempts: 3,
        };
        let mut swim = {
            let swim = make_protocol("node-local", 8000);
            let join_config: &JoinConfig = &config;
            Bootstrapper::new(join_config.tries(), swim).bootstrap()
        };

        let out = swim.take_outbound();
        assert_eq!(
            count_join_pings(&out, "127.0.0.1:9000".parse().unwrap()),
            1,
            "Ping sent immediately"
        );
    }

    #[test]
    fn delay_zero_schedules_retry_timer_and_decrements_attempts() {
        let swim = make_protocol("node-local", 8000);

        let mut swim = Bootstrapper::new(
            (&JoinConfig {
                seed_addrs: vec!["127.0.0.1:9000".parse().unwrap()],
                ticks_for_wait: 0,
                backoff_ticks: 10,
                multiplier: 2,
                max_attempts: 3,
            })
                .tries(),
            swim,
        )
        .bootstrap();

        let _ = swim.take_outbound();

        let cmds = swim.take_timer_commands();
        assert_eq!(cmds.len(), 1, "one retry timer scheduled");
    }

    // -----------------------------------------------------------------------
    // delay > 0
    // -----------------------------------------------------------------------

    #[test]
    fn no_retry_before_backoff_elapses() {
        let config = JoinConfig {
            seed_addrs: vec!["127.0.0.1:9000".parse().unwrap()],
            ticks_for_wait: 0,
            backoff_ticks: 10,
            multiplier: 2,
            max_attempts: 3,
        };
        let mut h = TestHarness {
            protocol: make_protocol("node-local", 8000),
            ticker: Ticker::new(),
        };
        h.protocol = Bootstrapper::new(config.tries(), h.protocol).bootstrap();
        h.apply_timer_commands();

        // First ping sent immediately during start_join
        let out = h.protocol.take_outbound();
        assert_eq!(
            count_join_pings(&out, "127.0.0.1:9000".parse().unwrap()),
            1,
            "immediate Ping"
        );

        // backoff = 10 * 2^0 = 10 ticks. No retry before that.
        let out = tick_n_collect(&mut h, 9);
        assert_eq!(
            count_join_pings(&out, "127.0.0.1:9000".parse().unwrap()),
            0,
            "no retry before backoff elapses"
        );
    }

    #[test]
    fn retry_fires_when_backoff_elapses() {
        let config = JoinConfig {
            seed_addrs: vec!["127.0.0.1:9000".parse().unwrap()],
            ticks_for_wait: 0,
            backoff_ticks: 10,
            multiplier: 2,
            max_attempts: 3,
        };
        let mut h = TestHarness {
            protocol: make_protocol("node-local", 8000),
            ticker: Ticker::new(),
        };
        h.protocol = Bootstrapper::new(config.tries(), h.protocol).bootstrap();
        h.apply_timer_commands();

        let _ = h.protocol.take_outbound(); // consume immediate ping

        // backoff = 10 * 2^0 = 10 ticks
        let out = tick_n_collect(&mut h, 10);
        assert_eq!(
            count_join_pings(&out, "127.0.0.1:9000".parse().unwrap()),
            1,
            "retry Ping when backoff elapses"
        );
    }

    // -----------------------------------------------------------------------
    // Exponential backoff
    // -----------------------------------------------------------------------

    #[test]
    fn exponential_backoff_fires_at_correct_intervals() {
        // backoff=2, multiplier=3, max_attempts=2
        //   immediate:       attempt 0 → Ping, next = 2 * 3^0 = 2 ticks
        //   tick 2:          attempt 1 → Ping, next = 2 * 3^1 = 6 ticks
        //   tick 2+6=8:      remaining=0 → no Ping
        let config = JoinConfig {
            seed_addrs: vec!["127.0.0.1:9000".parse().unwrap()],
            ticks_for_wait: 0,
            backoff_ticks: 2,
            multiplier: 3,
            max_attempts: 2,
        };
        let mut h = TestHarness {
            protocol: make_protocol("node-local", 8000),
            ticker: Ticker::new(),
        };
        h.protocol = Bootstrapper::new(config.tries(), h.protocol).bootstrap();
        h.apply_timer_commands();

        // immediate: first Ping
        let out = h.protocol.take_outbound();
        assert_eq!(
            count_join_pings(&out, "127.0.0.1:9000".parse().unwrap()),
            1,
            "first Ping sent immediately"
        );

        // tick 1: no Ping (backoff = 2 ticks)
        let out = tick_n_collect(&mut h, 1);
        assert_eq!(
            count_join_pings(&out, "127.0.0.1:9000".parse().unwrap()),
            0,
            "no Ping at tick 1"
        );

        // tick 2: second Ping
        let out = tick_n_collect(&mut h, 1);
        assert_eq!(
            count_join_pings(&out, "127.0.0.1:9000".parse().unwrap()),
            1,
            "second Ping at tick 2"
        );

        // ticks 3–8: retries exhausted, no more Pings
        let out = tick_n_collect(&mut h, 6);
        assert_eq!(
            count_join_pings(&out, "127.0.0.1:9000".parse().unwrap()),
            0,
            "no Pings after exhaustion"
        );
    }

    #[test]
    fn max_attempts_limits_total_pings_sent() {
        // multiplier=1 and backoff=1 → uniform 1-tick retries, easy counting
        let max: u32 = 3;
        let config = JoinConfig {
            seed_addrs: vec!["127.0.0.1:9000".parse().unwrap()],
            ticks_for_wait: 0,
            backoff_ticks: 1,
            multiplier: 1,
            max_attempts: max,
        };
        let mut h = TestHarness {
            protocol: make_protocol("node-local", 8000),
            ticker: Ticker::new(),
        };

        h.protocol = Bootstrapper::new(config.tries(), h.protocol).bootstrap();
        h.apply_timer_commands();

        // 1 immediate ping + (max - 1) retries via ticking
        let immediate = h.protocol.take_outbound();
        let retries = tick_n_collect(&mut h, max + 1);
        let total = count_join_pings(&immediate, "127.0.0.1:9000".parse().unwrap())
            + count_join_pings(&retries, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(total, max as usize, "exactly max_attempts Pings sent");
    }
}
