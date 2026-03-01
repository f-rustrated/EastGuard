use super::*;

use crate::clusters::swims::topology::Topology;

use crate::clusters::{JoinConfig, NodeId, PendingJoin, SwimNode, SwimNodeState};
use crate::schedulers::ticker_message::TimerCommand;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::SocketAddr;

const INDIRECT_PING_COUNT: usize = 3;

/// SWIM protocol state machine. No async, no channels, no timers.
///
/// Driven by two kinds of inputs:
///   - `step(src, packet)` — a packet arrived from the network
///   - Event handlers       — timer events dispatched by `SwimTicker` via the actor
///
/// All outbound packets are buffered in `pending_outbound`; drain with `take_outbound()`.
/// All timer commands are buffered in `pending_timer_commands`; drain with `take_timer_commands()`.
///
/// ```text
///                    TickEvent::ProtocolPeriodElapsed
///                    └─ on_protocol_period() → start_probe() sends Ping
///                              │
///                              ▼
///   (new node) ──────────► Alive
///        ▲                    │  step: Ack with matching seq
///        │                    │  └─ emits CancelProbe — probe cancelled, stays Alive
///        │                    │
///        │                    │  TickEvent::DirectProbeTimedOut
///        │                    │  └─ on_direct_probe_timeout() → start_indirect_probe()
///        │                    │     sends PingReq to K helpers
///        │                    │
///        │                    │  TickEvent::IndirectProbeTimedOut
///        │                    │  (or no helpers available → skip indirect immediately)
///        │                    │  └─ on_indirect_probe_timeout() → try_mark_suspect()
///        │                    │
///        │                    ▼
///        │                 Suspect
///        │                    │  step: higher-incarnation Alive gossip (refutation)
///        │◄───────────────────┘  └─ apply_membership_update() → Suspect → Alive
///        │
///        │                    │  TickEvent::SuspectTimedOut
///        │                    │  └─ on_suspect_timeout() → try_mark_dead()
///        │                    │
///        │                    ▼
///        │                  Dead
///        │                    │  step: higher-incarnation Alive gossip
///        └────────────────────┘  └─ update_member() — higher inc currently wins
///                                   (see TODO in update_member: per the original SWIM
///                                    paper, Dead is a terminal state and should NOT
///                                    be overridden by any incarnation number)
/// ```
pub struct Swim {
    // Identity
    pub(crate) node_id: NodeId,
    local_addr: SocketAddr,
    incarnation: u64,

    // Protocol state
    members: HashMap<NodeId, SwimNode>,
    live_node_tracker: LiveNodeTracker,
    gossip_buffer: GossipBuffer,
    pub(crate) topology: Topology,

    // Sequence
    seq_counter: u32,
    last_suspected_seqs: HashMap<NodeId, u32>,

    // Output buffers
    pending_outbound: Vec<OutboundPacket>,
    pending_timer_commands: Vec<TimerCommand<SwimTimer>>,

    // Join
    join_config: JoinConfig,
    pending_join_seqs: HashMap<u32, PendingJoin>,
}

impl Swim {
    pub fn new(
        node_id: NodeId,
        local_addr: SocketAddr,
        join_config: JoinConfig,
        topology: Topology,
    ) -> Self {
        let mut swim = Self {
            node_id,
            local_addr,
            incarnation: 0,
            topology,
            members: HashMap::new(),
            live_node_tracker: LiveNodeTracker::default(),
            gossip_buffer: GossipBuffer::default(),
            seq_counter: 0,
            last_suspected_seqs: HashMap::new(),
            pending_outbound: vec![],
            pending_timer_commands: vec![],
            join_config,
            pending_join_seqs: HashMap::new()
        };
        swim.update_member(
            swim.node_id.clone(),
            local_addr,
            SwimNodeState::Alive,
            swim.incarnation,
        );
        swim
    }

    pub(crate) fn initiate_join(&mut self) {
        let seeds: Vec<SocketAddr> = self.join_config.seed_addrs
            .iter()
            .filter(|&&addr| addr != self.local_addr)
            .copied()
            .collect();

        println!("[{}] InitiateJoin: {} seed(s) → {:?}", self.node_id, seeds.len(), seeds);

        for addr in seeds {
            if self.join_config.initial_delay_ticks == 0 {
                self.send_join_ping_with_retry(
                    addr,
                    self.join_config.max_attempts.saturating_sub(1),
                    self.join_config.interval_ticks,
                );
            } else {
                let seq = self.next_seq();
                println!("[{}] Scheduling join timer to {} (delay {} ticks)", self.node_id, addr, self.join_config.initial_delay_ticks);
                self.schedule_join_timer(seq, addr, self.join_config.max_attempts, self.join_config.initial_delay_ticks);
            }
        }
    }

    fn send_join_ping_with_retry(&mut self, addr: SocketAddr, left_attempts: u32, ticks: u32) {
        println!("[{}] → Sending join Ping to {} ({} attempt(s) left)", self.node_id, addr, left_attempts);
        let seq = self.next_seq();
        let ping = SwimPacket::Ping {
            seq,
            source_node_id: self.node_id.clone(),
            source_incarnation: self.incarnation,
            gossip: self.gossip_buffer.collect(),
        };
        self.pending_outbound.push(OutboundPacket::new(addr, ping));
        self.schedule_join_timer(seq, addr, left_attempts, ticks);
    }

    fn schedule_join_timer(&mut self, seq: u32, addr: SocketAddr, left_attempts: u32, ticks: u32) {
        self.pending_join_seqs.insert(seq, PendingJoin { addr, left_attempts });
        self.pending_timer_commands.push(TimerCommand::SetSchedule {
            seq,
            timer: SwimTimer::join_retry(ticks),
        });
    }

    // -----------------------------------------------------------------------
    // Core protocol logic
    // -----------------------------------------------------------------------
    pub(crate) fn handle_timeout(&mut self, event: SwimTimeOutCallback) {
        match event {
            SwimTimeOutCallback::ProtocolPeriodElapsed => self.start_probe(),
            SwimTimeOutCallback::TimedOut {
                seq,
                target_node_id,
                phase,
            } => match (phase, target_node_id) {
                (ProbePhase::Direct, Some(target))   => self.start_indirect_probe(target, seq),
                (ProbePhase::Indirect, Some(target)) => self.try_mark_suspect(target),
                (ProbePhase::Suspect, Some(target))  => self.try_mark_dead(target),
                (ProbePhase::JoinRetry, None)        => self.handle_join_retry(seq),
                _ => {}
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn handle_test_command(&self, test_command: SwimTestCommand) {
        match test_command {
            SwimTestCommand::TopologyValidationCount { reply } => {
                let _ = reply.send(self.topology.num_nodes()); 
            }
            SwimTestCommand::TopologyIncludesNode { node_id, reply } => {
                let _ = reply.send(self.topology.contains_node(&node_id)); 
            }
        }
    }


    fn start_probe(&mut self) {
        if self.live_node_tracker.is_empty() {
            return;
        }

        if let Some(target_node_id) = self.live_node_tracker.next() {
            if target_node_id == self.node_id {
                return;
            }

            let target = match self.members.get(&target_node_id).map(|m| m.addr) {
                Some(addr) => addr,
                None => return,
            };

            let seq = self.next_seq();
            let packet = SwimPacket::Ping {
                seq,
                source_node_id: self.node_id.clone(),
                source_incarnation: self.incarnation,
                gossip: self.gossip_buffer.collect(),
            };

            self.pending_outbound
                .push(OutboundPacket::new(target, packet));

            self.pending_timer_commands.push(TimerCommand::SetSchedule {
                seq,
                timer: SwimTimer::direct_probe(target_node_id),
            });
        }
    }

    // We preserve the previous direct probe's seq so that we can cancel
    // indirect probe timeout when we receive a long-running Ack message
    // from the previous direct probe
    fn start_indirect_probe(&mut self, target_node_id: NodeId, seq: u32) {
        let target_addr = match self.members.get(&target_node_id).map(|m| m.addr) {
            Some(addr) => addr,
            None => return,
        };

        let mut peer_addrs = Vec::new();
        for _ in 0..self.live_node_tracker.len() {
            if peer_addrs.len() >= INDIRECT_PING_COUNT {
                break;
            }

            if let Some(peer_node_id) = self.live_node_tracker.next() {
                if peer_node_id == target_node_id || peer_node_id == self.node_id {
                    continue;
                }

                if let Some(peer_addr) = self.members.get(&peer_node_id).map(|m| m.addr) {
                    peer_addrs.push(peer_addr);
                }
            }
        }

        if peer_addrs.is_empty() {
            self.try_mark_suspect(target_node_id);
            return;
        }

        let packet = SwimPacket::PingReq {
            seq,
            target: target_addr,
            source_node_id: self.node_id.clone(),
            source_incarnation: self.incarnation,
            gossip: self.gossip_buffer.collect(),
        };
        for target in peer_addrs {
            self.pending_outbound
                .push(OutboundPacket::new(target, packet.clone()));
        }

        self.pending_timer_commands.push(TimerCommand::SetSchedule {
            seq,
            timer: SwimTimer::indirect_probe(target_node_id),
        });
    }

    fn try_mark_suspect(&mut self, target_node_id: NodeId) {
        if let Some(member) = self.members.get(&target_node_id) {
            if member.state != SwimNodeState::Alive {
                return;
            }

            let (addr, incarnation) = (member.addr, member.incarnation);
            tracing::info!("Node {} is SUSPECT(Inc: {})", target_node_id, incarnation);
            self.update_member(target_node_id, addr, SwimNodeState::Suspect, incarnation);
        }
    }

    fn try_mark_dead(&mut self, target_node_id: NodeId) {
        if let Some(member) = self.members.get(&target_node_id) {
            if member.state != SwimNodeState::Suspect {
                return;
            }

            self.last_suspected_seqs.remove(&target_node_id);
            self.update_member(
                target_node_id,
                member.addr,
                SwimNodeState::Dead,
                member.incarnation,
            );
        }
    }

    fn handle_join_retry(&mut self, seq: u32) {
        let Some(pending_join) = self.pending_join_seqs.remove(&seq) else {
            println!("[{}] Join retry fired for seq {} but no pending join found (already acked?)", self.node_id, seq);
            return
        };

        if pending_join.left_attempts == 0 {
            println!("[{}] Join to {} exhausted all attempts — giving up", self.node_id, pending_join.addr);
            return;
        }

        let attempt = self.join_config.max_attempts - pending_join.left_attempts;
        let next_ticks = self.join_config.interval_ticks * self.join_config.multiplier.pow(attempt);

        self.send_join_ping_with_retry(
            pending_join.addr,
            pending_join.left_attempts - 1,
            next_ticks,
        );
    }

    pub fn step(&mut self, src: SocketAddr, packet: SwimPacket) {
        // 1. Process Gossip (Piggybacked updates)
        let gossip_list = match &packet {
            SwimPacket::Ping { gossip, .. }
            | SwimPacket::Ack { gossip, .. }
            | SwimPacket::PingReq { gossip, .. } => gossip,
        };
        for member in gossip_list {
            self.apply_membership_update(member.clone());
        }

        match packet {
            SwimPacket::Ping {
                seq,
                source_node_id,
                source_incarnation,
                ..
            } => {
                println!("[{}] ← Received Ping from {} ({}) seq={}", self.node_id, source_node_id, src, seq);
                self.handle_incarnation_check(source_node_id, src, source_incarnation);
                let ack = SwimPacket::Ack {
                    seq,
                    source_node_id: self.node_id.clone(),
                    source_incarnation: self.incarnation,
                    gossip: self.gossip_buffer.collect(),
                };

                self.pending_outbound.push(OutboundPacket::new(src, ack))
            }

            SwimPacket::Ack {
                seq,
                source_node_id,
                source_incarnation,
                ..
            } => {
                println!("[{}] ← Received Ack  from {} ({}) seq={}", self.node_id, source_node_id, src, seq);
                // TODO: should we ONLY handle Ack message with seq that we can identify?
                self.handle_incarnation_check(source_node_id, src, source_incarnation);
                self.pending_timer_commands
                    .push(TimerCommand::CancelSchedule { seq });
            }

            SwimPacket::PingReq {
                source_node_id,
                target,
                source_incarnation,
                ..
            } => {
                self.handle_incarnation_check(source_node_id, src, source_incarnation);
                let seq = self.next_seq();
                let ping = SwimPacket::Ping {
                    seq,
                    source_node_id: self.node_id.clone(),
                    source_incarnation: self.incarnation,
                    gossip: self.gossip_buffer.collect(),
                };
                self.pending_outbound
                    .push(OutboundPacket::new(target, ping));
            }
        }
    }

    fn apply_membership_update(&mut self, member: SwimNode) {
        // Refutation
        if member.node_id == self.node_id {
            if member.state.not_alive() && self.incarnation <= member.incarnation {
                let new_incarnation = member.incarnation + 1;
                tracing::info!(
                    "Refuting suspicion! (My Inc: {} -> {})",
                    self.incarnation,
                    new_incarnation
                );
                self.incarnation = new_incarnation;
                // Enqueue refutation so that the cluster learns quickly
                self.gossip_buffer.enqueue(
                    SwimNode {
                        node_id: self.node_id.clone(),
                        addr: self.local_addr,
                        state: SwimNodeState::Alive,
                        incarnation: new_incarnation,
                    },
                    self.members.len(),
                );
            }

            return;
        }

        self.update_member(
            member.node_id,
            member.addr,
            member.state,
            member.incarnation,
        )
    }

    fn handle_incarnation_check(
        &mut self,
        source_node_id: NodeId,
        addr: SocketAddr,
        remote_inc: u64,
    ) {
        // If we receive a direct message from someone, they are obviously Alive.
        // If their incarnation is higher than we thought, update it.
        if let Some(member) = self.members.get(&source_node_id) {
            if remote_inc > member.incarnation {
                if let Some(suspect_seq) = self.last_suspected_seqs.remove(&source_node_id) {
                    self.pending_timer_commands
                        .push(TimerCommand::CancelSchedule { seq: suspect_seq });
                }
                self.update_member(
                    source_node_id.clone(),
                    addr,
                    SwimNodeState::Alive,
                    remote_inc,
                );
            }
        } else {
            // New member discovered via direct message
            self.update_member(
                source_node_id.clone(),
                addr,
                SwimNodeState::Alive,
                remote_inc,
            );
        }
    }

    fn update_member(
        &mut self,
        node_id: NodeId,
        addr: SocketAddr,
        state: SwimNodeState,
        incarnation: u64,
    ) {
        let (new_state, changed) = match self.members.entry(node_id.clone()) {
            Entry::Vacant(e) => {
                e.insert(SwimNode {
                    node_id: node_id.clone(),
                    addr,
                    state,
                    incarnation,
                });

                (state, true)
            }

            Entry::Occupied(e) => {
                let entry = e.into_mut();
                let old_state = entry.state;
                let old_inc = entry.incarnation;

                // RULE: Higher incarnation always wins.
                // TODO: Per the original SWIM paper, Dead is a terminal state — a Dead node
                // should never be resurrected by a higher incarnation number. The current
                // implementation allows it, which deviates from the spec. Fix by adding a
                // guard: `if entry.state == Dead { return; }` before this block.
                if incarnation > entry.incarnation {
                    entry.incarnation = incarnation;
                    entry.state = state;
                }
                // RULE: Equal incarnation, stricter state wins (Dead > Suspect > Alive)
                else if incarnation == entry.incarnation {
                    match (&entry.state, &state) {
                        (SwimNodeState::Alive, SwimNodeState::Suspect) => {
                            entry.state = SwimNodeState::Suspect
                        }
                        (SwimNodeState::Alive, SwimNodeState::Dead) => {
                            entry.state = SwimNodeState::Dead
                        }
                        (SwimNodeState::Suspect, SwimNodeState::Dead) => {
                            entry.state = SwimNodeState::Dead
                        }
                        _ => {}
                    }
                }

                let changed = entry.state != old_state || entry.incarnation != old_inc;
                (entry.state, changed)
            }
        };

        self.live_node_tracker.update(node_id.clone(), new_state);
        self.topology.update(node_id.clone(), addr, new_state);

        if changed {
            println!("[{}] Member update: {} @ {} → {:?} (inc {})", self.node_id, node_id, addr, new_state, incarnation);
            let member = self.members[&node_id].clone();
            self.gossip_buffer.enqueue(member, self.members.len());

            if new_state == SwimNodeState::Suspect {
                let seq = self.next_seq();
                self.last_suspected_seqs.insert(node_id.clone(), seq);
                self.pending_timer_commands.push(TimerCommand::SetSchedule {
                    seq,
                    timer: SwimTimer::suspect_timer(node_id),
                });
            }
        }
    }

    fn next_seq(&mut self) -> u32 {
        self.seq_counter = self.seq_counter.wrapping_add(1);
        self.seq_counter
    }

    /// Drain all outbound packets buffered since the last call.
    pub(crate) fn take_outbound(&mut self) -> Vec<OutboundPacket> {
        std::mem::take(&mut self.pending_outbound)
    }

    /// Drain all timer commands buffered since the last call.
    pub(crate) fn take_timer_commands(&mut self) -> Vec<TimerCommand<SwimTimer>> {
        std::mem::take(&mut self.pending_timer_commands)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::schedulers::ticker::Ticker;

    use std::collections::HashMap;
    use std::net::SocketAddr;

    fn no_join_config() -> JoinConfig {
        JoinConfig { seed_addrs: vec![], initial_delay_ticks: 0, interval_ticks: 10, multiplier: 1, max_attempts: 0 }
    }

    fn make_protocol(local_id: &str, local_port: u16) -> Swim {
        let addr: SocketAddr = format!("127.0.0.1:{}", local_port).parse().unwrap();
        let topology = Topology::new(
            HashMap::new(),
            TopologyConfig { vnodes_per_pnode: 256 },
        );
        Swim::new(NodeId::new(local_id), addr, no_join_config(), topology)
    }

    /// Test harness that coordinates SwimProtocol + SwimTicker, mirroring
    /// what SwimActor does in production.
    struct TestHarness<T> {
        protocol: Swim,
        ticker: Ticker<T>,
    }

    impl TestHarness<SwimTimer> {
        fn new(local_id: &str, local_port: u16) -> Self {
            Self {
                protocol: make_protocol(local_id, local_port),
                ticker: Ticker::new(),
            }
        }

        fn tick(&mut self) {
            let events = self.ticker.advance_clock();
            for event in events {
                self.protocol.handle_timeout(event);
                self.apply_timer_commands();
            }
        }

        fn step(&mut self, src: SocketAddr, packet: SwimPacket) {
            self.protocol.step(src, packet);
            self.apply_timer_commands();
        }

        fn apply_timer_commands(&mut self) {
            for cmd in self.protocol.take_timer_commands() {
                self.ticker.apply(cmd);
            }
        }
    }

    fn ping(seq: u32, from_id: &str, from_inc: u64, gossip: Vec<SwimNode>) -> SwimPacket {
        SwimPacket::Ping {
            seq,
            source_node_id: NodeId::new(from_id),
            source_incarnation: from_inc,
            gossip,
        }
    }

    fn node(id: &str, port: u16, state: SwimNodeState, inc: u64) -> SwimNode {
        SwimNode {
            node_id: NodeId::new(id),
            addr: format!("127.0.0.1:{}", port).parse().unwrap(),
            state,
            incarnation: inc,
        }
    }

    fn ack(seq: u32, from_id: &str, from_inc: u64, gossip: Vec<SwimNode>) -> SwimPacket {
        SwimPacket::Ack {
            seq,
            source_node_id: NodeId::new(from_id),
            source_incarnation: from_inc,
            gossip,
        }
    }

    fn pingreq(
        seq: u32,
        from_id: &str,
        from_inc: u64,
        target: SocketAddr,
        gossip: Vec<SwimNode>,
    ) -> SwimPacket {
        SwimPacket::PingReq {
            seq,
            target,
            source_node_id: NodeId::new(from_id),
            source_incarnation: from_inc,
            gossip,
        }
    }

    fn add_node(m: &mut Swim, id: &str, addr: SocketAddr, inc: u64) {
        m.step(addr, ping(1, id, inc, vec![]));
        let _ = m.take_outbound();
        let _ = m.take_timer_commands();
    }

    fn add_node_harness(h: &mut TestHarness<SwimTimer>, id: &str, addr: SocketAddr, inc: u64) {
        h.step(addr, ping(1, id, inc, vec![]));
        let _ = h.protocol.take_outbound();
    }

    fn tick_until<T>(
        h: &mut TestHarness<SwimTimer>,
        max_ticks: u32,
        mut f: impl FnMut(&TestHarness<SwimTimer>) -> Option<T>,
    ) -> T {
        for _ in 0..max_ticks {
            h.tick();
            if let Some(v) = f(h) {
                return v;
            }
        }
        panic!("condition not met after {max_ticks} ticks");
    }

    // -----------------------------------------------------------------------
    // Ping tests
    // -----------------------------------------------------------------------

    mod ping {
        use super::*;
        use crate::clusters::{
            NodeId, SwimNodeState,
            swims::swim::tests::{make_protocol, node, ping},
        };
        use std::net::SocketAddr;

        #[test]
        fn ping_from_unknown_node() {
            let mut p = make_protocol("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();

            p.step(sender, ping(1, "node-b", 0, vec![]));

            let member = p
                .members
                .get("node-b")
                .expect("unknown node should be added");
            assert_eq!(member.state, SwimNodeState::Alive);
            assert_eq!(member.addr, sender);

            let out = p.take_outbound();
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].target, sender);
            assert!(matches!(&out[0].packet(), SwimPacket::Ack { seq: 1, .. }));
        }

        #[test]
        fn ping_from_known_alive_node() {
            let mut p = make_protocol("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();

            // First ping: introduces node-b into members
            p.step(sender, ping(1, "node-b", 2, vec![]));
            let _ = p.take_outbound();

            let state_before = p.members.get("node-b").unwrap().state;
            let inc_before = p.members.get("node-b").unwrap().incarnation;

            // Second ping: same node, same incarnation
            p.step(sender, ping(2, "node-b", 2, vec![]));

            let member = p.members.get("node-b").unwrap();
            assert_eq!(member.state, state_before);
            assert_eq!(member.incarnation, inc_before);

            let out = p.take_outbound();
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].target, sender);
            assert!(matches!(&out[0].packet(), SwimPacket::Ack { seq: 2, .. }));
        }

        #[test]
        fn ping_with_gossip_applied_before_ack() {
            let mut p = make_protocol("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();

            let gossip_entry = node("node-c", 9001, SwimNodeState::Alive, 1);
            p.step(sender, ping(1, "node-b", 0, vec![gossip_entry]));

            // Gossip applied: node-c is in members
            assert!(
                p.members.contains_key("node-c"),
                "gossiped node should be in members"
            );

            // Ack's gossip reflects the update applied during this step
            let out = p.take_outbound();
            assert_eq!(out.len(), 1);
            match &out[0].packet() {
                SwimPacket::Ack { gossip, .. } => {
                    assert!(
                        gossip.iter().any(|n| n.node_id == NodeId::new("node-c")),
                        "Ack gossip should contain node-c (applied before Ack was built)"
                    );
                }
                _ => panic!("expected Ack"),
            }
        }

        #[test]
        fn ping_sender_higher_incarnation_updates_member() {
            let mut p = make_protocol("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();

            // Introduce node-b at incarnation 1
            p.step(sender, ping(1, "node-b", 1, vec![]));
            let _ = p.take_outbound();
            assert_eq!(p.members.get("node-b").unwrap().incarnation, 1);

            // Ping from node-b with higher incarnation 5
            p.step(sender, ping(2, "node-b", 5, vec![]));
            let _ = p.take_outbound();

            let member = p.members.get("node-b").unwrap();
            assert_eq!(member.state, SwimNodeState::Alive);
            assert_eq!(member.incarnation, 5);
        }

        #[test]
        fn ping_sender_lower_incarnation_does_not_downgrade() {
            let mut p = make_protocol("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();

            // Introduce node-b at incarnation 5
            p.step(sender, ping(1, "node-b", 5, vec![]));
            let _ = p.take_outbound();
            assert_eq!(p.members.get("node-b").unwrap().incarnation, 5);

            // Ping from node-b with lower incarnation 1
            p.step(sender, ping(2, "node-b", 1, vec![]));
            let _ = p.take_outbound();

            let member = p.members.get("node-b").unwrap();
            assert_eq!(member.state, SwimNodeState::Alive);
            assert_eq!(member.incarnation, 5, "incarnation must not be downgraded");
        }
    }

    mod ack {
        use crate::clusters::SwimNodeState;
        use crate::clusters::swims::swim::tests::{TestHarness, ack, add_node_harness, tick_until};
        use crate::schedulers::ticker::{
            DIRECT_ACK_TIMEOUT_TICKS, INDIRECT_ACK_TIMEOUT_TICKS, PROBE_INTERVAL_TICKS,
            SUSPECT_TIMEOUT_TICKS,
        };

        use std::net::SocketAddr;

        #[test]
        fn ack_matching_direct_probe_removes_probe() {
            let mut h = TestHarness::new("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();
            let node_b_id = "node-b";
            add_node_harness(&mut h, node_b_id, sender, 1);

            // wait until direct ping is sent
            let seq = tick_until(&mut h, 2 * PROBE_INTERVAL_TICKS, |h| {
                h.ticker.probe_seq_for(node_b_id)
            });

            // let's Ack before direct ping times out
            h.step(sender, ack(seq, node_b_id, 1, vec![]));
            let _ = h.protocol.take_outbound();

            assert!(!h.ticker.has_timer(seq));
        }

        #[test]
        fn ack_matching_indirect_probe_removes_probe() {
            let mut h = TestHarness::new("node-local", 8000);
            let node_b = "node-b";
            let node_c = "node-c";
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            add_node_harness(&mut h, node_b, b_addr, 1);

            // Wait until node-b's direct probe starts
            let seq = tick_until(&mut h, 2 * PROBE_INTERVAL_TICKS, |h| {
                h.ticker.probe_seq_for(node_b)
            });
            let _ = h.protocol.take_outbound(); // discard the ping

            add_node_harness(&mut h, node_c, c_addr, 1);

            // Let direct probe timeout for node-b
            for _ in 0..DIRECT_ACK_TIMEOUT_TICKS {
                h.tick();
            }
            let _ = h.protocol.take_outbound(); // discard the PingReqs

            // Send Ack with the (reused) seq — clears the indirect probe
            h.step(b_addr, ack(seq, node_b, 1, vec![]));

            assert!(
                !h.ticker.has_timer(seq),
                "indirect probe should be removed after matching Ack"
            );
        }

        #[test]
        fn late_ack_same_incarnation_does_not_refute_suspect() {
            let mut h = TestHarness::new("node-local", 8000);
            let node_b = "node-b";
            let node_c = "node-c";
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            add_node_harness(&mut h, node_b, b_addr, 1);
            let seq = tick_until(&mut h, 2 * PROBE_INTERVAL_TICKS, |h| {
                h.ticker.probe_seq_for(node_b)
            });
            let _ = h.protocol.take_outbound();

            // we don't care about node c. It's for indirect request
            add_node_harness(&mut h, node_c, c_addr, 1);

            for _ in 0..DIRECT_ACK_TIMEOUT_TICKS {
                h.tick()
            } // we now send indirect ping for node-b
            let _ = h.protocol.take_outbound();
            for _ in 0..INDIRECT_ACK_TIMEOUT_TICKS {
                h.tick()
            } // node-b is now suspect

            assert!(matches!(
                h.protocol.members.get(node_b).unwrap().state,
                SwimNodeState::Suspect
            ));

            // Ack with same incarnation: remote_inc(1) > member.incarnation(1) → false → no-op
            h.step(b_addr, ack(seq, node_b, 1, vec![]));

            assert!(
                matches!(
                    h.protocol.members.get(node_b).unwrap().state,
                    SwimNodeState::Suspect
                ),
                "same incarnation Ack should not refute suspicion"
            );
        }

        #[test]
        fn late_ack_with_higher_incarnation_refutes_suspect() {
            let mut h = TestHarness::new("node-local", 8000);
            let node_b = "node-b";
            let node_c = "node-c";
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            add_node_harness(&mut h, node_b, b_addr, 1);
            let seq = tick_until(&mut h, 2 * PROBE_INTERVAL_TICKS, |h| {
                h.ticker.probe_seq_for(node_b)
            });
            let _ = h.protocol.take_outbound();

            // we don't care about node c. It's for indirect request
            add_node_harness(&mut h, node_c, c_addr, 1);

            for _ in 0..DIRECT_ACK_TIMEOUT_TICKS {
                h.tick()
            } // we now send indirect ping for node-b
            let _ = h.protocol.take_outbound();
            for _ in 0..INDIRECT_ACK_TIMEOUT_TICKS {
                h.tick()
            } // node-b is now suspect

            assert!(matches!(
                h.protocol.members.get(node_b).unwrap().state,
                SwimNodeState::Suspect
            ));

            // Ack with higher incarnation: remote_inc(2) > member.incarnation(1) → update to Alive
            h.step(b_addr, ack(seq, node_b, 2, vec![]));

            assert_eq!(
                h.protocol.members.get(node_b).unwrap().state,
                SwimNodeState::Alive,
                "higher incarnation Ack should refute suspicion"
            );

            // Probe timer already consumed by timeouts; suspect timer cancelled
            // by handle_incarnation_check on refutation
            assert!(!h.ticker.has_timer(seq), "probe timer should be gone");
            assert!(
                h.ticker.probe_seq_for(node_b).is_none(),
                "suspect timer should be cancelled after refutation"
            );
        }

        /// ABA scenario: Suspect → refuted to Alive → Suspect again.
        /// A stale suspect timer from the first suspicion period must NOT
        /// prematurely mark the node Dead during the second suspicion period.
        #[test]
        fn aba_stale_suspect_timer_does_not_cause_premature_death() {
            let mut h = TestHarness::new("node-local", 8000);
            let node_b = "node-b";
            let node_c = "node-c";
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            add_node_harness(&mut h, node_b, b_addr, 1);
            add_node_harness(&mut h, node_c, c_addr, 1);

            // --- First suspicion: drive node-b to Suspect ---

            let seq1 = tick_until(&mut h, 20 * PROBE_INTERVAL_TICKS, |h| {
                h.ticker.probe_seq_for(node_b)
            });
            let _ = h.protocol.take_outbound();

            for _ in 0..DIRECT_ACK_TIMEOUT_TICKS {
                h.tick();
            }
            let _ = h.protocol.take_outbound();
            for _ in 0..INDIRECT_ACK_TIMEOUT_TICKS {
                h.tick();
            }
            let _ = h.protocol.take_outbound();

            assert!(
                matches!(
                    h.protocol.members.get(node_b).unwrap().state,
                    SwimNodeState::Suspect
                ),
                "node-b should be Suspect after first probe failure"
            );

            // --- Refutation: node-b sends Ack with higher incarnation ---
            h.step(b_addr, ack(seq1, node_b, 2, vec![]));
            let _ = h.protocol.take_outbound();

            assert_eq!(
                h.protocol.members.get(node_b).unwrap().state,
                SwimNodeState::Alive,
                "node-b should be Alive after refutation"
            );
            assert!(
                h.ticker.probe_seq_for(node_b).is_none(),
                "first suspect timer should be cancelled after refutation"
            );

            // --- Second suspicion: drive node-b to Suspect again ---
            // (node-c may already be Suspect, so indirect phase may be skipped;
            //  use tick_until to find the exact moment node-b becomes Suspect)
            tick_until(&mut h, 20 * PROBE_INTERVAL_TICKS, |h| {
                if matches!(
                    h.protocol.members.get(node_b).unwrap().state,
                    SwimNodeState::Suspect
                ) {
                    Some(())
                } else {
                    None
                }
            });

            // Tick just short of SUSPECT_TIMEOUT_TICKS — node-b must still be Suspect,
            // proving no stale timer from the first round caused premature death.
            for _ in 0..SUSPECT_TIMEOUT_TICKS - 1 {
                h.tick();
            }

            assert!(
                matches!(
                    h.protocol.members.get(node_b).unwrap().state,
                    SwimNodeState::Suspect
                ),
                "node-b must remain Suspect — stale timer must not cause premature death"
            );

            // One more tick: the second suspect timer fires, NOW it's Dead
            h.tick();

            assert_eq!(
                h.protocol.members.get(node_b).unwrap().state,
                SwimNodeState::Dead,
                "node-b should be Dead only after the second suspect timer expires"
            );
        }
    }

    mod step_pingreq {
        use super::*;
        use crate::clusters::SwimNodeState;
        use crate::clusters::swims::swim::tests::{make_protocol, node, pingreq};
        use std::net::SocketAddr;

        #[test]
        fn pingreq_sends_ping_to_target() {
            let mut p = make_protocol("node-local", 8000);
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            p.step(b_addr, pingreq(1, "node-b", 1, c_addr, vec![]));

            // A Ping must be sent to the target (node-c)
            let out = p.take_outbound();
            assert_eq!(out.len(), 1);
            assert!(
                out.iter()
                    .any(|p| p.target == c_addr && matches!(p.packet(), SwimPacket::Ping { .. }))
            );

            // handle_incarnation_check ran: sender node-b should be in members
            let member = p
                .members
                .get("node-b")
                .expect("node-b should be added to members");
            assert_eq!(member.state, SwimNodeState::Alive);
        }

        #[test]
        fn pingreq_gossip_applied_before_proxy_ping() {
            let mut p = make_protocol("node-local", 8000);
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            let gossip_entry = node("node-d", 9003, SwimNodeState::Alive, 1);
            p.step(b_addr, pingreq(1, "node-b", 1, c_addr, vec![gossip_entry]));

            // Gossip applied: node-d should be in members
            assert!(
                p.members.contains_key("node-d"),
                "gossiped node-d should be in members"
            );

            // Proxy Ping's gossip should reflect the update (gossip applied in Phase 1, before Ping built)
            let out = p.take_outbound();
            assert_eq!(out.len(), 1);
            match &out[0].packet() {
                SwimPacket::Ping { gossip, .. } => {
                    assert!(
                        gossip.iter().any(|n| n.node_id == "node-d".into()),
                        "proxy Ping gossip should contain node-d"
                    );
                }
                _ => panic!("expected Ping"),
            }
        }
    }

    mod step_self_refutation {
        use super::*;
        use crate::clusters::SwimNodeState;
        use crate::clusters::swims::swim::tests::{make_protocol, node, ping};
        use std::net::SocketAddr;

        #[test]
        fn refutation_bumps_to_gossip_inc_plus_one() {
            let mut p = make_protocol("node-local", 8080);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();
            p.incarnation = 2;

            p.step(
                sender,
                ping(
                    1,
                    "node-b",
                    0,
                    vec![node("node-local", 8080, SwimNodeState::Suspect, 5)],
                ),
            );

            assert_eq!(p.incarnation, 6, "must bump to gossip.inc + 1 = 6");
            let out = p.take_outbound();
            match &out[0].packet() {
                SwimPacket::Ack {
                    source_incarnation,
                    gossip,
                    ..
                } => {
                    assert_eq!(*source_incarnation, 6);
                    assert!(
                        gossip
                            .iter()
                            .any(|n| n.node_id == "node-local".into() && n.incarnation == 6),
                        "self-refutation should be enqueued in gossip"
                    );
                }
                _ => panic!("expected Ack"),
            }
        }

        #[test]
        fn refutation_skipped_when_local_inc_is_higher() {
            let mut p = make_protocol("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();
            p.incarnation = 3;

            p.step(
                sender,
                ping(
                    1,
                    "node-b",
                    0,
                    vec![node("node-local", 8000, SwimNodeState::Suspect, 2)],
                ),
            );

            assert_eq!(p.incarnation, 3, "local inc must not change");
        }
    }

    #[test]
    fn refutation_on_dead_gossip_current_behavior() {
        // TODO: Per SWIM spec, Dead is terminal and should NOT trigger refutation.
        // Fix: add `if member.state == Dead { return; }` guard in apply_membership_update.
        let mut p = make_protocol("node-local", 8000);
        let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();

        p.step(
            sender,
            ping(
                1,
                "node-b",
                0,
                vec![node("node-local", 8000, SwimNodeState::Dead, 0)],
            ),
        );

        // TODO: Fix current (incorrect) behavior: Dead gossip triggers refutation
        assert_eq!(
            p.incarnation, 1,
            "current impl refutes Dead — this should change when TODO is fixed"
        );
    }

    mod join {
        use super::*;
        use crate::clusters::JoinConfig;
        use std::net::SocketAddr;

        fn seed_addr() -> SocketAddr {
            "127.0.0.1:9000".parse().unwrap()
        }

        fn make_join_harness(local_id: &str, local_port: u16, join_config: JoinConfig) -> TestHarness<SwimTimer> {
            let addr: SocketAddr = format!("127.0.0.1:{}", local_port).parse().unwrap();
            let topology = Topology::new(HashMap::new(), TopologyConfig { vnodes_per_pnode: 256 });
            TestHarness {
                protocol: Swim::new(NodeId::new(local_id), addr, join_config, topology),
                ticker: Ticker::new(),
            }
        }

        /// Calls initiate_join and applies resulting timer commands to the ticker.
        fn start_join(h: &mut TestHarness<SwimTimer>) {
            h.protocol.initiate_join();
            h.apply_timer_commands();
        }

        /// Advances the clock n ticks, collecting all outbound packets after each tick.
        fn tick_n_collect(h: &mut TestHarness<SwimTimer>, n: u32) -> Vec<OutboundPacket> {
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
                initial_delay_ticks: 1,
                interval_ticks: 10,
                multiplier: 2,
                max_attempts: 3,
            };
            let mut h = make_join_harness("node-local", 8000, config);
            h.protocol.initiate_join();

            assert!(h.protocol.take_outbound().is_empty());
            assert!(h.protocol.take_timer_commands().is_empty());
            assert!(h.protocol.pending_join_seqs.is_empty());
        }

        #[test]
        fn self_addr_in_seeds_excluded() {
            let local: SocketAddr = "127.0.0.1:8000".parse().unwrap();
            let config = JoinConfig {
                seed_addrs: vec![local],
                initial_delay_ticks: 1,
                interval_ticks: 10,
                multiplier: 2,
                max_attempts: 3,
            };
            let mut h = make_join_harness("node-local", 8000, config);
            h.protocol.initiate_join();

            assert!(h.protocol.take_outbound().is_empty());
            assert!(h.protocol.take_timer_commands().is_empty());
        }

        // -----------------------------------------------------------------------
        // delay = 0 — Ping sent immediately, retry timer with left_attempts = max - 1
        // -----------------------------------------------------------------------

        #[test]
        fn delay_zero_sends_ping_immediately() {
            let config = JoinConfig {
                seed_addrs: vec![seed_addr()],
                initial_delay_ticks: 0,
                interval_ticks: 10,
                multiplier: 2,
                max_attempts: 3,
            };
            let mut h = make_join_harness("node-local", 8000, config);
            h.protocol.initiate_join();

            let out = h.protocol.take_outbound();
            assert_eq!(count_join_pings(&out, seed_addr()), 1, "Ping sent immediately");
        }

        #[test]
        fn delay_zero_schedules_retry_timer_and_decrements_attempts() {
            let config = JoinConfig {
                seed_addrs: vec![seed_addr()],
                initial_delay_ticks: 0,
                interval_ticks: 10,
                multiplier: 2,
                max_attempts: 3,
            };
            let mut h = make_join_harness("node-local", 8000, config);
            h.protocol.initiate_join();
            let _ = h.protocol.take_outbound();

            assert_eq!(h.protocol.pending_join_seqs.len(), 1);
            let pending = h.protocol.pending_join_seqs.values().next().unwrap();
            assert_eq!(pending.left_attempts, 2, "immediate send counts as attempt 0");

            let cmds = h.protocol.take_timer_commands();
            assert_eq!(cmds.len(), 1, "one retry timer scheduled");
        }

        // -----------------------------------------------------------------------
        // delay > 0
        // -----------------------------------------------------------------------

        #[test]
        fn delay_greater_than_zero_no_ping_before_delay_elapses() {
            let delay = 5;
            let config = JoinConfig {
                seed_addrs: vec![seed_addr()],
                initial_delay_ticks: delay,
                interval_ticks: 10,
                multiplier: 2,
                max_attempts: 3,
            };
            let mut h = make_join_harness("node-local", 8000, config);
            start_join(&mut h);

            // delay - 1 ticks: timer not yet fired, no Ping
            let out = tick_n_collect(&mut h, delay - 1);
            assert_eq!(count_join_pings(&out, seed_addr()), 0, "no Ping before delay elapses");
        }

        #[test]
        fn delay_greater_than_zero_sends_ping_when_delay_elapses() {
            let delay = 5;
            let config = JoinConfig {
                seed_addrs: vec![seed_addr()],
                initial_delay_ticks: delay,
                interval_ticks: 10,
                multiplier: 2,
                max_attempts: 3,
            };
            let mut h = make_join_harness("node-local", 8000, config);
            start_join(&mut h);

            // Advance past delay
            let _ = tick_n_collect(&mut h, delay - 1);
            let out = tick_n_collect(&mut h, 1);
            assert_eq!(count_join_pings(&out, seed_addr()), 1, "Ping sent when delay elapses");
        }

        // -----------------------------------------------------------------------
        // Exponential backoff
        // -----------------------------------------------------------------------

        #[test]
        fn exponential_backoff_fires_at_correct_intervals() {
            // initial_delay=1, interval=2, multiplier=3, max_attempts=2
            //   tick 1:     attempt 0 → Ping, next = 2 * 3^0 = 2 ticks
            //   tick 1+2=3: attempt 1 → Ping, next = 2 * 3^1 = 6 ticks
            //   tick 3+6=9: left_attempts=0 → no Ping
            let config = JoinConfig {
                seed_addrs: vec![seed_addr()],
                initial_delay_ticks: 1,
                interval_ticks: 2,
                multiplier: 3,
                max_attempts: 2,
            };
            let mut h = make_join_harness("node-local", 8000, config);
            start_join(&mut h);

            // tick 1: first Ping
            let out = tick_n_collect(&mut h, 1);
            assert_eq!(count_join_pings(&out, seed_addr()), 1, "first Ping at tick 1");

            // tick 2: no Ping (next interval = 2 ticks)
            let out = tick_n_collect(&mut h, 1);
            assert_eq!(count_join_pings(&out, seed_addr()), 0, "no Ping at tick 2");

            // tick 3: second Ping
            let out = tick_n_collect(&mut h, 1);
            assert_eq!(count_join_pings(&out, seed_addr()), 1, "second Ping at tick 3");

            // ticks 4–9: retries exhausted, no more Pings
            let out = tick_n_collect(&mut h, 6);
            assert_eq!(count_join_pings(&out, seed_addr()), 0, "no Pings after exhaustion");
        }

        #[test]
        fn max_attempts_limits_total_pings_sent() {
            // multiplier=1 and interval=1 → uniform 1-tick retries, easy counting
            let max: u32 = 3;
            let config = JoinConfig {
                seed_addrs: vec![seed_addr()],
                initial_delay_ticks: 1,
                interval_ticks: 1,
                multiplier: 1,
                max_attempts: max,
            };
            let mut h = make_join_harness("node-local", 8000, config);
            start_join(&mut h);

            // Tick max + 2 to ensure all retries fire and exhaust
            let out = tick_n_collect(&mut h, max + 2);
            assert_eq!(
                count_join_pings(&out, seed_addr()),
                max as usize,
                "exactly max_attempts Pings sent"
            );
        }
    }
}
