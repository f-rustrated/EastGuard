use super::*;

use crate::clusters::swims::peer_discovery::JoinAttempt;
use crate::clusters::swims::topology::Topology;

use crate::clusters::{NodeId, SwimNode, SwimNodeState};
use crate::schedulers::ticker_message::TimerCommand;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
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
    pub(crate) advertise_addr: SocketAddr,
    incarnation: u64,

    // Protocol state
    members: BTreeMap<NodeId, SwimNode>,
    live_node_tracker: LiveNodeTracker,
    gossip_buffer: GossipBuffer,
    pub(crate) topology: Topology,

    // Sequence
    seq_counter: u32,
    last_suspected_seqs: BTreeMap<NodeId, u32>,

    // Output buffers
    pending_outbound: Vec<OutboundPacket>,
    pending_timer_commands: Vec<TimerCommand<SwimTimer>>,
    pending_indirect_pings: BTreeMap<u32, ProxyPing>,
}

impl Swim {
    pub fn new(node_id: NodeId, advertise_addr: SocketAddr, topology: Topology, rng_seed: u64) -> Self {
        let mut swim = Self {
            node_id,
            advertise_addr,
            incarnation: 0,
            topology,
            members: BTreeMap::new(),
            live_node_tracker: LiveNodeTracker::new(rng_seed),
            gossip_buffer: GossipBuffer::default(),
            seq_counter: 0,
            last_suspected_seqs: BTreeMap::new(),
            pending_outbound: vec![],
            pending_timer_commands: vec![],
            pending_indirect_pings: BTreeMap::new(),
        };
        swim.update_member(
            swim.node_id.clone(),
            advertise_addr,
            SwimNodeState::Alive,
            swim.incarnation,
        );
        swim
    }

    fn generate_swim_header(&mut self, seq: u32) -> SwimHeader {
        SwimHeader {
            seq,
            source_node_id: self.node_id.clone(),
            source_incarnation: self.incarnation,
            gossip: self.gossip_buffer.collect(),
        }
    }
    pub(crate) fn handle_join(&mut self, mut attempt: JoinAttempt) {
        let seq = self.next_seq();
        let ping = SwimPacket::Ping(self.generate_swim_header(seq));
        self.pending_outbound
            .push(OutboundPacket::new(attempt.seed_addr, ping));

        attempt.reset_next_ticks_for_wait();
        attempt.deduct_remaining_attempt();

        self.pending_timer_commands.push(TimerCommand::SetSchedule {
            seq,
            timer: SwimTimer::join_try(attempt),
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
                (SwimTimerKind::DirectProbe, Some(target)) => {
                    self.start_indirect_probe(target, seq)
                }
                (SwimTimerKind::IndirectProbe, Some(target)) => self.try_mark_suspect(target),
                (SwimTimerKind::Suspect, Some(target)) => self.try_mark_dead(target, seq),
                (SwimTimerKind::ProxyPing, None) => {
                    self.pending_indirect_pings.remove(&seq);
                }
                (SwimTimerKind::JoinTry(join_attempt), None) => {
                    if join_attempt.remaining_attempts == 0 {
                        tracing::warn!(
                            "[{}] Join to {} exhausted all attempts — giving up",
                            self.node_id,
                            join_attempt.seed_addr
                        );
                        return;
                    }
                    self.handle_join(join_attempt);
                }
                _ => {}
            },
        }
    }

    pub(crate) fn handle_query(&self, command: SwimQueryCommand) {
        match command {
            SwimQueryCommand::GetMembers { reply } => {
                let _ = reply.send(self.members.values().map(|s| s.clone()).collect());
            }
            SwimQueryCommand::GetTopology { reply } => {
                let _ = reply.send(self.topology.clone());
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
            let packet = SwimPacket::Ping(self.generate_swim_header(seq));

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

        // * OPT : pre-allocation
        let mut peer_addrs = Vec::with_capacity(INDIRECT_PING_COUNT);

        for _ in 0..self.live_node_tracker.len() {
            let Some(peer_node_id) = self.live_node_tracker.next() else {
                break;
            };

            if peer_node_id == target_node_id || peer_node_id == self.node_id {
                continue;
            }

            if let Some(peer_member) = self.members.get(&peer_node_id) {
                peer_addrs.push(peer_member.addr);

                // * OPT : Check length ONLY after a successful insertion
                if peer_addrs.len() == INDIRECT_PING_COUNT {
                    break;
                }
            }
        }

        if peer_addrs.is_empty() {
            self.try_mark_suspect(target_node_id);
            return;
        }

        let packet = SwimPacket::PingReq {
            header: self.generate_swim_header(seq),
            target: target_addr,
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

    fn try_mark_dead(&mut self, target_node_id: NodeId, registered_seq: u32) {
        if let Some(member) = self.members.get(&target_node_id) {
            if member.state != SwimNodeState::Suspect {
                return;
            }

            if let Some(seq) = self.last_suspected_seqs.get(&target_node_id) {
                if registered_seq != *seq {
                    return;
                }
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

    pub fn step(&mut self, src: SocketAddr, packet: SwimPacket) {
        // 1. Process Gossip (Piggybacked updates)
        for member in packet.gossip() {
            self.apply_membership_update(member.clone());
        }

        match packet {
            SwimPacket::Ping(header) => {
                tracing::info!(
                    "[{}] ← Received Ping from {} ({}) seq={}",
                    self.node_id,
                    header.source_node_id,
                    src,
                    header.seq
                );
                self.handle_incarnation_check(
                    header.source_node_id,
                    src,
                    header.source_incarnation,
                );
                let ack = SwimPacket::Ack(self.generate_swim_header(header.seq));

                self.pending_outbound.push(OutboundPacket::new(src, ack))
            }

            SwimPacket::Ack(header) => {
                tracing::info!(
                    "[{}] ← Received Ack  from {} ({}) seq={}",
                    self.node_id,
                    header.source_node_id,
                    src,
                    header.seq
                );
                // TODO: should we ONLY handle Ack message with seq that we can identify?
                self.handle_incarnation_check(
                    header.source_node_id.clone(),
                    src,
                    header.source_incarnation,
                );
                self.pending_timer_commands
                    .push(TimerCommand::CancelSchedule { seq: header.seq });

                if let Some(ProxyPing {
                    requester_addr,
                    request_seq,
                }) = self.pending_indirect_pings.remove(&header.seq)
                {
                    let forwarded_ack = SwimPacket::Ack(SwimHeader {
                        seq: request_seq,
                        source_node_id: header.source_node_id,
                        source_incarnation: header.source_incarnation,
                        gossip: self.gossip_buffer.collect(),
                    });

                    self.pending_outbound
                        .push(OutboundPacket::new(requester_addr, forwarded_ack));
                }
            }

            SwimPacket::PingReq { header, target, .. } => {
                self.handle_incarnation_check(
                    header.source_node_id.clone(),
                    src,
                    header.source_incarnation,
                );
                let seq = self.next_seq();

                self.pending_indirect_pings.insert(
                    seq,
                    ProxyPing {
                        request_seq: header.seq,
                        requester_addr: src,
                    },
                );
                let ping = SwimPacket::Ping(self.generate_swim_header(seq));
                self.pending_outbound
                    .push(OutboundPacket::new(target, ping));
                self.pending_timer_commands.push(TimerCommand::SetSchedule {
                    seq,
                    timer: SwimTimer::proxy_ping(),
                });
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
                        addr: self.advertise_addr,
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
        );
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
        let (changed, member) = match self.members.entry(node_id.clone()) {
            Entry::Vacant(e) => {
                let node = SwimNode {
                    node_id: node_id.clone(),
                    addr,
                    state,
                    incarnation,
                };
                e.insert(node.clone());

                (true, node)
            }

            Entry::Occupied(mut e) => {
                let node = e.get_mut();

                // Dead is a terminal state.
                // If it's dead, ignore the update and exit the function entirely.
                // After this "tombstone" period, the node is deleted from the HashMap entirely
                // To join the cluster again, a node should join it with new node ID.
                if node.state == SwimNodeState::Dead {
                    return;
                }

                let old_state = node.state;
                let old_inc = node.incarnation;

                // RULE: Higher incarnation always wins.
                if incarnation > node.incarnation {
                    node.incarnation = incarnation;
                    node.state = state;
                }
                // RULE: Equal incarnation, stricter state wins (Dead > Suspect > Alive)
                else if incarnation == node.incarnation {
                    // Requires `SwimNodeState` to derive `PartialOrd, Ord`
                    node.state = node.state.max(state);
                }

                let changed = node.state != old_state || node.incarnation != old_inc;
                (changed, node.clone())
            }
        };

        self.live_node_tracker
            .update(node_id.clone(), &member.state);
        self.topology.update(node_id.clone(), addr, &member.state);

        if changed {
            tracing::info!(
                "[{}] Member update: {} @ {} → {:?} (inc {})",
                self.node_id,
                node_id,
                addr,
                member.state,
                incarnation
            );

            match member.state {
                SwimNodeState::Alive => {
                    if let Some(suspect_seq) = self.last_suspected_seqs.remove(&node_id) {
                        self.pending_timer_commands
                            .push(TimerCommand::CancelSchedule { seq: suspect_seq });
                    }
                }
                SwimNodeState::Suspect => {
                    let seq = self.next_seq();
                    self.last_suspected_seqs.insert(node_id.clone(), seq);
                    self.pending_timer_commands.push(TimerCommand::SetSchedule {
                        seq,
                        timer: SwimTimer::suspect_timer(node_id),
                    });
                }
                _ => {}
            }

            self.gossip_buffer.enqueue(member, self.members.len());
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
    use crate::clusters::swims::common::{TestHarness, make_protocol};
    use std::net::SocketAddr;

    fn ping(seq: u32, from_id: &str, from_inc: u64, gossip: Vec<SwimNode>) -> SwimPacket {
        SwimPacket::Ping(SwimHeader {
            seq,
            source_node_id: NodeId::new(from_id),
            source_incarnation: from_inc,
            gossip,
        })
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
        SwimPacket::Ack(SwimHeader {
            seq,
            source_node_id: NodeId::new(from_id),
            source_incarnation: from_inc,
            gossip,
        })
    }

    fn pingreq(
        seq: u32,
        from_id: &str,
        from_inc: u64,
        target: SocketAddr,
        gossip: Vec<SwimNode>,
    ) -> SwimPacket {
        SwimPacket::PingReq {
            target,
            header: SwimHeader {
                seq,
                source_node_id: NodeId::new(from_id),
                source_incarnation: from_inc,
                gossip,
            },
        }
    }

    fn add_node_harness(h: &mut TestHarness<SwimTimer>, id: &str, addr: SocketAddr, inc: u64) {
        h.step(addr, ping(1, id, inc, vec![]));
        let _ = h.protocol.take_outbound();
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
            assert!(matches!(
                &out[0].packet(),
                SwimPacket::Ack(SwimHeader { seq: 1, .. })
            ));
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
            assert!(matches!(
                &out[0].packet(),
                SwimPacket::Ack(SwimHeader { seq: 2, .. })
            ));
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
                SwimPacket::Ack(header) => {
                    assert!(
                        header
                            .gossip
                            .iter()
                            .any(|n| n.node_id == NodeId::new("node-c")),
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
        use crate::clusters::swims::swim::tests::{TestHarness, ack, add_node_harness};
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
            let seq = h.tick_until(2 * PROBE_INTERVAL_TICKS, |h| {
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
            let seq = h.tick_until(2 * PROBE_INTERVAL_TICKS, |h| h.ticker.probe_seq_for(node_b));
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
            let seq = h.tick_until(2 * PROBE_INTERVAL_TICKS, |h| h.ticker.probe_seq_for(node_b));
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
            let seq = h.tick_until(2 * PROBE_INTERVAL_TICKS, |h| h.ticker.probe_seq_for(node_b));
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

            let seq1 = h.tick_until(20 * PROBE_INTERVAL_TICKS, |h| {
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
            h.tick_until(20 * PROBE_INTERVAL_TICKS, |h| {
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
                SwimPacket::Ping(header) => {
                    assert!(
                        header.gossip.iter().any(|n| n.node_id == "node-d".into()),
                        "proxy Ping gossip should contain node-d"
                    );
                }
                _ => panic!("expected Ping"),
            }
        }
    }

    mod proxy_ping {
        use super::*;
        use crate::clusters::swims::swim::tests::{ack, pingreq};
        use crate::schedulers::ticker::DIRECT_ACK_TIMEOUT_TICKS;
        use std::net::SocketAddr;

        #[test]
        fn proxy_ping_entry_cleaned_up_on_timeout() {
            let mut h = TestHarness::new("node-local", 8000);
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            // node-b asks us to ping node-c
            h.step(b_addr, pingreq(1, "node-b", 1, c_addr, vec![]));
            let _ = h.protocol.take_outbound();

            assert_eq!(h.protocol.pending_indirect_pings.len(), 1);

            // node-c never responds — tick until ProxyPing timer fires
            for _ in 0..DIRECT_ACK_TIMEOUT_TICKS {
                h.tick();
            }

            assert!(
                h.protocol.pending_indirect_pings.is_empty(),
                "stale proxy ping entry should be cleaned up after timeout"
            );
        }

        #[test]
        fn proxy_ping_ack_cancels_timer_and_forwards_ack() {
            let mut h = TestHarness::new("node-local", 8000);
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            // node-b asks us to ping node-c (original seq=42)
            h.step(b_addr, pingreq(42, "node-b", 1, c_addr, vec![]));

            // grab the proxy Ping's seq
            let out = h.protocol.take_outbound();
            let proxy_seq = match out[0].packet() {
                SwimPacket::Ping(header) => header.seq,
                _ => panic!("expected proxy Ping"),
            };

            assert_eq!(h.protocol.pending_indirect_pings.len(), 1);

            // node-c responds with Ack
            h.step(c_addr, ack(proxy_seq, "node-c", 1, vec![]));

            // entry removed, timer cancelled
            assert!(
                h.protocol.pending_indirect_pings.is_empty(),
                "entry should be removed after Ack"
            );
            assert!(
                !h.ticker.has_timer(proxy_seq),
                "proxy ping timer should be cancelled after Ack"
            );

            // forwarded Ack sent back to node-b with original seq=42
            let out = h.protocol.take_outbound();
            assert!(
                out.iter().any(|p| p.target == b_addr
                    && matches!(p.packet(), SwimPacket::Ack(SwimHeader { seq: 42, .. }))),
                "should forward Ack to original requester with original seq"
            );
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
                SwimPacket::Ack(header) => {
                    assert_eq!(header.source_incarnation, 6);
                    assert!(
                        header
                            .gossip
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
}
