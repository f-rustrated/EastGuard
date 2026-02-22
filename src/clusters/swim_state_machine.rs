use crate::clusters::gossip_buffer::GossipBuffer;
use crate::clusters::livenode_tracker::LiveNodeTracker;
use crate::clusters::topology::Topology;
use crate::clusters::{NodeId, OutboundPacket, SwimNode, SwimNodeState, SwimPacket};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::SocketAddr;

pub(crate) const PROBE_INTERVAL_TICKS: u32 = 10; // 10 × 100ms = 1s
pub(crate) const DIRECT_ACK_TIMEOUT_TICKS: u32 = 3; // 3 × 100ms = 300ms
pub(crate) const INDIRECT_ACK_TIMEOUT_TICKS: u32 = 3; // 3 × 100ms = 300ms
pub(crate) const SUSPECT_TIMEOUT_TICKS: u32 = 50; // 50 × 100ms = 5s
const INDIRECT_PING_COUNT: usize = 3;

/// SWIM protocol state machine. No async, no channels, no real timers.
///
/// Driven by two inputs:
///   - `step(src, packet)` — a packet arrived from the network
///   - `tick()`            — one unit of logical time has passed (100ms in production)
///
/// All outbound packets are buffered in `pending_outbound`; drain with `take_outbound()`.
///
/// # Member State Machine
///
/// ```text
///                    tick: every PROTOCOL_PERIOD_TICKS
///                    └─ start_probe() sends Ping
///                              │
///                              ▼
///   (new node) ──────────► Alive
///        ▲                    │  step: Ack with matching seq
///        │                    │  └─ pending_probes.remove() — probe cancelled, stays Alive
///        │                    │
///        │                    │  tick: ACK_TIMEOUT_TICKS elapse, no Ack
///        │                    │  └─ start_indirect_probe() sends PingReq to K helpers
///        │                    │
///        │                    │  tick: INDIRECT_TIMEOUT_TICKS elapse, still no Ack
///        │                    │  (or no helpers available → skip indirect immediately)
///        │                    │  └─ try_mark_suspect()
///        │                    │
///        │                    ▼
///        │                 Suspect
///        │                    │  step: higher-incarnation Alive gossip (refutation)
///        │◄───────────────────┘  └─ apply_membership_update() → Suspect → Alive
///        │
///        │                    │  tick: SUSPECT_TIMEOUT_TICKS elapse, no refutation
///        │                    │  └─ try_mark_dead()
///        │                    │
///        │                    ▼
///        │                  Dead
///        │                    │  step: higher-incarnation Alive gossip
///        └────────────────────┘  └─ update_member() — higher inc currently wins
///                                   (see TODO in update_member: per the original SWIM
///                                    paper, Dead is a terminal state and should NOT
///                                    be overridden by any incarnation number)
/// ```
pub struct SwimStateMachine {
    // Identity
    pub(crate) node_id: NodeId,
    pub(crate) local_addr: SocketAddr,
    pub(crate) incarnation: u64,

    // Protocol state
    pub(crate) members: HashMap<NodeId, SwimNode>,
    pub(crate) live_node_tracker: LiveNodeTracker,
    pub(crate) gossip_buffer: GossipBuffer,
    pub(crate) topology: Topology,

    // Tick-based timers
    pub(crate) protocol_elapsed: u32,
    pub(crate) pending_probes: HashMap<u32, ActiveProbe>,
    pub(crate) suspect_timers: HashMap<NodeId, u32>,

    // Sequence
    pub(crate) seq_counter: u32,

    // Output buffer
    pub(crate) pending_outbound: Vec<OutboundPacket>,
}

pub(crate) struct ActiveProbe {
    target_node_id: NodeId,
    phase: ProbePhase,
    ticks_remaining: u32,
}

pub(crate) enum ProbePhase {
    Direct,
    Indirect,
}

impl SwimStateMachine {
    pub fn new(node_id: NodeId, local_addr: SocketAddr, topology: Topology) -> Self {
        Self {
            node_id,
            local_addr,
            incarnation: 0,
            members: HashMap::new(),
            live_node_tracker: LiveNodeTracker::default(),
            gossip_buffer: GossipBuffer::default(),
            topology,
            protocol_elapsed: 0,
            pending_probes: HashMap::new(),
            suspect_timers: HashMap::new(),
            seq_counter: 0,
            pending_outbound: vec![],
        }
    }

    /// Register this node as Alive in its own member map and topology.
    /// Must be called once before the first `tick()` or `step()`.
    pub fn init_self(&mut self) {
        self.update_member(
            self.node_id.clone(),
            self.local_addr,
            SwimNodeState::Alive,
            self.incarnation,
        );
    }

    pub fn tick(&mut self) {
        // Age every in-flight probes
        let mut timeout_seqs: Vec<u32> = vec![];
        for (seq, probe) in self.pending_probes.iter_mut() {
            probe.ticks_remaining -= 1;
            if probe.ticks_remaining == 0 {
                timeout_seqs.push(*seq);
            }
        }

        for seq in timeout_seqs {
            let probe = self.pending_probes.remove(&seq).unwrap();
            match probe.phase {
                ProbePhase::Direct => {
                    // We preserve the previous direct probe's seq so that we can cancel indirect probe timeout when we receive long running Ack message from the previous direct probe
                    self.start_indirect_probe(probe.target_node_id, seq);
                }
                ProbePhase::Indirect => {
                    self.try_mark_suspect(probe.target_node_id);
                }
            }
        }

        // 2. Age suspect timers, collect the ones that expired
        let expired: Vec<NodeId> = self
            .suspect_timers
            .iter_mut()
            .filter_map(|(id, ticks)| {
                *ticks -= 1;
                (*ticks == 0).then(|| id.clone())
            })
            .collect();

        for node_id in expired {
            self.suspect_timers.remove(&node_id);
            self.try_mark_dead(node_id);
        }

        // 3. Advance the protocol clock - start a new probe when the period elapses
        self.protocol_elapsed += 1;
        if self.protocol_elapsed >= PROBE_INTERVAL_TICKS {
            self.protocol_elapsed = 0;
            self.start_probe();
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

            let target_addr = match self.members.get(&target_node_id).map(|m| m.addr) {
                Some(addr) => addr,
                None => return,
            };

            let seq = self.next_seq();
            let msg = SwimPacket::Ping {
                seq,
                source_node_id: self.node_id.clone(),
                source_incarnation: self.incarnation,
                gossip: self.gossip_buffer.collect(),
            };
            self.push_outbound(target_addr, msg);

            self.pending_probes.insert(
                seq,
                ActiveProbe {
                    target_node_id,
                    phase: ProbePhase::Direct,
                    ticks_remaining: DIRECT_ACK_TIMEOUT_TICKS,
                },
            );
        }
    }

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

        let msg = SwimPacket::PingReq {
            seq,
            target: target_addr,
            source_node_id: self.node_id.clone(),
            source_incarnation: self.incarnation,
            gossip: self.gossip_buffer.collect(),
        };
        for peer_addr in peer_addrs {
            self.push_outbound(peer_addr, msg.clone());
        }

        self.pending_probes.insert(
            seq,
            ActiveProbe {
                target_node_id,
                phase: ProbePhase::Indirect,
                ticks_remaining: INDIRECT_ACK_TIMEOUT_TICKS,
            },
        );
    }

    fn try_mark_suspect(&mut self, target_node_id: NodeId) {
        if let Some(member) = self.members.get(&target_node_id) {
            if member.state != SwimNodeState::Alive {
                return;
            }

            let (addr, incarnation) = (member.addr, member.incarnation);
            println!("Node {} is SUSPECT(Inc: {})", target_node_id, incarnation);
            self.update_member(
                target_node_id.clone(),
                addr,
                SwimNodeState::Suspect,
                incarnation,
            );
            self.suspect_timers
                .insert(target_node_id, SUSPECT_TIMEOUT_TICKS);
        }
    }

    fn try_mark_dead(&mut self, target_node_id: NodeId) {
        if let Some(member) = self.members.get(&target_node_id) {
            if member.state != SwimNodeState::Suspect {
                return;
            }

            let (addr, incarnation) = (member.addr, member.incarnation);
            println!("Node {} is DEAD(Inc: {})", target_node_id, incarnation);
            self.update_member(
                target_node_id.clone(),
                addr,
                SwimNodeState::Dead,
                incarnation,
            );
        }
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
                self.handle_incarnation_check(source_node_id, src, source_incarnation);
                let ack = SwimPacket::Ack {
                    seq,
                    source_node_id: self.node_id.clone(),
                    source_incarnation: self.incarnation,
                    gossip: self.gossip_buffer.collect(),
                };
                self.push_outbound(src, ack);
            }

            SwimPacket::Ack {
                seq,
                source_node_id,
                source_incarnation,
                ..
            } => {
                // TODO: should we ONLY handle Ack message with seq that we can identify?
                self.handle_incarnation_check(source_node_id, src, source_incarnation);
                self.pending_probes.remove(&seq);

                // Do NOT cancel the suspect timer here. A same-or-lower incarnation Ack
                // could be a delayed packet from before the node knew it was suspected.
                // Only `handle_incarnation_check` above can clear suspicion, and only
                // when the sender proves awareness by sending a strictly higher incarnation.
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
                self.push_outbound(target, ping);
            }
        }
    }

    fn apply_membership_update(&mut self, member: SwimNode) {
        // Refutation
        if member.node_id == self.node_id {
            if member.state.not_alive() && self.incarnation <= member.incarnation {
                let new_incarnation = member.incarnation + 1;
                println!(
                    "Refuting suspicion! (My Inc: {} -> {})",
                    self.incarnation, new_incarnation
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
        let (new_state, changed) = match (self.members).entry(node_id.clone()) {
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
            let member = self.members[&node_id].clone();
            self.gossip_buffer.enqueue(member, self.members.len());
        }
    }

    fn next_seq(&mut self) -> u32 {
        self.seq_counter = self.seq_counter.wrapping_add(1);
        self.seq_counter
    }

    fn push_outbound(&mut self, target: SocketAddr, packet: SwimPacket) {
        self.pending_outbound
            .push(OutboundPacket { target, packet })
    }

    /// Drain all outbound packets buffered since the last call.
    /// Call this after every `step()` or `tick()` to retrieve packets to send.
    pub fn take_outbound(&mut self) -> Vec<OutboundPacket> {
        std::mem::take(&mut self.pending_outbound)
    }

    #[cfg(test)]
    pub fn probe_seq_for(&self, node_id: &str) -> Option<u32> {
        self.pending_probes
            .iter()
            .find(|(_, probe)| &*probe.target_node_id == node_id)
            .map(|(&seq, _)| seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clusters::topology::{Topology, TopologyConfig};
    use std::collections::HashMap;
    use std::net::SocketAddr;

    fn make_machine(local_id: &str, local_port: u16) -> SwimStateMachine {
        let addr: SocketAddr = format!("127.0.0.1:{}", local_port).parse().unwrap();
        let topology = Topology::new(
            HashMap::new(),
            TopologyConfig {
                vnodes_per_pnode: 256,
            },
        );
        let mut m = SwimStateMachine::new(NodeId::new(local_id), addr, topology);
        m.init_self();
        m
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

    fn add_node(m: &mut SwimStateMachine, id: &str, addr: SocketAddr, inc: u64) {
        m.step(addr, ping(1, id, inc, vec![]));
        let _ = m.take_outbound();
    }

    fn tick_until<T>(
        m: &mut SwimStateMachine,
        max_ticks: u32,
        mut f: impl FnMut(&SwimStateMachine) -> Option<T>,
    ) -> T {
        for _ in 0..max_ticks {
            m.tick();
            if let Some(v) = f(m) {
                return v;
            }
        }
        panic!("condition not met after {max_ticks} ticks");
    }

    // -----------------------------------------------------------------------
    // Ping tests
    // -----------------------------------------------------------------------

    mod ping {
        use crate::clusters::swim_state_machine::tests::{make_machine, node, ping};
        use crate::clusters::{NodeId, SwimNodeState, SwimPacket};
        use std::net::SocketAddr;

        #[test]
        fn ping_from_unknown_node() {
            let mut m = make_machine("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();

            m.step(sender, ping(1, "node-b", 0, vec![]));

            let member = m
                .members
                .get("node-b")
                .expect("unknown node should be added");
            assert_eq!(member.state, SwimNodeState::Alive);
            assert_eq!(member.addr, sender);

            let out = m.take_outbound();
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].target, sender);
            assert!(matches!(&out[0].packet, SwimPacket::Ack { seq: 1, .. }));
        }

        #[test]
        fn ping_from_known_alive_node() {
            let mut m = make_machine("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();

            // First ping: introduces node-b into members
            m.step(sender, ping(1, "node-b", 2, vec![]));
            let _ = m.take_outbound();

            let state_before = m.members.get("node-b").unwrap().state;
            let inc_before = m.members.get("node-b").unwrap().incarnation;

            // Second ping: same node, same incarnation
            m.step(sender, ping(2, "node-b", 2, vec![]));

            let member = m.members.get("node-b").unwrap();
            assert_eq!(member.state, state_before);
            assert_eq!(member.incarnation, inc_before);

            let out = m.take_outbound();
            assert_eq!(out.len(), 1);
            assert_eq!(out[0].target, sender);
            assert!(matches!(&out[0].packet, SwimPacket::Ack { seq: 2, .. }));
        }

        #[test]
        fn ping_with_gossip_applied_before_ack() {
            let mut m = make_machine("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();

            let gossip_entry = node("node-c", 9001, SwimNodeState::Alive, 1);
            m.step(sender, ping(1, "node-b", 0, vec![gossip_entry]));

            // Gossip applied: node-c is in members
            assert!(
                m.members.contains_key("node-c"),
                "gossiped node should be in members"
            );

            // Ack's gossip reflects the update applied during this step
            let out = m.take_outbound();
            assert_eq!(out.len(), 1);
            match &out[0].packet {
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
            let mut m = make_machine("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();

            // Introduce node-b at incarnation 1
            m.step(sender, ping(1, "node-b", 1, vec![]));
            let _ = m.take_outbound();
            assert_eq!(m.members.get("node-b").unwrap().incarnation, 1);

            // Ping from node-b with higher incarnation 5
            m.step(sender, ping(2, "node-b", 5, vec![]));
            let _ = m.take_outbound();

            let member = m.members.get("node-b").unwrap();
            assert_eq!(member.state, SwimNodeState::Alive);
            assert_eq!(member.incarnation, 5);
        }

        #[test]
        fn ping_sender_lower_incarnation_does_not_downgrade() {
            let mut m = make_machine("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();

            // Introduce node-b at incarnation 5
            m.step(sender, ping(1, "node-b", 5, vec![]));
            let _ = m.take_outbound();
            assert_eq!(m.members.get("node-b").unwrap().incarnation, 5);

            // Ping from node-b with lower incarnation 1
            m.step(sender, ping(2, "node-b", 1, vec![]));
            let _ = m.take_outbound();

            let member = m.members.get("node-b").unwrap();
            assert_eq!(member.state, SwimNodeState::Alive);
            assert_eq!(member.incarnation, 5, "incarnation must not be downgraded");
        }
    }

    mod ack {
        use crate::clusters::SwimNodeState;
        use crate::clusters::swim_state_machine::tests::{ack, add_node, make_machine, tick_until};
        use crate::clusters::swim_state_machine::{
            DIRECT_ACK_TIMEOUT_TICKS, INDIRECT_ACK_TIMEOUT_TICKS, PROBE_INTERVAL_TICKS, ProbePhase,
        };
        use std::net::SocketAddr;

        #[test]
        fn ack_matching_direct_probe_removes_probe() {
            let mut m = make_machine("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();
            let node_b_id = "node-b";
            add_node(&mut m, node_b_id, sender, 1);

            // wait until direct ping is sent
            let seq = tick_until(&mut m, 2 * PROBE_INTERVAL_TICKS, |m| {
                m.probe_seq_for(node_b_id)
            });

            // let's Ack before direct ping times out
            m.step(sender, ack(seq, node_b_id, 1, vec![]));
            let _ = m.take_outbound();

            assert!(!m.pending_probes.contains_key(&seq));
        }

        #[test]
        fn ack_matching_indirect_probe_removes_probe() {
            let mut m = make_machine("node-local", 8000);
            let node_b = "node-b";
            let node_c = "node-c";
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            add_node(&mut m, node_b, b_addr, 1);

            // Wait until node-b's direct probe starts
            let seq = tick_until(&mut m, 2 * PROBE_INTERVAL_TICKS, |m| {
                m.probe_seq_for(node_b)
            });
            let _ = m.take_outbound(); // discard the ping

            add_node(&mut m, node_c, c_addr, 1);

            // Let direct probe timeout for node-b
            for _ in 0..DIRECT_ACK_TIMEOUT_TICKS {
                m.tick();
            }
            let _ = m.take_outbound(); // discard the PingReqs

            assert!(
                matches!(
                    m.pending_probes.get(&seq).unwrap().phase,
                    ProbePhase::Indirect
                ),
                "probe should be in Indirect phase after direct timeout"
            );

            // Send Ack with the (reused) seq — clears the indirect probe
            m.step(b_addr, ack(seq, node_b, 1, vec![]));

            assert!(
                m.pending_probes.get(&seq).is_none(),
                "indirect probe should be removed after matching Ack"
            );
        }

        #[test]
        fn late_ack_same_incarnation_does_not_refute_suspect() {
            let mut m = make_machine("node-local", 8000);
            let node_b = "node-b";
            let node_c = "node-c";
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            add_node(&mut m, node_b, b_addr, 1);
            let seq = tick_until(&mut m, 2 * PROBE_INTERVAL_TICKS, |m| {
                m.probe_seq_for(node_b)
            });
            let _ = m.take_outbound();

            // we don't care about node c. It's for indirect request
            add_node(&mut m, node_c, c_addr, 1);

            for _ in 0..DIRECT_ACK_TIMEOUT_TICKS {
                m.tick()
            } // we now send indirect ping for node-b
            let _ = m.take_outbound();
            for _ in 0..INDIRECT_ACK_TIMEOUT_TICKS {
                m.tick()
            } // node-b is not suspect

            assert_eq!(m.members.get(node_b).unwrap().state, SwimNodeState::Suspect);

            // Ack with same incarnation: remote_inc(1) > member.incarnation(1) → false → no-op
            m.step(b_addr, ack(seq, node_b, 1, vec![]));

            assert_eq!(
                m.members.get(node_b).unwrap().state,
                SwimNodeState::Suspect,
                "same incarnation Ack should not refute suspicion"
            );
        }

        #[test]
        fn late_ack_with_higher_incarnation_refutes_suspect() {
            let mut m = make_machine("node-local", 8000);
            let node_b = "node-b";
            let node_c = "node-c";
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            add_node(&mut m, node_b, b_addr, 1);
            let seq = tick_until(&mut m, 2 * PROBE_INTERVAL_TICKS, |m| {
                m.probe_seq_for(node_b)
            });
            let _ = m.take_outbound();

            // we don't care about node c. It's for indirect request
            add_node(&mut m, node_c, c_addr, 1);

            for _ in 0..DIRECT_ACK_TIMEOUT_TICKS {
                m.tick()
            } // we now send indirect ping for node-b
            let _ = m.take_outbound();
            for _ in 0..INDIRECT_ACK_TIMEOUT_TICKS {
                m.tick()
            } // node-b is now suspect

            assert_eq!(m.members.get(node_b).unwrap().state, SwimNodeState::Suspect);

            // Ack with higher incarnation: remote_inc(2) > member.incarnation(1) → update to Alive
            m.step(b_addr, ack(seq, node_b, 2, vec![]));

            assert_eq!(
                m.members.get(node_b).unwrap().state,
                SwimNodeState::Alive,
                "higher incarnation Ack should refute suspicion"
            );
            // Suspect timer still running — try_mark_dead guard will see Alive and no-op
            assert!(m.suspect_timers.contains_key(node_b));
        }
    }

    mod step_pingreq {
        use crate::clusters::swim_state_machine::tests::{make_machine, node, pingreq};
        use crate::clusters::{SwimNode, SwimNodeState, SwimPacket};
        use std::hint::assert_unchecked;
        use std::net::SocketAddr;

        #[test]
        fn pingreq_sends_ping_to_target() {
            let mut m = make_machine("node-local", 8000);
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            m.step(b_addr, pingreq(1, "node-b", 1, c_addr, vec![]));

            // A Ping must be sent to the target (node-c)
            let out = m.take_outbound();
            assert_eq!(out.len(), 1);
            assert!(
                out.iter()
                    .any(|p| p.target == c_addr && matches!(p.packet, SwimPacket::Ping { .. }))
            );

            // handle_incarnation_check ran: sender node-b should be in members
            let member = m
                .members
                .get("node-b")
                .expect("node-b should be added to members");
            assert_eq!(member.state, SwimNodeState::Alive);
        }

        #[test]
        fn pingreq_gossip_applied_before_proxy_ping() {
            let mut m = make_machine("node-local", 8000);
            let b_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
            let c_addr: SocketAddr = "127.0.0.1:9002".parse().unwrap();

            let gossip_entry = node("node-d", 9003, SwimNodeState::Alive, 1);
            m.step(b_addr, pingreq(1, "node-b", 1, c_addr, vec![gossip_entry]));

            // Gossip applied: node-d should be in members
            assert!(
                m.members.contains_key("node-d"),
                "gossiped node-d should be in members"
            );

            // Proxy Ping's gossip should reflect the update (gossip applied in Phase 1, before Ping built)
            let out = m.take_outbound();
            assert_eq!(out.len(), 1);
            match &out[0].packet {
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
        use crate::clusters::swim_state_machine::tests::{make_machine, node, ping};
        use crate::clusters::{SwimNodeState, SwimPacket};
        use std::net::SocketAddr;

        #[test]
        fn refutation_bumps_to_gossip_inc_plus_one() {
            let mut m = make_machine("node-local", 8080);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();
            m.incarnation = 2;

            m.step(
                sender,
                ping(
                    1,
                    "node-b",
                    0,
                    vec![node("node-local", 8080, SwimNodeState::Suspect, 5)],
                ),
            );

            assert_eq!(m.incarnation, 6, "must bump to gossip.inc + 1 = 6");
            let out = m.take_outbound();
            match &out[0].packet {
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
            let mut m = make_machine("node-local", 8000);
            let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();
            m.incarnation = 3;

            m.step(
                sender,
                ping(
                    1,
                    "node-b",
                    0,
                    vec![node("node-local", 8000, SwimNodeState::Suspect, 2)],
                ),
            );

            assert_eq!(m.incarnation, 3, "local inc must not change");
        }
    }

    #[test]
    fn refutation_on_dead_gossip_current_behavior() {
        // TODO: Per SWIM spec, Dead is terminal and should NOT trigger refutation.
        // Fix: add `if member.state == Dead { return; }` guard in apply_membership_update.
        let mut m = make_machine("node-local", 8000);
        let sender: SocketAddr = "127.0.0.1:9000".parse().unwrap();

        m.step(sender, ping(1, "node-b", 0, vec![
            node("node-local", 8000, SwimNodeState::Dead, 0),
        ]));

        // TODO: Fix current (incorrect) behavior: Dead gossip triggers refutation
        assert_eq!(m.incarnation, 1, "current impl refutes Dead — this should change when TODO is fixed");
    }
}
