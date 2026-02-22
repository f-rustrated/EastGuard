use crate::clusters::gossip_buffer::GossipBuffer;
use crate::clusters::livenode_tracker::LiveNodeTracker;
use crate::clusters::topology::Topology;
use crate::clusters::{NodeId, OutboundPacket, SwimNode, SwimNodeState, SwimPacket};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::SocketAddr;

const PROTOCOL_PERIOD_TICKS: u32 = 10; // 10 × 100ms = 1s
const ACK_TIMEOUT_TICKS: u32 = 3; // 3 × 100ms = 300ms
const INDIRECT_TIMEOUT_TICKS: u32 = 3; // 3 × 100ms = 300ms
const SUSPECT_TIMEOUT_TICKS: u32 = 50; // 50 × 100ms = 5s
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
                    self.start_indirect_probe(probe.target_node_id);
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
        if self.protocol_elapsed >= PROTOCOL_PERIOD_TICKS {
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
                    ticks_remaining: ACK_TIMEOUT_TICKS,
                },
            );
        }
    }

    fn start_indirect_probe(&mut self, target_node_id: NodeId) {
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

        let seq = self.next_seq();
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
                ticks_remaining: INDIRECT_TIMEOUT_TICKS,
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
                self.handle_incarnation_check(source_node_id, src, source_incarnation);
                self.pending_probes.remove(&seq);
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
}
