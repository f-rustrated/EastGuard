use crate::clusters::livenode_tracker::LiveNodeTracker;

use super::*;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

use crate::clusters::topology::Topology;

// --- CONFIGURATION ---
const PROTOCOL_PERIOD: Duration = Duration::from_secs(1);
const ACK_TIMEOUT: Duration = Duration::from_millis(300);
const SUSPECT_TIMEOUT: Duration = Duration::from_secs(5);
const INDIRECT_PING_COUNT: usize = 3;

// ==========================================
// PROTOCOL LAYER (SWIM Actor)
// ==========================================

pub struct SwimActor {
    // Communication
    mailbox: mpsc::Receiver<ActorEvent>,    // Inbound events
    self_tx: mpsc::Sender<ActorEvent>,      // To schedule own timers
    outbound: mpsc::Sender<OutboundPacket>, // To Network

    // State
    topology: Topology,
    node_id: NodeId,
    local_addr: SocketAddr,
    incarnation: u64,
    members: HashMap<NodeId, Member>,
    livenode_tracker: LiveNodeTracker,

    // Probe State
    seq_counter: u32,                   // "correlation ID" for the message
    pending_acks: HashMap<u32, NodeId>, // Seq -> Target node_id
}

impl SwimActor {
    pub fn new(
        local_addr: SocketAddr,
        node_id: NodeId,
        mailbox: mpsc::Receiver<ActorEvent>,
        self_tx: mpsc::Sender<ActorEvent>,
        outbound: mpsc::Sender<OutboundPacket>,
        topology: Topology,
    ) -> Self {
        Self {
            mailbox,
            self_tx,
            outbound,
            topology,
            node_id,
            local_addr,
            incarnation: 0,
            members: HashMap::new(),
            livenode_tracker: LiveNodeTracker::default(),
            seq_counter: 0,
            pending_acks: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        let mut ticker = time::interval(PROTOCOL_PERIOD);

        println!("SwimActor started. Local Incarnation: {}", self.incarnation);

        // Register ourselves so downstream components (e.g. topology ring) know we exist.
        self.update_member(
            self.node_id.clone(),
            self.local_addr,
            NodeState::Alive,
            self.incarnation,
        );

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.perform_protocol_tick().await;
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
                self.handle_packet(src, packet).await;
            }

            ActorEvent::ProtocolTick => { /* Handled by loop ticker */ }

            // --- FAILURE DETECTION TIMEOUTS ---
            ActorEvent::DirectProbeTimeout { target_node_id, seq, } => {
                // If the ACK for this sequence hasn't arrived yet
                if self.pending_acks.contains_key(&seq) {
                    // Direct ping failed. Try Indirect.
                    if let Some(target) = self.members.get(&target_node_id) {
                        self.start_indirect_probe(target.clone()).await;
                    }
                }
            }

            ActorEvent::IndirectProbeTimeout { target_node_id } => {
                // Indirect pings failed. Mark Suspect.
                if let Some(member) = self.members.get(&target_node_id) {
                    if member.state == NodeState::Alive {
                        println!("Node {} is SUSPECT (Inc: {})", target_node_id, member.incarnation);
                        let (addr, incarnation) = (member.addr, member.incarnation);
                        self.update_member(target_node_id.clone(), addr, NodeState::Suspect, incarnation, );

                        // Schedule Suspect -> Dead transition
                        let tx = self.self_tx.clone();
                        tokio::spawn(async move {
                            time::sleep(SUSPECT_TIMEOUT).await;
                            let _ = tx.send(ActorEvent::SuspectTimeout { target_node_id }).await;
                        });
                    }
                }
            }

            ActorEvent::SuspectTimeout { target_node_id } => {
                // If still Suspect after timeout, mark Dead.
                if let Some(member) = self.members.get(&target_node_id) {
                    if member.state == NodeState::Suspect {
                        println!("Node {} is DEAD", target_node_id);
                        let (addr, incarnation) = (member.addr, member.incarnation);
                        self.update_member(target_node_id, addr, NodeState::Dead, incarnation);
                    }
                }
            }
        }
    }

    // --- PACKET HANDLING LOGIC ---

    async fn handle_packet(&mut self, src: SocketAddr, packet: SwimPacket) {
        // 1. Process Gossip (Piggybacked updates)
        let (gossip_list, seq_opt) = match &packet {
            SwimPacket::Ping { gossip, seq, .. } => (gossip, Some(*seq)),
            SwimPacket::Ack { gossip, seq, .. } => (gossip, Some(*seq)),
            SwimPacket::PingReq { gossip, seq, .. } => (gossip, Some(*seq)),
        };

        for member in gossip_list {
            self.apply_membership_update(member.clone());
        }

        // 2. Handle Message Types
        match packet {
            SwimPacket::Ping {
                seq,
                source_node_id,
                source_incarnation,
                ..
            } => {
                self.handle_incarnation_check(source_node_id, src, source_incarnation);
                // Respond with Ack
                let ack = SwimPacket::Ack {
                    seq,
                    source_node_id: self.node_id.clone(),
                    source_incarnation: self.incarnation,
                    gossip: self.get_gossip(),
                };
                self.send(src, ack).await;
            }

            SwimPacket::Ack {
                seq,
                source_node_id,
                source_incarnation,
                ..
            } => {
                self.handle_incarnation_check(source_node_id, src, source_incarnation);
                // Success! Remove from pending acks.
                if self.pending_acks.remove(&seq).is_some() {
                    // We can optionally log latency here
                }
            }

            SwimPacket::PingReq {
                seq,
                source_node_id,
                target,
                source_incarnation,
                ..
            } => {
                self.handle_incarnation_check(source_node_id, src, source_incarnation);
                // Proxy Ping: We ping 'target'. If successful, we Ack 'src'.
                // Simplified: We send a standard Ping to target.
                // In a full implementation, we need to track that this Ack belongs to 'src'.
                // For this snippet, we just send a Ping to help the network.
                let my_seq = self.next_seq();
                let ping = SwimPacket::Ping {
                    seq: my_seq,
                    source_node_id: self.node_id.clone(),
                    source_incarnation: self.incarnation,
                    gossip: self.get_gossip(),
                };
                self.send(target, ping).await;
            }
        }
    }

    // --- CORE SWIM LOGIC (Incarnation & State) ---
    fn update_member(
        &mut self,
        node_id: NodeId,
        addr: SocketAddr,
        state: NodeState,
        incarnation: u64,
    ) {
        let new_state = match self.members.entry(node_id.clone()) {
            Entry::Vacant(e) => {
                // New member: set state directly, no SWIM rules apply yet.
                e.insert(Member {
                    node_id: node_id.clone(),
                    addr,
                    state,
                    incarnation,
                });
                state
            }
            Entry::Occupied(e) => {
                let entry = e.into_mut();
                // RULE: Higher incarnation always wins
                if incarnation > entry.incarnation {
                    entry.incarnation = incarnation;
                    entry.state = state;
                }
                // RULE: Equal incarnation, stricter state wins (Dead > Suspect > Alive)
                else if incarnation == entry.incarnation {
                    match (&entry.state, &state) {
                        (NodeState::Alive, NodeState::Suspect) => entry.state = NodeState::Suspect,
                        (NodeState::Alive, NodeState::Dead) => entry.state = NodeState::Dead,
                        (NodeState::Suspect, NodeState::Dead) => entry.state = NodeState::Dead,
                        _ => {}
                    }
                }
                entry.state
            }
        };

        self.livenode_tracker.update(node_id.clone(), new_state);
        self.topology.update(node_id, addr, new_state);
    }

    fn apply_membership_update(&mut self, member: Member) {
        // REFUTATION MECHANISM
        if member.node_id == self.node_id {
            // Someone is gossiping that I am Suspect or Dead?
            if (member.state == NodeState::Suspect || member.state == NodeState::Dead)
                && self.incarnation <= member.incarnation
            {
                let new_incarnation = member.incarnation + 1;
                println!(
                    "Refuting suspicion! (My Inc: {} -> {})",
                    self.incarnation, new_incarnation
                );
                self.incarnation = new_incarnation;
                // I don't need to broadcast immediately; next Ping/Ack will carry it.
            }
            return;
        }
        self.update_member(member.node_id, member.addr, member.state, member.incarnation);
    }

    fn handle_incarnation_check(&mut self, source_node_id: NodeId, addr: SocketAddr, remote_inc: u64) {
        // If we receive a direct message from someone, they are obviously Alive.
        // If their incarnation is higher than we thought, update it.
        if let Some(member) = self.members.get_mut(&source_node_id) {
            if remote_inc > member.incarnation {
                self.update_member(source_node_id.clone(), addr, NodeState::Alive, remote_inc);
            } else {
                // New member discovered via direct message
                self.update_member(source_node_id.clone(), addr, NodeState::Alive, remote_inc);
            }
        }
    }

    // --- PROBE MECHANICS ---

    async fn perform_protocol_tick(&mut self) {
        // 1. Pick random Alive member

        if self.livenode_tracker.is_empty() {
            return;
        }

        if let Some(target_node_id) = self.livenode_tracker.next() {
            if target_node_id == self.node_id {
                return;
            }

            let target_addr = match self.members.get(&target_node_id).map(|m| m.addr) {
                Some(addr) => addr,
                None => return,
            };

            let seq = self.next_seq();
            self.pending_acks.insert(seq, target_node_id.clone());

            let msg = SwimPacket::Ping {
                seq,
                source_node_id: self.node_id.clone(),
                source_incarnation: self.incarnation,
                gossip: self.get_gossip(),
            };
            self.send(target_addr, msg).await;

            // Schedule Direct Timeout
            let tx = self.self_tx.clone();
            tokio::spawn(async move {
                time::sleep(ACK_TIMEOUT).await;
                let _ = tx
                    .send(ActorEvent::DirectProbeTimeout { target_node_id, seq })
                    .await;
            });
        }
    }

    async fn start_indirect_probe(&mut self, target: Member) {
        let mut peer_addrs = Vec::new();

        for _ in 0..self.livenode_tracker.len() {
            // Stop once we have enough helpers
            if peer_addrs.len() >= INDIRECT_PING_COUNT {
                break;
            }

            // Round-robin
            if let Some(peer_node_id) = self.livenode_tracker.next() {
                // Filter: Don't ask the target or ourselves
                if peer_node_id != target.node_id && peer_node_id != self.node_id {
                    if let Some(peer_addr) = self.members.get(&peer_node_id).map(|m| m.addr) {
                        peer_addrs.push(peer_addr);
                    }
                }
            }
        }

        if peer_addrs.is_empty() {
            // No helpers -> Fail immediately.
            let _ = self
                .self_tx
                .send(ActorEvent::IndirectProbeTimeout {
                    target_node_id: target.node_id,
                })
                .await;
            return;
        }

        let seq = self.next_seq();

        // ! We don't track ACKs for PingReqs in this simple impl,
        // ! we just rely on the IndirectProbeTimeout to fire regardless.
        // ! If we get an ACK via a peer, the main handle_packet logic will clear the failure.
        let msg = SwimPacket::PingReq {
            seq,
            target: target.addr,
            source_node_id: self.node_id.clone(),
            source_incarnation: self.incarnation,
            gossip: self.get_gossip(),
        };

        for peer_addr in peer_addrs {
            self.send(peer_addr, msg.clone()).await;
        }

        // Schedule Indirect Timeout (Total time for buddy system)
        let tx = self.self_tx.clone();
        tokio::spawn(async move {
            time::sleep(ACK_TIMEOUT).await;
            let _ = tx
                .send(ActorEvent::IndirectProbeTimeout { target_node_id: target.node_id })
                .await;
        });
    }

    // --- HELPERS ---

    async fn send(&self, target: SocketAddr, packet: SwimPacket) {
        let _ = self.outbound.send(OutboundPacket { target, packet }).await;
    }

    // TODO need to keep track of recent events by
    // 1. Queuing : this will add some structural complexity with but more deterministic, fast propagation for the recent events if deduplication is done correctly.
    // 2. Random Selection : simple, not require heavy changes
    fn get_gossip(&self) -> Vec<Member> {
        // Send 3 random members (Optimized: prefer recent changes in real impl)
        self.members.values().take(3).cloned().collect()
    }

    fn next_seq(&mut self) -> u32 {
        self.seq_counter = self.seq_counter.wrapping_add(1);
        self.seq_counter
    }

    #[cfg(test)]
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    #[cfg(test)]
    pub fn init_self_for_test(&mut self) {
        self.update_member(
            self.node_id.clone(),
            self.local_addr,
            NodeState::Alive,
            self.incarnation,
        );
    }

    #[cfg(test)]
    pub async fn process_event_for_test(&mut self, event: ActorEvent) {
        self.handle_event(event).await;
    }
}
