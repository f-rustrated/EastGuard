use super::*;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use tokio::sync::mpsc;
use tokio::time::{self, Duration};

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
    receiver: mpsc::Receiver<ActorEvent>,   // Inbound events
    sender: mpsc::Sender<ActorEvent>,       // To schedule own timers
    outbound: mpsc::Sender<OutboundPacket>, // To Network

    // State
    local_addr: SocketAddr,
    incarnation: u64,
    members: HashMap<SocketAddr, Member>,
    alive_nodes: HashSet<SocketAddr>, // Optimization for random selection

    // Probe State
    seq_counter: u32,                       // "correlation ID" for the message
    pending_acks: HashMap<u32, SocketAddr>, // Seq -> Target
}

impl SwimActor {
    pub fn new(
        local_addr: SocketAddr,
        receiver: mpsc::Receiver<ActorEvent>,
        sender: mpsc::Sender<ActorEvent>,
        outbound: mpsc::Sender<OutboundPacket>,
    ) -> Self {
        Self {
            receiver,
            sender,
            outbound,
            local_addr,
            incarnation: 0,
            members: HashMap::new(),
            alive_nodes: HashSet::new(),
            seq_counter: 0,
            pending_acks: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        let mut rng = StdRng::from_entropy();
        let mut ticker = time::interval(PROTOCOL_PERIOD);

        println!("SwimActor started. Local Incarnation: {}", self.incarnation);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.perform_protocol_tick(&mut rng).await;
                }
                Some(event) = self.receiver.recv() => {
                    self.handle_event(event, &mut rng).await;
                }
            }
        }
    }

    async fn handle_event(&mut self, event: ActorEvent, rng: &mut StdRng) {
        match event {
            ActorEvent::PacketReceived { src, packet } => {
                self.handle_packet(src, packet).await;
            }

            ActorEvent::ProtocolTick => { /* Handled by loop ticker */ }

            // --- FAILURE DETECTION TIMEOUTS ---
            ActorEvent::DirectProbeTimeout { target, seq } => {
                // If the ACK for this sequence hasn't arrived yet
                if self.pending_acks.contains_key(&seq) {
                    // Direct ping failed. Try Indirect.
                    self.start_indirect_probe(target, rng).await;
                }
            }

            ActorEvent::IndirectProbeTimeout { target } => {
                // Indirect pings failed. Mark Suspect.
                if let Some(member) = self.members.get(&target) {
                    if member.state == NodeState::Alive {
                        println!("Node {} is SUSPECT (Inc: {})", target, member.incarnation);
                        self.update_member(target, NodeState::Suspect, member.incarnation);

                        // Schedule Suspect -> Dead transition
                        let tx = self.sender.clone();
                        tokio::spawn(async move {
                            time::sleep(SUSPECT_TIMEOUT).await;
                            let _ = tx.send(ActorEvent::SuspectTimeout { target }).await;
                        });
                    }
                }
            }

            ActorEvent::SuspectTimeout { target } => {
                // If still Suspect after timeout, mark Dead.
                if let Some(member) = self.members.get(&target) {
                    if member.state == NodeState::Suspect {
                        println!("Node {} is DEAD", target);
                        self.update_member(target, NodeState::Dead, member.incarnation);
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
                source_incarnation,
                ..
            } => {
                self.handle_incarnation_check(src, source_incarnation);
                // Respond with Ack
                let ack = SwimPacket::Ack {
                    seq,
                    source_incarnation: self.incarnation,
                    gossip: self.get_gossip(),
                };
                self.send(src, ack).await;
            }

            SwimPacket::Ack {
                seq,
                source_incarnation,
                ..
            } => {
                self.handle_incarnation_check(src, source_incarnation);
                // Success! Remove from pending acks.
                if self.pending_acks.remove(&seq).is_some() {
                    // We can optionally log latency here
                }
            }

            SwimPacket::PingReq {
                seq,
                target,
                source_incarnation,
                ..
            } => {
                self.handle_incarnation_check(src, source_incarnation);
                // Proxy Ping: We ping 'target'. If successful, we Ack 'src'.
                // Simplified: We send a standard Ping to target.
                // In a full implementation, we need to track that this Ack belongs to 'src'.
                // For this snippet, we just send a Ping to help the network.
                let my_seq = self.next_seq();
                let ping = SwimPacket::Ping {
                    seq: my_seq,
                    source_incarnation: self.incarnation,
                    gossip: self.get_gossip(),
                };
                self.send(target, ping).await;
            }
        }
    }

    // --- CORE SWIM LOGIC (Incarnation & State) ---

    fn update_member(&mut self, addr: SocketAddr, state: NodeState, incarnation: u64) {
        let entry = self.members.entry(addr).or_insert(Member {
            addr,
            state: NodeState::Dead, // Default until updated
            incarnation: 0,
        });

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
                _ => {} // No change
            }
        }

        // Maintain optimization set
        if entry.state == NodeState::Alive {
            self.alive_nodes.insert(addr);
        } else {
            self.alive_nodes.remove(&addr);
        }
    }

    fn apply_membership_update(&mut self, member: Member) {
        // REFUTATION MECHANISM
        if member.addr == self.local_addr {
            // Someone is gossiping that I am Suspect or Dead?
            if (member.state == NodeState::Suspect || member.state == NodeState::Dead)
                && self.incarnation <= member.incarnation
            {
                println!(
                    "Refuting suspicion! (My Inc: {} -> {})",
                    self.incarnation,
                    self.incarnation + 1
                );
                self.incarnation += 1;
                // I don't need to broadcast immediately; next Ping/Ack will carry it.
            }
            return;
        }
        self.update_member(member.addr, member.state, member.incarnation);
    }

    fn handle_incarnation_check(&mut self, src: SocketAddr, remote_inc: u64) {
        // If we receive a direct message from someone, they are obviously Alive.
        // If their incarnation is higher than we thought, update it.
        if let Some(member) = self.members.get_mut(&src) {
            if remote_inc > member.incarnation {
                member.incarnation = remote_inc;
                member.state = NodeState::Alive;
                self.alive_nodes.insert(src);
            }
        } else {
            // New member discovered via direct message
            self.update_member(src, NodeState::Alive, remote_inc);
        }
    }

    // --- PROBE MECHANICS ---

    async fn perform_protocol_tick(&mut self, rng: &mut StdRng) {
        // 1. Pick random Alive member

        if self.alive_nodes.is_empty() {
            return;
        }

        if let Some(&target) = self.alive_nodes.iter().choose(rng) {
            if target == self.local_addr {
                return;
            }

            let seq = self.next_seq();
            self.pending_acks.insert(seq, target);

            let msg = SwimPacket::Ping {
                seq,
                source_incarnation: self.incarnation,
                gossip: self.get_gossip(),
            };
            self.send(target, msg).await;

            // Schedule Direct Timeout
            let tx = self.sender.clone();
            tokio::spawn(async move {
                time::sleep(ACK_TIMEOUT).await;
                let _ = tx
                    .send(ActorEvent::DirectProbeTimeout { target, seq })
                    .await;
            });
        }
    }

    async fn start_indirect_probe(&mut self, target: SocketAddr, rng: &mut StdRng) {
        // Select K random peers
        let peers: Vec<SocketAddr> = self
            .alive_nodes
            .iter()
            .filter(|&&a| a != target && a != self.local_addr)
            .choose_multiple(rng, INDIRECT_PING_COUNT)
            .into_iter()
            .cloned()
            .collect();

        if peers.is_empty() {
            // No helpers -> Fail immediately.
            let _ = self
                .sender
                .send(ActorEvent::IndirectProbeTimeout { target })
                .await;
            return;
        }

        let seq = self.next_seq();

        // ! We don't track ACKs for PingReqs in this simple impl,
        // ! we just rely on the IndirectProbeTimeout to fire regardless.
        // ! If we get an ACK via a peer, the main handle_packet logic will clear the failure.
        let msg = SwimPacket::PingReq {
            seq,
            target,
            source_incarnation: self.incarnation,
            gossip: self.get_gossip(),
        };

        for peer in peers {
            self.send(peer, msg.clone()).await;
        }

        // Schedule Indirect Timeout (Total time for buddy system)
        let tx = self.sender.clone();
        tokio::spawn(async move {
            time::sleep(ACK_TIMEOUT).await;
            let _ = tx.send(ActorEvent::IndirectProbeTimeout { target }).await;
        });
    }

    // --- HELPERS ---

    async fn send(&self, target: SocketAddr, packet: SwimPacket) {
        let _ = self.outbound.send(OutboundPacket { target, packet }).await;
    }

    fn get_gossip(&self) -> Vec<Member> {
        // Send 3 random members (Optimized: prefer recent changes in real impl)
        self.members.values().take(3).cloned().collect()
    }

    fn next_seq(&mut self) -> u32 {
        self.seq_counter = self.seq_counter.wrapping_add(1);
        self.seq_counter
    }
}
