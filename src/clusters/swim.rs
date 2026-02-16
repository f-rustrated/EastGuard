use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

use crate::config::SERDE_CONFIG;
use anyhow::Result;
use bincode;

/// Messages that the SwimActor can receive.
#[derive(Debug, bincode::Decode, bincode::Encode)]
pub enum SwimMessage {
    /// A new peer has connected to this node.
    PeerConnected { addr: SocketAddr },
    /// Request to ping a specific member.
    Ping { target_addr: SocketAddr },
    /// Acknowledgment of a ping.
    PingAck { pinger_addr: SocketAddr },
    // TODO: Add more messages for SWIM protocol (e.g., MemberUpdate)
}

#[derive(Debug, Clone, PartialEq)]
pub enum MemberState {
    Alive,
    Suspect,
    Dead,
}

/// The SwimActor manages the cluster membership and SWIM protocol.
pub struct SwimActor {
    /// Receiver for incoming messages.
    receiver: mpsc::Receiver<SwimMessage>,

    membership: HashMap<SocketAddr, MemberState>,
    /// Sender to send messages to itself (for periodic tasks like pinging).
    self_sender: mpsc::Sender<SwimMessage>,
    /// The local address of this peer.
    local_peer_addr: SocketAddr,
    /// UDP socket for sending and receiving SWIM messages.
    socket: UdpSocket,
    // TODO: Add other SWIM-related state (e.g., incarnation numbers, suspect lists)
}

impl SwimActor {
    /// Creates a new SwimActor and its associated message sender.
    pub async fn new(local_peer_addr: SocketAddr) -> Result<(mpsc::Sender<SwimMessage>, Self)> {
        const QUEUE_SIZE: usize = 100;

        let socket = UdpSocket::bind(local_peer_addr).await?;

        let (sender, receiver) = mpsc::channel(QUEUE_SIZE);
        let actor = SwimActor {
            receiver,
            membership: HashMap::new(),
            self_sender: sender.clone(),
            local_peer_addr,
            socket,
        };
        Ok((sender, actor))
    }

    #[inline]
    async fn send_swim_message(
        &self,
        target_addr: SocketAddr,
        message: &SwimMessage,
    ) -> Result<()> {
        let encoded_message = bincode::encode_to_vec(message, SERDE_CONFIG)?;
        self.socket.send_to(&encoded_message, target_addr).await?;
        println!("Successfully sent SWIM message to {}", target_addr);
        Ok(())
    }

    pub async fn run(mut self) {
        // Periodic ping interval
        let mut ping_interval = time::interval(Duration::from_secs(1)); // Ping every 1 second for testing
        let mut buf = vec![0u8; 512]; // Buffer for incoming UDP messages
        let mut rng = StdRng::from_entropy(); // Initialize RNG

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    // Time to send a ping
                    if let Some(target_addr) = self.membership.keys().choose(&mut rng) {

                        if *target_addr == self.local_peer_addr {
                            // Don't ping ourselves
                            continue;
                        }

                        let self_addr = self.local_peer_addr;
                        if let Err(e) = self.send_swim_message( *target_addr, &SwimMessage::Ping { target_addr: self_addr }).await {
                            eprintln!("Error sending Ping message to {}: {}", target_addr, e);
                        }
                    }
                }

                // Handle incoming messages
                Ok((len, remote_addr)) = self.socket.recv_from(&mut buf) => {
                     // ! SAFETY: Failure on this means either implementation bug / attack
                    let (message,_)= bincode::decode_from_slice(&buf[..len], SERDE_CONFIG).unwrap();

                    println!("Received SWIM message from {}: {:?}", remote_addr, message);
                    // Forward the message to the actor's own message queue
                    if let Err(e) = self.self_sender.send(message).await {
                        eprintln!("Failed to send received SWIM message to SwimActor: {}", e);
                    };

                }
                Some(message) = self.receiver.recv() => {
                    match message {
                        SwimMessage::PeerConnected { addr } => {
                            println!("SwimActor received PeerConnected from: {}", addr);
                            self.membership.entry(addr).or_insert(MemberState::Alive);
                            println!("Current membership: {:?}", self.membership);
                        }
                        SwimMessage::Ping { target_addr } => {
                            println!("SwimActor received internal Ping request for: {}", target_addr);
                            // This is a Ping message received from another peer.
                            // We should respond with a PingAck.
                            let self_addr = self.local_peer_addr;
                            if let Err(e) = self.send_swim_message( target_addr, &SwimMessage::PingAck { pinger_addr: self_addr }).await {
                                eprintln!("Error sending PingAck message to {}: {}", target_addr, e);
                            }
                        }
                        SwimMessage::PingAck { pinger_addr } => {
                            println!("SwimActor received PingAck from: {}", pinger_addr);
                            // Mark member as Alive if it was Suspect
                            if let Some(state) = self.membership.get_mut(&pinger_addr) {
                                if *state == MemberState::Suspect {
                                    *state = MemberState::Alive;
                                    println!("Member {} is now Alive.", pinger_addr);
                                }
                            }
                        }
                    }
                }
                else => break, // All senders dropped, actor can stop
            }
        }
        println!("SwimActor stopped.");
    }
}
