use crate::clusters::swims::{OutboundPacket, SwimCommand};

// ==========================================
// TRANSPORT LAYER (Presentation)
// ==========================================
use super::*;
use crate::net::UdpSocket;
use tokio::sync::mpsc;

pub struct SwimTransportActor;

impl SwimTransportActor {
    pub async fn run(
        socket: UdpSocket,
        to_actor: mpsc::Sender<SwimCommand>,
        mut from_actor: mpsc::Receiver<OutboundPacket>,
    ) {
        tracing::info!(
            "Transport Layer listening on {}",
            socket.local_addr().unwrap()
        );

        let mut buf = [0u8; 1024];
        loop {
            tokio::select! {
                // INCOMING: Socket -> Decode -> Actor
                Ok((len, src)) = socket.recv_from(&mut buf) => {
                    match bincode::decode_from_slice(&buf[..len], BINCODE_CONFIG) {
                        Ok((packet, _)) => {
                             let _ = to_actor.send(SwimCommand::PacketReceived { src, packet }).await;
                        }
                        Err(e) => tracing::error!("Failed to decode packet from {}: {}", src, e),
                    }
                }

                // OUTGOING: Actor -> Encode -> Socket
                Some(msg) = from_actor.recv() => {
                    match bincode::encode_to_vec(msg.packet(), BINCODE_CONFIG) {
                        Ok(bytes) => {
                            let _ = socket.send_to(&bytes, msg.target).await;
                        }
                        Err(e) => tracing::error!("Failed to encode packet: {}", e),
                    }
                }
            }
        }
    }
}
