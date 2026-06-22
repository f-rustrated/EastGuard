use crate::control_plane::membership::{OutboundPacket, SwimCommand, actor::SwimSender};

// ==========================================
// TRANSPORT LAYER (Presentation)
// ==========================================
use crate::net::UdpSocket;
use tokio::sync::mpsc;

pub struct SwimTransportActor;

impl SwimTransportActor {
    pub async fn run(
        socket: UdpSocket,
        to_actor: SwimSender,
        mut from_actor: mpsc::Receiver<Box<[OutboundPacket]>>,
    ) {
        tracing::info!(
            "Transport Layer listening on {}",
            socket.local_addr().unwrap()
        );

        // MTU 1500 - IP header 20 - UDP header 8 = max unfragmented payload
        let mut buf = [0u8; 1472];
        loop {
            tokio::select! {
                // INCOMING: Socket -> Decode -> Actor
                Ok((len, src)) = socket.recv_from(&mut buf) => {
                    match borsh::from_slice(&buf[..len]) {
                        Ok(packet) => {
                             let _ = to_actor.send(SwimCommand::InboundRaftRpc { src, packet }).await;
                        }
                        Err(e) => tracing::error!("Failed to decode packet from {}: {}", src, e),
                    }
                }

                // OUTGOING: Actor -> Encode -> Socket
                Some(batch) = from_actor.recv() => {
                    for msg in batch {
                        match borsh::to_vec(msg.packet()) {
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
}
