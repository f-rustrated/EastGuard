// ==========================================
// TRANSPORT LAYER (Presentation)
// ==========================================
use super::*;
use std::sync::Arc;
use tokio::{net::UdpSocket, sync::mpsc};

pub struct TransportLayer {
    socket: Arc<UdpSocket>,
    to_actor: mpsc::Sender<ActorEvent>,
    from_actor: mpsc::Receiver<OutboundPacket>,
}

impl TransportLayer {
    pub async fn new(
        bind_addr: SocketAddr,
        to_actor: mpsc::Sender<ActorEvent>,
        from_actor: mpsc::Receiver<OutboundPacket>,
    ) -> anyhow::Result<Self> {
        let socket = Arc::new(UdpSocket::bind(bind_addr).await?);
        Ok(Self {
            socket,
            to_actor,
            from_actor,
        })
    }

    pub async fn run(mut self) {
        println!(
            "Transport Layer listening on {}",
            self.socket.local_addr().unwrap()
        );

        let mut buf = [0u8; 1024];
        loop {
            tokio::select! {
                // INCOMING: Socket -> Decode -> Actor
                Ok((len, src)) = self.socket.recv_from(&mut buf) => {
                    match bincode::decode_from_slice(&buf[..len], BINCODE_CONFIG) {
                        Ok((packet, _)) => {
                             let _ = self.to_actor.send(ActorEvent::PacketReceived { src, packet }).await;
                        }
                        Err(e) => eprintln!("Failed to decode packet from {}: {}", src, e),
                    }
                }

                // OUTGOING: Actor -> Encode -> Socket
                Some(msg) = self.from_actor.recv() => {
                    match bincode::encode_to_vec(msg.packet(), BINCODE_CONFIG) {
                        Ok(bytes) => {
                            let _ = self.socket.send_to(&bytes, msg.target).await;
                        }
                        Err(e) => eprintln!("Failed to encode packet: {}", e),
                    }
                }
            }
        }
    }
}
