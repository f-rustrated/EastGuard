use std::io;
use crate::clusters::swims::{OutboundPacket, SwimCommand};
// ==========================================
// TRANSPORT LAYER (Presentation)
// ==========================================
use super::*;
use tokio::{net::UdpSocket, sync::mpsc};

pub trait UdpTransport: Send + 'static {
    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize>;
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)>;
    fn local_addr(&self) -> io::Result<SocketAddr>;
}

impl UdpTransport for UdpSocket {
    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.send_to(buf, target).await
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.recv_from(buf).await
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local_addr()
    }
}

pub struct SwimTransportActor<S: UdpTransport> {
    socket: S,
    to_actor: mpsc::Sender<SwimCommand>,
    from_actor: mpsc::Receiver<OutboundPacket>,
}

impl <S: UdpTransport> SwimTransportActor<S> {
    pub async fn new(
        socket: S, 
        to_actor: mpsc::Sender<SwimCommand>,
        from_actor: mpsc::Receiver<OutboundPacket>,
    ) -> anyhow::Result<Self> {
        // let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self {
            socket,
            to_actor,
            from_actor,
        })
    }

    pub async fn run(mut self) {
        tracing::info!(
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
                             let _ = self.to_actor.send(SwimCommand::PacketReceived { src, packet }).await;
                        }
                        Err(e) => tracing::error!("Failed to decode packet from {}: {}", src, e),
                    }
                }

                // OUTGOING: Actor -> Encode -> Socket
                Some(msg) = self.from_actor.recv() => {
                    match bincode::encode_to_vec(msg.packet(), BINCODE_CONFIG) {
                        Ok(bytes) => {
                            let _ = self.socket.send_to(&bytes, msg.target).await;
                        }
                        Err(e) => tracing::error!("Failed to encode packet: {}", e),
                    }
                }
            }
        }
    }
}
