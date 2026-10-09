#![allow(clippy::disallowed_types)]

mod tcp;
pub(crate) mod udp;

#[cfg(test)]
pub use tcp::OwnedWriteHalf;
pub use tcp::{TcpListener, TcpStream, TransportReadHalf, TransportTcpStream, TransportWriteHalf};
pub use udp::UdpSocket;

/// The deadline wins even when the operation is ready at the same instant.
pub(crate) async fn before_deadline<T>(
    deadline: tokio::time::Instant,
    operation: impl std::future::Future<Output = T>,
) -> Option<T> {
    if deadline <= tokio::time::Instant::now() {
        return None;
    }
    tokio::select! {
        biased;
        _ = tokio::time::sleep_until(deadline) => None,
        value = operation => (tokio::time::Instant::now() < deadline).then_some(value),
    }
}

#[cfg(not(test))]
pub(super) mod inner {
    pub use tokio::net::{
        TcpListener, TcpStream, ToSocketAddrs, UdpSocket, tcp::OwnedReadHalf, tcp::OwnedWriteHalf,
    };
}

#[cfg(test)]
pub(super) mod inner {
    pub use turmoil::ToSocketAddrs;
    pub use turmoil::net::{
        TcpListener, TcpStream, UdpSocket, tcp::OwnedReadHalf, tcp::OwnedWriteHalf,
    };
}
