#![allow(clippy::disallowed_types)]

mod tcp;
pub(crate) mod udp;

pub use tcp::{OwnedReadHalf, OwnedWriteHalf, TcpListener, TcpStream};
pub use udp::UdpSocket;

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
