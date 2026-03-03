#[cfg(not(feature = "turmoil"))]
pub use tokio::{
    net::TcpListener, net::UdpSocket, net::tcp::OwnedReadHalf, net::tcp::OwnedWriteHalf, net::TcpStream,
};
#[cfg(feature = "turmoil")]
pub use turmoil::{
    net::TcpListener, net::UdpSocket, net::tcp::OwnedReadHalf, net::tcp::OwnedWriteHalf, net::TcpStream,
};
