#![allow(clippy::disallowed_types)]
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[cfg(not(test))]
pub mod inner {
    pub use tokio::net::{
        TcpListener, TcpStream, ToSocketAddrs, UdpSocket, tcp::OwnedReadHalf, tcp::OwnedWriteHalf,
    };
}

#[cfg(test)]
pub mod inner {
    pub use turmoil::ToSocketAddrs;
    pub use turmoil::net::{
        TcpListener, TcpStream, UdpSocket, tcp::OwnedReadHalf, tcp::OwnedWriteHalf,
    };
}
// 2. A single macro handles ALL types now!
macro_rules! wrap_type {
    ($name:ident) => {
        pub struct $name(inner::$name);

        impl Deref for $name {
            type Target = inner::$name;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        impl DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}

// 3. Generate the wrappers
wrap_type!(TcpListener);
wrap_type!(UdpSocket);
wrap_type!(TcpStream);
wrap_type!(OwnedReadHalf);
wrap_type!(OwnedWriteHalf);

// 4. Implement AsyncRead / AsyncWrite safely using `Unpin`

impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl AsyncRead for OwnedReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for OwnedWriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl TcpListener {
    pub async fn bind<A: inner::ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        let inner_listener = inner::TcpListener::bind(addr).await?;
        Ok(Self(inner_listener))
    }

    pub async fn accept(&self) -> std::io::Result<(TcpStream, std::net::SocketAddr)> {
        // We intercept accept() so it returns our wrapper TcpStream instead of the inner one
        let (inner_stream, addr) = self.0.accept().await?;
        Ok((TcpStream(inner_stream), addr))
    }
}

impl TcpStream {
    #[allow(dead_code)]
    pub async fn connect<A: inner::ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        let inner_stream = inner::TcpStream::connect(addr).await?;
        Ok(Self(inner_stream))
    }

    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        // We intercept into_split() so it returns our wrapped halves
        let (read_half, write_half) = self.0.into_split();
        (OwnedReadHalf(read_half), OwnedWriteHalf(write_half))
    }
}

impl UdpSocket {
    pub async fn bind<A: inner::ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        let inner_socket = inner::UdpSocket::bind(addr).await?;
        Ok(Self(inner_socket))
    }
}
