use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::inner;

macro_rules! tcp_wrapper {
    ($name:ident) => {
        pub struct $name(pub(super) inner::$name);

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

tcp_wrapper!(TcpListener);
tcp_wrapper!(TcpStream);
tcp_wrapper!(OwnedReadHalf);
tcp_wrapper!(OwnedWriteHalf);

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
        let listener = inner::TcpListener::bind(addr).await?;
        Ok(Self(listener))
    }

    pub async fn accept(&self) -> std::io::Result<(TcpStream, std::net::SocketAddr)> {
        let (stream, addr) = self.0.accept().await?;
        Ok((TcpStream(stream), addr))
    }
}

impl TcpStream {
    #[allow(dead_code)]
    pub async fn connect<A: inner::ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        let stream = inner::TcpStream::connect(addr).await?;
        Ok(Self(stream))
    }

    pub fn into_split(self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let (read_half, write_half) = self.0.into_split();
        (OwnedReadHalf(read_half), OwnedWriteHalf(write_half))
    }
}
