use crate::impl_new_struct_wrapper;

use super::inner;

pub struct UdpSocket(inner::UdpSocket);

impl_new_struct_wrapper!(UdpSocket, inner::UdpSocket);

impl UdpSocket {
    pub async fn bind<A: inner::ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        let socket = inner::UdpSocket::bind(addr).await?;
        Ok(Self(socket))
    }
}
