use anyhow::Result;
use borsh::{BorshDeserialize, BorshSerialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::control_plane::NodeId;
use crate::net::{TcpStream, TransportReadHalf, TransportTcpStream, TransportWriteHalf};
use crate::security::{NodeTransportSecurity, node_certificate_principal};

pub(super) const DATA_FRAME_MAX: usize = 64 * 1024 * 1024;
const HANDSHAKE_FRAME_MAX: usize = 8 * 1024;

pub(super) struct DataConnection {
    pub(super) peer: NodeId,
    pub(super) reader: TransportReadHalf,
    pub(super) writer: TransportWriteHalf,
}

impl DataConnection {
    pub(super) async fn accept(
        stream: TcpStream,
        local_node_id: &NodeId,
        node_transport: &NodeTransportSecurity,
    ) -> Result<Self> {
        let mut stream = TransportTcpStream::accept(
            stream,
            node_transport,
            node_certificate_principal,
            super::HANDSHAKE_TIMEOUT,
        )
        .await?;
        let peer = if node_transport.is_secure() {
            node_transport
                .exchange_node_identity(&mut stream, local_node_id)
                .await?
        } else {
            read_frame(&mut stream, HANDSHAKE_FRAME_MAX).await?
        };
        Self::from_stream(stream, peer)
    }

    pub(super) async fn connect(
        mut stream: TransportTcpStream,
        expected_peer: &NodeId,
        local_node_id: &NodeId,
        node_transport: &NodeTransportSecurity,
    ) -> Result<Self> {
        node_transport
            .authenticate_outbound(&mut stream, local_node_id, expected_peer)
            .await?;
        if !node_transport.is_secure() {
            write_frame(&mut stream, local_node_id, HANDSHAKE_FRAME_MAX).await?;
        }
        Self::from_stream(stream, expected_peer.clone())
    }

    fn from_stream(stream: TransportTcpStream, peer: NodeId) -> Result<Self> {
        anyhow::ensure!(
            !peer.is_empty() && peer.len() <= crate::security::MAX_SECURITY_ID_BYTES,
            "invalid data peer identity"
        );
        let (reader, writer) = stream.into_split();
        Ok(Self {
            peer,
            reader,
            writer,
        })
    }
}

pub(super) async fn read_frame<T: BorshDeserialize>(
    reader: &mut (impl AsyncRead + Unpin),
    maximum: usize,
) -> Result<T> {
    let len = reader.read_u32().await? as usize;
    anyhow::ensure!(len <= maximum, "data frame too large: {len} bytes");
    let mut bytes = vec![0; len];
    reader.read_exact(&mut bytes).await?;
    Ok(borsh::from_slice(&bytes)?)
}

pub(super) async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    value: &impl BorshSerialize,
    maximum: usize,
) -> Result<()> {
    let len = borsh::object_length(value)?;
    anyhow::ensure!(len <= maximum, "data frame too large: {len} bytes");
    let mut bytes = Vec::with_capacity(4 + len);
    bytes.extend_from_slice(&u32::try_from(len)?.to_be_bytes());
    value.serialize(&mut bytes)?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::TcpListener;
    use rcgen::{CertificateParams, KeyPair, SanType, string::Ia5String};
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    use std::time::Duration;

    fn credentials(principal: &str) -> (CertificateDer<'static>, Vec<u8>) {
        let mut params = CertificateParams::default();
        params.subject_alt_names = vec![SanType::URI(
            Ia5String::try_from(format!("urn:eastguard:node:{principal}")).unwrap(),
        )];
        let key = KeyPair::generate().unwrap();
        (
            params.self_signed(&key).unwrap().der().clone(),
            key.serialize_der(),
        )
    }

    #[test]
    fn data_authenticates_certificate_bound_nodes_without_metadata() -> turmoil::Result {
        let (client_cert, client_key) = credentials("client");
        let (server_cert, server_key) = credentials("server");
        let trust = [client_cert.clone(), server_cert.clone()];
        for (client_id, server_id, expected_id, accepted) in [
            ("client::1", "server::1", "server::1", true),
            // The same reusable certificates also work after a process restart.
            ("client::2", "server::2", "server::2", true),
            ("impostor::1", "server::1", "server::1", false),
            ("client::1", "impostor::1", "server::1", false),
            ("client::1", "server::1", "server::2", false),
            ("client::1", "server::1", "other-broker::1", false),
        ] {
            let client = NodeTransportSecurity::test_secure(
                vec![client_cert.clone()],
                PrivatePkcs8KeyDer::from(client_key.clone()).into(),
                &trust,
            )?;
            let server = NodeTransportSecurity::test_secure(
                vec![server_cert.clone()],
                PrivatePkcs8KeyDer::from(server_key.clone()).into(),
                &trust,
            )?;
            let mut sim = turmoil::Builder::new()
                .rng_seed(7)
                .simulation_duration(Duration::from_secs(80))
                .build();
            sim.host("server", move || {
                let server = server.clone();
                async move {
                    let listener = TcpListener::bind("0.0.0.0:9000").await?;
                    let (stream, _) = listener.accept().await?;
                    let result =
                        DataConnection::accept(stream, &NodeId::new(server_id), &server).await;
                    if accepted {
                        let mut connection = result?;
                        assert_eq!(connection.peer, NodeId::new(client_id));
                        assert_eq!(read_frame::<u64>(&mut connection.reader, 8).await?, 42);
                        write_frame(&mut connection.writer, &43_u64, 8).await?;
                    } else if let Ok(mut connection) = result {
                        assert!(
                            read_frame::<u64>(&mut connection.reader, 8).await.is_err(),
                            "rejected identity must not reveal application data"
                        );
                    }
                    Ok(())
                }
            });
            sim.client("client", async move {
                // A full minute without metadata must not disable peer transport.
                tokio::time::sleep(Duration::from_secs(61)).await;
                let stream =
                    TransportTcpStream::connect_node((turmoil::lookup("server"), 9000), &client)
                        .await?;
                let result = DataConnection::connect(
                    stream,
                    &NodeId::new(expected_id),
                    &NodeId::new(client_id),
                    &client,
                )
                .await;
                if accepted {
                    let mut connection = result?;
                    write_frame(&mut connection.writer, &42_u64, 8).await?;
                    assert_eq!(read_frame::<u64>(&mut connection.reader, 8).await?, 43);
                } else {
                    assert!(result.is_err(), "{client_id} -> {expected_id}");
                }
                Ok(())
            });
            sim.run()?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn data_frames_reject_oversized_lengths_before_reading_or_writing_payloads() {
        let mut announced = (DATA_FRAME_MAX as u32 + 1)
            .to_be_bytes()
            .as_slice()
            .to_owned();
        assert!(
            read_frame::<u64>(&mut announced.as_slice(), DATA_FRAME_MAX)
                .await
                .is_err()
        );
        announced.clear();
        assert!(write_frame(&mut announced, &42_u64, 7).await.is_err());
        assert!(announced.is_empty());
    }
}
