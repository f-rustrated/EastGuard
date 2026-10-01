use tokio::io::AsyncWriteExt;

use crate::{
    connections::{MAX_FRAME_SIZE, REQUEST_ID_SIZE, protocol::ClientResponse},
    net::TransportWriteHalf,
};
use tokio::sync::mpsc;

pub(crate) struct ClientRawWriter {
    stream: TransportWriteHalf,
}

impl ClientRawWriter {
    pub fn new(write_half: impl Into<TransportWriteHalf>) -> Self {
        Self {
            stream: write_half.into(),
        }
    }

    pub async fn write<T: borsh::BorshSerialize>(
        &mut self,
        request_id: u64,
        data: &T,
    ) -> anyhow::Result<()> {
        let encoded_len = borsh::object_length(data)?;
        anyhow::ensure!(
            encoded_len <= MAX_FRAME_SIZE - REQUEST_ID_SIZE,
            "Message exceeds the 4 MiB frame limit"
        );
        let mut encoded = Vec::with_capacity(encoded_len);
        data.serialize(&mut encoded)?;
        let len = (REQUEST_ID_SIZE + encoded_len) as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(&request_id.to_be_bytes()).await?;
        self.stream.write_all(&encoded).await?;
        Ok(())
    }
}

pub(crate) async fn run_client_writer(
    mut write_half: ClientRawWriter,
    mut rx: mpsc::Receiver<(u64, ClientResponse)>,
) -> anyhow::Result<()> {
    while let Some((request_id, response)) = rx.recv().await {
        if matches!(response, ClientResponse::Stop) {
            break;
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            write_half.write(request_id, &response),
        )
        .await??;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connections::reader::ClientStreamReader;
    use crate::net::{TcpListener, TcpStream};
    use std::cell::Cell;

    struct CountedPayload<'a> {
        bytes: &'a [u8],
        serializations: Cell<usize>,
    }

    impl borsh::BorshSerialize for CountedPayload<'_> {
        fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
            self.serializations.set(self.serializations.get() + 1);
            self.bytes.serialize(writer)
        }
    }

    #[test]
    fn writer_checks_size_before_encoding_and_accepts_the_frame_boundary() -> turmoil::Result {
        let mut sim = turmoil::Builder::new().rng_seed(7).build();
        sim.client("writer", async {
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let (client, server) = tokio::join!(
                TcpStream::connect((turmoil::lookup("writer"), 9000)),
                listener.accept(),
            );
            let (_client_read, write_half) = client?.into_split();
            let (read_half, _server_write) = server?.0.into_split();
            let mut writer = ClientRawWriter::new(write_half);
            let mut reader = ClientStreamReader::new(read_half);
            let oversized = CountedPayload {
                bytes: &vec![0; MAX_FRAME_SIZE],
                serializations: Cell::new(0),
            };
            assert!(writer.write(1, &oversized).await.is_err());
            assert_eq!(
                oversized.serializations.get(),
                1,
                "only size measurement should run"
            );

            let at_limit = vec![7u8; MAX_FRAME_SIZE - REQUEST_ID_SIZE - size_of::<u32>()];
            let (sent, received) =
                tokio::join!(writer.write(2, &at_limit), reader.read_request::<Vec<u8>>(),);
            sent?;
            let (id, payload) = received?;
            assert_eq!(id, 2, "an oversized frame must send no bytes");
            assert_eq!(payload, at_limit);
            Ok(())
        });
        sim.run()
    }
}
