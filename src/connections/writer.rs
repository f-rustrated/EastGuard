use tokio::io::AsyncWriteExt;

use crate::{
    config::SERDE_CONFIG,
    connections::{REQUEST_ID_SIZE, protocol::ClientResponse},
    net::OwnedWriteHalf,
};
use tokio::sync::mpsc;

pub(crate) struct ClientRawWriter {
    stream: OwnedWriteHalf,
}

impl ClientRawWriter {
    pub fn new(write_half: OwnedWriteHalf) -> Self {
        Self { stream: write_half }
    }

    pub async fn write<T: bincode::Encode>(
        &mut self,
        request_id: u64,
        data: &T,
    ) -> anyhow::Result<()> {
        let encoded = bincode::encode_to_vec(data, SERDE_CONFIG)?;
        let len = (REQUEST_ID_SIZE + encoded.len()) as u32;
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
        write_half.write(request_id, &response).await?;
    }
    Ok(())
}
