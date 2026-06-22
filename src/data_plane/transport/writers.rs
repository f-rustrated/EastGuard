use std::collections::{HashMap, HashSet};

use anyhow::Context;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::control_plane::membership::actor::SwimSender;
use crate::control_plane::NodeId;
use crate::data_plane::actor::DataPlaneSender;
use crate::data_plane::messages::command::{DataPlaneCommand, DataPlaneInterNodeCommand};
use crate::net::{OwnedWriteHalf, TcpStream};

use super::reader::DataReader;

const CONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);

pub(super) struct TransportState {
    node_id: NodeId,
    writers: HashMap<NodeId, OwnedWriteHalf>,
    dead_peers: HashSet<NodeId>,
    /// Tracks when the last connect attempt to a peer failed. Skips retry
    /// for CONNECT_BACKOFF (2s) to avoid blocking the select loop on repeated
    /// 3s TCP timeouts to unreachable peers. Cleared by periodic cleanup (300s).
    connect_backoffs: HashMap<NodeId, Instant>,
}

impl TransportState {
    #[allow(dead_code)]
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            writers: HashMap::new(),
            dead_peers: HashSet::new(),
            connect_backoffs: HashMap::new(),
        }
    }

    pub async fn accept(
        &mut self,
        stream: crate::net::TcpStream,
    ) -> anyhow::Result<(NodeId, DataReader)> {
        let (read_half, write_half) = stream.into_split();
        let mut reader = DataReader(read_half);

        let peer_id = reader.read_node_id().await?;

        anyhow::ensure!(
            !(self.writers.contains_key(&peer_id) && peer_id > self.node_id),
            "duplicate connection from {peer_id} dropped (lower NodeId wins)"
        );

        self.writers.insert(peer_id.clone(), write_half);
        Ok((peer_id, reader))
    }

    /// Drop a peer's cached writer (without marking it dead) so the next send
    /// re-establishes a fresh connection. Called when that peer's reader task
    /// ends — see `DataReader::run`.
    pub fn evict_writer(&mut self, peer: &NodeId) {
        self.writers.remove(peer);
    }

    pub async fn send(
        &mut self,
        targets: &[NodeId],
        msg: &DataPlaneInterNodeCommand,
        swim_tx: &SwimSender,
        data_plane_tx: &DataPlaneSender,
        disconnect_tx: &mpsc::Sender<NodeId>,
    ) {
        for target in targets {
            // Self-delivery: a node can be its own target (e.g. a SegmentAssignment
            // to `replica_set[0]`
            if *target == self.node_id {
                let _ =
                    data_plane_tx.send(DataPlaneCommand::DataPlaneInterNodeCommand(msg.clone()));
                continue;
            }

            if self.dead_peers.contains(target) {
                continue;
            }

            if let Some(&failed_at) = self.connect_backoffs.get(target) {
                if failed_at.elapsed() < CONNECT_BACKOFF {
                    continue;
                }
                self.connect_backoffs.remove(target);
            }

            if self.writers.contains_key(target) {
                if self.write_message(target, msg).await.is_ok() {
                    continue;
                }
                self.writers.remove(target);
            }

            match self.connect_and_send(target.clone(), msg, swim_tx).await {
                Ok(reader) => {
                    tokio::spawn(reader.run(
                        data_plane_tx.clone(),
                        target.clone(),
                        disconnect_tx.clone(),
                    ));
                }
                Err(e) => {
                    tracing::warn!(
                        "[{}] connect_and_send to {target} failed: {e}",
                        self.node_id
                    );
                    self.connect_backoffs.insert(target.clone(), Instant::now());
                }
            }
        }
    }

    async fn connect_and_send(
        &mut self,
        target_id: NodeId,
        msg: &DataPlaneInterNodeCommand,
        swim_tx: &SwimSender,
    ) -> anyhow::Result<DataReader> {
        let node_addr = swim_tx
            .resolve_address(target_id.clone())
            .await?
            .with_context(|| format!("no address known for {target_id}"))?;

        let stream = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            TcpStream::connect(node_addr.data_addr()),
        )
        .await
        .context("connect timed out")?
        .context("TCP connect failed")?;

        let (read_half, write_half) = stream.into_split();
        self.writers.insert(target_id.clone(), write_half);

        if let Err(e) = self.handshake(&target_id).await {
            self.writers.remove(&target_id);
            return Err(e).context("handshake failed");
        }

        if let Err(e) = self.write_message(&target_id, msg).await {
            self.writers.remove(&target_id);
            return Err(e).context("initial write failed");
        }

        Ok(DataReader(read_half))
    }

    pub fn disconnect(&mut self, peer_id: NodeId) {
        self.writers.remove(&peer_id);
        self.dead_peers.insert(peer_id);
    }

    pub fn cleanup_dead_peers(&mut self) {
        self.dead_peers.clear();
        self.connect_backoffs.clear();
    }

    async fn handshake(&mut self, target: &NodeId) -> anyhow::Result<()> {
        let writer = self
            .writers
            .get_mut(target)
            .context("no writer for target")?;
        let bytes = borsh::to_vec(&self.node_id)?;
        let len = bytes.len() as u32;
        writer.write_all(&len.to_be_bytes()).await?;
        writer.write_all(&bytes).await?;
        Ok(())
    }

    async fn write_message(
        &mut self,
        target: &NodeId,
        msg: &DataPlaneInterNodeCommand,
    ) -> anyhow::Result<()> {
        let writer = self
            .writers
            .get_mut(target)
            .context("no writer for target")?;
        let bytes = borsh::to_vec(msg)?;
        let len = bytes.len() as u32;
        let mut buf = Vec::with_capacity(4 + bytes.len());
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(&bytes);
        writer.write_all(&buf).await?;
        Ok(())
    }
}
