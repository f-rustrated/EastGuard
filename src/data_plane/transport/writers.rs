use std::collections::{HashMap, HashSet};

use anyhow::Context;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::control_plane::NodeId;
use crate::control_plane::membership::actor::SwimSender;
use crate::data_plane::actor::DataPlaneSender;
use crate::data_plane::messages::command::{
    DataPlaneCommand, DataPlanePeerMessage, ReceivePeerMessage,
};
use crate::net::{TransportTcpStream, TransportWriteHalf, before_deadline};
use crate::security::NodeTransportSecurity;

use super::connection::{DATA_FRAME_MAX, DataConnection, write_frame};
use super::reader::{ConnectionClosed, DataReader};

const CONNECT_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);
const WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(PartialEq)]
enum ConnectionOrigin {
    Local,
    Remote,
}

struct ActiveConnection {
    generation: u64,
    origin: ConnectionOrigin,
    writer: TransportWriteHalf,
    reader_task: JoinHandle<()>,
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.reader_task.abort();
    }
}

pub(super) struct TransportState {
    node_id: NodeId,
    writers: HashMap<NodeId, ActiveConnection>,
    dead_peers: HashSet<NodeId>,
    /// Tracks when the last connect attempt to a peer failed. Skips retry
    /// for CONNECT_BACKOFF (2s) to avoid blocking the select loop on repeated
    /// 3s TCP timeouts to unreachable peers. Cleared by periodic cleanup (300s).
    connect_backoffs: HashMap<NodeId, Instant>,
    node_transport: NodeTransportSecurity,
    next_generation: u64,
}

impl TransportState {
    pub fn new(node_id: NodeId, node_transport: NodeTransportSecurity) -> Self {
        Self {
            node_id,
            writers: HashMap::new(),
            dead_peers: HashSet::new(),
            connect_backoffs: HashMap::new(),
            node_transport,
            next_generation: 0,
        }
    }

    pub(super) fn accept(
        &mut self,
        connection: DataConnection,
        data_plane_tx: &DataPlaneSender,
        disconnect_tx: &mpsc::Sender<ConnectionClosed>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.writers.get(&connection.peer).is_some_and(|current| {
                current.origin == ConnectionOrigin::Local
                    && !current.reader_task.is_finished()
                    && connection.peer > self.node_id
            }),
            "duplicate data connection dropped (lower NodeId wins)"
        );
        self.install(
            connection,
            ConnectionOrigin::Remote,
            data_plane_tx,
            disconnect_tx,
        )
    }

    /// Drop a peer's cached writer (without marking it dead) so the next send
    /// re-establishes a fresh connection. Called when that peer's reader task
    /// ends — see `DataReader::run`.
    pub(super) fn evict_writer(&mut self, closed: ConnectionClosed) {
        if self
            .writers
            .get(&closed.peer)
            .is_some_and(|connection| connection.generation == closed.generation)
        {
            self.writers.remove(&closed.peer);
        }
    }

    pub async fn send(
        &mut self,
        targets: &[NodeId],
        msg: &DataPlanePeerMessage,
        swim_tx: &SwimSender,
        data_plane_tx: &DataPlaneSender,
        disconnect_tx: &mpsc::Sender<ConnectionClosed>,
    ) {
        for target in targets {
            // Self-delivery: a node can be its own target (e.g. a PlaceSegment
            // to `replica_set[0]`
            if *target == self.node_id {
                let _ = data_plane_tx
                    .send_async(DataPlaneCommand::ReceivePeerMessage(ReceivePeerMessage {
                        from: self.node_id.clone(),
                        message: Box::new(msg.clone()),
                    }))
                    .await;
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

            let result = async {
                let connection = tokio::time::timeout(super::HANDSHAKE_TIMEOUT, async {
                    let node_addr = swim_tx
                        .resolve_address(target.clone())
                        .await?
                        .with_context(|| format!("no address known for {target}"))?;
                    let stream = tokio::time::timeout(
                        std::time::Duration::from_secs(3),
                        TransportTcpStream::connect_node(
                            node_addr.data_addr(),
                            &self.node_transport,
                        ),
                    )
                    .await
                    .context("data TLS connect timed out")??;
                    DataConnection::connect(stream, target, &self.node_id, &self.node_transport)
                        .await
                })
                .await
                .context("data handshake timed out")??;
                self.install(
                    connection,
                    ConnectionOrigin::Local,
                    data_plane_tx,
                    disconnect_tx,
                )?;
                self.write_message(target, msg)
                    .await
                    .context("initial write failed")
            }
            .await;
            if let Err(e) = result {
                self.writers.remove(target);
                tracing::warn!(
                    "[{}] data connection or initial write to {target} failed: {e}",
                    self.node_id
                );
                self.connect_backoffs.insert(target.clone(), Instant::now());
            }
        }
    }

    fn install(
        &mut self,
        connection: DataConnection,
        origin: ConnectionOrigin,
        data_plane_tx: &DataPlaneSender,
        disconnect_tx: &mpsc::Sender<ConnectionClosed>,
    ) -> anyhow::Result<()> {
        let generation = self.next_generation;
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .context("data connection generation exhausted")?;
        let peer = connection.peer;
        let reader_task = tokio::spawn(DataReader::new(connection.reader).run(
            data_plane_tx.clone(),
            ConnectionClosed {
                peer: peer.clone(),
                generation,
            },
            disconnect_tx.clone(),
        ));
        self.dead_peers.remove(&peer);
        self.connect_backoffs.remove(&peer);
        self.writers.insert(
            peer,
            ActiveConnection {
                generation,
                origin,
                writer: connection.writer,
                reader_task,
            },
        );
        Ok(())
    }

    pub fn disconnect(&mut self, peer_id: NodeId) {
        self.writers.remove(&peer_id);
        self.dead_peers.insert(peer_id);
    }

    pub fn cleanup_dead_peers(&mut self) {
        self.dead_peers.clear();
        self.connect_backoffs.clear();
    }

    async fn write_message(
        &mut self,
        target: &NodeId,
        msg: &DataPlanePeerMessage,
    ) -> anyhow::Result<()> {
        let connection = self
            .writers
            .get_mut(target)
            .context("no writer for target")?;
        anyhow::ensure!(!connection.reader_task.is_finished(), "data reader closed");
        let message = ReceivePeerMessage {
            from: self.node_id.clone(),
            message: Box::new(msg.clone()),
        };
        let deadline = Instant::now() + WRITE_TIMEOUT;
        before_deadline(
            deadline,
            write_frame(&mut connection.writer, &message, DATA_FRAME_MAX),
        )
        .await
        .context("data write deadline expired")?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::{TcpListener, TcpStream};
    use std::time::Duration;

    #[test]
    fn replacement_closes_only_the_matching_connection() -> turmoil::Result {
        let mut sim = turmoil::Builder::new()
            .rng_seed(11)
            .simulation_duration(Duration::from_secs(5))
            .build();
        sim.host("server", || async {
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let mut sockets = Vec::new();
            loop {
                sockets.push(listener.accept().await?.0);
            }
        });
        sim.client("client", async {
            let local = NodeId::new("client");
            let peer = NodeId::new("server");
            let mut state = TransportState::new(local, NodeTransportSecurity::TrustedDevelopment);
            let (data, _data_rx) = flume::bounded(1);
            let data = DataPlaneSender(data);
            let (closed_tx, _closed_rx) = mpsc::channel(4);
            let address = (turmoil::lookup("server"), 9000);
            let first = TcpStream::connect(address).await?;
            let (reader, writer) = first.into_split();
            state.install(
                DataConnection {
                    peer: peer.clone(),
                    reader: reader.into(),
                    writer: writer.into(),
                },
                ConnectionOrigin::Remote,
                &data,
                &closed_tx,
            )?;
            let old_generation = state.writers[&peer].generation;
            let old_reader = state.writers[&peer].reader_task.abort_handle();

            let second = TcpStream::connect(address).await?;
            let (replacement_reader, replacement_writer) = second.into_split();
            state.install(
                DataConnection {
                    peer: peer.clone(),
                    reader: replacement_reader.into(),
                    writer: replacement_writer.into(),
                },
                ConnectionOrigin::Remote,
                &data,
                &closed_tx,
            )?;
            tokio::task::yield_now().await;
            assert!(old_reader.is_finished());
            state.evict_writer(ConnectionClosed {
                peer: peer.clone(),
                generation: old_generation,
            });
            assert_eq!(state.writers[&peer].generation, old_generation + 1);

            Ok(())
        });
        sim.run()
    }
}
