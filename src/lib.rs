#![deny(clippy::disallowed_types)]

pub(crate) mod channels;
pub mod client;
mod config;
mod connections;

mod control_plane;
mod data_plane;

mod net;
pub(crate) mod schedulers;
mod security;

pub(crate) mod impls;
#[cfg(test)]
mod it;
pub(crate) mod macros;

#[cfg(any(test, debug_assertions))]
mod test_traits;

use crate::config::Environment;
use crate::connections::controller::handle_client_stream;
use crate::control_plane::NodeId;
use crate::control_plane::consensus::actor::{MultiRaftActor, MutlRaftSender};
use crate::control_plane::consensus::messages::{
    MultiRaftActorCommand, RaftTimer, RaftTransportCommand,
};
use crate::control_plane::consensus::transport::RaftTransportActor;
use crate::control_plane::membership::OutboundPacket;
use crate::control_plane::membership::actor::SwimSender;

use crate::data_plane::actor::{DataPlaneActor, DataPlaneSender};
use crate::data_plane::checkpoint::CheckpointWorker;
use crate::data_plane::recovery;
use crate::data_plane::transport::DataTransportActor;
use crate::data_plane::transport::command::DataTransportCommand;
use crate::impls::metadata_storage::MetadataStorage;
use crate::net::{TcpListener, TransportTcpStream, UdpSocket};
use crate::schedulers::actor::spawn_scheduling_actor;
use crate::schedulers::ticker::{PROBE_INTERVAL_TICKS, TICK_PERIOD_100_MS};
use crate::security::{
    NodeTransportSecurity, SecurityActor, SecurityHandle, client_certificate_principal,
};
use crate::{
    config::ENV,
    control_plane::membership::{actor::SwimActor, transport::SwimTransportActor},
};
use anyhow::Result;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct StartUp {
    env: Environment,
    rng_seed: u64,
}

impl StartUp {
    pub fn new(rng_seed: u64) -> Self {
        Self {
            env: ENV.clone(),
            rng_seed,
        }
    }

    pub fn with_env(env: Environment, rng_seed: u64) -> Self {
        Self { env, rng_seed }
    }

    pub async fn run(self) -> Result<()> {
        let security = NodeTransportSecurity::load(&self.env)?;
        if security.is_secure() {
            anyhow::bail!(
                "secure startup is unavailable: authenticated SWIM datagrams, data-plane process admission, and admission/revocation operations are incomplete"
            );
        }

        // Bind sockets before spawning — fail fast on port conflicts
        let udp_socket = UdpSocket::bind(self.env.peer_bind_addr()).await?;
        let tcp_listener = TcpListener::bind(self.env.peer_bind_addr()).await?;
        let data_tcp_listener = TcpListener::bind(self.env.data_bind_addr()).await?;

        // Mailboxes for cross-actor channels (SWIM ↔ Raft is cyclic, so pre-create both)
        let (swim_sender, swim_mailbox) = SwimActor::channel(100);
        let (raft_tx, raft_mailbox) = MultiRaftActor::channel(4096);
        let (tx_outbound, rx_outbound) = mpsc::channel::<Box<[OutboundPacket]>>(100);
        let (raft_transport_tx, raft_transport_rx) =
            mpsc::channel::<Box<[RaftTransportCommand]>>(100);
        let (data_transport_tx, data_transport_rx) =
            mpsc::channel::<Box<[DataTransportCommand]>>(100);

        let swim = self.env.swim(self.rng_seed);
        let node_id = swim.node_id.clone();

        // Topology snapshot channel: SwimActor publishes, all other actors read.
        // Single-writer / many-readers via ArcSwap — no locks, no contention.
        let (topology_pub, topology_reader) = swim.topology.clone().channel();

        let security_handle = SecurityActor::spawn(
            node_id.clone(),
            swim_sender.clone(),
            raft_tx.clone(),
            topology_reader.clone(),
            security.clone(),
        );

        // Recover local durable state before this node serves or joins the
        // cluster: scan + replay the WAL into the segment files, then clear the
        // old WAL. Runs before any transport serves and before the SWIM join, so
        // the node only becomes visible to the cluster once it is recovered.
        let data_config = self.env.data_node_config();
        let sparse_index = self.env.sparse_index_db();
        let recovery_output = recovery::run(data_config.data_dir.clone(), &*sparse_index)?;

        // Transports
        tokio::spawn(SwimTransportActor::run(
            udp_socket,
            swim_sender.clone(),
            rx_outbound,
        ));
        tokio::spawn(RaftTransportActor::run(
            node_id.clone(),
            tcp_listener,
            raft_tx.clone(),
            raft_transport_rx,
            swim_sender.clone(),
            security_handle.clone(),
        ));

        // Protocol actors (each spawns its own scheduler internally)
        SwimActor::spawn(
            swim_sender.clone(),
            swim_mailbox,
            swim,
            tx_outbound,
            raft_tx.clone().into(),
            topology_pub,
        );

        let (checkpoint_tx, checkpoint_rx) = flume::bounded(64);
        let data_plane_tx = DataPlaneActor::spawn(
            node_id.clone(),
            data_config,
            checkpoint_tx,
            data_transport_tx.clone().into(),
            raft_tx.clone(),
            sparse_index.clone(),
            recovery_output,
        );
        CheckpointWorker::spawn(sparse_index, checkpoint_rx, data_plane_tx.clone());

        tokio::spawn(DataTransportActor::run(
            node_id.clone(),
            data_tcp_listener,
            data_plane_tx.clone(),
            data_transport_rx,
            swim_sender.clone(),
            topology_reader.clone(),
            security.clone(),
        ));

        MultiRaftActor::spawn(
            spawn_scheduling_actor::<RaftTimer, MultiRaftActorCommand>(
                raft_tx.clone().into(),
                self.env.vnodes_per_node as usize * 16,
                TICK_PERIOD_100_MS,
                Some(PROBE_INTERVAL_TICKS),
            ),
            raft_mailbox,
            node_id.clone(),
            self.env.election_jitter_seed(self.rng_seed),
            Box::new(MetadataStorage::open(self.env.raft_db_path())),
            raft_transport_tx,
            swim_sender.clone(),
            data_transport_tx,
            topology_reader,
            self.env.raft_snapshot_entry_threshold,
        );

        // Client handler
        let _ = self
            .receive_client_streams(
                node_id,
                swim_sender,
                raft_tx,
                data_plane_tx,
                security,
                security_handle,
            )
            .await;
        Ok(())
    }

    async fn receive_client_streams(
        self,
        node_id: NodeId,
        swim_sender: SwimSender,
        raft_tx: MutlRaftSender,
        data_plane_tx: DataPlaneSender,
        security: NodeTransportSecurity,
        security_handle: SecurityHandle,
    ) {
        let addr = self.env.bind_addr();
        let listener = TcpListener::bind(&addr).await.unwrap();
        tracing::info!(
            "[{}] EastGuard listening on {}",
            self.env.resolve_node_id(),
            addr
        );

        while let Ok((stream, _)) = listener.accept().await {
            let stream =
                match TransportTcpStream::accept(stream, &security, client_certificate_principal)
                    .await
                {
                    Ok(stream) => stream,
                    Err(error) => {
                        tracing::debug!("client authentication failed: {error}");
                        continue;
                    }
                };
            let node_id = node_id.clone();
            let swim_tx = swim_sender.clone();
            let raft = raft_tx.clone();
            let dp = data_plane_tx.clone();
            let security_handle = security_handle.clone();

            tokio::spawn(handle_client_stream(
                stream,
                node_id,
                swim_tx,
                raft,
                dp,
                security_handle,
            ));
        }
    }
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
