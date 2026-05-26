#![deny(clippy::disallowed_types)]

pub(crate) mod channels;
mod config;
mod connections;

mod control_plane;
#[allow(dead_code)]
mod data_plane;

mod net;
pub(crate) mod schedulers;

pub(crate) mod impls;
#[cfg(test)]
mod it;
pub(crate) mod macros;

#[cfg(any(test, debug_assertions))]
mod test_traits;

use crate::control_plane::consensus::actor::{MultiRaftActor, RaftSender};
use crate::control_plane::consensus::transport::RaftTransportActor;

use crate::control_plane::consensus::messages::{RaftTimer, RaftTransportCommand};
use crate::control_plane::membership::OutboundPacket;
use crate::control_plane::membership::SwimTimer;
use crate::control_plane::membership::actor::SwimSender;
use crate::config::Environment;
use crate::impls::metadata_storage::MetadataStorage;
use crate::net::{TcpListener, TcpStream};
use crate::schedulers::actor::run_scheduling_actor;
use crate::schedulers::ticker::{PROBE_INTERVAL_TICKS, TICK_PERIOD_100_MS};
use crate::schedulers::ticker_message::TickerCommand;
use crate::{
    control_plane::membership::{actor::SwimActor, transport::SwimTransportActor},
    config::ENV,
    connections::clients::{ClientHandler, ClientStreamReader, run_client_writer},
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
        // SWIM channels
        let (swim_sender, swim_mailbox) = SwimActor::channel(100);
        let (tx_outbound, rx_outbound) = mpsc::channel::<Box<[OutboundPacket]>>(100);
        let (swim_ticker_tx, swim_ticker_rx) = mpsc::channel::<Box<[TickerCommand<SwimTimer>]>>(64);

        // Raft channels
        let (raft_tx, raft_mailbox) = MultiRaftActor::channel(4096);
        let (raft_transport_tx, raft_transport_rx) =
            mpsc::channel::<Box<[RaftTransportCommand]>>(100);
        // Each vnode can have both an election and heartbeat timer active, so capacity
        // must comfortably exceed vnodes_per_node * 2; * 16 gives ample headroom.
        let (raft_ticker_tx, raft_ticker_rx) = mpsc::channel::<Box<[TickerCommand<RaftTimer>]>>(
            self.env.vnodes_per_node as usize * 16,
        );

        // Build SWIM state and extract node_id before handing state to the actor
        let state = self.env.swim(self.rng_seed);
        let node_id = state.node_id.clone();

        let peer_bind_addr = self.env.peer_bind_addr();

        // Bind sockets before spawning — fail fast on port conflicts
        let udp_socket = crate::net::UdpSocket::bind(peer_bind_addr).await?;
        let tcp_listener = crate::net::TcpListener::bind(peer_bind_addr).await?;

        // Spawn actors (order: tickers → transports → protocols → client)
        tokio::spawn(run_scheduling_actor(
            swim_sender.clone().into(),
            swim_ticker_rx,
            TICK_PERIOD_100_MS,
            Some(PROBE_INTERVAL_TICKS),
        ));
        tokio::spawn(run_scheduling_actor(
            raft_tx.clone().into(),
            raft_ticker_rx,
            TICK_PERIOD_100_MS,
            Some(PROBE_INTERVAL_TICKS),
        ));
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
        ));

        tokio::spawn(SwimActor::run(
            swim_mailbox,
            state,
            tx_outbound,
            swim_ticker_tx,
            raft_tx.clone().into(),
        ));
        let raft_db = MetadataStorage::open(self.env.raft_db_path());
        let election_jitter_seed = {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            std::hash::Hash::hash(&self.env.node_id_prefix, &mut hasher);
            std::hash::Hash::hash(&self.rng_seed, &mut hasher);
            std::hash::Hasher::finish(&hasher)
        };
        // D3: data_transport_tx wired to DataTransportActor in Batch 3.
        // For now, create a channel whose receiver is dropped — sends are no-ops.
        let (data_transport_tx, _) = mpsc::channel(1);
        tokio::spawn(MultiRaftActor::run(
            node_id,
            election_jitter_seed,
            Box::new(raft_db),
            raft_mailbox,
            raft_transport_tx,
            raft_ticker_tx.into(),
            swim_sender.clone(),
            data_transport_tx,
        ));

        // Client handler
        let _ = self.receive_client_streams(swim_sender, raft_tx).await;
        Ok(())
    }

    async fn receive_client_streams(self, swim_sender: SwimSender, raft_tx: RaftSender) {
        let addr = self.env.bind_addr();
        let listener = TcpListener::bind(&addr).await.unwrap();
        tracing::info!(
            "[{}] EastGuard listening on {}",
            self.env.resolve_node_id(),
            addr
        );

        while let Ok((stream, _)) = listener.accept().await {
            let swim_tx = swim_sender.clone();
            let raft = raft_tx.clone();

            tokio::spawn(async move {
                if let Err(err) = handle_client_stream(stream, swim_tx, raft).await {
                    tracing::error!("{}", err);
                }
            });
        }
    }
}

async fn handle_client_stream(
    stream: TcpStream,
    swim_sender: SwimSender,
    raft_sender: RaftSender,
) -> Result<()> {
    let (read_half, write_half) = stream.into_split();
    let (writer_tx, writer_rx) = mpsc::channel(128);
    let handler = ClientHandler::new(swim_sender, raft_sender);
    tokio::spawn(run_client_writer(write_half, writer_rx));
    handler
        .run(ClientStreamReader::new(read_half), writer_tx)
        .await;
    Ok(())
}
