#![deny(clippy::disallowed_types)]

mod config;
mod connections;

mod clusters;
#[allow(dead_code)]
mod data_plane;

mod net;
pub(crate) mod schedulers;

pub(crate) mod impls;
#[cfg(test)]
mod it;
pub(crate) mod macros;

#[cfg(test)]
mod test_traits;

use crate::clusters::raft::actor::{MultiRaftActor, RaftSender};
use crate::clusters::raft::transport::RaftTransportActor;

use crate::clusters::swims::actor::SwimSender;
use crate::config::Environment;
use crate::impls::metadata_storage::MetadataStorage;
use crate::net::{TcpListener, TcpStream};
use crate::schedulers::actor::run_scheduling_actor;
use crate::schedulers::ticker::TICK_PERIOD_100_MS;
use crate::{
    clusters::{swims::actor::SwimActor, transport::SwimTransportActor},
    config::ENV,
    connections::clients::{ClientStreamReader, ClientStreamWriter},
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
        let (tx_outbound, rx_outbound) = mpsc::channel(100);
        let (swim_ticker_tx, swim_ticker_rx) = mpsc::channel(64);

        // Raft channels
        let (raft_tx, raft_mailbox) = MultiRaftActor::channel(4096);
        let (raft_transport_tx, raft_transport_rx) = mpsc::channel(100);
        let (raft_ticker_tx, raft_ticker_rx) = mpsc::channel(self.env.vnodes_per_node as usize * 4);

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
        ));
        tokio::spawn(run_scheduling_actor(
            raft_tx.clone().into(),
            raft_ticker_rx,
            TICK_PERIOD_100_MS,
        ));
        tokio::spawn(SwimTransportActor::run(
            udp_socket,
            swim_sender.clone(),
            rx_outbound,
        ));
        tokio::spawn(RaftTransportActor::run(
            node_id.clone(),
            tcp_listener,
            raft_tx.clone().into(),
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
        tokio::spawn(MultiRaftActor::run(
            node_id,
            Box::new(raft_db),
            raft_mailbox,
            raft_transport_tx,
            raft_ticker_tx,
            swim_sender.clone(),
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
    let mut stream_reader = ClientStreamReader::new(read_half);
    let mut stream_writer = ClientStreamWriter::new(write_half, swim_sender, raft_sender);
    let request = stream_reader.read_request().await?;
    stream_writer.dispatch(request).await
}
