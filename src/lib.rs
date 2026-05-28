#![deny(clippy::disallowed_types)]

pub(crate) mod channels;
mod config;
mod connections;

mod control_plane;
mod data_plane;

mod net;
pub(crate) mod schedulers;

pub(crate) mod impls;
#[cfg(test)]
mod it;
pub(crate) mod macros;

#[cfg(any(test, debug_assertions))]
mod test_traits;

use crate::control_plane::consensus::actor::{MultiRaftActor, MutlRaftSender};
use crate::control_plane::consensus::transport::RaftTransportActor;

use crate::config::Environment;
use crate::control_plane::consensus::messages::{RaftTimer, RaftTransportCommand};
use crate::control_plane::membership::OutboundPacket;
use crate::control_plane::membership::SwimTimer;
use crate::control_plane::membership::actor::SwimSender;
use crate::data_plane::actor::DataPlaneActor;
use crate::data_plane::checkpoint::CheckpointWorker;
use crate::data_plane::messages::command::DataPlaneCommand;

use crate::data_plane::timer::{BatchFlushTimer, ReplicationTimer, SegmentAgeTimer};
use crate::data_plane::transport::DataTransportActor;
use crate::data_plane::transport::command::DataTransportCommand;
use crate::impls::metadata_storage::MetadataStorage;
use crate::net::{TcpListener, TcpStream, UdpSocket};
use crate::schedulers::actor::run_scheduling_actor;
use crate::schedulers::ticker::{PROBE_INTERVAL_TICKS, TICK_PERIOD_10_MS, TICK_PERIOD_100_MS};
use crate::schedulers::ticker_message::{SchedulerSender, TickerCommand};
use crate::{
    config::ENV,
    connections::clients::{ClientHandler, ClientStreamReader, run_client_writer},
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

        // Bind sockets before spawning — fail fast on port conflicts
        let udp_socket = UdpSocket::bind(self.env.peer_bind_addr()).await?;
        let tcp_listener = TcpListener::bind(self.env.peer_bind_addr()).await?;
        let data_tcp_listener = TcpListener::bind(self.env.data_bind_addr()).await?;

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

        let (data_transport_tx, data_transport_rx) =
            mpsc::channel::<Box<[DataTransportCommand]>>(100);

        // DataPlane stack

        let sparse_index_db = self.env.sparse_index_db();
        let (checkpoint_tx, checkpoint_rx) = crossbeam_channel::bounded(64);

        // Timer bridge: schedulers send via tokio mpsc, bridge forwards to crossbeam
        let (dp_timer_tx, mut dp_timer_rx) = mpsc::channel::<DataPlaneCommand>(64);

        let (batch_tick_tx, batch_tick_rx) = SchedulerSender::<BatchFlushTimer>::channel(64);

        let (repl_tick_tx, repl_tick_rx) = SchedulerSender::<ReplicationTimer>::channel(64);

        let (_age_tick_tx, age_tick_rx) = SchedulerSender::<SegmentAgeTimer>::channel(64);

        tokio::spawn(run_scheduling_actor(
            dp_timer_tx.clone(),
            batch_tick_rx,
            TICK_PERIOD_10_MS,
            None,
        ));
        tokio::spawn(run_scheduling_actor(
            dp_timer_tx.clone(),
            repl_tick_rx,
            TICK_PERIOD_10_MS,
            None,
        ));
        tokio::spawn(run_scheduling_actor(
            dp_timer_tx,
            age_tick_rx,
            TICK_PERIOD_100_MS,
            Some(self.env.age_check_ticks()),
        ));

        let data_plane_tx = DataPlaneActor::spawn(
            node_id.clone(),
            self.env.data_dir_path(),
            self.env.data_node_config(),
            checkpoint_tx,
            batch_tick_tx,
            repl_tick_tx,
            data_transport_tx.clone().into(),
            raft_tx.clone(),
        );

        CheckpointWorker::spawn(sparse_index_db, checkpoint_rx, data_plane_tx.clone());

        // Bridge: scheduler callbacks (tokio mpsc) → DataPlane mailbox (crossbeam)
        let dp_tx_bridge = data_plane_tx.clone();
        tokio::spawn(async move {
            while let Some(cmd) = dp_timer_rx.recv().await {
                if dp_tx_bridge.send(cmd).is_err() {
                    break;
                }
            }
        });

        tokio::spawn(DataTransportActor::run(
            node_id.clone(),
            data_tcp_listener,
            data_plane_tx,
            data_transport_rx,
            swim_sender.clone(),
        ));
        tokio::spawn(MultiRaftActor::run(
            node_id,
            self.env.election_jitter_seed(self.rng_seed),
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

    async fn receive_client_streams(self, swim_sender: SwimSender, raft_tx: MutlRaftSender) {
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
    raft_sender: MutlRaftSender,
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
