#![deny(clippy::disallowed_types)]

mod config;
mod connections;

mod clusters;
mod storage;

mod net;
pub(crate) mod schedulers;

#[cfg(test)]
mod it;
pub(crate) mod macros;

use crate::clusters::raft::actor::MultiRaftActor;
use crate::clusters::raft::transport::RaftTransportActor;

use crate::clusters::swims::{SwimCommand, SwimQueryCommand};
use crate::config::Environment;
use crate::connections::request::QueryCommand;
use crate::net::{TcpListener, TcpStream};
use crate::schedulers::actor::run_scheduling_actor;
use crate::{
    clusters::{swims::actor::SwimActor, transport::SwimTransportActor},
    config::ENV,
    connections::{
        clients::{ClientStreamReader, ClientStreamWriter},
        request::ConnectionRequests,
    },
};
use anyhow::Result;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

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
        let (swim_sender, swim_mailbox) = mpsc::channel(100);
        let (tx_outbound, rx_outbound) = mpsc::channel(100);
        let (swim_ticker_tx, swim_ticker_rx) = mpsc::channel(64);

        // Raft channels
        let (raft_tx, raft_mailbox) = mpsc::channel(4096);
        let (raft_transport_tx, raft_transport_rx) = mpsc::channel(100);
        let (raft_ticker_tx, raft_ticker_rx) = mpsc::channel(64);

        // Build SWIM state and extract node_id before handing state to the actor
        let state = self.env.swim(self.rng_seed);
        let node_id = state.node_id.clone();

        let peer_bind_addr = self.env.peer_bind_addr();

        // Bind sockets before spawning — fail fast on port conflicts
        let udp_socket = crate::net::UdpSocket::bind(peer_bind_addr).await?;
        let tcp_listener = crate::net::TcpListener::bind(peer_bind_addr).await?;

        // Spawn actors (order: tickers → transports → protocols → client)
        tokio::spawn(run_scheduling_actor(swim_sender.clone(), swim_ticker_rx));
        tokio::spawn(run_scheduling_actor(raft_tx.clone(), raft_ticker_rx));
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
            raft_tx,
        ));
        tokio::spawn(MultiRaftActor::run(
            node_id,
            raft_mailbox,
            raft_transport_tx,
            raft_ticker_tx,
            swim_sender.clone(),
        ));

        // Client handler
        let _ = self.receive_client_streams(swim_sender).await;
        Ok(())
    }

    async fn receive_client_streams(self, swim_sender: Sender<SwimCommand>) {
        let addr = self.env.bind_addr();
        let listener = TcpListener::bind(&addr).await.unwrap();
        tracing::info!(
            "[{}] EastGuard listening on {}",
            self.env.resolve_node_id(),
            addr
        );

        //TODO refactor: authentication should be simplified
        while let Ok((stream, _)) = listener.accept().await {
            if let Err(err) = self.handle_client_stream(stream, swim_sender.clone()).await {
                tracing::error!("{}", err);
                continue;
            }
        }
    }

    async fn handle_client_stream(
        &self,
        stream: TcpStream,
        swim_sender: Sender<SwimCommand>,
    ) -> Result<()> {
        let (read_half, write_half) = stream.into_split();
        // ! TBD writer needs to be run and read handler should hold sender to the writer
        let mut stream_reader = ClientStreamReader::new(read_half);
        let stream_writer = ClientStreamWriter::new(write_half);
        let request = stream_reader.read_request().await?;

        match request {
            ConnectionRequests::Discovery => {
                // exemplary request
            }
            ConnectionRequests::Connection(_request) => {
                // validate connection
                tokio::spawn(stream_reader.handle_client_stream());
            }
            ConnectionRequests::Query(query_type) => {
                self.handle_query(stream_writer, swim_sender, query_type)
                    .await?;
            }
        }
        Ok(())
    }

    async fn handle_query(
        &self,
        mut writer: ClientStreamWriter,
        swim_sender: Sender<SwimCommand>,
        query_type: QueryCommand,
    ) -> Result<()> {
        match query_type {
            QueryCommand::GetMembers => {
                let (send, recv) = tokio::sync::oneshot::channel();
                swim_sender
                    .send(SwimCommand::Query(SwimQueryCommand::GetMembers {
                        reply: send,
                    }))
                    .await?;

                let result = recv.await?;
                writer
                    .write(&result)
                    .await
                    .expect("Failed to write message");
                Ok(())
            }
        }
    }
}
