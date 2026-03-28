#![deny(clippy::disallowed_types)]

mod config;
mod connections;

mod clusters;

mod net;
pub(crate) mod schedulers;

#[cfg(test)]
mod it;

use crate::clusters::swims::peer_discovery::Bootstrapper;

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
        // Create a channel for the SwimActor
        let (swim_sender, swim_mailbox) = mpsc::channel(100); // Actor Events
        let (tx_outbound, rx_outbound) = mpsc::channel(100); // Network Packets

        let transport =
            SwimTransportActor::new(self.env.peer_bind_addr(), swim_sender.clone(), rx_outbound)
                .await?;

        let (ticker_cmd_tx, ticker_cmd_rx) = mpsc::channel(64);
        let swim_actor = SwimActor::new(swim_mailbox, tx_outbound, ticker_cmd_tx.clone());

        let mut state = self.env.swim(self.rng_seed);
        Bootstrapper::new(self.env.bootstrap_servers(), &mut state);

        // Spawn Actors
        tokio::spawn(run_scheduling_actor(swim_sender.clone(), ticker_cmd_rx));
        tokio::spawn(transport.run());
        tokio::spawn(swim_actor.run(state));

        // run handlers
        let _ = self.receive_client_streams(swim_sender.clone()).await;
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
