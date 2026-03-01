mod config;
mod connections;

mod clusters;
pub(crate) mod schedulers;

use crate::schedulers::actor::run_scheduling_actor;
use crate::{
    clusters::{NodeId, swims::actor::SwimActor, transport::SwimTransportActor},
    config::ENV,
    connections::{
        clients::{ClientStreamReader, ClientStreamWriter},
        request::ConnectionRequests,
    },
};
use anyhow::Result;

use tokio::{net::TcpListener, sync::mpsc};

#[derive(Debug)]
pub struct StartUp;

impl StartUp {
    pub async fn run(self) -> Result<()> {
        // Create a channel for the SwimActor
        let peer_bind_addr = ENV.peer_bind_addr();

        let (swim_sender, swim_mailbox) = mpsc::channel(100); // Actor Events
        let (tx_outbound, rx_outbound) = mpsc::channel(100); // Network Packets

        let transport =
            SwimTransportActor::new(peer_bind_addr, swim_sender.clone(), rx_outbound).await?;

        let (ticker_cmd_tx, ticker_cmd_rx) = mpsc::channel(64);
        let swim_actor = SwimActor::new(
            peer_bind_addr,
            NodeId::new(ENV.resolve_node_id()),
            swim_mailbox,
            tx_outbound,
            ticker_cmd_tx.clone(),
            ENV.vnodes_per_node,
        );

        // Spawn Actors
        tokio::spawn(run_scheduling_actor(swim_sender, ticker_cmd_rx));
        tokio::spawn(transport.run());
        tokio::spawn(swim_actor.run());

        // run handlers
        let _ = self.receive_client_streams().await;
        Ok(())
    }

    async fn receive_client_streams(self) {
        let addr = ENV.bind_addr();
        let listener = TcpListener::bind(&addr).await.unwrap();
        println!("EastGuard listening on {addr}");

        //TODO refactor: authentication should be simplified
        while let Ok((stream, _)) = listener.accept().await {
            if let Err(err) = self.handle_client_stream(stream).await {
                eprintln!("{}", err);
                continue;
            }
        }
    }

    async fn handle_client_stream(&self, stream: tokio::net::TcpStream) -> anyhow::Result<()> {
        let (read_half, write_half) = stream.into_split();
        let _stream_writer = ClientStreamWriter::new(write_half);
        // ! TBD writer needs to be run and read handler should hold sender to the writer

        let mut stream_reader = ClientStreamReader::new(read_half);
        let request = stream_reader.read_request().await?;

        match request {
            ConnectionRequests::Discovery => {
                // exemplary request
            }
            ConnectionRequests::Connection(_request) => {
                // validate connection

                tokio::spawn(stream_reader.handle_client_stream());
            }
        }
        Ok(())
    }
}
