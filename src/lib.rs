mod config;
mod connections;

use anyhow::Result;
use tokio::net::TcpListener;

use crate::{
    config::ENV,
    connections::{
        clients::{ClientStreamReader, ClientStreamWriter},
        request::ConnectionRequests,
    },
};

#[derive(Debug)]
pub struct StartUp;

impl StartUp {
    pub async fn run(self) -> Result<()> {
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
                eprintln!("{}", err.to_string());
                continue;
            }
        }
    }

    async fn handle_client_stream(&self, stream: tokio::net::TcpStream) -> anyhow::Result<()> {
        let (read_half, write_half) = stream.into_split();
        let stream_writer = ClientStreamWriter(write_half);
        // ! TBD writer needs to be run and read handler should hold sender to the writer

        let mut stream_reader = ClientStreamReader::new(read_half);
        let request = stream_reader.read_request().await?;

        match request {
            ConnectionRequests::Discovery => {
                // exemplary request
            }
            ConnectionRequests::Connection(request) => {
                // validate connection

                tokio::spawn(stream_reader.handle_client_stream());
            }
        }
        Ok(())
    }
}
