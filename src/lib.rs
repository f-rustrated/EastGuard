mod config;

use anyhow::Result;
use tokio::net::TcpListener;

use crate::config::ENV;

#[derive(Debug)]
pub struct StartUp;

impl StartUp {
    pub async fn run(self) -> Result<()> {
        // run handlers

        let _ = self.receive_client_streams().await;
        Ok(())
    }

    async fn receive_client_streams(self) {
        let listener = TcpListener::bind(ENV.bind_addr()).await.unwrap();

        //TODO refactor: authentication should be simplified
        while let Ok((stream, _)) = listener.accept().await {
            // if self.handle_client_stream(stream).await.is_err() {
            //     continue;
            // }
        }
    }
}
