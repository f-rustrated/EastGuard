pub(crate) mod command;
mod reader;
mod writers;

use tokio::sync::mpsc;

use crate::clusters::NodeId;
use crate::clusters::swims::actor::SwimSender;
use crate::data_plane::messages::command::DataPlaneCommand;
use crate::net::TcpListener;

use command::DataTransportCommand;
use writers::TransportState;

pub struct DataTransportActor;

impl DataTransportActor {
    pub async fn run(
        node_id: NodeId,
        listener: TcpListener,
        data_plane_tx: crossbeam_channel::Sender<DataPlaneCommand>,
        mut from_actor: mpsc::Receiver<DataTransportCommand>,
        swim_tx: SwimSender,
    ) {
        let mut state = TransportState::new(node_id);
        let mut cleanup_interval = tokio::time::interval(std::time::Duration::from_secs(300));
        cleanup_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                Some(cmd) = from_actor.recv() => {
                    match cmd {
                        DataTransportCommand::Send { targets, message } => {
                            state.send(&targets, &message, &swim_tx, &data_plane_tx).await;
                        }
                        DataTransportCommand::DisconnectPeer(peer_id) => {
                            state.disconnect(peer_id);
                        }
                    }
                }

                Ok((stream, _)) = listener.accept() => {
                    match state.accept(stream).await {
                        Ok(reader) => {
                            tokio::spawn(reader.run(data_plane_tx.clone()));
                        }
                        Err(e) => {
                            tracing::debug!("Data accept rejected: {e}");
                        }
                    }
                }

                _ = cleanup_interval.tick() => {
                    state.cleanup_dead_peers();
                }
            }
        }
    }
}
