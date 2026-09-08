use tokio::sync::mpsc;

use crate::control_plane::NodeId;
use crate::data_plane::actor::DataPlaneSender;
use crate::data_plane::messages::command::{DataPlaneCommand, ReceivePeerMessage};
use crate::net::TransportReadHalf;

use super::connection::{DATA_FRAME_MAX, read_frame};

pub(super) struct ConnectionClosed {
    pub(super) peer: NodeId,
    pub(super) generation: u64,
}

pub(super) struct DataReader {
    read_half: TransportReadHalf,
}

impl DataReader {
    pub(super) fn new(read_half: TransportReadHalf) -> Self {
        Self { read_half }
    }

    #[tracing::instrument(
        level = "trace",
        skip_all,
        fields(peer = %closed.peer)
    )]
    pub(crate) async fn run(
        mut self,
        data_plane_tx: DataPlaneSender,
        closed: ConnectionClosed,
        disconnect_tx: mpsc::Sender<ConnectionClosed>,
    ) {
        loop {
            match read_frame::<ReceivePeerMessage>(&mut self.read_half, DATA_FRAME_MAX).await {
                Ok(message) => {
                    if message.from != closed.peer {
                        tracing::warn!(
                            transport_peer = ?closed.peer,
                            claimed_sender = ?message.from,
                            "rejected peer message whose sender differs from the connection peer"
                        );
                        break;
                    }
                    if data_plane_tx
                        .send_async(DataPlaneCommand::ReceivePeerMessage(message))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(e) => {
                    tracing::debug!("DataReader connection closed: {e}");
                    break;
                }
            }
        }

        // A replaced reader must not evict a newer connection for the same peer.
        let _ = disconnect_tx.send(closed).await;
    }
}

#[cfg(test)]
mod tests {
    use super::super::connection::write_frame;
    use super::*;
    use crate::data_plane::messages::command::DeleteSegments;
    use crate::net::{TcpListener, TcpStream};
    use std::time::Duration;

    #[test]
    fn forged_sender_is_not_dispatched_and_evicts_its_writer() -> turmoil::Result {
        let mut sim = turmoil::Builder::new()
            .rng_seed(13)
            .simulation_duration(Duration::from_secs(5))
            .build();
        sim.host("server", || async {
            let listener = TcpListener::bind("0.0.0.0:9000").await?;
            let (mut stream, _) = listener.accept().await?;
            let message = ReceivePeerMessage {
                from: NodeId::new("forged"),
                message: Box::new(
                    DeleteSegments {
                        segment_keys: Box::new([]),
                    }
                    .into(),
                ),
            };
            write_frame(&mut stream, &message, DATA_FRAME_MAX).await?;
            std::future::pending::<turmoil::Result>().await
        });
        sim.client("client", async {
            let stream = TcpStream::connect((turmoil::lookup("server"), 9000)).await?;
            let (reader, _writer) = stream.into_split();
            let (data_tx, data_rx) = flume::bounded(1);
            let (closed_tx, mut closed_rx) = mpsc::channel(1);
            DataReader::new(reader.into())
                .run(
                    DataPlaneSender(data_tx),
                    ConnectionClosed {
                        peer: NodeId::new("server"),
                        generation: 7,
                    },
                    closed_tx,
                )
                .await;
            assert!(data_rx.is_empty());
            let closed = closed_rx.recv().await.unwrap();
            assert_eq!(closed.peer, NodeId::new("server"));
            assert_eq!(closed.generation, 7);
            Ok(())
        });
        sim.run()
    }
}
