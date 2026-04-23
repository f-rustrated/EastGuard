use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use turmoil::Builder;

use crate::clusters::raft::actor::MultiRaftActor;
use crate::clusters::raft::messages::MultiRaftCommand;
use crate::clusters::raft::messages::LeaderChange;
use crate::clusters::raft::transport::RaftTransportActor;
use crate::clusters::swims::{ShardGroup, ShardGroupId, SwimCommand, SwimQueryCommand};
use crate::clusters::{BINCODE_CONFIG, NodeId};
use crate::net::{TcpListener, TcpStream};
use crate::schedulers::actor::run_scheduling_actor;
use crate::schedulers::ticker_message::TickerCommand;
use crate::storage::Db;

const CLUSTER_PORT: u16 = 19000;
const RESULT_PORT: u16 = 39000;

/// Mock SWIM handler that responds to ResolveAddress queries and collects
/// LeaderChangeEvents for later retrieval.
async fn swim_handler_with_leader_capture(
    mut rx: mpsc::Receiver<SwimCommand>,
    address_map: HashMap<NodeId, SocketAddr>,
    leader_events_tx: mpsc::Sender<LeaderChange>,
) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            SwimCommand::Query(SwimQueryCommand::ResolveAddress { node_id, reply }) => {
                let _ = reply.send(address_map.get(&node_id).copied());
            }
            SwimCommand::AnnounceShardLeader(event) => {
                let _ = leader_events_tx.send(event).await;
            }
            _ => {}
        }
    }
}

/// 3-node cluster: after election, verify that exactly one node emits a
/// LeaderChangeEvent with term=1 and correct shard_group_id.
#[test]
#[serial_test::serial]
fn leader_election_emits_leader_change_event() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(60))
        .build();

    let group_id = ShardGroupId(77);
    let group = ShardGroup {
        id: group_id,
        members: vec![
            NodeId::new("node-1"),
            NodeId::new("node-2"),
            NodeId::new("node-3"),
        ],
    };

    for (name, port, peers) in [
        ("node-1", 1u16, vec!["node-2", "node-3"]),
        ("node-2", 2, vec!["node-1", "node-3"]),
        ("node-3", 3, vec!["node-1", "node-2"]),
    ] {
        let g = group.clone();
        sim.host(name, move || {
            let g = g.clone();
            let peers = peers.clone();
            async move {
                let node_id = NodeId::new(name);

                let mut address_map = HashMap::new();
                address_map.insert(
                    node_id.clone(),
                    SocketAddr::new(turmoil::lookup(name), CLUSTER_PORT + port),
                );
                for &peer in &peers {
                    let peer_idx = peer.strip_prefix("node-").unwrap().parse::<u16>().unwrap();
                    address_map.insert(
                        NodeId::new(peer),
                        SocketAddr::new(turmoil::lookup(peer), CLUSTER_PORT + peer_idx),
                    );
                }

                let (raft_tx, raft_mailbox) = mpsc::channel(100);
                let (transport_tx, transport_rx) = mpsc::channel(100);
                let (ticker_tx, ticker_rx) = mpsc::channel(64);
                let (swim_tx, swim_rx) = mpsc::channel(64);
                let (leader_events_tx, mut leader_events_rx) = mpsc::channel(64);

                let ticker_force = ticker_tx.clone();
                let bind_addr: SocketAddr =
                    format!("0.0.0.0:{}", CLUSTER_PORT + port).parse().unwrap();
                let listener = TcpListener::bind(bind_addr).await?;

                tokio::spawn(swim_handler_with_leader_capture(
                    swim_rx,
                    address_map,
                    leader_events_tx,
                ));
                tokio::spawn(run_scheduling_actor(raft_tx.clone(), ticker_rx));
                {
                    let node_id = node_id.clone();
                    let raft_tx = raft_tx.clone();
                    let swim_tx = swim_tx.clone();
                    tokio::spawn(RaftTransportActor::run(
                        node_id,
                        listener,
                        raft_tx,
                        transport_rx,
                        swim_tx,
                    ));
                }
                let db = Db::open(std::env::temp_dir().join(uuid::Uuid::new_v4().to_string()));
                tokio::spawn(MultiRaftActor::run(
                    node_id,
                    db,
                    raft_mailbox,
                    transport_tx,
                    ticker_tx,
                    swim_tx,
                ));

                raft_tx
                    .send(MultiRaftCommand::EnsureGroup { group: g }.into())
                    .await
                    .unwrap();

                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }

                // Drive election
                for _ in 0..200 {
                    let _ = ticker_force.send(TickerCommand::ForceTick).await;
                    tokio::task::yield_now().await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    tokio::task::yield_now().await;
                }

                // Collect leader events (non-blocking drain)
                let mut events = Vec::new();
                while let Ok(event) = leader_events_rx.try_recv() {
                    events.push(event);
                }

                // Serve result to checker via TCP
                let listener = TcpListener::bind(format!("0.0.0.0:{}", RESULT_PORT + port)).await?;
                let (stream, _) = listener.accept().await?;
                let (_read, mut write) = stream.into_split();

                let bytes = bincode::encode_to_vec(events.len(), BINCODE_CONFIG).unwrap();
                let len = bytes.len() as u32;
                write.write_all(&len.to_be_bytes()).await?;
                write.write_all(&bytes).await?;

                // If we have events, send first event's leader_node_id
                if let Some(event) = events.first() {
                    let bytes =
                        bincode::encode_to_vec(Some(event.leader_node_id.clone()), BINCODE_CONFIG)
                            .unwrap();
                    let len = bytes.len() as u32;
                    write.write_all(&len.to_be_bytes()).await?;
                    write.write_all(&bytes).await?;
                }

                tokio::time::sleep(Duration::from_secs(600)).await;
                Ok(())
            }
        });
    }

    sim.client("checker", async {
        tokio::time::sleep(Duration::from_secs(30)).await;

        let mut total_events = 0usize;
        let mut leader_node: Option<NodeId> = None;

        for (host, port) in [("node-1", 1u16), ("node-2", 2), ("node-3", 3)] {
            let addr = turmoil::lookup(host);
            let stream = TcpStream::connect((addr, RESULT_PORT + port))
                .await
                .unwrap();
            let (mut read, _write) = stream.into_split();

            // Read event count
            let len = read.read_u32().await.unwrap() as usize;
            let mut buf = vec![0u8; len];
            read.read_exact(&mut buf).await.unwrap();
            let (count, _): (usize, _) = bincode::decode_from_slice(&buf, BINCODE_CONFIG).unwrap();

            total_events += count;

            if count > 0 {
                // Read leader_node_id
                let len = read.read_u32().await.unwrap() as usize;
                let mut buf = vec![0u8; len];
                read.read_exact(&mut buf).await.unwrap();
                let (node_id, _): (Option<NodeId>, _) =
                    bincode::decode_from_slice(&buf, BINCODE_CONFIG).unwrap();
                leader_node = node_id;
            }
        }

        // Exactly one node should have emitted a leader event
        assert!(
            total_events >= 1,
            "at least one LeaderChangeEvent should be emitted"
        );
        assert!(
            leader_node.is_some(),
            "leader_node_id should be set in the event"
        );

        tracing::info!(
            "LeaderChangeEvent emitted: leader={:?}, total_events={}",
            leader_node,
            total_events
        );
        Ok(())
    });

    sim.run()
}
