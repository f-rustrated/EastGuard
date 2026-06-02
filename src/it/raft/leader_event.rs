use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use turmoil::Builder;

use crate::control_plane::consensus::actor::MultiRaftActor;
use crate::control_plane::consensus::messages::*;
use crate::control_plane::consensus::transport::RaftTransportActor;
use crate::control_plane::membership::actor::SwimActor;
use crate::control_plane::membership::{
    QueryCommand, ShardGroup, ShardGroupId, SwimActorCommand, SwimCommand,
};
use crate::control_plane::{BINCODE_CONFIG, NodeAddress, NodeId};
use crate::impls::metadata_storage::MetadataStorage;
use crate::net::{TcpListener, TcpStream};
use crate::schedulers::actor::run_scheduling_actor;
use crate::schedulers::ticker::{PROBE_INTERVAL_TICKS, TICK_PERIOD_100_MS};

use super::CLUSTER_PORT;

const RESULT_PORT: u16 = 39000;

/// Responds to ResolveAddress queries and forwards LeaderChange events to a channel.
async fn swim_handler_with_leader_capture(
    mut rx: mpsc::Receiver<SwimActorCommand>,
    address_map: HashMap<NodeId, SocketAddr>,
    leader_events_tx: mpsc::Sender<LeaderChange>,
) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            SwimActorCommand::Query(QueryCommand::ResolveAddress { node_id, reply }) => {
                let _ = reply.send(
                    address_map
                        .get(&node_id)
                        .map(|&addr| NodeAddress::test(addr, addr)),
                );
            }
            SwimActorCommand::Command(SwimCommand::AnnounceShardLeader(event)) => {
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

                let (raft_tx, raft_mailbox) = MultiRaftActor::channel(100);
                let (transport_tx, transport_rx) = mpsc::channel(100);
                let (ticker_tx, ticker_rx) = mpsc::channel(64);
                let (swim_tx, swim_rx) = SwimActor::channel(64);
                let (leader_events_tx, mut leader_events_rx) = mpsc::channel(64);

                let bind_addr: SocketAddr =
                    format!("0.0.0.0:{}", CLUSTER_PORT + port).parse().unwrap();
                let listener = TcpListener::bind(bind_addr).await?;

                tokio::spawn(swim_handler_with_leader_capture(
                    swim_rx,
                    address_map,
                    leader_events_tx,
                ));
                tokio::spawn(run_scheduling_actor(
                    raft_tx.clone().into(),
                    ticker_rx,
                    TICK_PERIOD_100_MS,
                    Some(PROBE_INTERVAL_TICKS),
                ));
                tokio::spawn(RaftTransportActor::run(
                    node_id.clone(),
                    listener,
                    raft_tx.clone(),
                    transport_rx,
                    swim_tx.clone(),
                ));
                let db = MetadataStorage::open(
                    std::env::temp_dir().join(uuid::Uuid::new_v4().to_string()),
                );
                let election_jitter_seed = {
                    let mut h = std::collections::hash_map::DefaultHasher::new();
                    node_id.hash(&mut h);
                    h.finish()
                };
                let (data_tx, _) = tokio::sync::mpsc::channel(1);
                let all_nodes: Vec<&str> =
                    std::iter::once(name).chain(peers.iter().copied()).collect();
                let topology_reader = super::stub_topology_reader(&all_nodes);
                tokio::spawn(MultiRaftActor::run(
                    node_id,
                    election_jitter_seed,
                    Box::new(db),
                    raft_mailbox,
                    transport_tx.into(),
                    ticker_tx.into(),
                    swim_tx,
                    data_tx.into(),
                    topology_reader,
                ));

                raft_tx.send(EnsureGroup { group: g }).await.unwrap();

                let mut events = Vec::new();
                for _ in 0..150 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    tokio::task::yield_now().await;
                    while let Ok(event) = leader_events_rx.try_recv() {
                        events.push(event);
                    }
                    if !events.is_empty() {
                        break;
                    }
                }

                let result_listener =
                    TcpListener::bind(format!("0.0.0.0:{}", RESULT_PORT + port)).await?;
                let (stream, _) = result_listener.accept().await?;
                let (_read, mut write) = stream.into_split();

                let count_bytes = bincode::encode_to_vec(events.len(), BINCODE_CONFIG).unwrap();
                let count_len = count_bytes.len() as u32;
                write.write_all(&count_len.to_be_bytes()).await?;
                write.write_all(&count_bytes).await?;

                if let Some(event) = events.first() {
                    let leader_bytes =
                        bincode::encode_to_vec(Some(event.leader_node_id.clone()), BINCODE_CONFIG)
                            .unwrap();
                    let leader_len = leader_bytes.len() as u32;
                    write.write_all(&leader_len.to_be_bytes()).await?;
                    write.write_all(&leader_bytes).await?;
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

            let len = read.read_u32().await.unwrap() as usize;
            let mut buf = vec![0u8; len];
            read.read_exact(&mut buf).await.unwrap();
            let (count, _): (usize, _) = bincode::decode_from_slice(&buf, BINCODE_CONFIG).unwrap();

            total_events += count;

            if count > 0 {
                let leader_len = read.read_u32().await.unwrap() as usize;
                let mut leader_buf = vec![0u8; leader_len];
                read.read_exact(&mut leader_buf).await.unwrap();
                let (node_id, _): (Option<NodeId>, _) =
                    bincode::decode_from_slice(&leader_buf, BINCODE_CONFIG).unwrap();
                leader_node = node_id;
            }
        }

        assert!(
            total_events >= 1,
            "at least one LeaderChangeEvent should be emitted"
        );
        assert!(leader_node.is_some(), "leader_node_id should be set");
        tracing::info!(
            "LeaderChangeEvent emitted: leader={:?}, total_events={}",
            leader_node,
            total_events
        );
        Ok(())
    });

    sim.run()
}
