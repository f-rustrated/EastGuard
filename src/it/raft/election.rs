use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use turmoil::Builder;

use crate::control_plane::consensus::actor::MultiRaftActor;
use crate::control_plane::consensus::messages::MultiRaftActorCommand;
use crate::control_plane::consensus::transport::RaftTransportActor;
use crate::control_plane::membership::actor::SwimActor;
use crate::control_plane::membership::{ShardGroup, ShardGroupId};
use crate::control_plane::{BINCODE_CONFIG, NodeId};
use crate::impls::metadata_storage::MetadataStorage;
use crate::net::{TcpListener, TcpStream};
use crate::schedulers::actor::run_scheduling_actor;
use crate::schedulers::ticker::{PROBE_INTERVAL_TICKS, TICK_PERIOD_100_MS};
use crate::schedulers::ticker_message::TickerCommand;

use super::{CLUSTER_PORT, QUERY_PORT, mock_swim_handler};

fn build_address_map(
    node_name: &str,
    cluster_port: u16,
    peer_names: &[&str],
) -> HashMap<NodeId, SocketAddr> {
    let mut address_map = HashMap::new();
    address_map.insert(
        NodeId::new(node_name),
        SocketAddr::new(turmoil::lookup(node_name), cluster_port),
    );
    for &peer in peer_names {
        let peer_idx = peer.strip_prefix("node-").unwrap().parse::<u16>().unwrap();
        address_map.insert(
            NodeId::new(peer),
            SocketAddr::new(turmoil::lookup(peer), CLUSTER_PORT + peer_idx),
        );
    }
    address_map
}

async fn drive_ticks(
    ticker: &mpsc::Sender<
        Box<[TickerCommand<crate::control_plane::consensus::messages::RaftTimer>]>,
    >,
    count: usize,
) {
    for _ in 0..count {
        let _ = ticker.send(Box::new([TickerCommand::ForceTick])).await;
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
    }
}

async fn serve_leader(
    query_port: u16,
    leader: Option<NodeId>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", query_port)).await?;
    let (stream, _) = listener.accept().await?;
    let (_read, mut write) = stream.into_split();
    let bytes = bincode::encode_to_vec(&leader, BINCODE_CONFIG).unwrap();
    let len = bytes.len() as u32;
    write.write_all(&len.to_be_bytes()).await?;
    write.write_all(&bytes).await?;
    Ok(())
}

async fn read_leader(host: &str, port: u16) -> Option<NodeId> {
    let addr = turmoil::lookup(host);
    let stream = TcpStream::connect((addr, port)).await.unwrap();
    let (mut read, _write) = stream.into_split();
    let len = read.read_u32().await.unwrap() as usize;
    let mut buf = vec![0u8; len];
    read.read_exact(&mut buf).await.unwrap();
    let (leader, _): (Option<NodeId>, _) =
        bincode::decode_from_slice(&buf, BINCODE_CONFIG).unwrap();
    leader
}

async fn run_raft_node(
    node_name: &str,
    cluster_port: u16,
    query_port: u16,
    group: ShardGroup,
    peer_names: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let node_id = NodeId::new(node_name);
    let address_map = build_address_map(node_name, cluster_port, peer_names);

    let (raft_tx, raft_mailbox) = MultiRaftActor::channel(100);
    let (transport_tx, transport_rx) = mpsc::channel(100);
    let (ticker_tx, ticker_rx) = mpsc::channel(64);
    let (swim_tx, swim_rx) = SwimActor::channel(64);
    let ticker_force = ticker_tx.clone();

    let bind_addr: SocketAddr = format!("0.0.0.0:{}", cluster_port).parse().unwrap();
    let listener = crate::net::TcpListener::bind(bind_addr).await?;

    tokio::spawn(mock_swim_handler(swim_rx, address_map));
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
    let db = MetadataStorage::open(std::env::temp_dir().join(uuid::Uuid::new_v4().to_string()));
    let election_jitter_seed = {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        node_id.hash(&mut h);
        h.finish()
    };
    let (data_tx, _) = tokio::sync::mpsc::channel(1);
    tokio::spawn(MultiRaftActor::run(
        node_id,
        election_jitter_seed,
        Box::new(db),
        raft_mailbox,
        transport_tx.into(),
        ticker_tx.into(),
        swim_tx,
        data_tx.into(),
    ));

    raft_tx
        .send(crate::control_plane::consensus::messages::EnsureGroup {
            group: group.clone(),
        })
        .await
        .unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    drive_ticks(&ticker_force, 200).await;

    let (reply_tx, reply_rx) = oneshot::channel();
    raft_tx
        .send(MultiRaftActorCommand::GetLeader {
            group_id: group.id,
            reply: reply_tx,
        })
        .await
        .unwrap();
    let leader = reply_rx.await.unwrap();
    serve_leader(query_port, leader).await?;

    tokio::time::sleep(Duration::from_secs(600)).await;
    Ok(())
}

/// 3 turmoil hosts each run a full Raft stack (MultiRaftActor + RaftTransportActor +
/// Ticker + mock SWIM). After ticking through election + heartbeat propagation,
/// the checker connects to each node and verifies that all agree on the same leader.
#[test]
#[serial_test::serial]
fn three_node_raft_elects_leader() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(30))
        .build();

    let group_id = ShardGroupId(42);
    let members: Vec<NodeId> = vec![
        NodeId::new("node-1"),
        NodeId::new("node-2"),
        NodeId::new("node-3"),
    ];
    let group = ShardGroup {
        id: group_id,
        members,
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
                run_raft_node(
                    name,
                    CLUSTER_PORT + port,
                    QUERY_PORT + port,
                    g,
                    &peers,
                )
                .await
            }
        });
    }

    sim.client("checker", async {
        tokio::time::sleep(Duration::from_secs(15)).await;

        let leader1 = read_leader("node-1", QUERY_PORT + 1).await;
        let leader2 = read_leader("node-2", QUERY_PORT + 2).await;
        let leader3 = read_leader("node-3", QUERY_PORT + 3).await;

        assert!(leader1.is_some(), "node-1 should know the leader");
        assert!(leader2.is_some(), "node-2 should know the leader");
        assert!(leader3.is_some(), "node-3 should know the leader");

        let leader = leader1.unwrap();
        assert_eq!(
            leader2.unwrap(),
            leader,
            "node-2 should agree on the leader"
        );
        assert_eq!(
            leader3.unwrap(),
            leader,
            "node-3 should agree on the leader"
        );

        tracing::info!("All nodes agree: leader = {:?}", leader);
        Ok(())
    });

    sim.run()
}
