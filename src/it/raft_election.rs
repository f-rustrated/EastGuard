use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use turmoil::Builder;

use crate::clusters::raft::actor::MultiRaftActor;
use crate::clusters::raft::messages::{MultiRaftActorCommand, MultiRaftCommand};
use crate::clusters::raft::transport::RaftTransportActor;
use crate::clusters::swims::{ShardGroup, ShardGroupId, SwimCommand, SwimQueryCommand};
use crate::clusters::{BINCODE_CONFIG, NodeId};
use crate::net::{TcpListener, TcpStream};
use crate::schedulers::actor::run_scheduling_actor;
use crate::schedulers::ticker_message::TickerCommand;
use crate::storage::Db;

const CLUSTER_PORT: u16 = 19000;
const QUERY_PORT: u16 = 29000;

/// Mock SWIM handler — only responds to ResolveAddress queries.
async fn mock_swim_handler(
    mut rx: mpsc::Receiver<SwimCommand>,
    address_map: HashMap<NodeId, SocketAddr>,
) {
    while let Some(cmd) = rx.recv().await {
        if let SwimCommand::Query(SwimQueryCommand::ResolveAddress { node_id, reply }) = cmd {
            let _ = reply.send(address_map.get(&node_id).copied());
        }
    }
}

/// Starts a full Raft stack on this turmoil host. Uses ForceTick to drive the
/// ticker deterministically.
///
/// After enough ticks for election + heartbeat propagation, queries the elected
/// leader and serves it to the checker via a TCP endpoint.
async fn run_raft_node(
    node_name: &str,
    cluster_port: u16,
    query_port: u16,
    group: ShardGroup,
    peer_names: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let node_id = NodeId::new(node_name);

    // Build address map from turmoil hostnames
    let mut address_map = HashMap::new();
    address_map.insert(
        node_id.clone(),
        SocketAddr::new(turmoil::lookup(node_name), cluster_port),
    );
    for &peer in peer_names {
        let peer_idx = peer.strip_prefix("node-").unwrap().parse::<u16>().unwrap();
        address_map.insert(
            NodeId::new(peer),
            SocketAddr::new(turmoil::lookup(peer), CLUSTER_PORT + peer_idx),
        );
    }

    // Create channels
    let (raft_tx, raft_mailbox) = mpsc::channel(100);
    let (transport_tx, transport_rx) = mpsc::channel(100);
    let (ticker_tx, ticker_rx) = mpsc::channel(64);
    let (swim_tx, swim_rx) = mpsc::channel(64);

    let ticker_force = ticker_tx.clone();

    // Create and spawn actors
    let bind_addr: SocketAddr = format!("0.0.0.0:{}", cluster_port).parse().unwrap();
    let listener = crate::net::TcpListener::bind(bind_addr).await?;

    tokio::spawn(mock_swim_handler(swim_rx, address_map));
    tokio::spawn(run_scheduling_actor(raft_tx.clone(), ticker_rx));

    tokio::spawn(RaftTransportActor::run(
        node_id.clone(),
        listener,
        raft_tx.clone(),
        transport_rx,
        swim_tx.clone(),
    ));

    let db = Db::open(std::env::temp_dir().join(uuid::Uuid::new_v4().to_string()));
    tokio::spawn(MultiRaftActor::run(
        node_id,
        db,
        raft_mailbox,
        transport_tx,
        ticker_tx,
        swim_tx,
    ));

    // Ensure the shard group
    raft_tx
        .send(MultiRaftCommand::EnsureGroup { group: group.clone() }.into())
        .await
        .unwrap();

    // Let the actor process EnsureGroup and send timer commands to the ticker
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    // Drive the ticker with ForceTick. Between each tick, yield + sleep to give
    // transport and actor tasks time to process RPCs across the network.
    //
    // Election timeout = 50 base + ~6-8 jitter ticks. After election, the leader
    // sends heartbeats (AppendEntries) which tell followers the leader_id.
    // 200 ticks with interleaved yields is more than enough for the full cycle.
    for _ in 0..200 {
        let _ = ticker_force.send(TickerCommand::ForceTick).await;
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
    }

    // Query the elected leader
    let (reply_tx, reply_rx) = oneshot::channel();
    raft_tx
        .send(MultiRaftActorCommand::GetLeader {
            group_id: group.id,
            reply: reply_tx,
        })
        .await
        .unwrap();
    let leader = reply_rx.await.unwrap();

    // Serve the result to the checker via TCP
    let listener = TcpListener::bind(format!("0.0.0.0:{}", query_port)).await?;
    let (stream, _) = listener.accept().await?;
    let (_read, mut write) = stream.into_split();

    let bytes = bincode::encode_to_vec(&leader, BINCODE_CONFIG).unwrap();
    let len = bytes.len() as u32;
    write.write_all(&len.to_be_bytes()).await?;
    write.write_all(&bytes).await?;

    // Keep actors alive for the rest of the simulation
    tokio::time::sleep(Duration::from_secs(600)).await;
    Ok(())
}

/// Read `Option<NodeId>` from a node's query server.
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

/// 3 turmoil hosts each run a full Raft stack (MultiRaftActor + RaftTransportActor +
/// Ticker + mock SWIM). After ticking through election + heartbeat propagation,
/// the checker connects to each node and verifies that all agree on the same
/// elected leader.
#[test]
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

    let g1 = group.clone();
    sim.host("node-1", move || {
        let g = g1.clone();
        async move {
            run_raft_node(
                "node-1",
                CLUSTER_PORT + 1,
                QUERY_PORT + 1,
                g,
                &["node-2", "node-3"],
            )
            .await
        }
    });

    let g2 = group.clone();
    sim.host("node-2", move || {
        let g = g2.clone();
        async move {
            run_raft_node(
                "node-2",
                CLUSTER_PORT + 2,
                QUERY_PORT + 2,
                g,
                &["node-1", "node-3"],
            )
            .await
        }
    });

    let g3 = group.clone();
    sim.host("node-3", move || {
        let g = g3.clone();
        async move {
            run_raft_node(
                "node-3",
                CLUSTER_PORT + 3,
                QUERY_PORT + 3,
                g,
                &["node-1", "node-2"],
            )
            .await
        }
    });

    sim.client("checker", async {
        // Wait for all nodes to complete their tick-driven election
        tokio::time::sleep(Duration::from_secs(15)).await;

        let leader1 = read_leader("node-1", QUERY_PORT + 1).await;
        let leader2 = read_leader("node-2", QUERY_PORT + 2).await;
        let leader3 = read_leader("node-3", QUERY_PORT + 3).await;

        // All nodes must have elected a leader
        assert!(leader1.is_some(), "node-1 should know the leader");
        assert!(leader2.is_some(), "node-2 should know the leader");
        assert!(leader3.is_some(), "node-3 should know the leader");

        // All nodes must agree on the same leader
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
