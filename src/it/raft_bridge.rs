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

/// Mock SWIM handler — responds to ResolveAddress queries.
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

/// Helper: build address map, start Raft stack, ensure group, elect leader, return raft_tx + ticker_force.
async fn start_raft_node(
    node_name: &str,
    cluster_port: u16,
    group: ShardGroup,
    peer_names: &[&str],
) -> Result<
    (
        mpsc::Sender<MultiRaftActorCommand>,
        mpsc::Sender<TickerCommand<crate::clusters::raft::messages::RaftTimer>>,
    ),
    Box<dyn std::error::Error>,
> {
    let node_id = NodeId::new(node_name);

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

    let (raft_tx, raft_mailbox) = mpsc::channel(100);
    let (transport_tx, transport_rx) = mpsc::channel(100);
    let (ticker_tx, ticker_rx) = mpsc::channel(64);
    let (swim_tx, swim_rx) = mpsc::channel(64);

    let ticker_force = ticker_tx.clone();
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
        Box::new(db),
        raft_mailbox,
        transport_tx,
        ticker_tx,
        swim_tx,
    ));

    raft_tx
        .send(MultiRaftCommand::EnsureGroup { group }.into())
        .await
        .unwrap();

    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    // Elect leader
    for _ in 0..200 {
        let _ = ticker_force.send(TickerCommand::ForceTick).await;
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
    }

    Ok((raft_tx, ticker_force))
}

/// Tick and yield.
async fn tick_n(
    ticker: &mpsc::Sender<TickerCommand<crate::clusters::raft::messages::RaftTimer>>,
    n: usize,
) {
    for _ in 0..n {
        let _ = ticker.send(TickerCommand::ForceTick).await;
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
    }
}

/// Query leader from MultiRaftActor and serve to checker via TCP.
async fn serve_leader(
    raft_tx: &mpsc::Sender<MultiRaftActorCommand>,
    group_id: ShardGroupId,
    query_port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    raft_tx
        .send(MultiRaftActorCommand::GetLeader {
            group_id,
            reply: reply_tx,
        })
        .await
        .unwrap();
    let leader = reply_rx.await.unwrap();

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

/// 3-node cluster: after election, inject HandleNodeDeath for node-3.
/// Leader proposes RemovePeer. Group continues with 2 members.
#[test]
#[serial_test::serial]
fn node_death_triggers_remove_peer() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(60))
        .build();

    let group_id = ShardGroupId(99);
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
                let (raft_tx, ticker) =
                    start_raft_node(name, CLUSTER_PORT + port, g.clone(), &peers).await?;

                // Inject node death
                let _ = raft_tx
                    .send(
                        MultiRaftCommand::HandleNodeDeath {
                            dead_node_id: NodeId::new("node-3"),
                        }
                        .into(),
                    )
                    .await;

                tick_n(&ticker, 100).await;
                serve_leader(&raft_tx, g.id, QUERY_PORT + port).await?;
                tokio::time::sleep(Duration::from_secs(600)).await;
                Ok(())
            }
        });
    }

    sim.client("checker", async {
        tokio::time::sleep(Duration::from_secs(30)).await;

        let leader1 = read_leader("node-1", QUERY_PORT + 1).await;
        let leader2 = read_leader("node-2", QUERY_PORT + 2).await;

        assert!(leader1.is_some(), "node-1 should know the leader");
        assert!(leader2.is_some(), "node-2 should know the leader");
        assert_eq!(
            leader1.unwrap(),
            leader2.unwrap(),
            "surviving nodes should agree on leader"
        );
        Ok(())
    });

    sim.run()
}

/// 2-node cluster: after election, inject HandleNodeJoin for node-3.
/// Leader proposes AddPeer. Group expands to include node-3.
#[test]
#[serial_test::serial]
fn node_join_triggers_add_peer() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(60))
        .build();

    let group_id = ShardGroupId(200);
    let initial_group = ShardGroup {
        id: group_id,
        members: vec![NodeId::new("node-1"), NodeId::new("node-2")],
    };
    let expanded_group = ShardGroup {
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
    ] {
        let ig = initial_group.clone();
        let eg = expanded_group.clone();
        sim.host(name, move || {
            let ig = ig.clone();
            let eg = eg.clone();
            let peers = peers.clone();
            async move {
                let (raft_tx, ticker) =
                    start_raft_node(name, CLUSTER_PORT + port, ig.clone(), &peers).await?;

                // Inject node join
                let _ = raft_tx
                    .send(
                        MultiRaftCommand::HandleNodeJoin {
                            new_node_id: NodeId::new("node-3"),
                            affected_groups: vec![eg],
                        }
                        .into(),
                    )
                    .await;

                tick_n(&ticker, 100).await;
                serve_leader(&raft_tx, ig.id, QUERY_PORT + port).await?;
                tokio::time::sleep(Duration::from_secs(600)).await;
                Ok(())
            }
        });
    }

    sim.client("checker", async {
        tokio::time::sleep(Duration::from_secs(30)).await;

        let leader1 = read_leader("node-1", QUERY_PORT + 1).await;
        let leader2 = read_leader("node-2", QUERY_PORT + 2).await;

        assert!(leader1.is_some(), "node-1 should know the leader");
        assert!(leader2.is_some(), "node-2 should know the leader");
        assert_eq!(
            leader1.unwrap(),
            leader2.unwrap(),
            "nodes should agree on leader after AddPeer"
        );
        Ok(())
    });

    sim.run()
}
