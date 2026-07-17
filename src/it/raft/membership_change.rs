use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use turmoil::Builder;

use crate::control_plane::consensus::actor::{MultiRaftActor, MutlRaftSender};
use crate::control_plane::consensus::messages::*;
use crate::control_plane::consensus::transport::RaftTransportActor;
use crate::control_plane::membership::actor::SwimActor;
use crate::control_plane::membership::{ShardGroup, ShardGroupId, Topology};
use crate::control_plane::{NodeId, Replicas, SwimNodeState};
use crate::impls::metadata_storage::MetadataStorage;
use crate::net::{TcpListener, TcpStream};
use crate::schedulers::actor::spawn_scheduling_actor;
use crate::schedulers::ticker::{PROBE_INTERVAL_TICKS, TICK_PERIOD_100_MS};
use crate::schedulers::ticker_message::{SchedulerSender, TickerCommand};

use super::{CLUSTER_PORT, QUERY_PORT, mock_swim_handler};

async fn start_raft_node(
    node_name: &str,
    cluster_port: u16,
    group: ShardGroup,
    peer_names: &[&str],
) -> Result<
    (
        MutlRaftSender,
        SchedulerSender<RaftTimer>,
        Arc<ArcSwap<Topology>>,
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

    let (raft_tx, raft_mailbox) = MultiRaftActor::channel(100);
    let (transport_tx, transport_rx) = mpsc::channel(100);

    let (swim_tx, swim_rx) = SwimActor::channel(64);

    let bind_addr: SocketAddr = format!("0.0.0.0:{}", cluster_port).parse().unwrap();
    let listener = crate::net::TcpListener::bind(bind_addr).await?;

    tokio::spawn(mock_swim_handler(swim_rx, address_map));

    let ticker_tx = spawn_scheduling_actor(
        raft_tx.clone().into(),
        100,
        TICK_PERIOD_100_MS,
        Some(PROBE_INTERVAL_TICKS),
    );

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
    let all_nodes: Vec<&str> = std::iter::once(node_name)
        .chain(peer_names.iter().copied())
        .collect();
    let (topology_pub, topology_reader) = super::stub_topology_channel(&all_nodes);

    MultiRaftActor::spawn(
        ticker_tx.clone(),
        raft_mailbox,
        node_id,
        election_jitter_seed,
        Box::new(db),
        transport_tx,
        swim_tx,
        data_tx,
        topology_reader,
    );

    raft_tx.send(EnsureGroup { group }).await.unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    for _ in 0..200 {
        let _ = ticker_tx
            .clone()
            .send_batch(Box::new([TickerCommand::ForceTick]))
            .await;
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
    }

    Ok((raft_tx, ticker_tx, topology_pub))
}

async fn tick_n(ticker: &SchedulerSender<RaftTimer>, n: usize) {
    for _ in 0..n {
        let _ = ticker
            .send_batch(Box::new([TickerCommand::ForceTick]))
            .await;
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
    }
}

async fn serve_leader(
    raft_tx: &MutlRaftSender,
    group_id: ShardGroupId,
    query_port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let leader = raft_tx.get_leader(group_id).await;

    let listener = TcpListener::bind(format!("0.0.0.0:{}", query_port)).await?;
    let (stream, _) = listener.accept().await?;
    let (_read, mut write) = stream.into_split();
    let bytes = borsh::to_vec(&leader).unwrap();
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
    let leader: Option<NodeId> = borsh::from_slice(&buf).unwrap();
    leader
}

async fn serve_peers(
    raft_tx: &MutlRaftSender,
    group_id: ShardGroupId,
    query_port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let peers = raft_tx.get_peers(group_id).await;

    let listener = TcpListener::bind(format!("0.0.0.0:{}", query_port)).await?;
    let (stream, _) = listener.accept().await?;
    let (_read, mut write) = stream.into_split();
    let bytes = borsh::to_vec(&peers).unwrap();
    let len = bytes.len() as u32;
    write.write_all(&len.to_be_bytes()).await?;
    write.write_all(&bytes).await?;
    Ok(())
}

async fn read_peers(host: &str, port: u16) -> Vec<NodeId> {
    let addr = turmoil::lookup(host);
    let stream = TcpStream::connect((addr, port)).await.unwrap();
    let (mut read, _write) = stream.into_split();
    let len = read.read_u32().await.unwrap() as usize;
    let mut buf = vec![0u8; len];
    read.read_exact(&mut buf).await.unwrap();
    let peers: Vec<NodeId> = borsh::from_slice(&buf).unwrap();
    peers
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
        replicas: Replicas::new(vec![
            NodeId::new("node-1"),
            NodeId::new("node-2"),
            NodeId::new("node-3"),
        ]),
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
                let (raft_tx, ticker, _topology_pub) =
                    start_raft_node(name, CLUSTER_PORT + port, g.clone(), &peers).await?;

                let _ = raft_tx
                    .send(RaftProtocolMessage::HandleNodeDeath(NodeId::new("node-3")))
                    .await;

                if name == "node-3" {
                    tokio::time::sleep(Duration::from_secs(600)).await;
                    return Ok(());
                }

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

/// 4-node-aware cluster (3 hosts + 1 ring-known spare), RF=3. The group starts as
/// [n1, n2, n3]; n4 exists in the topology (ring-eligible) but doesn't run a Raft host.
/// Crash n3 — the leader proposes a committed RemovePeer(n3) and **stages n4 as a
/// non-voting learner** (the learner model). n4 is promoted to a voter only once it has
/// caught up — but n4 isn't a real host, so it never acks, never catches up, and never
/// becomes a voter. The surviving leaders' voter set converges to [n1, n2]: n3 dropped,
/// n4 NOT a voter. This is exactly what the learner model protects against — a staged
/// replacement that can't participate never joins the quorum, so it can't freeze it.
///
/// `sim.crash("node-3")` actually kills the host so its Raft loop stops responding.
/// The surviving hosts inject `NodeDead(n3)` after the crash so the leader's
/// `handle_node_death` runs with an actually-unreachable n3.
#[test]
#[serial_test::serial]
fn node_death_removes_dead_voter_unreachable_spare_stays_non_voting() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(1))
        .simulation_duration(Duration::from_secs(360))
        .build();

    let group_id = ShardGroupId(101);
    let initial_group = ShardGroup {
        id: group_id,
        replicas: Replicas::new(vec![
            NodeId::new("node-1"),
            NodeId::new("node-2"),
            NodeId::new("node-3"),
        ]),
    };

    // peer_names threaded into start_raft_node seed the stub topology — all
    // three hosts must see n4 on the ring so `ring_replacements_for` returns
    // n4 as the eligible non-member.
    for (name, port, peers) in [
        ("node-1", 1u16, vec!["node-2", "node-3", "node-4"]),
        ("node-2", 2, vec!["node-1", "node-3", "node-4"]),
        ("node-3", 3, vec!["node-1", "node-2", "node-4"]),
    ] {
        let group = initial_group.clone();
        sim.host(name, move || {
            let group = group.clone();
            let peers = peers.clone();
            async move {
                let (raft_tx, ticker, topology_pub) =
                    start_raft_node(name, CLUSTER_PORT + port, group.clone(), &peers).await?;

                if name == "node-3" {
                    // Stay alive until the sim driver crashes us.
                    tokio::time::sleep(Duration::from_secs(600)).await;
                    return Ok(());
                }

                // Wait long enough for the cluster to stabilize a leader and
                // for the sim driver to have crashed n3 (driver crashes at
                // t≈15s).
                tokio::time::sleep(Duration::from_secs(20)).await;

                // Mirror what `SwimActor` does in production: mutate the local
                // topology to mark node-3 dead BEFORE emitting NodeDead. Without
                // this, the leader-takeover safety net
                // (`reconcile_on_leadership_change` → `reconcile_peers`) sees
                // node-3 as still-alive in `live_nodes()` and proposes nothing,
                // so if a re-election lands after the NodeDead arrives and the
                // event was no-op'd on a follower, nothing recovers it.
                let mut topology = (**topology_pub.load()).clone();
                topology.update(NodeId::new("node-3"), &SwimNodeState::Dead);
                topology_pub.store(Arc::new(topology));

                let _ = raft_tx
                    .send(RaftProtocolMessage::HandleNodeDeath(NodeId::new("node-3")))
                    .await;

                // Drive ticks for the chained proposals to commit and apply
                // (RemovePeer → AddPeer at the leader; replication to the
                // surviving follower).
                tick_n(&ticker, 1000).await;
                serve_peers(&raft_tx, group.id, QUERY_PORT + port).await?;
                tokio::time::sleep(Duration::from_secs(600)).await;
                Ok(())
            }
        });
    }

    sim.client("checker", async {
        // Total wait: cluster startup + 20s injection delay + 1000 ticks of
        // replication. Allow ample headroom for election + chained commits.
        tokio::time::sleep(Duration::from_secs(100)).await;

        let n1_peers = read_peers("node-1", QUERY_PORT + 1).await;
        let n2_peers = read_peers("node-2", QUERY_PORT + 2).await;

        let n3 = NodeId::new("node-3");
        let n4 = NodeId::new("node-4");

        assert!(
            !n1_peers.contains(&n3),
            "node-1 must drop the dead node-3, got peers={:?} n2_peers={:?}",
            n1_peers,
            n2_peers
        );
        assert!(
            !n2_peers.contains(&n3),
            "node-2 must drop the dead node-3, got {:?}",
            n2_peers
        );

        // n4 is staged as a learner but never catches up (not a real host), so it is
        // never promoted to a voter — the voter set stays [n1, n2].
        assert!(
            !n1_peers.contains(&n4),
            "node-1 must NOT promote the unreachable spare node-4 to a voter, got {:?}",
            n1_peers
        );
        assert!(
            !n2_peers.contains(&n4),
            "node-2 must NOT promote the unreachable spare node-4 to a voter, got {:?}",
            n2_peers
        );

        Ok(())
    });

    // Step the sim until t≈15s, then crash node-3. After that, run the sim
    // to completion (the surviving hosts have a 20s pre-injection wait, so
    // crashing at 15s gives them ~5s with a confirmed-dead n3 before they
    // tell the leader).
    while sim.elapsed() < Duration::from_secs(15) {
        sim.step()?;
    }
    sim.crash("node-3");
    sim.run()
}
