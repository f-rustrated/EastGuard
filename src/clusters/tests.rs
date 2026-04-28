use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::clusters::NodeAddress;
use crate::clusters::swims::actor::SwimActor;
use crate::clusters::swims::peer_discovery::JoinConfig;
use crate::clusters::swims::swim::Swim;
use crate::clusters::swims::{
    DIRECT_ACK_TIMEOUT_TICKS, OutboundPacket, SwimCommand, SwimHeader, SwimPacket,
    SwimQueryCommand, SwimTimer, Topology, TopologyConfig,
};

use crate::schedulers::actor::run_scheduling_actor;

use crate::schedulers::ticker::PROBE_INTERVAL_TICKS;
use crate::schedulers::ticker_message::TickerCommand;

use tokio::{sync::mpsc, time};

use super::*;

#[allow(dead_code)]
struct TestHarness {
    pub tx_in: mpsc::Sender<SwimCommand>,
    pub tx_out: mpsc::Sender<OutboundPacket>,
    pub rx_out: Option<mpsc::Receiver<OutboundPacket>>,
    pub ticker_tx: mpsc::Sender<TickerCommand<SwimTimer>>,
    pub local_addr: SocketAddr,
    pub config: JoinConfig,
}

impl TestHarness {
    pub async fn query_topology_count(&self) -> usize {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx_in
            .send(SwimCommand::Query(SwimQueryCommand::GetMembers {
                reply: tx,
            }))
            .await
            .unwrap();
        rx.await.unwrap().len()
    }

    /// Check if a node is in the topology ring by verifying it's a known
    /// member with Alive state (Alive → inserted into ring, Dead → removed).
    pub async fn query_topology_includes(&self, node_id: NodeId) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx_in
            .send(SwimCommand::Query(SwimQueryCommand::GetMembers {
                reply: tx,
            }))
            .await
            .unwrap();
        rx.await
            .unwrap()
            .iter()
            .any(|m| m.node_id == node_id && m.state == SwimNodeState::Alive)
    }
}

/// Routes `OutboundPacket`s between test harnesses, simulating a network.
/// Call `add()` for every harness, then `spawn()` to start routing.
struct NetworkBridge {
    routes: HashMap<SocketAddr, mpsc::Sender<SwimCommand>>,
    inbounds: Vec<(SocketAddr, mpsc::Receiver<OutboundPacket>)>,
}

impl NetworkBridge {
    fn new() -> Self {
        NetworkBridge {
            routes: HashMap::new(),
            inbounds: Vec::new(),
        }
    }

    fn add(&mut self, harness: &mut TestHarness) {
        let rx = harness.rx_out.take().expect("rx_out already taken");
        self.routes
            .insert(harness.local_addr, harness.tx_in.clone());
        self.inbounds.push((harness.local_addr, rx));
    }

    fn spawn(self) {
        let routes = Arc::new(self.routes);
        for (sender_addr, mut rx) in self.inbounds {
            let routes = Arc::clone(&routes);
            tokio::spawn(async move {
                while let Some(pkt) = rx.recv().await {
                    if let Some(tx) = routes.get(&pkt.target) {
                        let _ = tx
                            .send(SwimCommand::PacketReceived {
                                src: sender_addr,
                                packet: pkt.packet().clone(),
                            })
                            .await;
                    }
                }
            });
        }
    }
}

fn no_join_config() -> JoinConfig {
    JoinConfig {
        seed_addrs: vec![],
        ticks_for_wait: 0,
        backoff_ticks: 10,
        multiplier: 1,
        max_attempts: 0,
    }
}

// Helper to setup the test environment
async fn setup_single() -> TestHarness {
    setup_with_config(8000, no_join_config()).await
}

async fn setup_with_config(port: u32, join_config: JoinConfig) -> TestHarness {
    let (tx_in, rx_in) = mpsc::channel(100);
    let (tx_out, rx_out) = mpsc::channel(100);
    let (ticker_tx, ticker_rx) = mpsc::channel(100);

    let peer_addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let client_addr: SocketAddr = format!("127.0.0.1:{}", port + 1000).parse().unwrap();

    let swim = Swim::new(
        NodeId::new(format!("node-local-{}", port).as_str()),
        NodeAddress {
            cluster_addr: peer_addr,
            client_addr,
        },
        Topology::new(
            std::iter::empty(),
            TopologyConfig {
                vnodes_per_pnode: 256,
                replication_factor: 3,
            },
        ),
        0,
    )
    .bootstrap(join_config.tries());

    let (raft_tx, _raft_rx) = tokio::sync::mpsc::channel(64);

    tokio::spawn(run_scheduling_actor(tx_in.clone(), ticker_rx));
    tokio::spawn(SwimActor::run(
        rx_in,
        swim,
        tx_out.clone(),
        ticker_tx.clone(),
        raft_tx,
    ));

    TestHarness {
        tx_in,
        tx_out,
        rx_out: Some(rx_out),
        ticker_tx,
        local_addr: peer_addr,
        config: join_config,
    }
}

#[tokio::test]
async fn test_ping_response() {
    let mut harness = setup_single().await;
    let mut rx_out = harness.rx_out.take().unwrap();
    let remote_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // 1. Simulate receiving a Ping from a remote node
    let ping = SwimPacket::Ping(SwimHeader {
        seq: 100,
        source_node_id: "node-remote".into(),
        source_incarnation: 0,
        gossip: vec![],
        shard_leaders: vec![],
    });

    harness
        .tx_in
        .send(SwimCommand::PacketReceived {
            src: remote_addr,
            packet: ping,
        })
        .await
        .unwrap();

    // 2. Assert the Actor sends an Ack back
    let response = time::timeout(Duration::from_millis(100), rx_out.recv())
        .await
        .expect("Actor should respond immediately")
        .expect("Channel closed");

    assert_eq!(response.target, remote_addr);
    match response.packet() {
        SwimPacket::Ack(SwimHeader { seq, .. }) => assert_eq!(*seq, 100),
        _ => panic!("Expected Ack packet"),
    }
}

#[tokio::test]
async fn test_refutation_mechanism() {
    let mut harness = setup_single().await;
    let mut rx_out = harness.rx_out.take().unwrap();
    let remote_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // 1. Send a gossip message claiming WE (local_addr) are Suspect
    // The actor starts at Incarnation 0. We send Suspect with Incarnation 0.
    let lie = SwimNode {
        node_id: "node-local-8000".into(),
        addr: NodeAddress {
            cluster_addr: harness.local_addr,
            client_addr: harness.local_addr,
        },
        state: SwimNodeState::Suspect,
        incarnation: 0,
    };

    let ping = SwimPacket::Ping(SwimHeader {
        seq: 200,
        source_node_id: "node-remote".into(),
        source_incarnation: 0,
        gossip: vec![lie],
        shard_leaders: vec![],
    });

    harness
        .tx_in
        .send(SwimCommand::PacketReceived {
            src: remote_addr,
            packet: ping,
        })
        .await
        .unwrap();

    // 2. The Actor should respond with an Ack.
    // CHECK: Did the actor increment its incarnation to 1 to refute the lie?
    let response = rx_out.recv().await.unwrap();

    match response.packet() {
        SwimPacket::Ack(SwimHeader {
            source_incarnation, ..
        }) => {
            assert_eq!(
                *source_incarnation, 1,
                "Actor did not increment incarnation to refute suspicion!"
            );
        }
        _ => panic!("Expected Ack"),
    }
}

#[tokio::test]
async fn test_gossip_propagation() {
    let mut harness = setup_single().await;
    let mut rx_out = harness.rx_out.take().unwrap();
    let sender_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let dead_node: SocketAddr = "127.0.0.1:9999".parse().unwrap();
    let probe_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();

    // 1. Tell the actor that "Node 9999" is DEAD via gossip
    let gossip_msg = SwimNode {
        node_id: "node-dead".into(),
        addr: NodeAddress {
            cluster_addr: dead_node,
            client_addr: dead_node,
        },
        state: SwimNodeState::Dead,
        incarnation: 5,
    };

    harness
        .tx_in
        .send(SwimCommand::PacketReceived {
            src: sender_addr,
            packet: SwimPacket::Ping(SwimHeader {
                seq: 300,
                source_node_id: "node-sender".into(),
                source_incarnation: 0,
                gossip: vec![gossip_msg],
                shard_leaders: vec![],
            }),
        })
        .await
        .unwrap();

    // 2. Retry Loop: Probe until we hear the rumor or timeout
    // We give the actor 5 attempts (or 500ms) to propagate the info.
    let mut propagated = false;

    for i in 0..5 {
        // Send a fresh probe
        harness
            .tx_in
            .send(SwimCommand::PacketReceived {
                src: probe_addr,
                packet: SwimPacket::Ping(SwimHeader {
                    seq: 400 + i, // Increment seq to keep packets distinct
                    source_node_id: "node-probe".into(),
                    source_incarnation: 0,
                    gossip: vec![],
                    shard_leaders: vec![],
                }),
            })
            .await
            .unwrap();

        // Wait for response
        if let Some(response) = rx_out.recv().await
            && let SwimPacket::Ack(SwimHeader { gossip, .. }) = response.packet()
        {
            // Check if our rumor is in this specific Ack
            if let Some(rumor) = gossip.iter().find(|m| m.addr.cluster_addr == dead_node) {
                assert_eq!(rumor.state, SwimNodeState::Dead);
                propagated = true;
                break; // Success!
            }
        }

        // Brief sleep to allow the actor's async tasks to complete
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(
        propagated,
        "Actor failed to gossip the Dead node info after retries"
    );
}

#[tokio::test]
async fn test_indirect_ping_trigger() {
    // This tests the timer logic: Tick -> Ping -> Timeout -> Indirect Ping
    // Drives the TickerActor directly via TickerCommand::ForceTick.

    let mut harness = setup_single().await;
    let mut rx_out = harness.rx_out.take().unwrap();
    let peer_1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let peer_2: SocketAddr = "127.0.0.1:9002".parse().unwrap();

    // 1. Manually add two Alive peers to the actor
    // We do this by sending them as gossip from a "bootstrap" packet
    let p1 = SwimNode {
        node_id: "node-peer-1".into(),
        addr: NodeAddress {
            cluster_addr: peer_1,
            client_addr: peer_1,
        },
        state: SwimNodeState::Alive,
        incarnation: 1,
    };
    let p2 = SwimNode {
        node_id: "node-peer-2".into(),
        addr: NodeAddress {
            cluster_addr: peer_2,
            client_addr: peer_2,
        },
        state: SwimNodeState::Alive,
        incarnation: 1,
    };

    harness
        .tx_in
        .send(SwimCommand::PacketReceived {
            src: peer_1,
            packet: SwimPacket::Ping(SwimHeader {
                seq: 1,
                source_node_id: "node-peer-1".into(),
                source_incarnation: 1,
                gossip: vec![p1, p2],
                shard_leaders: vec![],
            }),
        })
        .await
        .unwrap();

    let _ack = rx_out.recv().await.unwrap(); // Clear the Ack

    // 2. Force-tick PROBE_INTERVAL_TICKS times so the ticker fires a
    //    ProtocolPeriodElapsed, which makes SwimProtocol start a probe.
    //    LiveNodeTracker inserts at a random position, so start_probe() may land
    //    on self and skip. Retry up to 3 periods to guarantee hitting a peer.
    let mut target_addr = None;
    for _ in 0..3 {
        for _ in 0..PROBE_INTERVAL_TICKS {
            harness
                .ticker_tx
                .send(TickerCommand::ForceTick)
                .await
                .unwrap();
        }
        match time::timeout(Duration::from_millis(100), rx_out.recv()).await {
            Ok(Some(pkt)) if matches!(pkt.packet(), SwimPacket::Ping(_)) => {
                target_addr = Some(pkt.target);
                break;
            }
            _ => {}
        }
    }
    let target_addr = target_addr.expect("Actor should send a Ping within 3 protocol periods");

    // 3. DON'T send an Ack. Force-tick DIRECT_ACK_TIMEOUT_TICKS times so the
    //    direct probe timeout and the state machine transitions to indirect probing.
    for _ in 0..DIRECT_ACK_TIMEOUT_TICKS {
        harness
            .ticker_tx
            .send(TickerCommand::ForceTick)
            .await
            .unwrap();
    }

    // 4. Expect Indirect Pings (PingReq) sent to the *other* peer
    let indirect_ping = time::timeout(Duration::from_millis(100), rx_out.recv())
        .await
        .expect("Should send indirect ping")
        .unwrap();

    match indirect_ping.packet() {
        SwimPacket::PingReq {
            target: req_target, ..
        } => {
            assert_eq!(
                *req_target, target_addr,
                "PingReq should target the failed node"
            );
        }
        _ => panic!("Expected PingReq, got {:?}", indirect_ping.packet()),
    }
}

#[tokio::test]
async fn test_self_registers_in_topology_on_startup() {
    let harness = setup_single().await;

    assert!(
        harness
            .query_topology_includes("node-local-8000".into())
            .await,
        "SwimActor should register itself in the topology ring on startup"
    );
}

#[tokio::test]
async fn test_alive_gossip_adds_node_to_topology() {
    let harness = setup_single().await;
    let sender_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let new_node: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let _ = harness
        .tx_in
        .send(SwimCommand::PacketReceived {
            src: sender_addr,
            packet: SwimPacket::Ping(SwimHeader {
                seq: 1,
                source_node_id: "node-sender".into(),
                source_incarnation: 1,
                gossip: vec![SwimNode {
                    node_id: "node-new".into(),
                    addr: NodeAddress {
                        cluster_addr: new_node,
                        client_addr: new_node,
                    },
                    state: SwimNodeState::Alive,
                    incarnation: 1,
                }],
                shard_leaders: vec![],
            }),
        })
        .await;

    assert!(
        harness.query_topology_includes("node-new".into()).await,
        "Alive gossip should add the node to the topology ring"
    );
}

#[tokio::test]
async fn test_dead_gossip_removes_node_from_topology() {
    let harness = setup_single().await;
    let sender_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let node: SocketAddr = "127.0.0.1:9001".parse().unwrap();

    {
        // Step 1: add the node via Alive gossip
        let _ = harness
            .tx_in
            .send(SwimCommand::PacketReceived {
                src: sender_addr,
                packet: SwimPacket::Ping(SwimHeader {
                    seq: 1,
                    source_node_id: "node-sender".into(),
                    source_incarnation: 1,
                    gossip: vec![SwimNode {
                        node_id: "node-target".into(),
                        addr: NodeAddress {
                            cluster_addr: node,
                            client_addr: node,
                        },
                        state: SwimNodeState::Alive,
                        incarnation: 1,
                    }],
                    shard_leaders: vec![],
                }),
            })
            .await;
    }

    assert!(
        harness.query_topology_includes("node-target".into()).await,
        "Node should be present in topology after Alive gossip"
    );

    // Step 2: mark the node as Dead via gossip
    let _ = harness
        .tx_in
        .send(SwimCommand::PacketReceived {
            src: sender_addr,
            packet: SwimPacket::Ping(SwimHeader {
                seq: 2,
                source_node_id: "node-sender".into(),
                source_incarnation: 1,
                gossip: vec![SwimNode {
                    node_id: "node-target".into(),
                    addr: NodeAddress {
                        cluster_addr: node,
                        client_addr: node,
                    },
                    state: SwimNodeState::Dead,
                    incarnation: 2,
                }],
                shard_leaders: vec![],
            }),
        })
        .await;

    assert!(
        !harness.query_topology_includes("node-target".into()).await,
        "Dead gossip should remove the node from the topology ring"
    );
}

#[tokio::test]
async fn cluster_formation_using_join() {
    let join_config = JoinConfig {
        seed_addrs: ["127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"]
            .iter()
            .map(|addr| addr.parse().unwrap())
            .collect(),
        ticks_for_wait: 1,
        backoff_ticks: 1,
        multiplier: 2,
        max_attempts: 3,
    };
    let mut h1 = setup_with_config(8001, join_config.clone()).await;
    let mut h2 = setup_with_config(8002, join_config.clone()).await;
    let mut h3 = setup_with_config(8003, join_config.clone()).await;

    let mut bridge = NetworkBridge::new();
    bridge.add(&mut h1);
    bridge.add(&mut h2);
    bridge.add(&mut h3);
    bridge.spawn();

    // Synchronize: round-trip through each actor's mailbox to guarantee it has
    // started and flushed its initial join timer commands to the scheduler.
    let _ = h1.query_topology_count().await;
    let _ = h2.query_topology_count().await;
    let _ = h3.query_topology_count().await;

    for _ in 0..PROBE_INTERVAL_TICKS * 2 {
        h1.ticker_tx.send(TickerCommand::ForceTick).await.unwrap();
        h2.ticker_tx.send(TickerCommand::ForceTick).await.unwrap();
        h3.ticker_tx.send(TickerCommand::ForceTick).await.unwrap();
    }

    assert_eq!(h1.query_topology_count().await, 3);
    assert_eq!(h2.query_topology_count().await, 3);
    assert_eq!(h3.query_topology_count().await, 3);
}
