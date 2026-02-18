use std::collections::HashMap;
use std::sync::{Arc};
use std::time::Duration;

use tokio::{sync::mpsc, time};
use tokio::sync::RwLock;
use crate::clusters::swim::SwimActor;
use crate::clusters::topology::{PhysicalNodeId, Topology, TopologyConfig};

use super::*;

// Helper to setup the test environment
async fn setup() -> (
    mpsc::Sender<ActorEvent>,       // To send "Fake Network" events
    mpsc::Receiver<OutboundPacket>, // To catch "Outbound" commands
    SocketAddr,                     // The actor's local address
    Arc<RwLock<Topology>>,          // Handle to observe topology state
) {
    let (tx_in, rx_in) = mpsc::channel(100);
    let (tx_out, rx_out) = mpsc::channel(100);

    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

    let topology = Topology::new(HashMap::new(), TopologyConfig { replicas_per_node: 256 });
    let topo_handle = Arc::new(RwLock::new(topology));

    // Spawn the actor in the background
    let actor = SwimActor::new(addr, rx_in, tx_in.clone(), tx_out, topo_handle.clone());
    tokio::spawn(actor.run());

    (tx_in, rx_out, addr, topo_handle)
}

#[tokio::test]
async fn test_ping_response() {
    let (tx, mut rx, local_addr, _) = setup().await;
    let remote_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // 1. Simulate receiving a Ping from a remote node
    let ping = SwimPacket::Ping {
        seq: 100,
        source_incarnation: 0,
        gossip: vec![],
    };

    tx.send(ActorEvent::PacketReceived {
        src: remote_addr,
        packet: ping,
    })
    .await
    .unwrap();

    // 2. Assert the Actor sends an Ack back
    let response = time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("Actor should respond immediately")
        .expect("Channel closed");

    assert_eq!(response.target, remote_addr);
    match response.packet {
        SwimPacket::Ack { seq, .. } => assert_eq!(seq, 100),
        _ => panic!("Expected Ack packet"),
    }
}

#[tokio::test]
async fn test_refutation_mechanism() {
    let (tx, mut rx, local_addr, _) = setup().await;
    let remote_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // 1. Send a gossip message claiming WE (local_addr) are Suspect
    // The actor starts at Incarnation 0. We send Suspect with Incarnation 0.
    let lie = Member {
        addr: local_addr,
        state: NodeState::Suspect,
        incarnation: 0,
    };

    let ping = SwimPacket::Ping {
        seq: 200,
        source_incarnation: 0,
        gossip: vec![lie],
    };

    tx.send(ActorEvent::PacketReceived {
        src: remote_addr,
        packet: ping,
    })
    .await
    .unwrap();

    // 2. The Actor should respond with an Ack.
    // CHECK: Did the actor increment its incarnation to 1 to refute the lie?
    let response = rx.recv().await.unwrap();

    match response.packet {
        SwimPacket::Ack {
            source_incarnation, ..
        } => {
            assert_eq!(
                source_incarnation, 1,
                "Actor did not increment incarnation to refute suspicion!"
            );
        }
        _ => panic!("Expected Ack"),
    }
}

#[tokio::test]
async fn test_gossip_propagation() {
    let (tx, mut rx, _, _) = setup().await;
    let sender_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let dead_node: SocketAddr = "127.0.0.1:9999".parse().unwrap();

    // 1. Tell the actor that "Node 9999" is DEAD via gossip
    let gossip_msg = Member {
        addr: dead_node,
        state: NodeState::Dead,
        incarnation: 5,
    };

    let ping = SwimPacket::Ping {
        seq: 300,
        source_incarnation: 0,
        gossip: vec![gossip_msg],
    };

    tx.send(ActorEvent::PacketReceived {
        src: sender_addr,
        packet: ping,
    })
    .await
    .unwrap();

    // 2. Receive the Ack (flush the immediate response)
    let _ = rx.recv().await.unwrap();

    // 3. Now send a NEW ping from a different node.
    // We expect the actor to gossip "Node 9999 is Dead" back to us.
    let probe_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    tx.send(ActorEvent::PacketReceived {
        src: probe_addr,
        packet: SwimPacket::Ping {
            seq: 400,
            source_incarnation: 0,
            gossip: vec![],
        },
    })
    .await
    .unwrap();

    let response = rx.recv().await.unwrap();

    match response.packet {
        SwimPacket::Ack { gossip, .. } => {
            // Find the dead node in the gossip list
            let rumor = gossip.iter().find(|m| m.addr == dead_node);
            assert!(rumor.is_some(), "Actor did not gossip the Dead node info");
            assert_eq!(rumor.unwrap().state, NodeState::Dead);
        }
        _ => panic!("Expected Ack"),
    }
}

#[tokio::test]
async fn test_indirect_ping_trigger() {
    // This tests the timer logic: Tick -> Ping -> Timeout -> Indirect Ping

    let (tx, mut rx, _, _) = setup().await;
    let peer_1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let peer_2: SocketAddr = "127.0.0.1:9002".parse().unwrap();

    // 1. Manually add two Alive peers to the actor
    // We do this by sending them as gossip from a "bootstrap" packet
    let p1 = Member {
        addr: peer_1,
        state: NodeState::Alive,
        incarnation: 1,
    };
    let p2 = Member {
        addr: peer_2,
        state: NodeState::Alive,
        incarnation: 1,
    };

    tx.send(ActorEvent::PacketReceived {
        src: peer_1,
        packet: SwimPacket::Ping {
            seq: 1,
            source_incarnation: 1,
            gossip: vec![p1, p2],
        },
    })
    .await
    .unwrap();

    let _ack = rx.recv().await.unwrap(); // Clear the Ack

    // 2. Trigger the Protocol Tick manually
    tx.send(ActorEvent::ProtocolTick).await.unwrap();

    // 3. Expect a Direct Ping to one of them (e.g., Peer 1)
    let direct_ping = rx.recv().await.unwrap();
    let target = direct_ping.target;
    let seq = match direct_ping.packet {
        SwimPacket::Ping { seq, .. } => seq,
        _ => panic!("Expected Ping"),
    };

    // 4. DON'T send an Ack. Simulate a timeout.
    // We manually trigger the timeout event the actor would have scheduled.
    tx.send(ActorEvent::DirectProbeTimeout { target, seq })
        .await
        .unwrap();

    // 5. Expect Indirect Pings (PingReq) sent to the *other* peer
    let indirect_ping = time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("Should send indirect ping")
        .unwrap();

    match indirect_ping.packet {
        SwimPacket::PingReq {
            target: req_target, ..
        } => {
            assert_eq!(req_target, target, "PingReq should target the failed node");
        }
        _ => panic!("Expected PingReq, got {:?}", indirect_ping.packet),
    }
}

#[tokio::test]
async fn test_self_registers_in_topology_on_startup() {
    let (_, _, addr, topo_handle) = setup().await;

    // Yield to allow the spawned SwimActor task to start and self-register.
    time::sleep(Duration::from_millis(50)).await;

    let topo = topo_handle.read().await;
    assert!(
        topo.contains_node(&PhysicalNodeId::from(addr)),
        "SwimActor should register itself in the topology ring on startup"
    );
}

#[tokio::test]
async fn test_alive_gossip_adds_node_to_topology() {
    let (tx, mut rx, _, topo_handle) = setup().await;
    let sender_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let new_node: SocketAddr = "127.0.0.1:9001".parse().unwrap();

    // Gossip that new_node is Alive
    tx.send(ActorEvent::PacketReceived {
        src: sender_addr,
        packet: SwimPacket::Ping {
            seq: 1,
            source_incarnation: 1,
            gossip: vec![Member {
                addr: new_node,
                state: NodeState::Alive,
                incarnation: 1,
            }],
        },
    })
    .await
    .unwrap();

    // Topology is updated synchronously before the Ack is sent, so receiving
    // the Ack guarantees the topology has already been updated.
    let _ = rx.recv().await.unwrap();

    let topo = topo_handle.read().await;
    assert!(
        topo.contains_node(&PhysicalNodeId::from(new_node)),
        "Alive gossip should add the node to the topology ring"
    );
}

#[tokio::test]
async fn test_dead_gossip_removes_node_from_topology() {
    let (tx, mut rx, _, topo_handle) = setup().await;
    let sender_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let node: SocketAddr = "127.0.0.1:9001".parse().unwrap();

    // Step 1: add the node via Alive gossip
    tx.send(ActorEvent::PacketReceived {
        src: sender_addr,
        packet: SwimPacket::Ping {
            seq: 1,
            source_incarnation: 1,
            gossip: vec![Member {
                addr: node,
                state: NodeState::Alive,
                incarnation: 1,
            }],
        },
    })
    .await
    .unwrap();

    let _ = rx.recv().await.unwrap(); // Ack received → topology already updated

    assert!(
        topo_handle.read().await.contains_node(&PhysicalNodeId::from(node)),
        "Node should be present in topology after Alive gossip"
    );

    // Step 2: mark the node as Dead via gossip
    tx.send(ActorEvent::PacketReceived {
        src: sender_addr,
        packet: SwimPacket::Ping {
            seq: 2,
            source_incarnation: 1,
            gossip: vec![Member {
                addr: node,
                state: NodeState::Dead,
                incarnation: 2,
            }],
        },
    })
    .await
    .unwrap();

    let _ = rx.recv().await.unwrap(); // Ack received → topology already updated

    assert!(
        !topo_handle.read().await.contains_node(&PhysicalNodeId::from(node)),
        "Dead gossip should remove the node from the topology ring"
    );
}
