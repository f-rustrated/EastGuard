use std::collections::HashMap;
use std::collections::btree_map::Keys;
use std::time::Duration;

use crate::clusters::swim::SwimActor;
use crate::clusters::topology::{Topology, TopologyConfig};
use tokio::{sync::mpsc, time};

use super::*;

fn make_topology() -> Topology {
    Topology::new(
        HashMap::new(),
        TopologyConfig {
            vnodes_per_pnode: 256,
        },
    )
}

// Helper to setup the test environment
async fn setup() -> (
    mpsc::Sender<ActorEvent>,       // To send "Fake Network" events
    mpsc::Receiver<OutboundPacket>, // To catch "Outbound" commands
    SocketAddr,                     // The actor's local address
) {
    let (tx_in, rx_in) = mpsc::channel(100);
    let (tx_out, rx_out) = mpsc::channel(100);

    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

    let actor = SwimActor::new(
        addr,
        "node-local".into(),
        rx_in,
        tx_in.clone(),
        tx_out,
        make_topology(),
    );
    tokio::spawn(actor.run());

    (tx_in, rx_out, addr)
}

#[tokio::test]
async fn test_ping_response() {
    let (tx, mut rx, _local_addr) = setup().await;
    let remote_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // 1. Simulate receiving a Ping from a remote node
    let ping = SwimPacket::Ping {
        seq: 100,
        source_node_id: "node-remote".into(),
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
    let (tx, mut rx, local_addr) = setup().await;
    let remote_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // 1. Send a gossip message claiming WE (local_addr) are Suspect
    // The actor starts at Incarnation 0. We send Suspect with Incarnation 0.
    let lie = SwimNode {
        node_id: "node-local".into(),
        addr: local_addr,
        state: SwimNodeState::Suspect,
        incarnation: 0,
    };

    let ping = SwimPacket::Ping {
        seq: 200,
        source_node_id: "node-remote".into(),
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
    let (tx, mut rx, _) = setup().await;
    let sender_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let dead_node: SocketAddr = "127.0.0.1:9999".parse().unwrap();
    let probe_addr: SocketAddr = "127.0.0.1:8001".parse().unwrap();

    // 1. Tell the actor that "Node 9999" is DEAD via gossip
    let gossip_msg = SwimNode {
        node_id: "node-dead".into(),
        addr: dead_node,
        state: SwimNodeState::Dead,
        incarnation: 5,
    };

    tx.send(ActorEvent::PacketReceived {
        src: sender_addr,
        packet: SwimPacket::Ping {
            seq: 300,
            source_node_id: "node-sender".into(),
            source_incarnation: 0,
            gossip: vec![gossip_msg],
        },
    })
    .await
    .unwrap();

    // 2. Retry Loop: Probe until we hear the rumor or timeout
    // We give the actor 5 attempts (or 500ms) to propagate the info.
    let mut propagated = false;

    for i in 0..5 {
        // Send a fresh probe
        tx.send(ActorEvent::PacketReceived {
            src: probe_addr,
            packet: SwimPacket::Ping {
                seq: 400 + i, // Increment seq to keep packets distinct
                source_node_id: "node-probe".into(),
                source_incarnation: 0,
                gossip: vec![],
            },
        })
        .await
        .unwrap();

        // Wait for response
        if let Some(response) = rx.recv().await {
            if let SwimPacket::Ack { gossip, .. } = response.packet {
                // Check if our rumor is in this specific Ack
                if let Some(rumor) = gossip.iter().find(|m| m.addr == dead_node) {
                    assert_eq!(rumor.state, SwimNodeState::Dead);
                    propagated = true;
                    break; // Success!
                }
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

    let (tx, mut rx, _) = setup().await;
    let peer_1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let peer_2: SocketAddr = "127.0.0.1:9002".parse().unwrap();

    // 1. Manually add two Alive peers to the actor
    // We do this by sending them as gossip from a "bootstrap" packet
    let p1 = SwimNode {
        node_id: "node-peer-1".into(),
        addr: peer_1,
        state: SwimNodeState::Alive,
        incarnation: 1,
    };
    let p2 = SwimNode {
        node_id: "node-peer-2".into(),
        addr: peer_2,
        state: SwimNodeState::Alive,
        incarnation: 1,
    };

    tx.send(ActorEvent::PacketReceived {
        src: peer_1,
        packet: SwimPacket::Ping {
            seq: 1,
            source_node_id: "node-peer-1".into(),
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
    let target_addr = direct_ping.target;
    let target_node_id = if target_addr == peer_1 {
        "node-peer-1"
    } else {
        "node-peer-2"
    };
    let seq = match direct_ping.packet {
        SwimPacket::Ping { seq, .. } => seq,
        _ => panic!("Expected Ping"),
    };

    // 4. DON'T send an Ack. Simulate a timeout.
    // We manually trigger the timeout event the actor would have scheduled.
    tx.send(ActorEvent::DirectProbeTimeout {
        target_node_id: target_node_id.into(),
        seq,
    })
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
            assert_eq!(
                req_target, target_addr,
                "PingReq should target the failed node"
            );
        }
        _ => panic!("Expected PingReq, got {:?}", indirect_ping.packet),
    }
}

#[tokio::test]
async fn test_suspect_timeout_mark_node_dead() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let sender_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let target_addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let (tx_in, rx_in) = mpsc::channel(100);
    let (tx_out, rx_out) = mpsc::channel(100);

    let mut actor = SwimActor::new(
        addr,
        "node-local".into(),
        rx_in,
        tx_in.clone(),
        tx_out,
        make_topology(),
    );

    actor.process_event_for_test(ActorEvent::PacketReceived {
        src: (sender_addr), packet: (SwimPacket::Ping {
            seq: (1),
            source_node_id: ("node-sender".into()),
            source_incarnation: (1),
            gossip: (vec![SwimNode {
                node_id: "node-target".into(),
                addr: target_addr,
                state: SwimNodeState::Alive,
                incarnation: 1,
            }])
        })
    })
    .await;

    assert!(
        actor.topology().contains_node(&"node-target".into()),
        "Node should be in topology after Alive gossip"
    );

    actor.process_event_for_test(ActorEvent::IndirectProbeTimeout {
        target_node_id: ("node-target".into())
    })
    .await;

    assert!(
        actor.topology().contains_node(&"node-target".into()),
        "Suspect node should be in topology"
    );

    actor.process_event_for_test(ActorEvent::SuspectTimeout {
        target_node_id: ("node-target".into())
    })
    .await;

    assert!(
        !actor.topology().contains_node(&"node-target".into()),
        "Dead node should be removed from the topology after the SuspectTimeout"
    )

}

#[tokio::test]
async fn test_self_registers_in_topology_on_startup() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let (tx_in, rx_in) = mpsc::channel(100);
    let (tx_out, _rx_out) = mpsc::channel(100);

    let mut actor = SwimActor::new(
        addr,
        "node-local".into(),
        rx_in,
        tx_in.clone(),
        tx_out,
        make_topology(),
    );
    actor.init_self_for_test();

    assert!(
        actor.topology().contains_node(&"node-local".into()),
        "SwimActor should register itself in the topology ring on startup"
    );
}

#[tokio::test]
async fn test_alive_gossip_adds_node_to_topology() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let sender_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let new_node: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let (tx_in, rx_in) = mpsc::channel(100);
    let (tx_out, _rx_out) = mpsc::channel(100);

    let mut actor = SwimActor::new(
        addr,
        "node-local".into(),
        rx_in,
        tx_in.clone(),
        tx_out,
        make_topology(),
    );

    actor
        .process_event_for_test(ActorEvent::PacketReceived {
            src: sender_addr,
            packet: SwimPacket::Ping {
                seq: 1,
                source_node_id: "node-sender".into(),
                source_incarnation: 1,
                gossip: vec![SwimNode {
                    node_id: "node-new".into(),
                    addr: new_node,
                    state: SwimNodeState::Alive,
                    incarnation: 1,
                }],
            },
        })
        .await;

    assert!(
        actor.topology().contains_node(&"node-new".into()),
        "Alive gossip should add the node to the topology ring"
    );
}

#[tokio::test]
async fn test_dead_gossip_removes_node_from_topology() {
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let sender_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let node: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let (tx_in, rx_in) = mpsc::channel(100);
    let (tx_out, _rx_out) = mpsc::channel(100);

    let mut actor = SwimActor::new(
        addr,
        "node-local".into(),
        rx_in,
        tx_in.clone(),
        tx_out,
        make_topology(),
    );

    // Step 1: add the node via Alive gossip
    actor
        .process_event_for_test(ActorEvent::PacketReceived {
            src: sender_addr,
            packet: SwimPacket::Ping {
                seq: 1,
                source_node_id: "node-sender".into(),
                source_incarnation: 1,
                gossip: vec![SwimNode {
                    node_id: "node-target".into(),
                    addr: node,
                    state: SwimNodeState::Alive,
                    incarnation: 1,
                }],
            },
        })
        .await;

    assert!(
        actor.topology().contains_node(&"node-target".into()),
        "Node should be present in topology after Alive gossip"
    );

    // Step 2: mark the node as Dead via gossip
    actor
        .process_event_for_test(ActorEvent::PacketReceived {
            src: sender_addr,
            packet: SwimPacket::Ping {
                seq: 2,
                source_node_id: "node-sender".into(),
                source_incarnation: 1,
                gossip: vec![SwimNode {
                    node_id: "node-target".into(),
                    addr: node,
                    state: SwimNodeState::Dead,
                    incarnation: 2,
                }],
            },
        })
        .await;

    assert!(
        !actor.topology().contains_node(&"node-target".into()),
        "Dead gossip should remove the node from the topology ring"
    );
}
