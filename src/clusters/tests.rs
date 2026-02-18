use tokio::{sync::mpsc, time};

use crate::clusters::swim::SwimActor;
use crate::clusters::topology::{Topology, TopologyActor, TopologyConfig};

use super::*;

use std::collections::HashMap;
use std::time::Duration;

// Helper to setup the test environment
async fn setup() -> (
    mpsc::Sender<ActorEvent>,       // To send "Fake Network" events
    mpsc::Receiver<OutboundPacket>, // To catch "Outbound" commands
    SocketAddr,                     // The actor's local address
) {
    let (tx_in, rx_in) = mpsc::channel(100);
    let (tx_out, rx_out) = mpsc::channel(100);
    let (tx_cluster, rx_cluster) = mpsc::channel(100);

    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

    let topology = Topology::new(HashMap::new(), TopologyConfig { replicas_per_node: 256 });
    let topo_actor = TopologyActor::new(topology, rx_cluster);
    tokio::spawn(topo_actor.run());

    // Spawn the actor in the background
    let actor = SwimActor::new(addr, rx_in, tx_in.clone(), tx_out, tx_cluster);
    tokio::spawn(actor.run());

    (tx_in, rx_out, addr)
}

#[tokio::test]
async fn test_ping_response() {
    let (tx, mut rx, local_addr) = setup().await;
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
    let (tx, mut rx, local_addr) = setup().await;
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
    let (tx, mut rx, _) = setup().await;
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

    let (tx, mut rx, _) = setup().await;
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
