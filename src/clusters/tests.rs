use std::time::Duration;

use crate::clusters::swims::actor::SwimActor;
use crate::clusters::swims::{OutboundPacket, SwimCommand, SwimPacket, SwimTimer};

use crate::schedulers::actor::run_scheduling_actor;
use crate::schedulers::ticker::{DIRECT_ACK_TIMEOUT_TICKS, PROBE_INTERVAL_TICKS};
use crate::schedulers::ticker_message::TickerCommand;

use tokio::{sync::mpsc, time};

use super::*;

// Helper to setup the test environment
async fn setup() -> (
    mpsc::Sender<SwimCommand>,              // To send "Fake Network" events
    mpsc::Receiver<OutboundPacket>,         // To catch "Outbound" commands
    mpsc::Sender<TickerCommand<SwimTimer>>, // To drive the ticker directly
    SocketAddr,                             // The actor's local address
) {
    let (tx_in, rx_in) = mpsc::channel(100);
    let (tx_out, rx_out) = mpsc::channel(100);
    let (ticker_cmd_tx, ticker_cmd_rx) = mpsc::channel(100);

    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

    // TickerActor sends tick events back into the SwimActor's mailbox.
    tokio::spawn(run_scheduling_actor(tx_in.clone(), ticker_cmd_rx));

    let actor = SwimActor::new(
        addr,
        "node-local".into(),
        rx_in,
        tx_out,
        ticker_cmd_tx.clone(),
        256,
    );
    tokio::spawn(actor.run());

    (tx_in, rx_out, ticker_cmd_tx, addr)
}

#[tokio::test]
async fn test_ping_response() {
    let (tx, mut rx, _, _local_addr) = setup().await;
    let remote_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // 1. Simulate receiving a Ping from a remote node
    let ping = SwimPacket::Ping {
        seq: 100,
        source_node_id: "node-remote".into(),
        source_incarnation: 0,
        gossip: vec![],
    };

    tx.send(SwimCommand::PacketReceived {
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
    match response.packet() {
        SwimPacket::Ack { seq, .. } => assert_eq!(*seq, 100),
        _ => panic!("Expected Ack packet"),
    }
}

#[tokio::test]
async fn test_refutation_mechanism() {
    let (tx, mut rx, _, local_addr) = setup().await;
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

    tx.send(SwimCommand::PacketReceived {
        src: remote_addr,
        packet: ping,
    })
    .await
    .unwrap();

    // 2. The Actor should respond with an Ack.
    // CHECK: Did the actor increment its incarnation to 1 to refute the lie?
    let response = rx.recv().await.unwrap();

    match response.packet() {
        SwimPacket::Ack {
            source_incarnation, ..
        } => {
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
    let (tx, mut rx, _, _) = setup().await;
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

    tx.send(SwimCommand::PacketReceived {
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
        tx.send(SwimCommand::PacketReceived {
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
            if let SwimPacket::Ack { gossip, .. } = response.packet() {
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
    // Drives the TickerActor directly via TickerCommand::ForceTick.

    let (tx, mut rx, ticker_tx, _) = setup().await;
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

    tx.send(SwimCommand::PacketReceived {
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

    // 2. Force-tick PROBE_INTERVAL_TICKS times so the ticker fires a
    //    ProtocolPeriodElapsed, which makes SwimProtocol start a probe.
    //    LiveNodeTracker inserts at a random position, so start_probe() may land
    //    on self and skip. Retry up to 3 periods to guarantee hitting a peer.
    let mut target_addr = None;
    for _ in 0..3 {
        for _ in 0..PROBE_INTERVAL_TICKS {
            ticker_tx.send(TickerCommand::ForceTick).await.unwrap();
        }
        match time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(pkt)) if matches!(pkt.packet(), SwimPacket::Ping { .. }) => {
                target_addr = Some(pkt.target);
                break;
            }
            _ => {}
        }
    }
    let target_addr = target_addr.expect("Actor should send a Ping within 3 protocol periods");

    // 3. DON'T send an Ack. Force-tick DIRECT_ACK_TIMEOUT_TICKS times so the
    //    direct probe times out and the state machine transitions to indirect probing.
    for _ in 0..DIRECT_ACK_TIMEOUT_TICKS {
        ticker_tx.send(TickerCommand::ForceTick).await.unwrap();
    }

    // 4. Expect Indirect Pings (PingReq) sent to the *other* peer
    let indirect_ping = time::timeout(Duration::from_millis(100), rx.recv())
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
    let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();
    let (_tx_in, rx_in) = mpsc::channel(100);
    let (tx_out, _rx_out) = mpsc::channel(100);
    let (ticker_send, _ticker_recv) = mpsc::channel(100);

    let actor = SwimActor::new(addr, "node-local".into(), rx_in, tx_out, ticker_send, 256);

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
    let (_tx_in, rx_in) = mpsc::channel(100);
    let (tx_out, _rx_out) = mpsc::channel(100);
    let (ticker_send, _ticker_recv) = mpsc::channel(100);

    let mut actor = SwimActor::new(addr, "node-local".into(), rx_in, tx_out, ticker_send, 256);

    actor
        .process_event_for_test(SwimCommand::PacketReceived {
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
    let (_tx_in, rx_in) = mpsc::channel(100);
    let (tx_out, _rx_out) = mpsc::channel(100);
    let (ticker_send, _ticker_recv) = mpsc::channel(100);

    let mut actor = SwimActor::new(addr, "node-local".into(), rx_in, tx_out, ticker_send, 256);

    // Step 1: add the node via Alive gossip
    actor
        .process_event_for_test(SwimCommand::PacketReceived {
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
        .process_event_for_test(SwimCommand::PacketReceived {
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
