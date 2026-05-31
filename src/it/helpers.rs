use std::time::Duration;

use crate::config::Environment;
use crate::connections::clients::{ClientRawWriter, ClientStreamReader};
use crate::connections::protocol::{
    AdminRequest, AdminResponse, ClientRequest, ClientResponse, ControlPlaneResponse, NodeState,
    TestRequest, TestResponse,
};
use crate::control_plane::SwimNode;
use crate::control_plane::SwimNodeState;
use crate::net::TcpStream;

pub fn default_env(idx: u32, node_id: String, client_port: u16, cluster_port: u16) -> Environment {
    Environment {
        config_dir: std::env::temp_dir()
            .join(format!("eastguard-config-{}-{}", idx, uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned(),
        data_dir: std::env::temp_dir()
            .join(format!("eastguard-data-{}-{}", idx, uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned(),
        meta_dir: std::env::temp_dir()
            .join(format!("eastguard-meta-{}-{}", idx, uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned(),
        node_id_prefix: Some(node_id),
        client_port,
        cluster_port,
        host: "0.0.0.0".into(),
        advertise_host: None,
        vnodes_per_node: 256,
        join_seed_nodes: vec![],
        join_initial_delay_ms: 1000,
        join_interval_ms: 1000,
        join_multiplier: 2,
        join_max_attempts: 5,
        data_port: 2923,
        max_segment_age_secs: 3600,
        segment_age_check_interval_secs: 60,
        segment_size_limit_bytes: 1024 * 1024 * 1024,
        batch_max_bytes: 10 * 1024 * 1024,
        seal_request_timeout_secs: 5,
    }
}

pub async fn get_members(host: &str, port: u16) -> turmoil::Result<Vec<SwimNode>> {
    let stream = TcpStream::connect((host, port)).await?;
    let (read_half, write_half) = stream.into_split();
    let mut writer = ClientRawWriter::new(write_half);
    let mut reader = ClientStreamReader::new(read_half);
    writer
        .write(0, &ClientRequest::Admin(AdminRequest::DescribeCluster))
        .await?;
    let (_, response): (_, ClientResponse) = reader.read_request().await?;
    let nodes = match response {
        ClientResponse::Admin(AdminResponse::ClusterInfo { nodes }) => nodes,
        other => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected response: {:?}", std::mem::discriminant(&other)),
            )
            .into());
        }
    };
    let members = nodes
        .into_iter()
        .map(|n| SwimNode {
            node_id: crate::control_plane::NodeId::new(&n.node_id),
            addr: crate::control_plane::NodeAddress {
                cluster_addr: n.addr,
                client_addr: n.addr,
                data_addr: n.addr,
            },
            state: match n.state {
                NodeState::Alive => SwimNodeState::Alive,
                NodeState::Suspect => SwimNodeState::Suspect,
                NodeState::Dead => SwimNodeState::Dead,
            },
            incarnation: 0,
        })
        .collect();
    Ok(members)
}

pub async fn check_alive_count(host: &str, port: u16, expected: usize) -> turmoil::Result {
    let members = get_members(host, port).await?;
    tracing::info!("[TEST] INSPECTING host: {}", host);
    let alive_count = members
        .iter()
        .filter(|m| m.state == SwimNodeState::Alive)
        .count();
    assert_eq!(
        alive_count, expected,
        "{host} should have {expected} alive nodes, got {:?}",
        members
    );
    Ok(())
}

pub async fn check_dead_or_not_exist(host: &str, port: u16, target: &str) -> bool {
    let Ok(members) = get_members(host, port).await else {
        tracing::warn!("[TEST] check_dead_or_not_exist: could not reach {host}");
        return false;
    };
    match members.iter().find(|m| m.node_id.starts_with(target)) {
        None => {
            tracing::info!("[TEST] {host} has no entry for '{target}' — fully removed");
            true
        }
        Some(n) if n.state == SwimNodeState::Dead => {
            tracing::info!(
                "[TEST] {host} sees '{target}' as Dead (incarnation={})",
                n.incarnation
            );
            true
        }
        Some(n) => {
            tracing::info!(
                "[TEST] {host} sees '{target}' as {:?} — not dead yet (incarnation={})",
                n.state,
                n.incarnation
            );
            false
        }
    }
}

/// Sends a `ClientRequest` to a node and returns the raw `ClientResponse`.
pub async fn send_request(host: &str, port: u16, req: ClientRequest) -> ClientResponse {
    try_send_request(host, port, req)
        .await
        .expect("send_request: failed to connect or read response")
}

/// Sends a `ClientRequest` directly to a `SocketAddr` (e.g. a `leader_addr` from `NotLeader`).
pub async fn send_request_to_addr(
    addr: std::net::SocketAddr,
    req: ClientRequest,
) -> ClientResponse {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, write_half) = stream.into_split();
    let mut writer = ClientRawWriter::new(write_half);
    let mut reader = ClientStreamReader::new(read_half);
    writer.write(0, &req).await.unwrap();
    let (_, response) = reader.read_request().await.unwrap();
    response
}

/// polls until every node in `nodes` reports at least `expected_alive` SWIM-alive members. Panics after 30s.
pub async fn wait_swim_ready(nodes: &[(&str, u16)], expected_alive: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "SWIM did not converge to {expected_alive} alive nodes within 30s"
        );
        let mut all_ready = true;
        for &(host, port) in nodes {
            match get_members(host, port).await {
                Ok(members) => {
                    let alive = members
                        .iter()
                        .filter(|m| m.state == SwimNodeState::Alive)
                        .count();
                    if alive < expected_alive {
                        all_ready = false;
                        break;
                    }
                }
                Err(_) => {
                    all_ready = false;
                    break;
                }
            }
        }
        if all_ready {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Step 2a of cluster initialization: polls until every shard group on every node has an
/// elected Raft leader. Uses the `#[cfg(test)] IsClusterReady` admin command — no write
/// side effects. Panics after 15s.
pub async fn wait_raft_ready(nodes: &[(&str, u16)]) {
    let probe = ClientRequest::Test(TestRequest::IsClusterReady);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "Raft leader not elected within 15s after SWIM convergence"
        );
        let mut all_ready = true;
        for &(host, port) in nodes {
            match try_send_request(host, port, probe.clone()).await {
                Some(ClientResponse::Test(TestResponse::ClusterReady(true))) => {}
                _ => {
                    all_ready = false;
                    break;
                }
            }
        }
        if all_ready {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Step 2b of cluster initialization: calls `wait_raft_ready`, then sends `req` to whichever
/// node is the leader (following any `NotLeader { leader_addr: Some }` redirect).
///
/// Returns `(leader_port, response)` where `leader_port` is the client port of the node that
/// handled the request — useful for tests that need to identify and avoid the leader.
pub async fn wait_leader_ready(nodes: &[(&str, u16)], req: ClientRequest) -> (u16, ClientResponse) {
    wait_raft_ready(nodes).await;
    for &(host, port) in nodes {
        let Some(resp) = try_send_request(host, port, req.clone()).await else {
            continue;
        };
        match resp {
            ClientResponse::ControlPlane(ControlPlaneResponse::NotLeader {
                leader_addr: None,
            }) => continue,
            ClientResponse::ControlPlane(ControlPlaneResponse::NotLeader {
                leader_addr: Some(addr),
            }) => {
                let leader_resp = send_request_to_addr(addr, req).await;
                return (addr.port(), leader_resp);
            }
            other => return (port, other),
        }
    }
    panic!("no leader found after IsClusterReady returned true — this is a bug");
}

async fn try_send_request(host: &str, port: u16, req: ClientRequest) -> Option<ClientResponse> {
    let stream = TcpStream::connect((host, port)).await.ok()?;
    let (read_half, write_half) = stream.into_split();
    let mut writer = ClientRawWriter::new(write_half);
    let mut reader = ClientStreamReader::new(read_half);
    writer.write(0, &req).await.ok()?;
    let (_, response) = reader.read_request().await.ok()?;
    Some(response)
}
