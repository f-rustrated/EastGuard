use crate::config::Environment;
use crate::connections::protocol::{
    AdminRequest, AdminResponse, ClientRequest, ClientResponse, NodeState,
};
use crate::connections::reader::ClientStreamReader;
use crate::connections::writer::ClientRawWriter;
use crate::control_plane::{NodeAddress, SwimNode};
use crate::control_plane::{NodeId, SwimNodeState};
use crate::net::TcpStream;

pub fn default_env(idx: u32, node_id: String, client_port: u16, cluster_port: u16) -> Environment {
    Environment {
        config_dir: std::env::temp_dir()
            .join(format!("eastguard-config-{}-{}", idx, uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned(),
        config_file: None,
        data_dir: std::env::temp_dir()
            .join(format!("eastguard-data-{}-{}", idx, uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned(),
        meta_dir: std::env::temp_dir()
            .join(format!("eastguard-meta-{}-{}", idx, uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned(),
        node_id_prefix: Some(node_id),
        node_id_suffix: None,
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
        // Short in tests so the orphan-GC sweep actually fires within a sim run; still
        // comfortably longer than re-fill + catch-up, so the lottery completes first.
        orphan_gc_interval_secs: 60,
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
            node_id: NodeId::new(&n.node_id),
            addr: NodeAddress::new(n.addr, n.addr, n.addr),
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
/// Upper bound on a single client round-trip. Healthy produce/fetch/metadata
/// requests complete in well under a second; this only trips when the server
/// holds the reply indefinitely (e.g. a produce parked on a stalled commit).
/// Failing fast with a clear message beats hanging the whole (serial) suite —
/// `send_request` has no other escape, since the data plane can legitimately
/// park a reply until commit.
const SEND_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Best-effort request: `None` on any failure (connect refused, timeout, I/O), with a
/// short timeout — for polling a node that may be down (e.g. mid-bounce/restart) without
/// `send_request`'s 30s panic-timeout stalling on a crashed host.
pub async fn try_send_request(host: &str, port: u16, req: ClientRequest) -> Option<ClientResponse> {
    let exchange = async {
        let stream = TcpStream::connect((host, port)).await.ok()?;
        let (read_half, write_half) = stream.into_split();
        let mut writer = ClientRawWriter::new(write_half);
        let mut reader = ClientStreamReader::new(read_half);
        writer.write(0, &req).await.ok()?;
        let (_, response) = reader.read_request().await.ok()?;
        Some(response)
    };
    tokio::time::timeout(std::time::Duration::from_secs(3), exchange)
        .await
        .ok()
        .flatten()
}

pub async fn send_request(host: &str, port: u16, req: ClientRequest) -> ClientResponse {
    let exchange = async {
        let stream = TcpStream::connect((host, port)).await.unwrap();
        let (read_half, write_half) = stream.into_split();
        let mut writer = ClientRawWriter::new(write_half);
        let mut reader = ClientStreamReader::new(read_half);
        writer.write(0, &req).await.unwrap();
        let (_, response) = reader.read_request().await.unwrap();
        response
    };
    tokio::time::timeout(SEND_REQUEST_TIMEOUT, exchange)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "send_request to {host}:{port} timed out after {SEND_REQUEST_TIMEOUT:?} \
                 (server never replied — likely a parked/stalled request)"
            )
        })
}
