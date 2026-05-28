use std::time::Duration;

use crate::control_plane::SwimNodeState;
use crate::connections::clients::{ClientRawWriter, ClientStreamReader};
use crate::connections::protocol::{
    AdminRequest, AdminResponse, ClientRequest, ClientResponse, ControlPlaneRequest,
    ControlPlaneResponse, ShardDetail, TopicSummary,
};
use crate::it::helpers::get_members;
use crate::net::TcpStream;

/// Queries `GetMembers` from every node and asserts all see the same sorted set of alive IDs.
/// Retries up to `max_attempts` times with `tick` between each attempt.
pub async fn assert_membership_converged(
    nodes: &[(&str, u16)],
    max_attempts: u32,
    timeout: Duration,
) -> turmoil::Result {
    for attempt in 0..max_attempts {
        let mut views: Vec<Vec<String>> = Vec::new();
        let mut query_failed = false;
        for &(host, port) in nodes {
            match get_members(host, port).await {
                Ok(members) => {
                    let mut alive: Vec<String> = members
                        .into_iter()
                        .filter(|m| m.state == SwimNodeState::Alive)
                        .map(|m| m.node_id.to_string())
                        .collect();
                    alive.sort();
                    views.push(alive);
                }
                Err(_) => {
                    query_failed = true;
                    break;
                }
            }
        }
        if !query_failed && views.len() >= 2 {
            let canonical = views[0].clone();
            if views.iter().all(|v| *v == canonical) {
                return Ok(());
            }
        }
        if attempt < max_attempts - 1 {
            tokio::time::sleep(timeout).await;
        }
    }
    // Final check with assertion to produce a clear failure message.
    let mut views: Vec<Vec<String>> = Vec::new();
    for &(host, port) in nodes {
        let members = get_members(host, port).await?;
        let mut alive: Vec<String> = members
            .into_iter()
            .filter(|m| m.state == SwimNodeState::Alive)
            .map(|m| m.node_id.to_string())
            .collect();
        alive.sort();
        views.push(alive);
    }
    let canonical = &views[0];
    for (i, view) in views[1..].iter().enumerate() {
        assert_eq!(
            view, canonical,
            "membership divergence: node[{}] disagrees (got {:?}, expected {:?})",
            i + 1,
            view,
            canonical
        );
    }
    Ok(())
}

/// Queries `GetShardLeader` for the given shard group from every node and asserts
/// all non-None answers name the same leader.
pub async fn assert_single_leader(
    nodes: &[(&str, u16)],
    shard_group_id: u64,
    timeout: Duration,
) -> turmoil::Result {
    tokio::time::sleep(timeout).await;
    let mut leaders: Vec<Option<String>> = Vec::new();
    for &(host, port) in nodes {
        let leader = query_shard_leader(host, port, shard_group_id).await?;
        leaders.push(leader);
    }
    let known: Vec<&String> = leaders.iter().flatten().collect();
    if known.len() >= 2 {
        let first = known[0];
        for (i, l) in known[1..].iter().enumerate() {
            assert_eq!(
                *l, first,
                "split-brain detected for shard {}: node[{}] reports leader {:?}, node[0] reports {:?}",
                shard_group_id, i + 1, l, first
            );
        }
    }
    Ok(())
}

/// After `CreateTopic` has been acked, retries `GetTopics` across alive nodes until
/// a quorum (majority) report the topic or `max_attempts` is exhausted.
///
/// Raft guarantees the entry is durable on a quorum; the remaining nodes apply it
/// on the next heartbeat and are not required to be visible at response time.
pub async fn assert_topic_visible_on_quorum(
    nodes: &[(&str, u16)],
    topic_name: &str,
    max_attempts: u32,
    tick: Duration,
) -> turmoil::Result {
    let quorum = nodes.len() / 2 + 1;
    for attempt in 0..max_attempts {
        let mut visible_count = 0;
        for &(host, port) in nodes {
            let visible = query_topics(host, port)
                .await
                .map(|topics| topics.iter().any(|t| t.name == topic_name))
                .unwrap_or(false);
            if visible {
                visible_count += 1;
            }
        }
        if visible_count >= quorum {
            return Ok(());
        }
        if attempt < max_attempts - 1 {
            tokio::time::sleep(tick).await;
        }
    }
    panic!(
        "topic '{}' not visible on quorum of nodes after {} attempts",
        topic_name, max_attempts
    );
}

pub(super) async fn query_shard_leader(
    host: &str,
    port: u16,
    shard_group_id: u64,
) -> turmoil::Result<Option<String>> {
    let stream = tokio::time::timeout(
        Duration::from_secs(2),
        TcpStream::connect((host, port)),
    )
    .await??;
    let (read_half, write_half) = stream.into_split();
    let mut writer = ClientRawWriter::new(write_half);
    let mut reader = ClientStreamReader::new(read_half);
    writer
        .write(0, &ClientRequest::Admin(AdminRequest::GetShardLeader { shard_group_id }))
        .await?;
    let (_, response): (_, ClientResponse) =
        tokio::time::timeout(Duration::from_secs(3), reader.read_request()).await??;
    match response {
        ClientResponse::Admin(AdminResponse::ShardLeader { leader }) => Ok(leader),
        _ => Ok(None),
    }
}

pub(super) async fn query_shard_info(
    host: &str,
    port: u16,
    key: &[u8],
) -> turmoil::Result<Option<ShardDetail>> {
    let stream = tokio::time::timeout(
        Duration::from_secs(2),
        TcpStream::connect((host, port)),
    )
    .await??;
    let (read_half, write_half) = stream.into_split();
    let mut writer = ClientRawWriter::new(write_half);
    let mut reader = ClientStreamReader::new(read_half);
    writer
        .write(0, &ClientRequest::Admin(AdminRequest::GetShardInfo { key: key.to_vec() }))
        .await?;
    let (_, response): (_, ClientResponse) =
        tokio::time::timeout(Duration::from_secs(3), reader.read_request()).await??;
    match response {
        ClientResponse::Admin(AdminResponse::ShardInfo { detail }) => Ok(detail),
        _ => Ok(None),
    }
}

pub(super) async fn query_topics(host: &str, port: u16) -> turmoil::Result<Vec<TopicSummary>> {
    let stream = tokio::time::timeout(
        Duration::from_secs(2),
        TcpStream::connect((host, port)),
    )
    .await??;
    let (read_half, write_half) = stream.into_split();
    let mut writer = ClientRawWriter::new(write_half);
    let mut reader = ClientStreamReader::new(read_half);
    writer
        .write(0, &ClientRequest::ControlPlane(ControlPlaneRequest::ListHostedTopics))
        .await?;
    let (_, response): (_, ClientResponse) =
        tokio::time::timeout(Duration::from_secs(3), reader.read_request()).await??;
    match response {
        ClientResponse::ControlPlane(ControlPlaneResponse::TopicList { topics }) => Ok(topics),
        _ => Ok(vec![]),
    }
}
