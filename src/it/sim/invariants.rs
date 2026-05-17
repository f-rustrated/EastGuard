use std::time::Duration;

use crate::clusters::SwimNodeState;
use crate::connections::clients::{ClientRawWriter, ClientStreamReader};
use crate::connections::request::{ConnectionRequests, QueryCommand, TopicSummary};
use crate::it::helpers::get_members;
use crate::net::TcpStream;

#[allow(dead_code)]
/// Queries `GetMembers` from every node and asserts all see the same sorted set of alive IDs.
pub async fn assert_membership_converged(
    nodes: &[(&str, u16)],
    timeout: Duration,
) -> turmoil::Result {
    tokio::time::sleep(timeout).await;
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

#[allow(dead_code)]
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

#[allow(dead_code)]
/// After `CreateTopic` has been acked, retries `GetTopics` across all alive nodes
/// until all report the topic or `max_attempts` is exhausted.
pub async fn assert_topic_visible(
    nodes: &[(&str, u16)],
    topic_name: &str,
    max_attempts: u32,
    tick: Duration,
) -> turmoil::Result {
    for attempt in 0..max_attempts {
        let mut all_visible = true;
        for &(host, port) in nodes {
            let topics = query_topics(host, port).await?;
            if !topics.iter().any(|t| t.name == topic_name) {
                all_visible = false;
                break;
            }
        }
        if all_visible {
            return Ok(());
        }
        if attempt < max_attempts - 1 {
            tokio::time::sleep(tick).await;
        }
    }
    panic!(
        "topic '{}' not visible on all nodes after {} attempts",
        topic_name, max_attempts
    );
}

#[allow(dead_code)]
async fn query_shard_leader(
    host: &str,
    port: u16,
    shard_group_id: u64,
) -> turmoil::Result<Option<String>> {
    let stream = TcpStream::connect((host, port)).await?;
    let (read_half, write_half) = stream.into_split();
    let mut writer = ClientRawWriter::new(write_half);
    let mut reader = ClientStreamReader::new(read_half);
    writer
        .write(&ConnectionRequests::Query(QueryCommand::GetShardLeader {
            shard_group_id,
        }))
        .await?;
    Ok(reader.read_request().await?)
}

#[allow(dead_code)]
async fn query_topics(host: &str, port: u16) -> turmoil::Result<Vec<TopicSummary>> {
    let stream = TcpStream::connect((host, port)).await?;
    let (read_half, write_half) = stream.into_split();
    let mut writer = ClientRawWriter::new(write_half);
    let mut reader = ClientStreamReader::new(read_half);
    writer
        .write(&ConnectionRequests::Query(QueryCommand::GetTopics))
        .await?;
    Ok(reader.read_request().await?)
}
