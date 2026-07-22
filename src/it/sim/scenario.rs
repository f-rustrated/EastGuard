use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
use std::time::Duration;

use rand::{SeedableRng, rngs::StdRng, seq::IndexedRandom};
use serde::{Deserialize, Serialize};
use turmoil::Builder;

use crate::StartUp;
use crate::impl_from_variant;

use crate::connections::protocol::{
    ClientDataPlaneRequest, ClientRequest, ClientResponse, ControlPlaneRequest,
    ControlPlaneResponse, DataPlaneResponse, FetchByIdRequest, ProduceRequest, TopicDetail,
};
use crate::connections::reader::ClientStreamReader;
use crate::connections::writer::ClientRawWriter;
use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
use crate::control_plane::metadata::{EntryId, RangeId, TopicId};
use crate::it::helpers::{check_dead_or_not_exist, default_env};
use crate::it::sim::invariants::{
    assert_membership_converged, assert_topic_visible_on_nodes, query_shard_info,
    query_shard_leader,
};
use crate::net::TcpStream;

pub(super) fn node_name(i: u8) -> String {
    format!("node-{i}")
}

pub(super) fn client_port(i: u8) -> u16 {
    8080 + i as u16
}

pub(super) fn cluster_port(i: u8) -> u16 {
    18000 + i as u16
}

#[derive(Clone, Serialize, Deserialize)]
pub struct SimScenario {
    pub seed: u64,
    pub node_count: u8,
    pub simulation_secs: u64,
    pub faults: Vec<FaultEvent>,
    pub commands: Vec<CommandEvent>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FaultEvent {
    pub at_secs: u64,
    pub kind: FaultKind,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum FaultKind {
    KillNode(KillNode),
    KillDataReplica(KillDataReplica),
    PartitionNode(PartitionNode),
    HealNode(HealNode),
    PartitionLeader(PartitionLeader),
    FlappingPartition(FlappingPartition),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct KillNode {
    pub node: u8,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct KillDataReplica {
    pub topic_name: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PartitionNode {
    pub node: u8,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct HealNode {
    pub node: u8,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PartitionLeader {
    pub topic_name: String,
    pub heal_after_secs: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FlappingPartition {
    pub topic_name: String,
    pub heal_after_secs: u64,
}

impl_from_variant!(
    FaultKind,
    KillNode,
    KillDataReplica,
    PartitionNode,
    HealNode,
    PartitionLeader,
    FlappingPartition,
);

#[derive(Clone, Serialize, Deserialize)]
pub struct CommandEvent {
    pub at_secs: u64,
    pub kind: CommandKind,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum CommandKind {
    CreateTopic { name: String },
    DeleteTopic { name: String },
}

fn pick_u8(rng: &mut StdRng, min: u8, max: u8) -> u8 {
    *(min..=max).collect::<Vec<_>>().choose(rng).unwrap()
}

fn pick_u64(rng: &mut StdRng, min: u64, max: u64) -> u64 {
    *(min..=max).collect::<Vec<_>>().choose(rng).unwrap()
}

impl SimScenario {
    pub fn from_seed(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let generated_node_count = pick_u8(&mut rng, 3, 5);
        let node_count = if seed % 5 == 1 {
            generated_node_count.max(4)
        } else {
            generated_node_count
        };
        let simulation_secs = pick_u64(&mut rng, 120, 180);

        let node = pick_u8(&mut rng, 1, node_count);
        let fault_at = pick_u64(&mut rng, 75, simulation_secs.saturating_sub(30).max(75));

        let mut commands = Vec::new();
        for i in 0..1 {
            // Establish acknowledged state before injecting a disruptive fault;
            // the post-fault oracle then tests durability rather than conflating
            // it with the separate contract of accepting new work during recovery.
            let window_end = if seed.is_multiple_of(5) {
                simulation_secs.saturating_sub(30)
            } else {
                fault_at.saturating_sub(45)
            }
            .max(9);
            let at_secs = pick_u64(&mut rng, 8, window_end);
            commands.push(CommandEvent {
                at_secs,
                kind: CommandKind::CreateTopic {
                    name: format!("topic-{seed}-{i}"),
                },
            });
        }
        commands.sort_by_key(|c| c.at_secs);

        // Cycle fault profiles by seed so every bounded campaign covers every
        // fault class. A scenario carries at most one disruptive profile; with
        // three-replica shard groups this preserves a metadata quorum and keeps
        // post-fault durability assertions meaningful.
        let faults = match seed % 5 {
            0 => Vec::new(),
            1 => vec![FaultEvent {
                at_secs: fault_at,
                kind: KillDataReplica {
                    topic_name: format!("topic-{seed}-0"),
                }
                .into(),
            }],
            2 => vec![
                FaultEvent {
                    at_secs: fault_at,
                    kind: PartitionNode { node }.into(),
                },
                FaultEvent {
                    at_secs: fault_at + 12,
                    kind: HealNode { node }.into(),
                },
            ],
            3 => vec![FaultEvent {
                at_secs: fault_at,
                kind: FlappingPartition {
                    topic_name: format!("topic-{seed}-0"),
                    heal_after_secs: 12,
                }
                .into(),
            }],
            4 => vec![FaultEvent {
                at_secs: fault_at,
                kind: PartitionLeader {
                    topic_name: format!("topic-{seed}-0"),
                    heal_after_secs: 12,
                }
                .into(),
            }],
            _ => unreachable!(),
        };

        SimScenario {
            seed,
            node_count,
            simulation_secs,
            faults,
            commands,
        }
    }

    /// Nodes reachable by the checker at the end of the scenario.
    pub fn alive_at_end(&self) -> Vec<u8> {
        let mut reachable: std::collections::BTreeSet<u8> = (1..=self.node_count).collect();
        for event in &self.faults {
            match &event.kind {
                FaultKind::KillNode(fault) => {
                    reachable.remove(&fault.node);
                }
                FaultKind::KillDataReplica(_) => {}
                FaultKind::PartitionNode(fault) => {
                    reachable.remove(&fault.node);
                }
                FaultKind::HealNode(fault) => {
                    reachable.insert(fault.node);
                }
                FaultKind::PartitionLeader(_) | FaultKind::FlappingPartition(_) => {}
            }
        }
        reachable.into_iter().collect()
    }

    pub(super) fn is_valid(&self) -> bool {
        let valid_node = |node: u8| (1..=self.node_count).contains(&node);
        let mut kill_count = 0;
        for event in &self.faults {
            if event.at_secs >= self.simulation_secs {
                return false;
            }
            match &event.kind {
                FaultKind::KillNode(fault) => {
                    kill_count += 1;
                    if !valid_node(fault.node) {
                        return false;
                    }
                }
                FaultKind::KillDataReplica(fault) => {
                    kill_count += 1;
                    let topic_exists = self.commands.iter().any(|command| {
                        matches!(
                            &command.kind,
                            CommandKind::CreateTopic { name } if name == &fault.topic_name
                        )
                    });
                    if !topic_exists || self.node_count < 4 {
                        return false;
                    }
                }
                FaultKind::PartitionNode(fault) => {
                    if !valid_node(fault.node) {
                        return false;
                    }
                }
                FaultKind::HealNode(fault) => {
                    if !valid_node(fault.node) {
                        return false;
                    }
                }
                FaultKind::FlappingPartition(fault) => {
                    let topic_exists = self.commands.iter().any(|command| {
                        matches!(
                            &command.kind,
                            CommandKind::CreateTopic { name } if name == &fault.topic_name
                        )
                    });
                    if !topic_exists
                        || event.at_secs + fault.heal_after_secs >= self.simulation_secs
                    {
                        return false;
                    }
                }
                FaultKind::PartitionLeader(fault) => {
                    let topic_exists = self.commands.iter().any(|command| {
                        matches!(
                            &command.kind,
                            CommandKind::CreateTopic { name } if name == &fault.topic_name
                        )
                    });
                    if !topic_exists
                        || event.at_secs + fault.heal_after_secs >= self.simulation_secs
                    {
                        return false;
                    }
                }
            }
        }
        kill_count <= 1
    }
}

fn partition_node(sim: &turmoil::Sim<'_>, node: u8, node_count: u8) {
    let target = node_name(node);
    for peer in 1..=node_count {
        if peer != node {
            sim.partition(target.as_str(), node_name(peer).as_str());
        }
    }
    sim.partition(target.as_str(), "checker");
}

fn heal_node(sim: &turmoil::Sim<'_>, node: u8, node_count: u8) {
    let target = node_name(node);
    for peer in 1..=node_count {
        if peer != node {
            sim.repair(target.as_str(), node_name(peer).as_str());
        }
    }
    sim.repair(target.as_str(), "checker");
}

/// Attempt a CreateTopic to one node. Returns `Some(true)` on success, `None` on
/// network/decode error or when the node is not the leader.
pub(super) async fn try_propose(
    host: &str,
    port: u16,
    req: &ClientRequest,
) -> Option<ControlPlaneResponse> {
    let stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect((host, port)))
        .await
        .ok()
        .and_then(|r| r.ok())?;
    let (read_half, write_half) = stream.into_split();
    let mut writer = ClientRawWriter::new(write_half);
    let mut reader = ClientStreamReader::new(read_half);
    writer.write(0, req).await.ok()?;
    let (_, response): (_, ClientResponse) =
        tokio::time::timeout(Duration::from_secs(5), reader.read_request())
            .await
            .ok()
            .and_then(|r| r.ok())?;
    match response {
        ClientResponse::ControlPlane(cp) => Some(cp),
        _ => None,
    }
}

async fn send_request(
    host: &str,
    port: u16,
    request: &ClientRequest,
) -> turmoil::Result<ClientResponse> {
    let stream =
        tokio::time::timeout(Duration::from_secs(2), TcpStream::connect((host, port))).await??;
    let (read_half, write_half) = stream.into_split();
    let mut writer = ClientRawWriter::new(write_half);
    let mut reader = ClientStreamReader::new(read_half);
    writer.write(0, request).await?;
    let (_, response) =
        tokio::time::timeout(Duration::from_secs(5), reader.read_request()).await??;
    Ok(response)
}

async fn describe_topic_anywhere(
    topic: &str,
    nodes: &[(String, u16)],
    budget: Duration,
) -> TopicDetail {
    let deadline = tokio::time::Instant::now() + budget;
    let request = ClientRequest::ControlPlane(ControlPlaneRequest::DescribeTopic {
        name: topic.to_string(),
    });
    loop {
        for (host, port) in nodes {
            match send_request(host, *port, &request).await {
                Ok(ClientResponse::ControlPlane(ControlPlaneResponse::TopicDetail(detail))) => {
                    return detail;
                }
                Ok(_) => {}
                Err(error) => tracing::debug!(%error, %host, "describe-topic retry failed"),
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "topic {topic:?} was not describable within {budget:?}",
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn produce_until_acked(
    topic: &str,
    range_id: RangeId,
    payload: &[u8],
    nodes: &[(String, u16)],
    budget: Duration,
) -> EntryId {
    let deadline = tokio::time::Instant::now() + budget;
    let request = ClientRequest::DataPlane(ClientDataPlaneRequest::Produce(ProduceRequest {
        topic_name: topic.to_string(),
        range_id,
        routing_key: b"campaign-key".to_vec(),
        data: payload.to_vec(),
        record_count: 1,
        producer: None,
    }));
    loop {
        for (host, port) in nodes {
            match send_request(host, *port, &request).await {
                Ok(ClientResponse::DataPlane(DataPlaneResponse::Produced { entry_id })) => {
                    return entry_id;
                }
                Ok(_) => {}
                Err(error) => tracing::debug!(%error, %host, "produce retry failed"),
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "produce to topic {topic:?} was not acknowledged within {budget:?}",
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn assert_record_readable(
    topic_id: TopicId,
    range_id: RangeId,
    entry_id: EntryId,
    expected: &[u8],
    nodes: &[(String, u16)],
    budget: Duration,
) {
    let deadline = tokio::time::Instant::now() + budget;
    let request = ClientRequest::DataPlane(ClientDataPlaneRequest::FetchById(FetchByIdRequest {
        topic_id,
        range_id,
        entry_id,
        max_bytes: 1 << 20,
    }));
    loop {
        for (host, port) in nodes {
            match send_request(host, *port, &request).await {
                Ok(ClientResponse::DataPlane(DataPlaneResponse::Fetched { entries, .. }))
                    if entries
                        .iter()
                        .any(|entry| entry.entry_id == entry_id && entry.data == expected) =>
                {
                    return;
                }
                Ok(_) => {}
                Err(error) => tracing::debug!(%error, %host, "fetch retry failed"),
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "acknowledged entry {entry_id:?} was not readable within {budget:?}",
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub(super) fn make_create_topic_req(name: &str) -> ClientRequest {
    ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
        name: name.to_string(),
        storage_policy: StoragePolicy {
            retention_ms: Some(3_600_000),
            replication_factor: 3,
            partition_strategy: PartitionStrategy::AutoSplit,
        },
    })
}

pub fn run_for_scenario(scenario: &SimScenario) -> turmoil::Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let total_secs = scenario.simulation_secs + 90 + scenario.commands.len() as u64 * 60;
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(total_secs))
        .tcp_capacity(4096)
        .rng_seed(scenario.seed)
        .build();

    let node_count = scenario.node_count;
    let scenario_seed = scenario.seed;

    for i in 1..=node_count {
        let cp = client_port(i);
        let rp = cluster_port(i);
        sim.host(node_name(i), move || async move {
            let name = node_name(i);
            let me = turmoil::lookup(name.as_str());
            let seeds: Vec<String> = (1..=node_count)
                .filter(|&j| j != i)
                .map(|j| {
                    format!(
                        "{}:{}",
                        turmoil::lookup(node_name(j).as_str()),
                        cluster_port(j)
                    )
                })
                .collect();
            let mut env = default_env(i as u32, name.clone(), cp, rp);
            env.advertise_host = Some(me.to_string());
            env.join_seed_nodes = seeds;
            env.node_id_suffix = Some(format!("sim-{scenario_seed}"));
            env.vnodes_per_node = 16;
            env.segment_size_limit_bytes = 2048;
            StartUp::with_env(env, scenario_seed).run().await?;
            Ok(())
        });
    }

    let commands = scenario.commands.clone();
    let checker_faults = scenario.faults.clone();
    let alive_at_end = scenario.alive_at_end();
    let alive_addrs: Vec<(String, u16)> = alive_at_end
        .iter()
        .map(|&i| (node_name(i), client_port(i)))
        .collect();
    let simulation_secs = scenario.simulation_secs;
    let killed_replica = Arc::new(AtomicU8::new(0));
    let checker_killed_replica = killed_replica.clone();

    sim.client("checker", async move {
        let started_at = tokio::time::Instant::now();
        tokio::time::sleep(Duration::from_secs(10)).await;

        let initial_nodes: Vec<(&str, u16)> = alive_addrs
            .iter()
            .map(|(name, port)| (name.as_str(), *port))
            .collect();
        assert_membership_converged(&initial_nodes, 60, Duration::from_millis(250)).await?;

        let mut elapsed_secs = 10u64;
        // Topic metadata, original sealed replicas, and acknowledged records.
        let mut created_topics = Vec::new();

        for cmd in &commands {
            if cmd.at_secs > elapsed_secs {
                tokio::time::sleep(Duration::from_secs(cmd.at_secs - elapsed_secs)).await;
                elapsed_secs = cmd.at_secs;
            }
            if let CommandKind::CreateTopic { name } = &cmd.kind {
                let req = make_create_topic_req(name);
                let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                let mut last_responses = Vec::new();
                'await_commit: loop {
                    last_responses.clear();
                    for node in 1..=node_count {
                        let response =
                            try_propose(&node_name(node), client_port(node), &req).await;
                        if matches!(
                            response,
                            Some(
                                ControlPlaneResponse::TopicCreated
                                    | ControlPlaneResponse::AlreadyExists
                            )
                        ) {
                            break 'await_commit;
                        }
                        last_responses.push((node, response));
                    }
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "command for topic {name:?} was not committed within 30s; last responses: {last_responses:?}",
                    );
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }

                let info_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                let info = 'await_info: loop {
                    for node in 1..=node_count {
                        if let Ok(Some(info)) =
                            query_shard_info(&node_name(node), client_port(node), name.as_bytes())
                                .await
                        {
                            break 'await_info info;
                        }
                    }
                    assert!(
                        tokio::time::Instant::now() < info_deadline,
                        "acked topic {name:?} had no resolvable shard info within 30s",
                    );
                    tokio::time::sleep(Duration::from_millis(250)).await;
                };

                let (member_addrs, quorum) = {
                    let original_members = info.member_node_ids.len();
                    let surviving: Vec<(String, u16)> = alive_addrs
                        .iter()
                        .filter(|(n, _)| {
                            info.member_node_ids
                                .iter()
                                .any(|m| m.starts_with(n.as_str()))
                        })
                        .cloned()
                        .collect();
                    (surviving, original_members / 2 + 1)
                };

                let mut detail = describe_topic_anywhere(name, &alive_addrs, Duration::from_secs(30))
                    .await;
                let range_id = detail
                    .ranges
                    .iter()
                    .find(|range| range.active_segment.is_some())
                    .map(|range| range.range_id)
                    .expect("new topic must have an active range");
                let mut records = Vec::new();
                for record_index in 0..3u8 {
                    let mut payload = format!("seed-{scenario_seed}-record-{record_index}")
                        .into_bytes();
                    payload.resize(1024, b'-');
                    let entry_id = produce_until_acked(
                        name,
                        range_id,
                        &payload,
                        &alive_addrs,
                        Duration::from_secs(30),
                    )
                    .await;
                    records.push((entry_id, payload));
                }

                let roll_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                loop {
                    detail =
                        describe_topic_anywhere(name, &alive_addrs, Duration::from_secs(5)).await;
                    let rolled = detail.ranges.iter().find_map(|range| {
                        let active = range.active_segment.as_ref()?;
                        let sealed = range.sealed_segments.last()?;
                        sealed
                            .end_entry_id
                            .is_some_and(|end| *end + 1 == *active.start_entry_id)
                            .then_some(())
                    });
                    if rolled.is_some() {
                        break;
                    }
                    assert!(
                        tokio::time::Instant::now() < roll_deadline,
                        "topic {name:?} did not expose a continuous sealed-to-active roll",
                    );
                    tokio::time::sleep(Duration::from_millis(250)).await;
                };

                created_topics.push((
                    name.clone(),
                    member_addrs,
                    quorum,
                    detail.topic_id,
                    range_id,
                    records,
                ));
            }
        }

        if let Some((fault_at, topic_name)) = checker_faults.iter().find_map(|fault| {
            if let FaultKind::FlappingPartition(profile) = &fault.kind {
                Some((fault.at_secs, profile.topic_name.as_str()))
            } else {
                None
            }
        }) {
            tokio::time::sleep_until(started_at + Duration::from_secs(fault_at + 1)).await;
            let (_, _, _, _, range_id, records) = created_topics
                .iter_mut()
                .find(|(topic, ..)| topic == topic_name)
                .expect("flapping profile topic must be created");
            let mut payload = format!("seed-{scenario_seed}-during-flap").into_bytes();
            payload.resize(1024, b'!');
            let entry_id = produce_until_acked(
                topic_name,
                *range_id,
                &payload,
                &alive_addrs,
                Duration::from_secs(40),
            )
            .await;
            records.push((entry_id, payload));
        }

        // Wait until simulation_secs + 10 before checking invariants.
        // Faults fire at most at simulation_secs - 20, so this gives at least 30s for SWIM
        // convergence and any connect backoffs (max 10s) to fully expire.
        let check_at = simulation_secs + 10;
        if check_at > elapsed_secs {
            tokio::time::sleep(Duration::from_secs(check_at - elapsed_secs)).await;
        }

        let killed_node = checker_killed_replica.load(Ordering::SeqCst);
        let reachable_addrs: Vec<(String, u16)> = alive_addrs
            .iter()
            .filter(|(name, _)| killed_node == 0 || name != &node_name(killed_node))
            .cloned()
            .collect();
        let node_refs: Vec<(&str, u16)> = reachable_addrs
            .iter()
            .map(|(n, p)| (n.as_str(), *p))
            .collect();

        if node_refs.len() >= 2 {
            assert_membership_converged(&node_refs, 60, Duration::from_secs(1)).await?;
        }
        if killed_node != 0 {
            for (host, port) in &reachable_addrs {
                assert!(
                    check_dead_or_not_exist(host, *port, &node_name(killed_node)).await,
                    "{host} did not classify node-{killed_node} as dead",
                );
            }
        }
        for (topic, member_addrs, quorum, topic_id, range_id, records) in &created_topics {
            let surviving_members: Vec<(String, u16)> = member_addrs
                .iter()
                .filter(|member| reachable_addrs.contains(member))
                .cloned()
                .collect();
            if surviving_members.len() < *quorum {
                continue;
            }
            let refs: Vec<(&str, u16)> = surviving_members
                .iter()
                .map(|(n, p)| (n.as_str(), *p))
                .collect();
            assert_topic_visible_on_nodes(&refs, topic, *quorum, 20, Duration::from_secs(1))
                .await?;
            for (entry_id, payload) in records {
                assert_record_readable(
                    *topic_id,
                    *range_id,
                    *entry_id,
                    payload,
                    &reachable_addrs,
                    Duration::from_secs(30),
                )
                .await;
            }

        }
        Ok(())
    });

    let leader_targets: Vec<(usize, Arc<AtomicU8>)> = scenario
        .faults
        .iter()
        .enumerate()
        .filter_map(|(index, fault)| {
            let FaultKind::PartitionLeader(target) = &fault.kind else {
                return None;
            };
            let selected = Arc::new(AtomicU8::new(0));
            let selected_tx = selected.clone();
            let topic_name = target.topic_name.clone();
            let at_secs = fault.at_secs;
            sim.client(format!("leader-resolver-{index}"), async move {
                tokio::time::sleep(Duration::from_secs(at_secs)).await;
                let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
                while tokio::time::Instant::now() < deadline {
                    for node in 1..=node_count {
                        let Some(info) = query_shard_info(
                            &node_name(node),
                            client_port(node),
                            topic_name.as_bytes(),
                        )
                        .await
                        .ok()
                        .flatten() else {
                            continue;
                        };
                        let Some(leader) = query_shard_leader(
                            &node_name(node),
                            client_port(node),
                            info.shard_group_id,
                        )
                        .await
                        .ok()
                        .flatten() else {
                            continue;
                        };
                        if let Some(leader_node) = (1..=node_count)
                            .find(|candidate| leader.starts_with(node_name(*candidate).as_str()))
                        {
                            selected_tx.store(leader_node, Ordering::SeqCst);
                            return Ok(());
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                panic!("could not resolve leader for topic {topic_name:?}");
            });
            Some((index, selected))
        })
        .collect();

    let flapping_targets: Vec<(usize, Arc<AtomicU8>)> = scenario
        .faults
        .iter()
        .enumerate()
        .filter_map(|(index, fault)| {
            let FaultKind::FlappingPartition(profile) = &fault.kind else {
                return None;
            };
            let selected = Arc::new(AtomicU8::new(0));
            let selected_tx = selected.clone();
            let topic_name = profile.topic_name.clone();
            let at_secs = fault.at_secs;
            let nodes: Vec<(String, u16)> = (1..=node_count)
                .map(|node| (node_name(node), client_port(node)))
                .collect();
            sim.client(format!("replica-resolver-{index}"), async move {
                tokio::time::sleep(Duration::from_secs(at_secs)).await;
                let detail =
                    describe_topic_anywhere(&topic_name, &nodes, Duration::from_secs(20)).await;
                let replica = detail
                    .ranges
                    .iter()
                    .find_map(|range| range.active_segment.as_ref())
                    .and_then(|segment| segment.replica_set.first())
                    .expect("active segment must have a data replica");
                let target = (1..=node_count)
                    .find(|candidate| replica.node_id.starts_with(node_name(*candidate).as_str()))
                    .expect("data replica must map to a simulation node");
                selected_tx.store(target, Ordering::SeqCst);
                Ok(())
            });
            Some((index, selected))
        })
        .collect();

    for (index, fault) in scenario.faults.iter().enumerate() {
        let FaultKind::KillDataReplica(profile) = &fault.kind else {
            continue;
        };
        let selected_tx = killed_replica.clone();
        let topic_name = profile.topic_name.clone();
        let at_secs = fault.at_secs;
        let nodes: Vec<(String, u16)> = (1..=node_count)
            .map(|node| (node_name(node), client_port(node)))
            .collect();
        sim.client(format!("kill-replica-resolver-{index}"), async move {
            tokio::time::sleep(Duration::from_secs(at_secs)).await;
            let detail =
                describe_topic_anywhere(&topic_name, &nodes, Duration::from_secs(20)).await;
            let replicas = detail
                .ranges
                .iter()
                .flat_map(|range| range.sealed_segments.iter())
                .next()
                .map(|segment| &segment.replica_set)
                .expect("sealed segment must have data replicas");
            let mut metadata_leader = None;
            let mut metadata_group = None;
            for (host, port) in &nodes {
                let Some(info) = query_shard_info(host, *port, topic_name.as_bytes())
                    .await
                    .ok()
                    .flatten()
                else {
                    continue;
                };
                metadata_group = Some(info.shard_group_id);
                metadata_leader = query_shard_leader(host, *port, info.shard_group_id)
                    .await
                    .ok()
                    .flatten();
                if metadata_leader.is_some() {
                    break;
                }
            }
            let replica = replicas
                .iter()
                .find(|replica| {
                    metadata_leader
                        .as_ref()
                        .is_none_or(|leader| !replica.node_id.starts_with(leader))
                })
                .or_else(|| replicas.first())
                .expect("sealed segment must have a selectable data replica");
            let target = (1..=node_count)
                .find(|candidate| replica.node_id.starts_with(node_name(*candidate).as_str()))
                .expect("data replica must map to a simulation node");
            println!(
                "replica fault target: topic={topic_name} group={metadata_group:?} metadata_leader={metadata_leader:?} data_replica={} host=node-{target}",
                replica.node_id,
            );
            selected_tx.store(target, Ordering::SeqCst);
            Ok(())
        });
    }

    for (index, fault) in scenario.faults.iter().enumerate() {
        let target = Duration::from_secs(fault.at_secs);
        while sim.elapsed() < target {
            sim.step()?;
        }
        match &fault.kind {
            FaultKind::KillNode(fault) => sim.crash(node_name(fault.node).as_str()),
            FaultKind::KillDataReplica(_) => {
                let deadline = sim.elapsed() + Duration::from_secs(20);
                while killed_replica.load(Ordering::SeqCst) == 0 && sim.elapsed() < deadline {
                    sim.step()?;
                }
                let node = killed_replica.load(Ordering::SeqCst);
                assert_ne!(node, 0, "data-replica resolver did not publish a target");
                sim.crash(node_name(node).as_str());
            }
            FaultKind::PartitionNode(fault) => partition_node(&sim, fault.node, node_count),
            FaultKind::HealNode(fault) => heal_node(&sim, fault.node, node_count),
            FaultKind::FlappingPartition(fault) => {
                let selected = flapping_targets
                    .iter()
                    .find_map(|(target_index, selected)| {
                        (*target_index == index).then_some(selected)
                    })
                    .expect("flapping fault must have a replica resolver");
                let deadline = sim.elapsed() + Duration::from_secs(20);
                while selected.load(Ordering::SeqCst) == 0 && sim.elapsed() < deadline {
                    sim.step()?;
                }
                let node = selected.load(Ordering::SeqCst);
                assert_ne!(node, 0, "replica resolver did not publish a target");
                partition_node(&sim, node, node_count);
                let heal_at = sim.elapsed() + Duration::from_secs(fault.heal_after_secs);
                while sim.elapsed() < heal_at {
                    sim.step()?;
                }
                heal_node(&sim, node, node_count);
            }
            FaultKind::PartitionLeader(fault) => {
                let selected = leader_targets
                    .iter()
                    .find_map(|(target_index, selected)| {
                        (*target_index == index).then_some(selected)
                    })
                    .expect("leader fault must have a resolver");
                let deadline = sim.elapsed() + Duration::from_secs(20);
                while selected.load(Ordering::SeqCst) == 0 && sim.elapsed() < deadline {
                    sim.step()?;
                }
                let node = selected.load(Ordering::SeqCst);
                assert_ne!(node, 0, "leader resolver did not publish a target");
                partition_node(&sim, node, node_count);
                let heal_at = sim.elapsed() + Duration::from_secs(fault.heal_after_secs);
                while sim.elapsed() < heal_at {
                    sim.step()?;
                }
                heal_node(&sim, node, node_count);
            }
        }
    }

    sim.run()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::it::sim::bugbase::{record_failure, shrink_scenario};

    #[test]
    #[serial_test::serial]
    fn sim_campaign() {
        let seeds: Box<dyn Iterator<Item = u64>> = if let Some(seed) = std::env::var("EG_SIM_SEED")
            .ok()
            .and_then(|value| value.parse().ok())
        {
            Box::new(std::iter::once(seed))
        } else {
            let seed_count = std::env::var("EG_SIM_SEED_COUNT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10);
            Box::new(0..seed_count)
        };
        for seed in seeds {
            let scenario = SimScenario::from_seed(seed);
            println!(
                "simulation scenario: {}",
                serde_json::to_string(&scenario).expect("scenario serialization failed")
            );
            if let Err(e) = run_for_scenario(&scenario) {
                let shrunk = shrink_scenario(&scenario);
                let path = record_failure(&shrunk).expect("failed to write bugbase entry");
                panic!("seed={seed} failed: {e}\nshrunk scenario saved to {path:?}");
            }
        }
    }

    #[test]
    fn bounded_campaign_covers_every_fault_profile() {
        let scenarios: Vec<SimScenario> = (0..5).map(SimScenario::from_seed).collect();
        assert!(scenarios[0].faults.is_empty());
        assert!(matches!(
            scenarios[1].faults[0].kind,
            FaultKind::KillDataReplica(_)
        ));
        assert!(matches!(
            scenarios[2].faults.as_slice(),
            [
                FaultEvent {
                    kind: FaultKind::PartitionNode(_),
                    ..
                },
                FaultEvent {
                    kind: FaultKind::HealNode(_),
                    ..
                }
            ]
        ));
        assert!(matches!(
            scenarios[3].faults[0].kind,
            FaultKind::FlappingPartition(_)
        ));
        assert!(matches!(
            scenarios[4].faults[0].kind,
            FaultKind::PartitionLeader(_)
        ));
    }
}
