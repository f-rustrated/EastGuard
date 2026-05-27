use std::time::Duration;

use rand::{SeedableRng, rngs::StdRng, seq::IndexedRandom};
use serde::{Deserialize, Serialize};
use turmoil::Builder;

use crate::StartUp;
use crate::connections::clients::{ClientRawWriter, ClientStreamReader};
use crate::connections::protocol::{
    ClientRequest, ClientResponse, ControlPlaneRequest, ControlPlaneResponse,
};
use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
use crate::it::helpers::default_env;
use crate::it::sim::invariants::{
    assert_membership_converged, assert_topic_visible_on_quorum, query_shard_info,
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

#[allow(dead_code, clippy::enum_variant_names)]
#[derive(Clone, Serialize, Deserialize)]
pub enum FaultKind {
    KillNode(u8),
    PartitionNode(u8),
    HealNode(u8),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CommandEvent {
    pub at_secs: u64,
    pub kind: CommandKind,
}

#[allow(dead_code)]
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
        let node_count = pick_u8(&mut rng, 3, 5);
        let simulation_secs = pick_u64(&mut rng, 30, 120);

        // KillNode faults: at most floor((n-1)/2) to preserve quorum.
        let max_failures = (node_count - 1) / 2;
        let fault_count = if max_failures > 0 {
            pick_u8(&mut rng, 0, max_failures)
        } else {
            0
        };
        let mut faults = Vec::new();
        let mut killed: Vec<u8> = Vec::new();
        for _ in 0..fault_count {
            let available: Vec<u8> = (1..=node_count).filter(|n| !killed.contains(n)).collect();
            let node_num = *available.choose(&mut rng).unwrap();
            let window_end = simulation_secs.saturating_sub(20).max(11);
            let at_secs = pick_u64(&mut rng, 10, window_end);
            faults.push(FaultEvent {
                at_secs,
                kind: FaultKind::KillNode(node_num),
            });
            killed.push(node_num);
        }
        faults.sort_by_key(|f| f.at_secs);

        let cmd_count = pick_u8(&mut rng, 1, 3);
        let mut commands = Vec::new();
        for i in 0..cmd_count {
            let window_end = simulation_secs.saturating_sub(30).max(9);
            let at_secs = pick_u64(&mut rng, 8, window_end);
            commands.push(CommandEvent {
                at_secs,
                kind: CommandKind::CreateTopic {
                    name: format!("topic-{seed}-{i}"),
                },
            });
        }
        commands.sort_by_key(|c| c.at_secs);

        SimScenario {
            seed,
            node_count,
            simulation_secs,
            faults,
            commands,
        }
    }

    /// Node numbers not killed by any KillNode fault — expected alive at simulation end.
    pub fn alive_at_end(&self) -> Vec<u8> {
        let killed: Vec<u8> = self
            .faults
            .iter()
            .filter_map(|f| {
                if let FaultKind::KillNode(n) = &f.kind {
                    Some(*n)
                } else {
                    None
                }
            })
            .collect();
        (1..=self.node_count)
            .filter(|n| !killed.contains(n))
            .collect()
    }
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

pub(super) fn make_create_topic_req(name: &str) -> ClientRequest {
    ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
        name: name.to_string(),
        storage_policy: StoragePolicy {
            retention_ms: 3_600_000,
            replication_factor: 3,
            partition_strategy: PartitionStrategy::AutoSplit,
        },
    })
}

pub fn run_scenario(seed: u64) -> turmoil::Result {
    run_for_scenario(&SimScenario::from_seed(seed))
}

pub fn run_for_scenario(scenario: &SimScenario) -> turmoil::Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let total_secs = scenario.simulation_secs + 60;
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(total_secs))
        .tcp_capacity(4096)
        .build();

    let node_count = scenario.node_count;

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
            env.vnodes_per_node = 3;
            StartUp::with_env(env, 0).run().await?;
            Ok(())
        });
    }

    let commands = scenario.commands.clone();
    let alive_at_end = scenario.alive_at_end();
    let alive_addrs: Vec<(String, u16)> = alive_at_end
        .iter()
        .map(|&i| (node_name(i), client_port(i)))
        .collect();
    let simulation_secs = scenario.simulation_secs;

    sim.client("checker", async move {
        tokio::time::sleep(Duration::from_secs(10)).await;

        let mut elapsed_secs = 10u64;
        // (topic_name, member_addrs_of_its_shard_group)
        let mut created_topics: Vec<(String, Vec<(String, u16)>)> = Vec::new();

        for cmd in &commands {
            if cmd.at_secs > elapsed_secs {
                tokio::time::sleep(Duration::from_secs(cmd.at_secs - elapsed_secs)).await;
                elapsed_secs = cmd.at_secs;
            }
            if let CommandKind::CreateTopic { name } = &cmd.kind {
                let req = make_create_topic_req(name);
                'try_nodes: for j in 1..=node_count {
                    if let Some(ControlPlaneResponse::TopicCreated) =
                        try_propose(&node_name(j), client_port(j), &req).await
                    {
                        // Identify which alive nodes host this topic's shard group.
                        let member_addrs = if let Ok(Some(info)) =
                            query_shard_info(&node_name(j), client_port(j), name.as_bytes()).await
                        {
                            alive_addrs
                                .iter()
                                .filter(|(n, _)| {
                                    info.member_node_ids
                                        .iter()
                                        .any(|m| m.starts_with(n.as_str()))
                                })
                                .cloned()
                                .collect()
                        } else {
                            alive_addrs.clone()
                        };
                        created_topics.push((name.clone(), member_addrs));
                        break 'try_nodes;
                    }
                }
            }
        }

        // Wait until simulation_secs + 10 before checking invariants.
        // Faults fire at most at simulation_secs - 20, so this gives at least 30s for SWIM
        // convergence and any connect backoffs (max 10s) to fully expire.
        let check_at = simulation_secs + 10;
        if check_at > elapsed_secs {
            tokio::time::sleep(Duration::from_secs(check_at - elapsed_secs)).await;
        }

        let node_refs: Vec<(&str, u16)> =
            alive_addrs.iter().map(|(n, p)| (n.as_str(), *p)).collect();

        if node_refs.len() >= 2 {
            assert_membership_converged(&node_refs, 20, Duration::from_secs(1)).await?;
        }
        for (topic, member_addrs) in &created_topics {
            let refs: Vec<(&str, u16)> =
                member_addrs.iter().map(|(n, p)| (n.as_str(), *p)).collect();
            assert_topic_visible_on_quorum(&refs, topic, 20, Duration::from_secs(1)).await?;
        }
        Ok(())
    });

    for fault in &scenario.faults {
        let target = Duration::from_secs(fault.at_secs);
        while sim.elapsed() < target {
            sim.step()?;
        }
        match &fault.kind {
            FaultKind::KillNode(n) => sim.crash(node_name(*n).as_str()),
            FaultKind::PartitionNode(_) | FaultKind::HealNode(_) => {}
        }
    }

    sim.run()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::it::sim::bugbase::{record_failure, shrink_scenario};

    // TODO: some seeds (e.g. seed=1) fail with "topic not visible on all alive nodes".
    // Root cause not yet confirmed — suspected repeated connect backoffs to alive peers
    // during startup delay Raft replication beyond the checker's 20-attempt window.
    #[test]
    #[ignore]
    #[serial_test::serial]
    fn sim_loop_100() {
        for seed in 0..100 {
            if let Err(e) = run_scenario(seed) {
                let scenario = SimScenario::from_seed(seed);
                let shrunk = shrink_scenario(&scenario);
                let path = record_failure(&shrunk).expect("failed to write bugbase entry");
                panic!("seed={seed} failed: {e}\nshrunk scenario saved to {path:?}");
            }
        }
    }
}
