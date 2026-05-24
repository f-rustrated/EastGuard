use std::time::Duration;

use turmoil::Builder;

use crate::StartUp;
use crate::it::helpers::{check_alive_count, check_dead_or_not_exist, default_env};
use crate::it::sim::invariants::{
    assert_membership_converged, assert_single_leader, assert_topic_visible_on_quorum,
    query_shard_info, query_shard_leader,
};
use crate::it::sim::scenario::{
    client_port, cluster_port, make_create_topic_req, node_name, try_propose,
};

fn three_node_sim(simulation_secs: u64) -> turmoil::Sim<'static> {
    Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(simulation_secs))
        .tcp_capacity(4096)
        .build()
}

fn spawn_node(sim: &mut turmoil::Sim<'static>, i: u8, total: u8) {
    let cp = client_port(i);
    let rp = cluster_port(i);
    sim.host(node_name(i), move || async move {
        let name = node_name(i);
        let me = turmoil::lookup(name.as_str());
        let seeds: Vec<String> = (1..=total)
            .filter(|&j| j != i)
            .map(|j| format!("{}:{}", turmoil::lookup(node_name(j).as_str()), cluster_port(j)))
            .collect();
        let mut env = default_env(i as u32, name.clone(), cp, rp);
        env.advertise_host = Some(me.to_string());
        env.join_seed_nodes = seeds;
        env.vnodes_per_node = 3;
        StartUp::with_env(env, 0).run().await?;
        Ok(())
    });
}

/// After `CreateTopic` is acked, all alive nodes must eventually expose the topic via `GetTopics`.
#[test]
#[serial_test::serial]
fn metadata_visible() -> turmoil::Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let mut sim = three_node_sim(90);
    for i in 1..=3u8 {
        spawn_node(&mut sim, i, 3);
    }

    sim.client("checker", async {
        // Retry until a leader is elected and the propose is acked (up to 30s).
        let req = make_create_topic_req("visible-test");
        let mut acked = false;
        for _ in 0..30u32 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            for i in 1..=3u8 {
                if let Some(crate::connections::request::ProposeResponse::Success) =
                    try_propose(&node_name(i), client_port(i), &req).await
                {
                    acked = true;
                    break;
                }
            }
            if acked {
                break;
            }
        }
        assert!(acked, "CreateTopic was not acked by any node within 30s");

        let nodes: &[(&str, u16)] =
            &[("node-1", 8081), ("node-2", 8082), ("node-3", 8083)];
        assert_topic_visible_on_quorum(nodes, "visible-test", 60, Duration::from_millis(100)).await?;
        Ok(())
    });

    sim.run()
}

/// After `CreateTopic` is acked, a killed leader is replaced by a new one within 30s.
/// The two surviving nodes must agree on the same leader (no split-brain).
#[test]
#[serial_test::serial]
fn leader_elects_after_kill() -> turmoil::Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let mut sim = three_node_sim(120);
    for i in 1..=3u8 {
        spawn_node(&mut sim, i, 3);
    }

    sim.client("checker", async {
        // Wait for leader election then create a topic (up to 30s).
        let req = make_create_topic_req("leader-kill-test");
        let mut shard_group_id: Option<u64> = None;
        for _ in 0..30u32 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            for i in 1..=3u8 {
                if let Some(crate::connections::request::ProposeResponse::Success) =
                    try_propose(&node_name(i), client_port(i), &req).await
                {
                    break;
                }
            }
            // Learn the shard group id once the topology is populated.
            if let Ok(Some(info)) = query_shard_info("node-2", 8082, b"leader-kill-test").await {
                shard_group_id = Some(info.shard_group_id);
                break;
            }
        }
        let shard_group_id = shard_group_id.expect("topology not populated within 30s");

        // node-1 is crashed at t≈20s from the test thread.
        // Sleep until we are safely past the crash, then assert re-election.
        tokio::time::sleep(Duration::from_secs(10)).await; // ensures we are past t=20s

        // Retry until at least one surviving node reports a leader that isn't node-1.
        let mut elected = false;
        for _ in 0..300u32 {
            let l2 = query_shard_leader("node-2", 8082, shard_group_id).await?;
            let l3 = query_shard_leader("node-3", 8083, shard_group_id).await?;
            let any_new = [&l2, &l3].into_iter().flatten().any(|l| !l.starts_with("node-1"));
            if any_new {
                elected = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(elected, "no new leader elected within 30s after killing node-1");

        let survivors: &[(&str, u16)] = &[("node-2", 8082), ("node-3", 8083)];
        assert_single_leader(survivors, shard_group_id, Duration::ZERO).await?;
        Ok(())
    });

    // Crash node-1 at t≈20s.
    while sim.elapsed() < Duration::from_secs(20) {
        sim.step()?;
    }
    sim.crash("node-1");

    sim.run()
}

/// After crashing then restarting a node, all surviving nodes eventually see it as `Alive`.
#[test]
#[serial_test::serial]
fn membership_converges_after_rejoin() -> turmoil::Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    let mut sim = three_node_sim(120);
    for i in 1..=3u8 {
        spawn_node(&mut sim, i, 3);
    }

    sim.client("checker", async {
        tokio::time::sleep(Duration::from_secs(10)).await;
        check_alive_count("node-2", 8082, 3).await?;
        check_alive_count("node-3", 8083, 3).await?;

        // Wait for crash + death detection (crash at t≈15s, detectable within ~20s).
        tokio::time::sleep(Duration::from_secs(25)).await; // now at t≈35s
        assert!(
            check_dead_or_not_exist("node-2", 8082, "node-1").await,
            "node-2 should see node-1 as Dead"
        );
        assert!(
            check_dead_or_not_exist("node-3", 8083, "node-1").await,
            "node-3 should see node-1 as Dead"
        );

        // node-1 is bounced at t≈40s. Wait for rejoin + membership convergence.
        tokio::time::sleep(Duration::from_secs(30)).await; // now at t≈65s

        let all_nodes: &[(&str, u16)] =
            &[("node-1", 8081), ("node-2", 8082), ("node-3", 8083)];
        assert_membership_converged(all_nodes, 30, Duration::from_secs(1)).await?;
        Ok(())
    });

    while sim.elapsed() < Duration::from_secs(15) {
        sim.step()?;
    }
    sim.crash("node-1");

    while sim.elapsed() < Duration::from_secs(40) {
        sim.step()?;
    }
    sim.bounce("node-1");

    sim.run()
}
