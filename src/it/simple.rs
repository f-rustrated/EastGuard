/// Make sure to run the test using RUST_LOG=debug cargo test -- --nocapture
use crate::StartUp;
use crate::clusters::{SwimNode, SwimNodeState};
use crate::config::Environment;
use crate::connections::clients::{ClientStreamReader, ClientStreamWriter};
use crate::connections::request::ConnectionRequests;
use crate::connections::request::QueryCommand::GetMembers;
use crate::net::TcpStream;
use std::time::Duration;
use turmoil::Builder;

fn default_env(idx: u32, node_id: String, port: u16, cluster_port: u16) -> Environment {
    Environment {
        config_dir: format!("./eastguard/config/{}", idx).into(),
        data_dir: format!("./eastguard/data/{}", idx).into(),
        node_id_prefix: Some(node_id),
        port,
        cluster_port,
        host: "0.0.0.0".into(),
        advertise_host: None,
        vnodes_per_node: 256,
        join_seed_nodes: vec![],
        join_initial_delay_ms: 1000,
        join_interval_ms: 1000,
        join_multiplier: 2,
        join_max_attempts: 5,
    }
}

async fn get_members(host: &str, port: u16) -> turmoil::Result<Vec<SwimNode>> {
    let stream = TcpStream::connect((host, port)).await?;
    let (read_half, write_half) = stream.into_split();
    let mut writer = ClientStreamWriter::new(write_half);
    let mut reader = ClientStreamReader::new(read_half);
    writer.write(&ConnectionRequests::Query(GetMembers)).await?;
    Ok(reader.read_request().await?)
}

async fn check_alive_count(host: &str, port: u16, expected: usize) -> turmoil::Result {
    let members = get_members(host, port).await?;
    tracing::info!("INSPECTING host: {}", host);
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

/// Returns `true` if the node whose id starts with `target` is Dead or absent
/// from the member list reported by `host`. Returns `false` on connection error
/// or if the node is still Alive/Suspect.
async fn check_dead_or_not_exist(host: &str, port: u16, target: &str) -> bool {
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
            tracing::info!("[TEST] {host} sees '{target}' as Dead (incarnation={})", n.incarnation);
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

async fn check_node_is_all_alive(host: &str, port: u16) -> turmoil::Result {
    check_alive_count(host, port, 3).await
}

#[test]
fn cluster_setup() -> turmoil::Result {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(30))
        .build();

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    sim.host("node-1", || async {
        let me = turmoil::lookup("node-1");
        let n2 = turmoil::lookup("node-2");
        let n3 = turmoil::lookup("node-3");

        let mut env = default_env(1, "node-1".to_string(), 8081, 18001);
        env.advertise_host = Some(me.to_string());
        env.join_seed_nodes = vec![format!("{n2}:18002"), format!("{n3}:18003")];
        StartUp::with_env(env, 0).run().await?;
        tracing::info!("NODE-1 is running");
        Ok(())
    });

    sim.host("node-2", || async {
        let me = turmoil::lookup("node-2");
        let n1 = turmoil::lookup("node-1");
        let n3 = turmoil::lookup("node-3");

        let mut env = default_env(2, "node-2".to_string(), 8082, 18002);
        env.advertise_host = Some(me.to_string());
        env.join_seed_nodes = vec![format!("{n1}:18001"), format!("{n3}:18003")];
        StartUp::with_env(env, 0).run().await?;
        tracing::info!("NODE-2 is running");
        Ok(())
    });

    sim.host("node-3", || async {
        let me = turmoil::lookup("node-3");
        let n1 = turmoil::lookup("node-1");
        let n2 = turmoil::lookup("node-2");

        let mut env = default_env(3, "node-3".to_string(), 8083, 18003);
        env.advertise_host = Some(me.to_string());
        env.join_seed_nodes = vec![format!("{n1}:18001"), format!("{n2}:18002")];
        StartUp::with_env(env, 0).run().await?;
        tracing::info!("NODE-3 is running");
        Ok(())
    });

    sim.client("checker", async {
        // wait for cluster to form
        tokio::time::sleep(Duration::from_secs(10)).await;

        check_node_is_all_alive("node-1", 8081).await?;
        check_node_is_all_alive("node-2", 8082).await?;
        check_node_is_all_alive("node-3", 8083).await?;

        Ok(())
    });

    sim.run()
}

/// node-1 and node-3 are partitioned from each other.
/// They must learn about each other exclusively through node-2's gossip.
#[test]
fn partition_gossip() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(120))
        .build();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    sim.host("node-1", || async {
        let me = turmoil::lookup("node-1");
        let n2 = turmoil::lookup("node-2");
        let n3 = turmoil::lookup("node-3");
        let mut env = default_env(1, "node-1".to_string(), 8081, 18001);
        env.advertise_host = Some(me.to_string());
        env.join_seed_nodes = vec![format!("{n2}:18002"), format!("{n3}:18003")];
        StartUp::with_env(env, 0).run().await?;
        Ok(())
    });

    sim.host("node-2", || async {
        let me = turmoil::lookup("node-2");
        let n1 = turmoil::lookup("node-1");
        let n3 = turmoil::lookup("node-3");
        let mut env = default_env(2, "node-2".to_string(), 8082, 18002);
        env.advertise_host = Some(me.to_string());
        env.join_seed_nodes = vec![format!("{n1}:18001"), format!("{n3}:18003")];
        StartUp::with_env(env, 0).run().await?;
        Ok(())
    });

    sim.host("node-3", || async {
        let me = turmoil::lookup("node-3");
        let n1 = turmoil::lookup("node-1");
        let n2 = turmoil::lookup("node-2");
        let mut env = default_env(3, "node-3".to_string(), 8083, 18003);
        env.advertise_host = Some(me.to_string());
        env.join_seed_nodes = vec![format!("{n1}:18001"), format!("{n2}:18002")];
        StartUp::with_env(env, 0).run().await?;
        Ok(())
    });

    sim.client("checker", async {
        // wait for nodes to form cluster
        tracing::info!("[TEST] WAIT FOR NODES TO FORM CLUSTER");
        tokio::time::sleep(Duration::from_secs(30)).await;

        // block all direct traffic between node-1 and node-3
        tracing::info!("[TEST] BLOCK ALL TRAFFIC BETWEEN node-1 and  node-3 !!");
        turmoil::partition("node-1", "node-3");

        // wait for gossip through node-2 to propagate membership
        tokio::time::sleep(Duration::from_secs(60)).await;
        tracing::info!("[TEST] WAKEUP!!");

        check_node_is_all_alive("node-1", 8081).await?;
        check_node_is_all_alive("node-2", 8082).await?;
        check_node_is_all_alive("node-3", 8083).await?;

        Ok(())
    });

    sim.run()
}

/// node-3 process is killed and restarted.
#[test]
fn dead_node_rejoin_after_process_restart() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(120))
        .build();

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    sim.host("node-1", || async {
        let me = turmoil::lookup("node-1");
        let n2 = turmoil::lookup("node-2");
        let n3 = turmoil::lookup("node-3");
        let mut env = default_env(1, "node-1".to_string(), 8081, 18001);
        env.advertise_host = Some(me.to_string());
        env.join_seed_nodes = vec![format!("{n2}:18002"), format!("{n3}:18003")];
        StartUp::with_env(env, 0).run().await?;
        Ok(())
    });

    sim.host("node-2", || async {
        let me = turmoil::lookup("node-2");
        let n1 = turmoil::lookup("node-1");
        let n3 = turmoil::lookup("node-3");
        let mut env = default_env(2, "node-2".to_string(), 8082, 18002);
        env.advertise_host = Some(me.to_string());
        env.join_seed_nodes = vec![format!("{n1}:18001"), format!("{n3}:18003")];
        StartUp::with_env(env, 0).run().await?;
        Ok(())
    });

    sim.host("node-3", || async {
        let me = turmoil::lookup("node-3");
        let n1 = turmoil::lookup("node-1");
        let n2 = turmoil::lookup("node-2");
        let mut env = default_env(3, "node-3".to_string(), 8083, 18003);
        env.advertise_host = Some(me.to_string());
        env.join_seed_nodes = vec![format!("{n1}:18001"), format!("{n2}:18002")];
        StartUp::with_env(env, 0).run().await?;
        Ok(())
    });

    sim.client("checker", async {
        // Phase 1: Wait for the cluster to form, then verify all 3 nodes are alive.
        tracing::info!("[TEST] Phase 1: waiting 10s for initial cluster formation...");
        tokio::time::sleep(Duration::from_secs(10)).await;
        tracing::info!("[TEST] Phase 1: verifying all 3 nodes are alive before crash");
        check_node_is_all_alive("node-1", 8081).await?;
        check_node_is_all_alive("node-2", 8082).await?;
        check_node_is_all_alive("node-3", 8083).await?;
        tracing::info!("[TEST] Phase 1: OK — all 3 nodes alive");

        // Phase 2: node-3 is crashed at ~15s sim time.
        // Wait 20s for the SWIM failure detector to mark it Dead
        // (suspect window ~5s + probe round-trips → Dead within ~10-15s of the crash).
        tracing::info!("[TEST] Phase 2: waiting 20s for SWIM to detect node-3 as dead...");
        tokio::time::sleep(Duration::from_secs(20)).await; // ~30s total sim time
        tracing::info!("[TEST] Phase 2: verifying node-3 is dead or absent on all surviving nodes");
        assert!(
            check_dead_or_not_exist("node-1", 8081, "node-3").await,
            "node-1 should see node-3 as Dead or absent"
        );
        assert!(
            check_dead_or_not_exist("node-2", 8082, "node-3").await,
            "node-2 should see node-3 as Dead or absent"
        );
        tracing::info!("[TEST] Phase 2: OK — node-3 confirmed dead or removed on all surviving nodes");

        // Phase 3: node-3 is restarted at ~40s sim time with a fresh UUID.
        // Wait 50s for its join pings to reach the cluster and gossip to converge.
        tracing::info!("[TEST] Phase 3: waiting 50s for node-3 to rejoin the cluster...");
        tokio::time::sleep(Duration::from_secs(50)).await; // ~80s total sim time
        tracing::info!("[TEST] Phase 3: verifying node-3 has rejoined");
        check_node_is_all_alive("node-1", 8081).await?;
        check_node_is_all_alive("node-2", 8082).await?;
        check_node_is_all_alive("node-3", 8083).await?;
        tracing::info!("[TEST] Phase 3: OK — all 3 nodes alive, node-3 successfully rejoined");

        Ok(())
    });

    // Crash node-3 after Phase 1 check has completed (~10s).
    while sim.elapsed() < Duration::from_secs(15) {
        sim.step()?;
    }
    tracing::info!("[TEST] Crashing node-3 at {}s sim time", sim.elapsed().as_secs());
    sim.crash("node-3");

    // Step to ~40s — Phase 2 check completes by ~30s, giving a 10s gap before restart.
    while sim.elapsed() < Duration::from_secs(40) {
        sim.step()?;
    }
    // Restart node-3 — the host factory runs again with a fresh UUID.
    tracing::info!("[TEST] Restarting node-3 with fresh UUID at {}s sim time", sim.elapsed().as_secs());
    sim.bounce("node-3");

    // Run remaining simulation until the checker client completes (~80s).
    sim.run()
}
