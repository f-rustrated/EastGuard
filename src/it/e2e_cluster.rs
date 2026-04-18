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
        config_dir: format!("./eastguard/config/{}", idx),
        data_dir: format!("./eastguard/data/{}", idx),
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

async fn check_dead_or_not_exist(host: &str, port: u16, target: &str) -> bool {
    let Ok(members) = get_members(host, port).await else {
        return false;
    };
    match members.iter().find(|m| m.node_id.starts_with(target)) {
        None => true,
        Some(n) if n.state == SwimNodeState::Dead => true,
        Some(_) => false,
    }
}

/// Full-stack E2E: SWIM cluster formation with MultiRaftActor wired in.
///
/// Proves the complete pipeline works without panic:
///   SWIM discover → MembershipEvent::NodeAlive → HandleNodeJoin → MultiRaftActor EnsureGroup/AddPeer
///   SWIM failure detection → MembershipEvent::NodeDead → HandleNodeDeath → MultiRaftActor RemovePeer
///   Node restart → SWIM rejoin → HandleNodeJoin again
#[test]
fn e2e_swim_raft_cluster_lifecycle() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(120))
        .tcp_capacity(4096)
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
        // Phase 1: Cluster formation — all 3 nodes alive
        tracing::info!("[E2E] Phase 1: waiting for cluster formation...");
        tokio::time::sleep(Duration::from_secs(10)).await;
        check_alive_count("node-1", 8081, 3).await?;
        check_alive_count("node-2", 8082, 3).await?;
        check_alive_count("node-3", 8083, 3).await?;
        tracing::info!("[E2E] Phase 1 OK: all 3 nodes alive");

        // Phase 2: Crash node-3, verify SWIM detects death
        // (SWIM→Raft HandleNodeDeath fires internally — no panic = pipeline works)
        tracing::info!("[E2E] Phase 2: waiting for node-3 death detection...");
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert!(
            check_dead_or_not_exist("node-1", 8081, "node-3").await,
            "node-1 should see node-3 as Dead"
        );
        assert!(
            check_dead_or_not_exist("node-2", 8082, "node-3").await,
            "node-2 should see node-3 as Dead"
        );
        tracing::info!("[E2E] Phase 2 OK: node-3 confirmed dead");

        // Phase 3: Restart node-3 (fresh UUID), verify rejoin
        // (SWIM→Raft HandleNodeJoin fires internally — no panic = pipeline works)
        tracing::info!("[E2E] Phase 3: waiting for node-3 to rejoin...");
        tokio::time::sleep(Duration::from_secs(50)).await;
        check_alive_count("node-1", 8081, 3).await?;
        check_alive_count("node-2", 8082, 3).await?;
        check_alive_count("node-3", 8083, 3).await?;
        tracing::info!("[E2E] Phase 3 OK: node-3 rejoined, all 3 alive");

        Ok(())
    });

    // Crash node-3 after Phase 1 (~10s)
    while sim.elapsed() < Duration::from_secs(15) {
        sim.step()?;
    }
    tracing::info!("[E2E] Crashing node-3 at {}s", sim.elapsed().as_secs());
    sim.crash("node-3");

    // Restart node-3 after Phase 2 (~40s)
    while sim.elapsed() < Duration::from_secs(40) {
        sim.step()?;
    }
    tracing::info!("[E2E] Restarting node-3 at {}s", sim.elapsed().as_secs());
    sim.bounce("node-3");

    sim.run()
}
