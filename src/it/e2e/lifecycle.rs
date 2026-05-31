use std::time::Duration;

use turmoil::Builder;

use crate::StartUp;
use crate::it::helpers::{check_dead_or_not_exist, default_env, wait_swim_ready};

/// Full-stack E2E: SWIM cluster formation with MultiRaftActor wired in.
///
/// Proves the complete pipeline works without panic:
///   SWIM discover → MembershipEvent::NodeAlive → HandleNodeJoin → MultiRaftActor EnsureGroup/AddPeer
///   SWIM failure detection → MembershipEvent::NodeDead → HandleNodeDeath → MultiRaftActor RemovePeer
///   Node restart → SWIM rejoin → HandleNodeJoin again
// TODO: remove once Phase 3 properties exercise the same SWIM→Raft pipeline reliably across seeds
#[test]
#[serial_test::serial]
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
        let nodes = [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)];

        tracing::info!("[E2E] Phase 1: waiting for cluster formation");
        wait_swim_ready(&nodes, 3).await;
        tracing::info!("[E2E] Phase 1 OK: all 3 nodes alive");

        tracing::info!("[E2E] Phase 2: waiting for node-3 death detection");
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

        tracing::info!("[E2E] Phase 3: waiting for node-3 to rejoin");
        wait_swim_ready(&nodes, 3).await;
        tracing::info!("[E2E] Phase 3 OK: node-3 rejoined, all 3 alive");

        Ok(())
    });

    while sim.elapsed() < Duration::from_secs(15) {
        sim.step()?;
    }
    tracing::info!("[E2E] crashing node-3 at {}s", sim.elapsed().as_secs());
    sim.crash("node-3");

    while sim.elapsed() < Duration::from_secs(40) {
        sim.step()?;
    }
    tracing::info!("[E2E] restarting node-3 at {}s", sim.elapsed().as_secs());
    sim.bounce("node-3");

    sim.run()
}
