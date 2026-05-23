use std::time::Duration;

use turmoil::Builder;

use crate::StartUp;
use crate::it::helpers::{check_alive_count, check_dead_or_not_exist, default_env};

async fn check_all_alive(host: &str, port: u16) -> turmoil::Result {
    check_alive_count(host, port, 3).await
}

#[test]
#[serial_test::serial]
fn cluster_setup() -> turmoil::Result {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(30))
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
        tokio::time::sleep(Duration::from_secs(10)).await;
        check_all_alive("node-1", 8081).await?;
        check_all_alive("node-2", 8082).await?;
        check_all_alive("node-3", 8083).await?;
        Ok(())
    });

    sim.run()
}

/// node-1 and node-3 are partitioned from each other.
/// They must learn about each other exclusively through node-2's gossip.
#[test]
#[serial_test::serial]
fn partition_gossip() -> turmoil::Result {
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
        tracing::info!("[TEST] waiting for initial cluster formation");
        tokio::time::sleep(Duration::from_secs(30)).await;

        tracing::info!("[TEST] partitioning node-1 and node-3");
        turmoil::partition("node-1", "node-3");

        tokio::time::sleep(Duration::from_secs(60)).await;

        check_all_alive("node-1", 8081).await?;
        check_all_alive("node-2", 8082).await?;
        check_all_alive("node-3", 8083).await?;
        Ok(())
    });

    sim.run()
}

// TODO: remove after Phase 3 MembershipConvergesAfterRejoin is passing across seeds
/// node-3 process is killed and restarted.
#[test]
#[serial_test::serial]
fn dead_node_rejoin_after_process_restart() -> turmoil::Result {
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
        tracing::info!("[TEST] Phase 1: waiting for initial cluster formation");
        tokio::time::sleep(Duration::from_secs(10)).await;
        check_all_alive("node-1", 8081).await?;
        check_all_alive("node-2", 8082).await?;
        check_all_alive("node-3", 8083).await?;
        tracing::info!("[TEST] Phase 1 OK");

        tracing::info!("[TEST] Phase 2: waiting for SWIM to detect node-3 as dead");
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert!(
            check_dead_or_not_exist("node-1", 8081, "node-3").await,
            "node-1 should see node-3 as Dead or absent"
        );
        assert!(
            check_dead_or_not_exist("node-2", 8082, "node-3").await,
            "node-2 should see node-3 as Dead or absent"
        );
        tracing::info!("[TEST] Phase 2 OK");

        tracing::info!("[TEST] Phase 3: waiting for node-3 to rejoin");
        tokio::time::sleep(Duration::from_secs(50)).await;
        check_all_alive("node-1", 8081).await?;
        check_all_alive("node-2", 8082).await?;
        check_all_alive("node-3", 8083).await?;
        tracing::info!("[TEST] Phase 3 OK");

        Ok(())
    });

    while sim.elapsed() < Duration::from_secs(15) {
        sim.step()?;
    }
    tracing::info!("[TEST] crashing node-3 at {}s", sim.elapsed().as_secs());
    sim.crash("node-3");

    while sim.elapsed() < Duration::from_secs(40) {
        sim.step()?;
    }
    tracing::info!("[TEST] restarting node-3 at {}s", sim.elapsed().as_secs());
    sim.bounce("node-3");

    sim.run()
}
