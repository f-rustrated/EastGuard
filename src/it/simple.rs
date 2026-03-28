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
        node_id: Some(node_id),
        node_id_file_name: "node_id".into(),
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

async fn check_node_not_in_members(host: &str, port: u16, excluded_node_id: &str) -> turmoil::Result {
    let members = get_members(host, port).await?;
    let exists = members.iter().any(|m| &*m.node_id == excluded_node_id);
    assert!(
        !exists,
        "{host} should not have {excluded_node_id} in membership at all, got {:?}",
        members
    );
    Ok(())
}

async fn check_node_is_all_alive(host: &str, port: u16) -> turmoil::Result {
    let members = get_members(host, port).await?;
    tracing::info!("INSPECTING host: {}", host);
    let alive_count = members
        .iter()
        .filter(|m| m.state == SwimNodeState::Alive)
        .count();
    assert_eq!(
        alive_count, 3,
        "{host} should have 3 alive nodes, got {:?}",
        members
    );
    Ok(())
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

#[test]
fn dead_node_rejoin_after_full_partition() -> turmoil::Result {
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
        // wait for cluster to form
        tokio::time::sleep(Duration::from_secs(10)).await;
        check_node_is_all_alive("node-1", 8081).await?;
        check_node_is_all_alive("node-2", 8082).await?;
        check_node_is_all_alive("node-3", 8083).await?;

        // full bidirectional partition — both sides lose each other, node-3 starts re-join process
        tracing::info!("[TEST] Partitioning node-3 from cluster");
        turmoil::partition("node-3", "node-1");
        turmoil::partition("node-3", "node-2");

        // wait for Suspect(5s) + Dead + tombstone window(10s) to expire on ALL sides with buffer
        tokio::time::sleep(Duration::from_secs(30)).await;
        check_node_not_in_members("node-1", 8081, "node-3").await?;
        check_node_not_in_members("node-2", 8082, "node-3").await?;

        // Phase 4: repair — node-3's re-join retries (stored JoinAttempts) now reach node-1/node-2.
        tracing::info!("[TEST] Repairing partition");
        turmoil::repair("node-3", "node-1");
        turmoil::repair("node-3", "node-2");

        // Phase 5: wait for re-join Ping + gossip to propagate
        tokio::time::sleep(Duration::from_secs(20)).await;

        tracing::info!("[TEST] Asserting rejoin");
        check_node_is_all_alive("node-1", 8081).await?;
        check_node_is_all_alive("node-2", 8082).await?;
        check_node_is_all_alive("node-3", 8083).await?;

        Ok(())
    });

    sim.run()
}
