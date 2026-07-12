use std::time::Duration;

use turmoil::Builder;

use crate::it::e2e::{NODES, host_cluster};
use crate::it::helpers::check_alive_count;

async fn check_all_alive(host: &str, port: u16) -> turmoil::Result {
    check_alive_count(host, port, 3).await
}

// Boots three complete EastGuard server nodes under the turmoil network simulator.
// Asserts that the nodes discover each other using seed node configuration, exchange SWIM pings/gossips, and successfully converge on a 3-node alive cluster view.
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

    host_cluster(&mut sim, &NODES, |_| {});

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
// Partitions Node 1 and Node 3 in a 3-node turmoil cluster so they cannot communicate directly.
// Asserts that Node 2 acts as a transit node, successfully routing membership gossip between Node 1 and Node 3 to keep the cluster views alive and synchronized.
#[test]
#[serial_test::serial]
fn partition_gossip() -> turmoil::Result {
    let build: turmoil::Sim<'_> = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(15))
        .tcp_capacity(4096)
        .build();
    let mut sim = build;

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    host_cluster(&mut sim, &NODES, |_| {});

    sim.client("checker", async {
        tracing::info!("[TEST] waiting for initial cluster formation");
        tokio::time::sleep(Duration::from_secs(5)).await;

        tracing::info!("[TEST] partitioning node-1 and node-3");

        turmoil::partition(NODES[0].0, NODES[2].0);

        tokio::time::sleep(Duration::from_secs(5)).await;

        for (node_name, port, _cluster_p) in &NODES {
            check_all_alive(node_name, *port).await?;
        }

        Ok(())
    });

    sim.run()
}
