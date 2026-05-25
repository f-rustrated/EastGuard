use std::time::Duration;

use turmoil::Builder;

use crate::StartUp;
use crate::connections::protocol::{
    AdminRequest, AdminResponse, ClientRequest, ClientResponse, ControlPlaneRequest,
    ControlPlaneResponse,
};
use crate::it::helpers::{default_env, send_request};

/// CreateTopic eventually succeeds when tried across all nodes (exactly one is the leader),
/// and DescribeCluster returns a non-empty node list from every node.
#[test]
#[serial_test::serial]
fn create_topic_and_describe_cluster() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(60))
        .tcp_capacity(4096)
        .build();

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    for (name, idx, cp, rp) in [
        ("node-1", 1u32, 8081u16, 18001u16),
        ("node-2", 2, 8082, 18002),
        ("node-3", 3, 8083, 18003),
    ] {
        sim.host(name, move || async move {
            let me = turmoil::lookup(name);
            let seeds: Vec<String> = [("node-1", 18001u16), ("node-2", 18002), ("node-3", 18003)]
                .iter()
                .filter(|(n, _)| *n != name)
                .map(|(n, p)| format!("{}:{}", turmoil::lookup(*n), p))
                .collect();
            let mut env = default_env(idx, name.to_string(), cp, rp);
            env.advertise_host = Some(me.to_string());
            env.join_seed_nodes = seeds;
            StartUp::with_env(env, 0).run().await?;
            Ok(())
        });
    }

    sim.client("test-client", async {
        // Wait for leader election then create a topic by trying all nodes.
        let req = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
            name: "test-topic".to_string(),
            retention_ms: 3_600_000,
            replication_factor: 3,
        });
        let mut created = false;
        'outer: for _ in 0..20 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
                match send_request(host, port, req.clone()).await {
                    ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated) => {
                        created = true;
                        break 'outer;
                    }
                    ClientResponse::ControlPlane(ControlPlaneResponse::AlreadyExists) => {
                        created = true;
                        break 'outer;
                    }
                    _ => {}
                }
            }
        }
        assert!(created, "CreateTopic not acked by any node within 40s");

        // DescribeCluster must return a non-empty node list from every node.
        for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
            let resp = send_request(
                host,
                port,
                ClientRequest::Admin(AdminRequest::DescribeCluster),
            )
            .await;
            match resp {
                ClientResponse::Admin(AdminResponse::ClusterInfo { nodes }) => {
                    assert!(!nodes.is_empty(), "{host} returned empty node list");
                }
                other => panic!("{host}: unexpected DescribeCluster response: {other:?}"),
            }
        }

        Ok(())
    });

    sim.run()
}
