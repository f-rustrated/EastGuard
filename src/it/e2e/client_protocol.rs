use std::time::Duration;

use turmoil::Builder;

use crate::StartUp;
use crate::clusters::metadata::strategy::{PartitionStrategy, StoragePolicy};
use crate::clusters::raft::messages::ProposeError;
use crate::connections::request::{ClientCommand, ConnectionRequests, ProposeRequest, ProposeResponse};
use crate::it::helpers::{default_env, send_propose};

fn test_propose_request(name: &str, forwarded: bool) -> ConnectionRequests {
    ConnectionRequests::Propose(ProposeRequest {
        resource_key: name.as_bytes().to_vec(),
        command: ClientCommand::CreateTopic {
            name: name.to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
        },
        forwarded,
    })
}

/// Propose with forwarded=true to a follower should return NotLeader
/// without attempting to forward again.
#[test]
#[serial_test::serial]
fn forwarded_request_not_forwarded_again() -> turmoil::Result {
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
        let mut converged = false;
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let req = test_propose_request("convergence-probe", false);
            if let Ok(resp) =
                tokio::time::timeout(Duration::from_secs(3), send_propose("node-1", 8081, req))
                    .await
            {
                match resp {
                    ProposeResponse::Success => {
                        converged = true;
                        break;
                    }
                    ProposeResponse::Error(ProposeError::NotLeader(_)) => {
                        converged = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
        assert!(converged, "cluster did not converge within timeout");

        let mut not_leader_count = 0;
        for (host, port) in [("node-1", 8081), ("node-2", 8082), ("node-3", 8083)] {
            let req = test_propose_request("test-topic-fwd", true);
            let resp = send_propose(host, port, req).await;
            match resp {
                ProposeResponse::Error(ProposeError::NotLeader(_)) => {
                    not_leader_count += 1;
                }
                ProposeResponse::Success => {}
                other => panic!("unexpected response from {}: {:?}", host, other),
            }
        }
        assert!(
            not_leader_count >= 2,
            "at least 2 followers should return NotLeader for forwarded=true, got {}",
            not_leader_count
        );

        Ok(())
    });

    sim.run()
}
