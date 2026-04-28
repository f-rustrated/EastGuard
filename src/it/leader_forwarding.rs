use std::time::Duration;

use turmoil::Builder;

use crate::StartUp;
use crate::clusters::metadata::strategy::{PartitionStrategy, StoragePolicy};
use crate::clusters::raft::messages::ProposeError;
use crate::config::Environment;
use crate::connections::clients::{ClientStreamReader, ClientStreamWriter};
use crate::connections::request::{
    ClientCommand, ConnectionRequests, ProposeRequest, ProposeResponse,
};
use crate::net::TcpStream;

fn default_env(idx: u32, node_id: String, client_port: u16, cluster_port: u16) -> Environment {
    Environment {
        config_dir: std::env::temp_dir()
            .join(format!("eastguard-config-{}-{}", idx, uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned(),
        data_dir: std::env::temp_dir()
            .join(format!("eastguard-data-{}-{}", idx, uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned(),
        meta_dir: std::env::temp_dir()
            .join(format!("eastguard-meta-{}-{}", idx, uuid::Uuid::new_v4()))
            .to_string_lossy()
            .into_owned(),
        node_id_prefix: Some(node_id),
        client_port,
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

async fn send_propose(host: &str, port: u16, req: ConnectionRequests) -> ProposeResponse {
    let stream = TcpStream::connect((host, port)).await.unwrap();
    let (read_half, write_half) = stream.into_split();
    let mut writer = ClientStreamWriter::new(write_half);
    let mut reader = ClientStreamReader::new(read_half);
    writer.write(&req).await.unwrap();
    reader.read_request().await.unwrap()
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
        tokio::time::sleep(Duration::from_secs(15)).await;

        // Send forwarded=true to all nodes — followers should return NotLeader
        // (not attempt forwarding), leader should succeed normally
        let mut not_leader_count = 0;
        for (host, port) in [("node-1", 8081), ("node-2", 8082), ("node-3", 8083)] {
            let req = test_propose_request("test-topic-fwd", true);
            let resp = send_propose(host, port, req).await;
            match resp {
                ProposeResponse::Error(ProposeError::NotLeader(_)) => {
                    not_leader_count += 1;
                }
                ProposeResponse::Success => {}
                other => panic!("Unexpected response from {}: {:?}", host, other),
            }
        }
        // At least 2 of 3 nodes are followers and should return NotLeader
        // without forwarding (since forwarded=true)
        assert!(
            not_leader_count >= 2,
            "At least 2 followers should return NotLeader for forwarded=true, got {}",
            not_leader_count
        );

        Ok(())
    });

    sim.run()
}
