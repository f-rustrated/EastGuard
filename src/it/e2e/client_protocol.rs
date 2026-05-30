use std::time::Duration;

use turmoil::Builder;

use crate::StartUp;
use crate::connections::protocol::{
    AdminRequest, AdminResponse, ClientRequest, ClientResponse, ControlPlaneRequest,
    ControlPlaneResponse,
};
use crate::connections::protocol::ControlPlaneResponse::NotLeader;
use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
use crate::it::helpers::{default_env, send_request, send_request_to_addr};

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
        let req = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
            name: "test-topic".to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
        });
        // Allow time for SWIM convergence then retry across all nodes until a leader
        // responds — different shard groups finish elections at different times.
        tokio::time::sleep(Duration::from_secs(4)).await;
        let mut create_ok = false;
        'create: for _ in 0..20u32 {
            for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
                let r = send_request(host, port, req.clone()).await;
                if matches!(
                    r,
                    ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated)
                        | ClientResponse::ControlPlane(ControlPlaneResponse::AlreadyExists)
                ) {
                    create_ok = true;
                    break 'create;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        assert!(create_ok, "CreateTopic did not succeed on any node within 24s");

        // DescribeCluster must return a non-empty node list from every node.
        for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
            let cluster_resp = send_request(
                host,
                port,
                ClientRequest::Admin(AdminRequest::DescribeCluster),
            )
            .await;
            match cluster_resp {
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

#[test]
#[serial_test::serial]
fn delete_topic() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(60))
        .tcp_capacity(4096)
        .build();

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    for (name, idx, cp, rp) in [
        ("node-1", 1, 8081, 18001),
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
        let create_req = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
            name: "del-test".to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
        });

        tokio::time::sleep(Duration::from_secs(4)).await;
        let mut create_ok = false;
        'create: for _ in 0..20u32 {
            for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
                let r = send_request(host, port, create_req.clone()).await;
                if matches!(
                    r,
                    ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated)
                        | ClientResponse::ControlPlane(ControlPlaneResponse::AlreadyExists)
                ) {
                    create_ok = true;
                    break 'create;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        assert!(create_ok, "CreateTopic did not succeed on any node within 24s");
        // Wait for Raft replication to reach all nodes (one heartbeat period = 1s).
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify visible on at least one node before deleting.
        let list_resp = send_request(
            "node-1",
            8081,
            ClientRequest::ControlPlane(ControlPlaneRequest::ListHostedTopics),
        )
        .await;
        let ClientResponse::ControlPlane(ControlPlaneResponse::TopicList { topics }) = list_resp
        else {
            panic!("expected TopicList, got {list_resp:?}");
        };
        assert!(
            topics.iter().any(|t| t.name == "del-test"),
            "del-test not listed before delete"
        );

        // Delete: try each node until the leader responds — no server-side forwarding.
        let mut delete_ok = false;
        for (h, p) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
            let r = send_request(
                h,
                p,
                ClientRequest::ControlPlane(ControlPlaneRequest::DeleteTopic {
                    name: "del-test".into(),
                }),
            )
            .await;
            if matches!(r, ClientResponse::ControlPlane(ControlPlaneResponse::TopicDeleted)) {
                delete_ok = true;
                break;
            }
        }
        assert!(delete_ok, "DeleteTopic did not succeed on any node");

        // Wait up to 5s for deletion to propagate and disappear from all nodes.
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let mut still_present = false;
            for (h, p) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
                if let ClientResponse::ControlPlane(ControlPlaneResponse::TopicList {
                    topics: listed,
                }) = send_request(
                    h,
                    p,
                    ClientRequest::ControlPlane(ControlPlaneRequest::ListHostedTopics),
                )
                .await
                    && listed.iter().any(|t| t.name == "del-test")
                {
                    still_present = true;
                }
            }
            if !still_present {
                break;
            }
        }

        for (h, p) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
            if let ClientResponse::ControlPlane(ControlPlaneResponse::TopicList {
                topics: listed,
            }) = send_request(
                h,
                p,
                ClientRequest::ControlPlane(ControlPlaneRequest::ListHostedTopics),
            )
            .await
            {
                assert!(
                    !listed.iter().any(|t| t.name == "del-test"),
                    "{h} still lists del-test after deletion"
                );
            }
        }

        Ok(())
    });

    sim.run()
}

#[test]
#[serial_test::serial]
fn list_topic_stats_after_create() -> turmoil::Result {
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
        let create_req = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
            name: "stats-test".to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
        });

        tokio::time::sleep(Duration::from_secs(4)).await;
        let mut create_ok = false;
        'create: for _ in 0..20u32 {
            for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
                let r = send_request(host, port, create_req.clone()).await;
                if matches!(
                    r,
                    ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated)
                        | ClientResponse::ControlPlane(ControlPlaneResponse::AlreadyExists)
                ) {
                    create_ok = true;
                    break 'create;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        assert!(create_ok, "CreateTopic did not succeed on any node within 24s");

        // At least one node must report stats for "stats-test".
        let mut found = false;
        for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
            if let ClientResponse::Admin(AdminResponse::TopicStats { topics }) = send_request(
                host,
                port,
                ClientRequest::Admin(AdminRequest::ListHostedTopicsWithStats),
            )
            .await
                && topics.iter().any(|t| t.name == "stats-test")
            {
                found = true;
                break;
            }
        }
        assert!(
            found,
            "stats-test not found in ListHostedTopicsWithStats on any node"
        );

        Ok(())
    });

    sim.run()
}

/// Verifies that `DeleteTopic` sent to a non-leader node returns `NotLeader { leader_addr: Some(_) }`
/// and that following the redirect to `leader_addr` succeeds with `TopicDeleted`.
#[test]
#[serial_test::serial]
fn delete_topic_redirects_to_leader() -> turmoil::Result {
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
        let topic = "del-redirect";
        let create_req = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
            name: topic.to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
        });

        // Wait for SWIM convergence + Raft election, then create via retry loop.
        tokio::time::sleep(Duration::from_secs(4)).await;
        let mut leader_port: Option<u16> = None;
        'create: for _ in 0..20u32 {
            for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
                let r = send_request(host, port, create_req.clone()).await;
                if matches!(
                    r,
                    ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated)
                        | ClientResponse::ControlPlane(ControlPlaneResponse::AlreadyExists)
                ) {
                    leader_port = Some(port);
                    break 'create;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let leader_port = leader_port.expect("CreateTopic did not succeed on any node within 24s");

        // Wait for replication.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Pick any non-leader node.
        let non_leader = [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)]
            .into_iter()
            .find(|(_, p)| *p != leader_port)
            .expect("no non-leader node found");

        let del_req =
            ClientRequest::ControlPlane(ControlPlaneRequest::DeleteTopic { name: topic.into() });

        // Non-leader must redirect.
        let redirect_resp = send_request(non_leader.0, non_leader.1, del_req.clone()).await;
        let ClientResponse::ControlPlane(NotLeader { leader_addr: Some(leader_addr) }) =
            redirect_resp
        else {
            panic!("expected NotLeader{{Some}}, got {redirect_resp:?}");
        };

        // Following the redirect must succeed.
        let del_resp = send_request_to_addr(leader_addr, del_req).await;
        assert!(
            matches!(del_resp, ClientResponse::ControlPlane(ControlPlaneResponse::TopicDeleted)),
            "expected TopicDeleted after redirect, got {del_resp:?}"
        );

        Ok(())
    });

    sim.run()
}

/// Verifies that `DescribeTopic` is served by every replica — followers included —
/// with no redirect required.
#[test]
#[serial_test::serial]
fn describe_topic_served_by_all_replicas() -> turmoil::Result {
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
        let topic = "describe-replicas";
        let create_req = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
            name: topic.to_string(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
        });

        // Wait for SWIM convergence + Raft election, then create.
        tokio::time::sleep(Duration::from_secs(4)).await;
        let mut create_ok = false;
        'create: for _ in 0..20u32 {
            for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
                let r = send_request(host, port, create_req.clone()).await;
                if matches!(
                    r,
                    ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated)
                        | ClientResponse::ControlPlane(ControlPlaneResponse::AlreadyExists)
                ) {
                    create_ok = true;
                    break 'create;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        assert!(create_ok, "CreateTopic did not succeed within 24s");

        // Wait for the committed entry to replicate to all followers.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let describe_req = ClientRequest::ControlPlane(ControlPlaneRequest::DescribeTopic {
            name: topic.to_string(),
        });

        // Every node must serve the read — followers included.
        for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
            let resp = send_request(host, port, describe_req.clone()).await;
            match resp {
                ClientResponse::ControlPlane(ControlPlaneResponse::TopicDetail(detail)) => {
                    assert_eq!(detail.name, topic, "{host}: wrong topic name in TopicDetail");
                    assert!(!detail.ranges.is_empty(), "{host}: TopicDetail has no ranges");
                }
                other => panic!("{host}: expected TopicDetail, got {other:?}"),
            }
        }

        Ok(())
    });

    sim.run()
}
