use std::time::Duration;

use turmoil::Builder;

use crate::StartUp;
use crate::connections::protocol::{
    AdminRequest, AdminResponse, ClientRequest, ClientResponse, ControlPlaneRequest,
    ControlPlaneResponse,
};
use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
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
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
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

        let mut acked: Option<(&str, u16)> = None;
        'outer: for _ in 0..20 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
                match send_request(host, port, create_req.clone()).await {
                    ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated)
                    | ClientResponse::ControlPlane(ControlPlaneResponse::AlreadyExists) => {
                        acked = Some((host, port));
                        break 'outer;
                    }
                    _ => {}
                }
            }
        }
        let (host, port) = acked.expect("CreateTopic not acked by any node within 40s");

        // Verify visible on the acked node before deleting.
        let list_resp = send_request(
            host,
            port,
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

        // Delete the topic (same node is the leader for the shard group).
        let del_resp = send_request(
            host,
            port,
            ClientRequest::ControlPlane(ControlPlaneRequest::DeleteTopic {
                name: "del-test".into(),
            }),
        )
        .await;
        assert!(
            matches!(
                del_resp,
                ClientResponse::ControlPlane(ControlPlaneResponse::TopicDeleted)
            ),
            "expected TopicDeleted, got {del_resp:?}"
        );

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

        let mut created = false;
        'outer: for _ in 0..20 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
                match send_request(host, port, create_req.clone()).await {
                    ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated)
                    | ClientResponse::ControlPlane(ControlPlaneResponse::AlreadyExists) => {
                        created = true;
                        break 'outer;
                    }
                    _ => {}
                }
            }
        }
        assert!(created, "CreateTopic not acked by any node within 40s");

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

/// `DescribeTopic` happy path — the consumer can read back a topic's full
/// metadata (with one active range covering the whole keyspace, and its
/// active segment with the replica set). Exercises the GetTopicMetadata RPC,
/// the TopicDetail::from_meta translation, and the SWIM address-snapshot
/// path end-to-end.
#[test]
#[serial_test::serial]
fn describe_topic_returns_topic_metadata() -> turmoil::Result {
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
            name: "describe-test".into(),
            storage_policy: StoragePolicy {
                retention_ms: 3_600_000,
                replication_factor: 3,
                partition_strategy: PartitionStrategy::AutoSplit,
            },
        });

        // Create via whichever node turns out to be the topic's metadata leader.
        let mut created = false;
        'outer: for _ in 0..20 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
                match send_request(host, port, create_req.clone()).await {
                    ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated)
                    | ClientResponse::ControlPlane(ControlPlaneResponse::AlreadyExists) => {
                        created = true;
                        break 'outer;
                    }
                    _ => {}
                }
            }
        }
        assert!(created, "CreateTopic not acked by any node within 40s");

        // DescribeTopic against each node — at least one returns TopicDetail
        // directly (the owner); the others return TopicMetadataRedirect. Follow
        // a redirect by re-querying the owner.
        let describe = ClientRequest::ControlPlane(ControlPlaneRequest::DescribeTopic {
            name: "describe-test".into(),
        });
        let mut found_detail = None;
        for (host, port) in [("node-1", 8081u16), ("node-2", 8082), ("node-3", 8083)] {
            let resp = send_request(host, port, describe.clone()).await;
            match resp {
                ClientResponse::ControlPlane(ControlPlaneResponse::TopicDetail(detail)) => {
                    found_detail = Some(detail);
                    break;
                }
                ClientResponse::ControlPlane(ControlPlaneResponse::TopicMetadataRedirect {
                    owner,
                }) => {
                    // Follow the redirect — owner.client_addr is the SWIM-resolved
                    // address; for turmoil tests we use the well-known port map.
                    let owner_port = match owner.node_id.as_str() {
                        "node-1" => 8081,
                        "node-2" => 8082,
                        "node-3" => 8083,
                        other => panic!("unexpected redirect to {other}"),
                    };
                    let owner_host = owner.node_id.as_str();
                    let resp2 = send_request(owner_host, owner_port, describe.clone()).await;
                    if let ClientResponse::ControlPlane(ControlPlaneResponse::TopicDetail(d)) =
                        resp2
                    {
                        found_detail = Some(d);
                        break;
                    }
                }
                _ => {}
            }
        }
        let detail = found_detail.expect("DescribeTopic via redirect-follow yielded no TopicDetail");
        assert_eq!(detail.name, "describe-test");
        assert!(
            !detail.ranges.is_empty(),
            "topic should have at least one range"
        );
        // Fresh topic → exactly one full-keyspace range.
        assert_eq!(detail.ranges.len(), 1);
        let range = &detail.ranges[0];
        assert!(range.active_segment.is_some(), "active range needs a segment");
        let seg = range.active_segment.as_ref().unwrap();
        assert_eq!(
            seg.replica_set.len(),
            3,
            "replication_factor=3 → 3 replicas"
        );

        Ok(())
    });

    sim.run()
}

// `DescribeTopic` redirect path is covered by unit tests in
// `connections::clients::tests::describe_topic_redirects_when_not_owner` and
// `describe_topic_not_found_on_owner`. An e2e redirect test would need a
// cluster larger than 3 nodes: with 256 vnodes × 3 nodes, the ring's
// shard-group construction includes all 3 distinct pnodes in every shard
// group, so no node is ever a "non-member" — the redirect branch can't be
// reached in this sim shape. Worth adding back if/when a multi-shard test
// harness with >3 nodes lands.
