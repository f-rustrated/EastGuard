use std::time::Duration;

use turmoil::Builder;

use crate::StartUp;
use crate::connections::protocol::{
    AdminRequest, AdminResponse, ClientDataPlaneRequest, ClientRequest, ClientResponse,
    ControlPlaneRequest, ControlPlaneResponse, DataPlaneResponse, FetchRequest, NodeState,
    ProduceRequest, RangeProgressSignal, TopicDetail,
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
        let detail =
            found_detail.expect("DescribeTopic via redirect-follow yielded no TopicDetail");
        assert_eq!(detail.name, "describe-test");
        assert!(
            !detail.ranges.is_empty(),
            "topic should have at least one range"
        );
        // Fresh topic → exactly one full-keyspace range.
        assert_eq!(detail.ranges.len(), 1);
        let range = &detail.ranges[0];
        assert!(
            range.active_segment.is_some(),
            "active range needs a segment"
        );
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

/// Full produce → fetch loop on a hot (active) range. Creates a topic, produces
/// a few records to the range's data-leader, then fetches them back from offset
/// 0 and checks the in-band `progress_signal` is `Active`. Exercises the produce
/// wire handler, the data-transport replication path, and the hot fetch path
/// end-to-end.
#[test]
#[serial_test::serial]
fn produce_then_fetch_hot() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(150))
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
        const NODES: [(&str, u16); 3] = [("node-1", 8081), ("node-2", 8082), ("node-3", 8083)];
        const TOPIC: &str = "hot";

        wait_for_cluster(&NODES, 3).await;
        create_topic_anywhere(TOPIC, &NODES, 3).await;

        let payloads: Vec<Vec<u8>> = (0..3).map(|i| format!("rec-{i}").into_bytes()).collect();

        // Find the data-leader by producing the first record (only the segment's
        // leader hosts a writable tracker; others reply with an error).
        let leader = produce_until_acked(TOPIC, &payloads[0], &NODES)
            .await
            .expect("first produce never acked by any node");

        // Remaining records go to the same leader (retry to absorb timing jitter
        // from the real-thread data plane).
        for payload in &payloads[1..] {
            let mut acked = false;
            for _ in 0..40 {
                if produce_to(TOPIC, payload, leader).await {
                    acked = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            assert!(acked, "produce to leader {leader:?} not acked");
        }

        // Fetch from offset 0, accumulating across next_entry_id. If the range
        // rolled mid-produce (a replication timeout sealed the active segment),
        // the records span the sealed segment + its successor, so one fetch
        // isn't enough — follow next_entry_id across the boundary. A short or
        // empty response just means "keep polling"; never reaching all three is
        // a failure.
        let mut entries = Vec::new();
        let mut next_entry_id = 0u64;
        let mut progress_signal = RangeProgressSignal::Active;
        for _ in 0..80 {
            let fetch = ClientRequest::DataPlane(ClientDataPlaneRequest::Fetch(FetchRequest {
                topic_name: TOPIC.into(),
                range_id: 0,
                entry_id: next_entry_id,
                record_index: 0,
                max_bytes: 1 << 20,
                keyspace_bound: None,
            }));
            if let ClientResponse::DataPlane(DataPlaneResponse::Fetched {
                entries: batch,
                next_entry_id: next,
                progress_signal: signal,
            }) = send_request(leader.0, leader.1, fetch).await
                && !batch.is_empty()
            {
                progress_signal = signal;
                next_entry_id = next;
                entries.extend(batch);
                if entries.len() >= 3 {
                    break;
                }
                continue;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        assert_eq!(
            entries.len(),
            3,
            "all three produced records should fetch, got {}",
            entries.len()
        );
        assert_eq!(entries[0].entry_id, 0);
        assert_eq!(entries[0].data, b"rec-0");
        assert_eq!(entries[1].data, b"rec-1");
        assert_eq!(entries[2].data, b"rec-2");
        assert_eq!(next_entry_id, 3);
        assert!(
            matches!(progress_signal, RangeProgressSignal::Active),
            "fresh single-range topic stays Active, got {progress_signal:?}"
        );

        Ok(())
    });

    sim.run()
}

/// Prerequisite for sealed-segment repair: the leader-initiated seal pipeline must
/// commit a `RollSegment` end-to-end. Produce a few records, let the segment age
/// out, and confirm the active segment rolls to a successor whose `start_offset`
/// advanced past 0 — which only a *known-end* seal produces (an unknown-end roll
/// opens the successor at 0). Without a known-end sealed segment there is nothing
/// for repair to reassign, so this gates the repair e2e.
#[test]
#[serial_test::serial]
fn age_seal_rolls_the_active_segment() -> turmoil::Result {
    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(180))
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
            // Force a fast age-based seal.
            env.max_segment_age_secs = 5;
            env.segment_age_check_interval_secs = 1;
            StartUp::with_env(env, 0).run().await?;
            Ok(())
        });
    }

    sim.client("test-client", async {
        const NODES: [(&str, u16); 3] = [("node-1", 8081), ("node-2", 8082), ("node-3", 8083)];
        const TOPIC: &str = "age-seal";

        wait_for_cluster(&NODES, 3).await;
        create_topic_anywhere(TOPIC, &NODES, 3).await;

        // Real committed data so the seal carries a meaningful known end.
        let leader = produce_until_acked(TOPIC, b"rec-0", &NODES)
            .await
            .expect("first produce never acked");
        for i in 1..3 {
            let payload = format!("rec-{i}").into_bytes();
            let mut acked = false;
            for _ in 0..40 {
                if produce_to(TOPIC, &payload, leader).await {
                    acked = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            assert!(acked, "produce {i} not acked by {leader:?}");
        }

        // The segment ages past `max_segment_age_secs`; the leader enqueues a
        // SealRequest, the coordinator commits a RollSegment, and the active
        // segment rolls. Poll DescribeTopic until the successor's start_offset
        // advances past 0 (the known-end seal landed).
        let mut rolled_start = None;
        for _ in 0..80 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Some(detail) = describe_topic(TOPIC, &NODES).await
                && let Some(seg) = detail.ranges.first().and_then(|r| r.active_segment.as_ref())
                && seg.start_offset > 0
            {
                rolled_start = Some(seg.start_offset);
                break;
            }
        }
        assert!(
            rolled_start.is_some(),
            "active segment never rolled — the leader-initiated seal pipeline did not \
             commit a RollSegment e2e (start_offset stayed 0)"
        );

        Ok(())
    });

    sim.run()
}

// ── produce/fetch e2e helpers ──────────────────────────────────────────────

/// Wait until every node sees `expected` alive members. Topic creation emits the
/// initial `SegmentAssignment` exactly once (fire-and-forget, no retry); if it's
/// sent before SWIM has converged, the data-leader's address isn't yet resolvable
/// on the metadata leader and the assignment is dropped permanently. Gating
/// creation on convergence keeps the produce path off that drop window.
async fn wait_for_cluster(nodes: &[(&str, u16)], expected: usize) {
    for _ in 0..60 {
        let mut all_converged = true;
        for &(host, port) in nodes {
            let alive = match send_request(
                host,
                port,
                ClientRequest::Admin(AdminRequest::DescribeCluster),
            )
            .await
            {
                ClientResponse::Admin(AdminResponse::ClusterInfo { nodes: members }) => members
                    .iter()
                    .filter(|m| matches!(m.state, NodeState::Alive))
                    .count(),
                _ => 0,
            };
            if alive < expected {
                all_converged = false;
                break;
            }
        }
        if all_converged {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Create a topic by trying every node until one acks.
async fn create_topic_anywhere(topic: &str, nodes: &[(&str, u16)], replication_factor: u64) {
    let req = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
        name: topic.into(),
        storage_policy: StoragePolicy {
            retention_ms: 3_600_000,
            replication_factor,
            partition_strategy: PartitionStrategy::AutoSplit,
        },
    });
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        for &(host, port) in nodes {
            match send_request(host, port, req.clone()).await {
                ClientResponse::ControlPlane(ControlPlaneResponse::TopicCreated)
                | ClientResponse::ControlPlane(ControlPlaneResponse::AlreadyExists) => return,
                _ => {}
            }
        }
    }
    panic!("CreateTopic({topic}) not acked by any node");
}

/// Produce one record, retrying across all nodes until acked. Returns the node
/// that accepted it (the segment's data-leader).
async fn produce_until_acked<'a>(
    topic: &str,
    payload: &[u8],
    nodes: &'a [(&'a str, u16)],
) -> Option<(&'a str, u16)> {
    // Generous budget: the data plane, replication, and checkpoint run on real
    // OS threads (outside turmoil's deterministic runtime), so SegmentAssignment
    // delivery + first commit can take a while on a slow interleaving.
    for _ in 0..80 {
        for &(host, port) in nodes {
            if produce_to(topic, payload, (host, port)).await {
                return Some((host, port));
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    None
}

/// Produce one record to a specific node. Returns true if it was accepted.
async fn produce_to(topic: &str, payload: &[u8], node: (&str, u16)) -> bool {
    let req = ClientRequest::DataPlane(ClientDataPlaneRequest::Produce(ProduceRequest {
        topic_name: topic.into(),
        routing_key: b"k".to_vec(),
        data: payload.to_vec(),
        record_count: 1,
    }));
    matches!(
        send_request(node.0, node.1, req).await,
        ClientResponse::DataPlane(DataPlaneResponse::Produced { .. })
    )
}

/// Fetch a topic's metadata, following a `TopicMetadataRedirect` to the owner.
async fn describe_topic(topic: &str, nodes: &[(&str, u16)]) -> Option<TopicDetail> {
    let req =
        ClientRequest::ControlPlane(ControlPlaneRequest::DescribeTopic { name: topic.into() });
    for &(host, port) in nodes {
        match send_request(host, port, req.clone()).await {
            ClientResponse::ControlPlane(ControlPlaneResponse::TopicDetail(d)) => return Some(d),
            ClientResponse::ControlPlane(ControlPlaneResponse::TopicMetadataRedirect { owner }) => {
                if let Some(&(owner_host, owner_port)) =
                    nodes.iter().find(|(n, _)| owner.node_id.starts_with(n))
                    && let ClientResponse::ControlPlane(ControlPlaneResponse::TopicDetail(d)) =
                        send_request(owner_host, owner_port, req.clone()).await
                {
                    return Some(d);
                }
            }
            _ => {}
        }
    }
    None
}
