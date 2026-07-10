use std::net::SocketAddr;
use std::time::Duration;

use turmoil::Builder;

use crate::StartUp;
use crate::connections::protocol::{
    AdminRequest, AdminResponse, ClientDataPlaneRequest, ClientRequest, ClientResponse,
    ControlPlaneRequest, ControlPlaneResponse, DataPlaneResponse, FetchByIdRequest, FetchRequest,
    NodeState, ProduceRequest, RangeProgressSignal, TopicDetail,
};
use crate::control_plane::membership::ShardGroupId;
use crate::control_plane::metadata::strategy::{PartitionStrategy, StoragePolicy};
use crate::control_plane::metadata::{EntryId, RangeId, RangeState};
use crate::it::helpers::{default_env, send_request, try_send_request};
use crate::it::sim::invariants::{query_shard_info, query_shard_leader};

use super::{NodeSpec, host_cluster};

/// Standard 3- and 4-node clusters (name, client_port, cluster_port).
static NODES_3: [NodeSpec; 3] = [
    ("node-1", 8081, 18001),
    ("node-2", 8082, 18002),
    ("node-3", 8083, 18003),
];
static NODES_4: [NodeSpec; 4] = [
    ("node-1", 8081, 18001),
    ("node-2", 8082, 18002),
    ("node-3", 8083, 18003),
    ("node-4", 8084, 18004),
];

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

    host_cluster(&mut sim, &NODES_3, |_| {});

    sim.client("test-client", async {
        // Wait for leader election then create a topic by trying all nodes.
        let req = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
            name: "test-topic".to_string(),
            storage_policy: StoragePolicy {
                retention_ms: Some(3_600_000),
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

    host_cluster(&mut sim, &NODES_3, |_| {});

    sim.client("test-client", async {
        let create_req = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
            name: "del-test".to_string(),
            storage_policy: StoragePolicy {
                retention_ms: Some(3_600_000),
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

    host_cluster(&mut sim, &NODES_3, |_| {});

    sim.client("test-client", async {
        let create_req = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
            name: "stats-test".to_string(),
            storage_policy: StoragePolicy {
                retention_ms: Some(3_600_000),
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

    host_cluster(&mut sim, &NODES_3, |_| {});

    sim.client("test-client", async {
        let create_req = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
            name: "describe-test".into(),
            storage_policy: StoragePolicy {
                retention_ms: Some(3_600_000),
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

    host_cluster(&mut sim, &NODES_3, |_| {});

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
        let mut next_entry_id = EntryId(0u64);
        let mut progress_signal = RangeProgressSignal::Active;
        for _ in 0..80 {
            let fetch = ClientRequest::DataPlane(ClientDataPlaneRequest::Fetch(FetchRequest {
                topic_name: TOPIC.into(),
                range_id: RangeId(0),
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
        assert_eq!(entries[0].entry_id, EntryId(0));
        assert_eq!(entries[0].data, b"rec-0");
        assert_eq!(entries[1].data, b"rec-1");
        assert_eq!(entries[2].data, b"rec-2");
        assert_eq!(*next_entry_id, 3);
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

    host_cluster(&mut sim, &NODES_3, |env| {
        // Force a fast age-based seal.
        env.max_segment_age_secs = 5;
        env.segment_age_check_interval_secs = 1;
    });

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
                && let Some(seg) = detail
                    .ranges
                    .first()
                    .and_then(|r| r.active_segment.as_ref())
                && *seg.start_entry_id > 0
            {
                rolled_start = Some(seg.start_entry_id);
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

/// End-to-end sealed-segment repair. Produce + age-seal a segment, kill one of its
/// replicas, and confirm the coordinator reassigns the segment to the spare node,
/// which catches up and serves the old data on a cold fetch. Exercises the whole
/// loop: SWIM death → reconcile → `ReassignSegment` → `CatchUpAssignment` →
/// catch-up → cold-serve, across the control/data-plane seam.
#[test]
#[serial_test::serial]
fn sealed_segment_repair_catches_up_the_spare() -> turmoil::Result {
    use std::sync::{Arc, Mutex};

    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(300))
        .tcp_capacity(4096)
        // Pin turmoil's RNG: it's randomly seeded by default, so network
        // latency/ordering — and thus gossip-convergence timing — varies per run,
        // making this multi-node test flaky. A fixed seed + deterministic node ids
        // make the sim reproducible (CLAUDE.md "Testing").
        .rng_seed(1)
        .build();

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // 4 nodes, RF=3 → the topic lands on 3, leaving one spare for the reassign.
    host_cluster(&mut sim, &NODES_4, |env| {
        // Pin the node-id suffix → deterministic hash-ring placement across runs
        // (the default UUID is OS-random, which turmoil can't seed). This test
        // never restarts a node, so a fixed id is safe. See CLAUDE.md "Testing".
        env.node_id_suffix = Some("sim".to_string());
        // Low vnodes → few Raft groups → fast sim. This is a *speed* choice: the
        // #135 MAP-EMPTY gossip gap that once forced it is fixed by the data-plane
        // topology fallback (the coordinator-crash test runs at 256 to guard that),
        // so it's no longer a correctness workaround. See CLAUDE.md "Testing".
        env.vnodes_per_node = 16;
        // Seal by SIZE, not age: the size check is a pure byte-count on commit
        // (deterministic), whereas age uses real wall-clock (`std::time::Instant`),
        // which turmoil doesn't virtualize (CLAUDE.md "Testing"). 2 KiB rolls the
        // ~1 KiB records once; a tiny limit would roll per-record into a storm.
        env.segment_size_limit_bytes = 2048;
    });

    // The victim must be a *replica* of the sealed segment, but placement is
    // ring-determined — only the client knows it at runtime. It picks one and
    // hands it to the driver to crash.
    let victim = Arc::new(Mutex::new(None::<String>));
    let victim_w = victim.clone();

    sim.client("test-client", async move {
        const NODES: [(&str, u16); 4] = [
            ("node-1", 8081),
            ("node-2", 8082),
            ("node-3", 8083),
            ("node-4", 8084),
        ];
        const TOPIC: &str = "repair";

        wait_for_cluster(&NODES, 4).await;
        create_topic_anywhere(TOPIC, &NODES, 3).await;

        // Produce three ~1 KiB records; with a 2 KiB size limit they roll the
        // segment once (not per-record), sealing rec-0..2 into a known-end segment.
        let rec = |i: usize| -> Vec<u8> {
            let mut v = format!("rec-{i}").into_bytes();
            v.resize(1024, b'-');
            v
        };
        let leader = produce_until_acked(TOPIC, &rec(0), &NODES)
            .await
            .expect("first produce never acked");
        for i in 1..3 {
            let payload = rec(i);
            let mut acked = false;
            for _ in 0..40 {
                if produce_to(TOPIC, &payload, leader).await {
                    acked = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            assert!(acked, "produce {i} not acked");
        }

        // The age-seal rolls the segment; rec-0..2 now live in a sealed segment
        // whose replica set the successor still carries. Capture the topic id too —
        // the consumer reads the reassigned replica by id, not by name (it isn't a
        // metadata peer), so a DescribeTopic resolves the id once up front.
        let mut sealed = None;
        for _ in 0..80 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Some(detail) = describe_topic(TOPIC, &NODES).await
                && let Some(seg) = detail
                    .ranges
                    .first()
                    .and_then(|r| r.active_segment.as_ref())
                && *seg.start_entry_id > 0
            {
                let replicas = seg
                    .replica_set
                    .iter()
                    .map(|r| r.node_id.clone())
                    .collect::<Vec<String>>();
                sealed = Some((detail.topic_id, replicas));
                break;
            }
        }
        let (topic_id, replicas) = sealed.expect("segment never sealed");
        assert_eq!(replicas.len(), 3, "RF=3");

        // Spare = the node not in the replica set; victim = a replica to kill.
        let spare = NODES
            .iter()
            .copied()
            .find(|(n, _)| !replicas.iter().any(|r| r.starts_with(n)))
            .expect("a spare node");
        let victim_node = NODES
            .iter()
            .map(|(n, _)| *n)
            .find(|n| replicas.iter().any(|r| r.starts_with(n)))
            .expect("a replica to kill");
        tracing::info!(
            "[REPAIR] sealed replicas={replicas:?} spare={spare:?} victim={victim_node}"
        );
        *victim_w.lock().unwrap() = Some(victim_node.to_string());

        // The driver crashes the victim; the coordinator then reassigns the sealed
        // segment to the spare, which catches up. Poll a cold by-id fetch against
        // the spare — it holds the bytes but isn't a metadata peer, so a by-name
        // fetch couldn't resolve there — until it serves the old data.
        let mut served = false;
        for _ in 0..120 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let fetch =
                ClientRequest::DataPlane(ClientDataPlaneRequest::FetchById(FetchByIdRequest {
                    topic_id,
                    range_id: RangeId(0),
                    entry_id: EntryId(0),
                    max_bytes: 1 << 20,
                }));
            if let ClientResponse::DataPlane(DataPlaneResponse::Fetched { entries, .. }) =
                send_request(spare.0, spare.1, fetch).await
                && !entries.is_empty()
                && entries[0].data[..] == rec(0)[..]
            {
                served = true;
                break;
            }
        }
        assert!(
            served,
            "spare {spare:?} never caught up and served the reassigned sealed segment"
        );

        Ok(())
    });

    // Driver: step until the client has chosen a victim, crash it, then run out.
    while victim.lock().unwrap().is_none() {
        assert!(
            sim.elapsed() < Duration::from_secs(150),
            "client never chose a victim"
        );
        sim.step()?;
    }
    let victim_node = victim.lock().unwrap().clone().unwrap();
    tracing::info!(
        "[REPAIR] crashing {victim_node} at {}s",
        sim.elapsed().as_secs()
    );
    sim.crash(victim_node);

    sim.run()
}

/// End-to-end leader-crash boundary recovery. One record goes into an *active*
/// segment, then its write-leader (`replica_set[0]`) is killed. The coordinator
/// gathers each survivor's durable extent, seals at their `min`, and opens the
/// successor at `min+1` — continuous, never a 0-restart. Covers commit 28
/// (raft-actor #6–8, recover-then-roll) end to end: the sibling repair test above
/// covers the *sealed*-segment reassign path; this covers the *active* leader-crash
/// path that turns an unknown-end seal into a known-end one.
#[test]
#[serial_test::serial]
fn leader_crash_seals_active_segment_at_min_and_continues() -> turmoil::Result {
    use std::sync::{Arc, Mutex};

    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(300))
        .tcp_capacity(4096)
        .rng_seed(1)
        .build();

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // 4 nodes, RF=3 → the topic lands on 3; killing exactly one (the write-leader)
    // keeps metadata quorum (2 of 3) so the recovery RollSegment can commit.
    host_cluster(&mut sim, &NODES_4, |env| {
        env.node_id_suffix = Some("sim".to_string());
        // Low vnodes for sim speed; #135 MAP-EMPTY is fixed (topology fallback) —
        // the coordinator-crash test runs at 256 to guard the fix.
        env.vnodes_per_node = 16;
        // Huge size limit so one ~1 KiB record never rolls: the segment must stay
        // *active* at kill time, so the only thing that can seal it is the
        // leader-crash boundary recovery — not a size roll. (See CLAUDE.md "Testing".)
        env.segment_size_limit_bytes = 1 << 20;
    });

    // The active segment's write-leader (`replica_set[0]`) is ring-determined; only
    // the client learns it at runtime. It produces, reads the leader, and hands it to
    // the driver to crash.
    let victim = Arc::new(Mutex::new(None::<String>));
    let victim_w = victim.clone();

    sim.client("test-client", async move {
        const NODES: [(&str, u16); 4] = [
            ("node-1", 8081),
            ("node-2", 8082),
            ("node-3", 8083),
            ("node-4", 8084),
        ];
        const TOPIC: &str = "leadercrash";

        wait_for_cluster(&NODES, 4).await;
        create_topic_anywhere(TOPIC, &NODES, 3).await;

        // One ~1 KiB record into the active segment (entry 0). A produce-ack means the
        // entry is committed = durable on every replica (data-plane commit is an
        // all-replica ack), so each survivor's durable extent is ≥ 0 → min == 0.
        let rec0 = {
            let mut v = b"rec-0".to_vec();
            v.resize(1024, b'-');
            v
        };
        produce_until_acked(TOPIC, &rec0, &NODES)
            .await
            .expect("first produce never acked");

        // Read the *active* segment's write-leader = `replica_set[0]`. (The node that
        // accepted the produce only forwards to the leader — it need not be it.)
        let mut chosen = None;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Some(detail) = describe_topic(TOPIC, &NODES).await
                && let Some(seg) = detail
                    .ranges
                    .first()
                    .and_then(|r| r.active_segment.as_ref())
                && let Some(write_leader) = seg.replica_set.first()
            {
                assert_eq!(
                    seg.start_entry_id,
                    EntryId(0),
                    "segment must still be active at offset 0"
                );
                chosen = Some((detail.topic_id, write_leader.node_id.clone()));
                break;
            }
        }
        let (topic_id, leader_id) = chosen.expect("active segment never observed");
        let victim_node = NODES
            .iter()
            .map(|(n, _)| *n)
            .find(|n| leader_id.starts_with(n))
            .expect("write-leader must be one of the nodes");
        tracing::info!(
            "[LEADERCRASH] topic_id={topic_id:?} write-leader={leader_id} victim={victim_node}"
        );
        *victim_w.lock().unwrap() = Some(victim_node.to_string());

        // After the crash, poll survivors only: a connect to a turmoil-crashed host
        // hangs until the send_request timeout, so touching the dead leader would stall
        // the whole test (the sibling repair test sidesteps this the same way).
        let live: Vec<(&str, u16)> = NODES
            .iter()
            .copied()
            .filter(|(n, _)| *n != victim_node)
            .collect();

        // After the crash: boundary recovery seals entry 0 (the min across survivors)
        // and opens the successor at start_offset == 1 — continuous, never reset to 0,
        // and led by a survivor (not the dead leader).
        let mut continued = false;
        for _ in 0..150 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Some(detail) = describe_topic(TOPIC, &live).await
                && let Some(seg) = detail
                    .ranges
                    .first()
                    .and_then(|r| r.active_segment.as_ref())
                && *seg.start_entry_id == 1
            {
                if let Some(new_leader) = seg.replica_set.first() {
                    assert!(
                        !new_leader.node_id.starts_with(victim_node),
                        "recovered write-leader must be a survivor, not the crashed node"
                    );
                }
                continued = true;
                break;
            }
        }
        assert!(
            continued,
            "successor never opened at offset 1 — boundary recovery did not seal at min"
        );

        // The recovered (now sealed) segment still holds rec-0, readable by id from a
        // survivor — the seal preserved the committed entry rather than dropping it.
        let mut served = false;
        'outer: for _ in 0..60 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            for &(host, port) in &live {
                let fetch =
                    ClientRequest::DataPlane(ClientDataPlaneRequest::FetchById(FetchByIdRequest {
                        topic_id,
                        range_id: RangeId(0),
                        entry_id: EntryId(0),
                        max_bytes: 1 << 20,
                    }));
                if let ClientResponse::DataPlane(DataPlaneResponse::Fetched { entries, .. }) =
                    send_request(host, port, fetch).await
                    && !entries.is_empty()
                    && entries[0].data[..] == rec0[..]
                {
                    served = true;
                    break 'outer;
                }
            }
        }
        assert!(
            served,
            "no survivor served rec-0 from the recovered sealed segment"
        );

        Ok(())
    });

    // Driver: step until the client has chosen the write-leader, crash it, run out.
    while victim.lock().unwrap().is_none() {
        assert!(
            sim.elapsed() < Duration::from_secs(150),
            "client never chose a victim"
        );
        sim.step()?;
    }
    let victim_node = victim.lock().unwrap().clone().unwrap();
    tracing::info!(
        "[LEADERCRASH] crashing write-leader {victim_node} at {}s",
        sim.elapsed().as_secs()
    );
    sim.crash(victim_node);

    sim.run()
}

/// End-to-end: a sealed-segment repair survives the *coordinator* dying. We seal a
/// segment (like the sibling repair test), then crash the topic shard group's *metadata
/// leader* — the node that would otherwise drive the reassign. Its death opens a
/// no-leader window (ds-rsm #10); a new leader is elected and its takeover backstop
/// (raft-actor #2, #6) re-runs reconcile, reassigns the dead member's sealed segment to
/// the spare, and drives the catch-up to completion. The spare then serves the old data
/// by id — proving the repair is not stranded by losing the coordinator.
#[test]
#[serial_test::serial]
fn sealed_repair_survives_coordinator_crash() -> turmoil::Result {
    use std::sync::{Arc, Mutex};

    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(600))
        .tcp_capacity(4096)
        .rng_seed(1)
        .build();

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    host_cluster(&mut sim, &NODES_4, |env| {
        env.node_id_suffix = Some("sim".to_string());
        // Realistic scale (256 vnodes ⇒ ~1000 groups): deliberately exercises the
        // gossip pressure that exposed #135 — the one-shot shard-leader announce
        // gets evicted before reaching every node → MAP-EMPTY → coordinator
        // unresolvable. The data-plane topology fallback recovers resolution from
        // MAP-EMPTY (broadcast to the ring's members), so the repair still completes
        // at scale. This test guards that fix — and the learner model — end to end.
        env.vnodes_per_node = 256;
        env.segment_size_limit_bytes = 2048;
    });

    // The metadata leader is ring-/election-determined; only the client learns it at
    // runtime. It seals a segment, resolves the leader, and hands it to the driver.
    let victim = Arc::new(Mutex::new(None::<String>));
    let victim_w = victim.clone();

    sim.client("test-client", async move {
        const NODES: [(&str, u16); 4] = [
            ("node-1", 8081),
            ("node-2", 8082),
            ("node-3", 8083),
            ("node-4", 8084),
        ];
        const TOPIC: &str = "coordcrash";

        wait_for_cluster(&NODES, 4).await;
        create_topic_anywhere(TOPIC, &NODES, 3).await;

        // Seal a segment: three ~1 KiB records roll once under the 2 KiB limit, sealing
        // rec-0 (entry id 0) into a known-end segment.
        let rec = |i: usize| -> Vec<u8> {
            let mut v = format!("rec-{i}").into_bytes();
            v.resize(1024, b'-');
            v
        };
        let leader = produce_until_acked(TOPIC, &rec(0), &NODES)
            .await
            .expect("first produce never acked");
        for i in 1..3 {
            let payload = rec(i);
            let mut acked = false;
            for _ in 0..40 {
                if produce_to(TOPIC, &payload, leader).await {
                    acked = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            assert!(acked, "produce {i} not acked");
        }

        let mut sealed = None;
        for _ in 0..80 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Some(detail) = describe_topic(TOPIC, &NODES).await
                && let Some(seg) = detail
                    .ranges
                    .first()
                    .and_then(|r| r.active_segment.as_ref())
                && *seg.start_entry_id > 0
            {
                let replicas = seg
                    .replica_set
                    .iter()
                    .map(|r| r.node_id.clone())
                    .collect::<Vec<String>>();
                sealed = Some((detail.topic_id, replicas));
                break;
            }
        }
        let (topic_id, replicas) = sealed.expect("segment never sealed");
        assert_eq!(replicas.len(), 3, "RF=3");

        // Resolve the topic shard group's metadata leader — the coordinator we crash.
        let (shard_group_id, leader_id) = metadata_leader(TOPIC, &NODES)
            .await
            .expect("metadata leader never resolved");
        let victim_node = NODES
            .iter()
            .map(|(n, _)| *n)
            .find(|n| leader_id.starts_with(n))
            .expect("leader maps to a node");
        // The leader must hold the sealed segment, so its death actually needs a repair.
        assert!(
            replicas.contains(&leader_id),
            "metadata leader {leader_id} not in sealed replica set {replicas:?}"
        );
        let spare = NODES
            .iter()
            .copied()
            .find(|(n, _)| !replicas.iter().any(|r| r.starts_with(n)))
            .expect("a spare node");
        tracing::info!(
            "[COORDCRASH] shard={shard_group_id:?} leader/victim={leader_id} spare={spare:?}"
        );
        *victim_w.lock().unwrap() = Some(victim_node.to_string());

        // Survivors only — a connect to the crashed coordinator would hang to timeout.
        let live: Vec<(&str, u16)> = NODES
            .iter()
            .copied()
            .filter(|(n, _)| *n != victim_node)
            .collect();

        // A new metadata leader (≠ the crashed one) must emerge — the repair below can
        // only be driven by it. Any one survivor's view suffices (non-members reply None).
        let mut took_over = false;
        'lead: for _ in 0..150 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            for &(h, p) in &live {
                if let Ok(Some(new_leader)) = query_shard_leader(h, p, shard_group_id).await
                    && new_leader != leader_id
                {
                    took_over = true;
                    break 'lead;
                }
            }
        }
        assert!(
            took_over,
            "no new metadata leader after crashing {leader_id} — takeover did not happen"
        );

        // The new leader's takeover backstop reassigns the dead coordinator's sealed
        // segment to the spare, which catches up and serves rec-0 by id.
        let mut served = false;
        for _ in 0..120 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let fetch =
                ClientRequest::DataPlane(ClientDataPlaneRequest::FetchById(FetchByIdRequest {
                    topic_id,
                    range_id: RangeId(0),
                    entry_id: EntryId(0),
                    max_bytes: 1 << 20,
                }));
            if let ClientResponse::DataPlane(DataPlaneResponse::Fetched { entries, .. }) =
                send_request(spare.0, spare.1, fetch).await
                && !entries.is_empty()
                && entries[0].data[..] == rec(0)[..]
            {
                served = true;
                break;
            }
        }
        assert!(
            served,
            "repair stranded: spare {spare:?} never caught up and served rec-0 after the coordinator crash"
        );

        Ok(())
    });

    while victim.lock().unwrap().is_none() {
        assert!(
            sim.elapsed() < Duration::from_secs(200),
            "client never chose a victim"
        );
        sim.step()?;
    }
    let victim_node = victim.lock().unwrap().clone().unwrap();
    tracing::info!(
        "[COORDCRASH] crashing metadata leader {victim_node} at {}s",
        sim.elapsed().as_secs()
    );
    sim.crash(victim_node);

    sim.run()
}

/// End-to-end catch-up redrive under message loss. Seal a segment, then **partition
/// the spare from the coordinator** before killing one of the segment's replicas (a
/// *non-coordinator* one, so the coordinator survives to drive the repair). With the
/// link down, the coordinator's `CatchUpAssignment` to the spare — and every heartbeat
/// redrive of it — are dropped. After a window we heal the link; the next redrive
/// delivers, the spare pulls the segment from a surviving source (never partitioned),
/// and serves the old data. Guards the redrive hardening (`raft-actor.md` #9): a lost
/// assignment can't strand the repair. A single-link partition doesn't kill the spare —
/// SWIM refutes the resulting suspicion via indirect ping.
#[test]
#[serial_test::serial]
fn catch_up_redrive_recovers_dropped_assignment() -> turmoil::Result {
    use std::sync::{Arc, Mutex};

    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(400))
        .tcp_capacity(4096)
        .rng_seed(1)
        .build();

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    host_cluster(&mut sim, &NODES_4, |env| {
        env.node_id_suffix = Some("sim".to_string());
        env.vnodes_per_node = 16;
        env.segment_size_limit_bytes = 2048;
    });

    // The victim (a non-coordinator replica) is ring-determined; the client picks it at
    // runtime and hands it to the driver to crash.
    let victim = Arc::new(Mutex::new(None::<String>));
    let victim_w = victim.clone();

    sim.client("test-client", async move {
        const NODES: [(&str, u16); 4] = [
            ("node-1", 8081),
            ("node-2", 8082),
            ("node-3", 8083),
            ("node-4", 8084),
        ];
        const TOPIC: &str = "redrive";

        wait_for_cluster(&NODES, 4).await;
        create_topic_anywhere(TOPIC, &NODES, 3).await;

        let rec = |i: usize| -> Vec<u8> {
            let mut v = format!("rec-{i}").into_bytes();
            v.resize(1024, b'-');
            v
        };
        let leader = produce_until_acked(TOPIC, &rec(0), &NODES)
            .await
            .expect("first produce never acked");
        for i in 1..3 {
            let payload = rec(i);
            let mut acked = false;
            for _ in 0..40 {
                if produce_to(TOPIC, &payload, leader).await {
                    acked = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            assert!(acked, "produce {i} not acked");
        }

        let mut sealed = None;
        for _ in 0..80 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Some(detail) = describe_topic(TOPIC, &NODES).await
                && let Some(seg) = detail
                    .ranges
                    .first()
                    .and_then(|r| r.active_segment.as_ref())
                && *seg.start_entry_id > 0
            {
                let replicas = seg
                    .replica_set
                    .iter()
                    .map(|r| r.node_id.clone())
                    .collect::<Vec<String>>();
                sealed = Some((detail.topic_id, replicas));
                break;
            }
        }
        let (topic_id, replicas) = sealed.expect("segment never sealed");
        assert_eq!(replicas.len(), 3, "RF=3");

        // The coordinator (metadata leader) drives the repair; crash a *different*
        // replica so it survives, and partition the spare from it.
        let (_group, leader_id) = metadata_leader(TOPIC, &NODES)
            .await
            .expect("metadata leader never resolved");
        let coordinator = NODES
            .iter()
            .map(|(n, _)| *n)
            .find(|n| leader_id.starts_with(n))
            .expect("coordinator host");
        let victim_node = NODES
            .iter()
            .map(|(n, _)| *n)
            .find(|n| *n != coordinator && replicas.iter().any(|r| r.starts_with(n)))
            .expect("a non-coordinator replica to kill");
        let spare = NODES
            .iter()
            .copied()
            .find(|(n, _)| !replicas.iter().any(|r| r.starts_with(n)))
            .expect("a spare node");
        tracing::info!("[REDRIVE] coordinator={coordinator} victim={victim_node} spare={spare:?}");

        // Drop the coordinator→spare link, then trigger the repair: the assignment and
        // its redrives are lost while partitioned.
        turmoil::partition(spare.0, coordinator);
        *victim_w.lock().unwrap() = Some(victim_node.to_string());

        let fetch = ClientRequest::DataPlane(ClientDataPlaneRequest::FetchById(FetchByIdRequest {
            topic_id,
            range_id: RangeId(0),
            entry_id: EntryId(0),
            max_bytes: 1 << 20,
        }));

        // The death is detected, the segment reassigned, and the assignment (re)driven —
        // all dropped while partitioned. Confirm the spare can't serve: with the
        // assignment lost it has nothing to catch up from. (If it serves here, the
        // partition didn't actually drop the assignment — the test would be vacuous.)
        tokio::time::sleep(Duration::from_secs(15)).await;
        for _ in 0..5 {
            if let ClientResponse::DataPlane(DataPlaneResponse::Fetched { entries, .. }) =
                send_request(spare.0, spare.1, fetch.clone()).await
            {
                assert!(
                    entries.is_empty() || entries[0].data[..] != rec(0)[..],
                    "spare served rec-0 while partitioned — the catch-up assignment wasn't dropped"
                );
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Heal: the next redrive delivers; the spare pulls from a surviving source
        // (never partitioned) and serves rec-0.
        turmoil::repair(spare.0, coordinator);
        let mut served = false;
        for _ in 0..60 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let ClientResponse::DataPlane(DataPlaneResponse::Fetched { entries, .. }) =
                send_request(spare.0, spare.1, fetch.clone()).await
                && !entries.is_empty()
                && entries[0].data[..] == rec(0)[..]
            {
                served = true;
                break;
            }
        }
        assert!(
            served,
            "redrive did not recover the dropped assignment: spare {spare:?} never served rec-0"
        );

        Ok(())
    });

    while victim.lock().unwrap().is_none() {
        assert!(
            sim.elapsed() < Duration::from_secs(200),
            "client never chose a victim"
        );
        sim.step()?;
    }
    let victim_node = victim.lock().unwrap().clone().unwrap();
    tracing::info!(
        "[REDRIVE] crashing {victim_node} at {}s",
        sim.elapsed().as_secs()
    );
    sim.crash(victim_node);

    sim.run()
}

/// End-to-end recovery lottery (D5). A node holding a sealed segment is hard-killed and
/// restarted with the **same data dir** but a **fresh NodeId** (SWIM's rejoin model).
/// On restart it recovers the segment from disk into its inventory — an orphan candidate
/// (no replica set names the fresh id). Then, because the cluster is back to 3 live
/// identities at RF=3, the coordinator reassigns the segment to the restarted node, whose
/// catch-up is a **zero-transfer full match** against the recovered inventory
/// (`catch_up_full_match_registers_without_transfer`). The node then serves the old data.
/// This is "the recovery payoff is observable only as a zero-transfer catch-up": the
/// restarted node's on-disk copy is *reused* rather than re-downloaded, and orphan GC's
/// confirmation-typed deletion (`recovery::orphan`) spares it because it is, by delete
/// time, named again. See `docs/data-plane/d5_crash_recovery.md` § "Orphaned Data
/// Cleanup".
///
/// Specifically exercises the **capacity-return re-fill** (raft-actor.md #10), the harder
/// path: the driver does *not* race the restart against death detection. It waits for the
/// cluster to detect node-3 dead and **shrink** the sealed segment to its two survivors
/// (a death with no live replacement at RF=3), and only then restarts node-3 with a fresh
/// id. The shrunk segment names none of node-3's identities, so a death-only repair would
/// never revisit it — the segment has no dead member left to flag. The periodic ring check
/// re-fills it back toward RF, reassigns it to the rejoined node (now a ring member again),
/// and that node's catch-up is the zero-transfer full match. Without the re-fill this test
/// would hang forever.
#[test]
#[serial_test::serial]
fn restarted_node_reuses_recovered_segment_on_reassignment() -> turmoil::Result {
    use std::sync::{Arc, Mutex};

    let mut sim = Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(600))
        .tcp_capacity(4096)
        .rng_seed(1)
        .build();

    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // Per-run-unique but bounce-stable data dirs: the restarted node must find its own
    // segments on disk. `node_id_suffix` is left unset so the bounce mints a fresh id
    // (D5's "restart = new member"); only the data dir is pinned.
    let run_id = uuid::Uuid::new_v4().to_string();
    for (name, idx, cp, rp) in [
        ("node-1", 1u32, 8081u16, 18001u16),
        ("node-2", 2, 8082, 18002),
        ("node-3", 3, 8083, 18003),
    ] {
        let run_id = run_id.clone();
        sim.host(name, move || {
            let run_id = run_id.clone();
            async move {
                let me = turmoil::lookup(name);
                let seeds: Vec<String> =
                    [("node-1", 18001u16), ("node-2", 18002), ("node-3", 18003)]
                        .iter()
                        .filter(|(n, _)| *n != name)
                        .map(|(n, p)| format!("{}:{}", turmoil::lookup(*n), p))
                        .collect();
                let mut env = default_env(idx, name.to_string(), cp, rp);
                env.advertise_host = Some(me.to_string());
                env.join_seed_nodes = seeds;
                env.vnodes_per_node = 16;
                env.segment_size_limit_bytes = 2048;
                // Pin only the data dir (survives the bounce → recovery finds segments).
                env.data_dir = std::env::temp_dir()
                    .join(format!("eg-lottery-{run_id}-{idx}"))
                    .to_string_lossy()
                    .into_owned();
                StartUp::with_env(env, 0).run().await?;
                Ok(())
            }
        });
    }

    // The client seals a segment, then signals the driver it's safe to crash node-3.
    let ready = Arc::new(Mutex::new(false));
    let ready_w = ready.clone();

    sim.client("test-client", async move {
        const NODES: [(&str, u16); 3] = [("node-1", 8081), ("node-2", 8082), ("node-3", 8083)];
        const VICTIM: (&str, u16) = ("node-3", 8083);
        const TOPIC: &str = "lottery";

        wait_for_cluster(&NODES, 3).await;
        create_topic_anywhere(TOPIC, &NODES, 3).await;

        let rec = |i: usize| -> Vec<u8> {
            let mut v = format!("rec-{i}").into_bytes();
            v.resize(1024, b'-');
            v
        };
        let leader = produce_until_acked(TOPIC, &rec(0), &NODES)
            .await
            .expect("first produce never acked");
        for i in 1..3 {
            let payload = rec(i);
            let mut acked = false;
            for _ in 0..40 {
                if produce_to(TOPIC, &payload, leader).await {
                    acked = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            assert!(acked, "produce {i} not acked");
        }

        // Wait for the segment to seal (RF=3 → node-3 holds it).
        let mut topic_id = None;
        for _ in 0..80 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Some(detail) = describe_topic(TOPIC, &NODES).await
                && let Some(seg) = detail
                    .ranges
                    .first()
                    .and_then(|r| r.active_segment.as_ref())
                && *seg.start_entry_id > 0
            {
                topic_id = Some(detail.topic_id);
                break;
            }
        }
        let topic_id = topic_id.expect("segment never sealed");

        // Give the checkpoint worker time to write node-3's sealed segment file to disk
        // (so recovery has something to find after the restart).
        tokio::time::sleep(Duration::from_secs(10)).await;
        *ready_w.lock().unwrap() = true;

        // After the driver crashes + bounces node-3, it comes back with a fresh id, no
        // Raft state, but its recovered segment on disk. The cluster reassigns the
        // segment to it; its catch-up is a zero-transfer full match, then it serves rec-0.
        // Poll tolerantly — node-3 is unreachable during the crash/bounce window.
        let fetch = ClientRequest::DataPlane(ClientDataPlaneRequest::FetchById(FetchByIdRequest {
            topic_id,
            range_id: RangeId(0),
            entry_id: EntryId(0),
            max_bytes: 1 << 20,
        }));
        let mut served = false;
        for _ in 0..180 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Some(ClientResponse::DataPlane(DataPlaneResponse::Fetched { entries, .. })) =
                try_send_request(VICTIM.0, VICTIM.1, fetch.clone()).await
                && !entries.is_empty()
                && entries[0].data[..] == rec(0)[..]
            {
                served = true;
                break;
            }
        }
        assert!(
            served,
            "restarted node-3 never served rec-0 — recovered segment was not reused on reassignment"
        );

        Ok(())
    });

    // Driver: step until the segment is sealed + checkpointed, then crash node-3 and,
    // after the death is detected, bounce it (restart → recover with a fresh id).
    while !*ready.lock().unwrap() {
        assert!(
            sim.elapsed() < Duration::from_secs(250),
            "client never signalled ready"
        );
        sim.step()?;
    }
    tracing::info!("[LOTTERY] crashing node-3 at {}s", sim.elapsed().as_secs());
    sim.crash("node-3");
    // Deliberately wait out SWIM death detection + the leader's death-repair, which shrinks
    // the sealed segment to the two survivors (no live replacement at RF=3 with 2 nodes).
    // Only *after* the shrink do we restart node-3 — so what brings its data back is the
    // capacity-return re-fill (next ring check grows the segment toward RF and reassigns it
    // to the rejoined node), not a race-win. 35s comfortably exceeds detection + a possible
    // leader election; it's virtual time, so wall-clock cost is negligible.
    let crash_at = sim.elapsed();
    while sim.elapsed() < crash_at + Duration::from_secs(35) {
        sim.step()?;
    }
    tracing::info!(
        "[LOTTERY] restarting node-3 at {}s",
        sim.elapsed().as_secs()
    );
    sim.bounce("node-3");

    sim.run()
}

/// Resolve the topic shard group's metadata leader: `(shard_group_id, leader node id)`.
/// Reads the group + members from any node (a ring lookup), then polls members for an
/// authoritative Raft leader (a non-member's `GetShardLeader` is `None`).
async fn metadata_leader(topic: &str, nodes: &[(&str, u16)]) -> Option<(ShardGroupId, String)> {
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut info = None;
        for &(h, p) in nodes {
            if let Ok(Some(d)) = query_shard_info(h, p, topic.as_bytes()).await {
                info = Some(d);
                break;
            }
        }
        let Some(d) = info else { continue };
        for member in d.member_node_ids.iter() {
            if let Some(&(h, p)) = nodes.iter().find(|(n, _)| member.starts_with(n))
                && let Ok(Some(leader)) = query_shard_leader(h, p, d.shard_group_id).await
            {
                return Some((d.shard_group_id, leader));
            }
        }
    }
    None
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

/// Map a redirect address back to a test node by client port (unique per node).
fn redirect_addr_to_node<'a>(
    addr: SocketAddr,
    nodes: &'a [(&'a str, u16)],
) -> Option<(&'a str, u16)> {
    nodes.iter().copied().find(|&(_, port)| port == addr.port())
}

/// Create a topic, following the `NotRaftLeader` redirect to the leader. Sweeps all
/// nodes too — a fresh cluster may have no leader yet.
async fn create_topic_anywhere(topic: &str, nodes: &[(&str, u16)], replication_factor: u64) {
    let req = ClientRequest::ControlPlane(ControlPlaneRequest::CreateTopic {
        name: topic.into(),
        storage_policy: StoragePolicy {
            retention_ms: Some(3_600_000),
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
                ClientResponse::ControlPlane(ControlPlaneResponse::NotRaftLeader {
                    leader_addr: Some(info),
                }) => {
                    if let Some((lh, lp)) = redirect_addr_to_node(info.client_addr, nodes)
                        && matches!(
                            send_request(lh, lp, req.clone()).await,
                            ClientResponse::ControlPlane(
                                ControlPlaneResponse::TopicCreated
                                    | ControlPlaneResponse::AlreadyExists
                            )
                        )
                    {
                        return;
                    }
                }
                _ => {}
            }
        }
    }
    panic!("CreateTopic({topic}) not acked by any node");
}

/// Produce one record, following a redirect to the segment's write leader; returns
/// the acking leader. Sweeps all nodes so it's robust when any node is down
/// (crash/repair tests).
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
            match produce_once(topic, payload, (host, port)).await {
                ProduceOutcome::Acked => return Some((host, port)),
                ProduceOutcome::Redirect(addr) => {
                    if let Some(leader) = redirect_addr_to_node(addr, nodes)
                        && matches!(
                            produce_once(topic, payload, leader).await,
                            ProduceOutcome::Acked
                        )
                    {
                        return Some(leader);
                    }
                }
                ProduceOutcome::Retry => {}
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    None
}

enum ProduceOutcome {
    Acked,
    /// Redirect to the write leader at this address.
    Redirect(SocketAddr),
    /// Transient (not-yet-leader, mid-split, error) — back off and retry.
    Retry,
}

/// Produce one record to a specific node. Returns true if it was accepted.
async fn produce_to(topic: &str, payload: &[u8], node: (&str, u16)) -> bool {
    matches!(
        produce_once(topic, payload, node).await,
        ProduceOutcome::Acked
    )
}

/// Send one produce to a specific node and classify the response.
async fn produce_once(topic: &str, payload: &[u8], node: (&str, u16)) -> ProduceOutcome {
    let range_id = match send_request(
        node.0,
        node.1,
        ClientRequest::ControlPlane(ControlPlaneRequest::DescribeTopic { name: topic.into() }),
    )
    .await
    {
        ClientResponse::ControlPlane(ControlPlaneResponse::TopicDetail(d)) => match d
            .ranges
            .iter()
            .filter(|r| {
                r.state == RangeState::Active && r.keyspace_start.as_slice() <= b"k".as_slice()
            })
            .max_by_key(|r| &r.keyspace_start)
        {
            Some(range) => range.range_id,
            None => return ProduceOutcome::Retry,
        },
        _ => return ProduceOutcome::Retry,
    };

    let req = ClientRequest::DataPlane(ClientDataPlaneRequest::Produce(ProduceRequest {
        topic_name: topic.into(),
        range_id,
        routing_key: b"k".to_vec(),
        data: payload.to_vec(),
        record_count: 1,
    }));
    match send_request(node.0, node.1, req).await {
        ClientResponse::DataPlane(DataPlaneResponse::Produced { .. }) => ProduceOutcome::Acked,
        ClientResponse::DataPlane(DataPlaneResponse::NotWriteLeader {
            leader_addr: Some(addr),
        }) => ProduceOutcome::Redirect(addr),
        ClientResponse::DataPlane(DataPlaneResponse::ShardNotLocal {
            hint_node: Some(addr),
        }) => ProduceOutcome::Redirect(addr),
        _ => ProduceOutcome::Retry,
    }
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
