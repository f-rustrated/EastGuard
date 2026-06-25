//! End-to-end tests for the C1 client SDK against the simulated cluster.
//!
//! These exercise the SDK's own machinery — the multiplexed connection pool, the
//! routing cache, and the one redirect-follow loop — rather than the raw wire
//! helpers the other e2e tests use. Each runs on a fresh 3-node turmoil cluster
//! with the RNG and node ids pinned for reproducibility (CLAUDE.md "Testing").

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use turmoil::Builder;

use super::{NodeSpec, host_cluster};
use crate::client::{Client, ClientError, PartitionStrategy, RetryPolicy, StoragePolicy};
use crate::config::Environment;

/// (name, client_port, raft/cluster_port) for the standard 3-node cluster.
static NODES: [NodeSpec; 3] = [
    ("node-1", 8081, 18001),
    ("node-2", 8082, 18002),
    ("node-3", 8083, 18003),
];

/// Per-test cluster tweaks for the SDK suite: pinned node-id suffix + low vnode
/// count keep the run deterministic and fast (no node is restarted in these tests).
fn sim_cluster(env: &mut Environment) {
    env.node_id_suffix = Some("sim".to_string());
    env.vnodes_per_node = 16;
}

/// Client seeds = each node's resolved client address. Call inside a sim task.
fn client_seeds() -> Vec<SocketAddr> {
    NODES
        .iter()
        .map(|(name, cp, _)| SocketAddr::new(turmoil::lookup(*name), *cp))
        .collect()
}

fn policy() -> StoragePolicy {
    StoragePolicy {
        retention_ms: Some(3_600_000),
        replication_factor: 3,
        partition_strategy: PartitionStrategy::AutoSplit,
    }
}

// No retry helpers: the SDK owns retry-within-deadline, so a single `create_topic` /
// `produce` / `describe_topic` call already rides out leader election, metadata
// propagation lag, and segment-assignment lag. Tests call the SDK directly.

fn build_sim(secs: u64) -> turmoil::Sim<'static> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
    Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(secs))
        .tcp_capacity(4096)
        .rng_seed(1)
        .build()
}

/// First contact: a fresh client creates + describes a topic, following the
/// control-plane redirects (`NotRaftLeader`, `TopicMetadataRedirect`) on its own.
#[test]
#[serial_test::serial]
fn client_first_contact_resolves_via_redirect() -> turmoil::Result {
    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, sim_cluster);

    sim.client("test-client", async {
        let client = Client::connect(client_seeds()).expect("client connects");
        // One call each — the SDK retries through election + metadata propagation.
        client
            .create_topic("first-contact", policy())
            .await
            .expect("create resolves to the raft leader");

        let detail = client
            .resolve_topic("first-contact")
            .await
            .expect("describe resolves through redirects + propagation lag");
        assert_eq!(detail.name, "first-contact");
        assert!(!detail.ranges.is_empty(), "topic must have ranges");
        assert!(
            detail.ranges.iter().any(|r| r.active_segment.is_some()),
            "an active range with a placed segment must exist"
        );
        Ok(())
    });

    sim.run()
}

/// Produce routes to the segment write leader through the cache, following
/// `NotWriteLeader` if the first hop is wrong, and commits — entry ids advance.
#[test]
#[serial_test::serial]
fn client_produce_routes_to_write_leader() -> turmoil::Result {
    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, sim_cluster);

    sim.client("test-client", async {
        let client = Client::connect(client_seeds()).expect("client connects");
        client
            .create_topic("produce-route", policy())
            .await
            .expect("create topic");

        // The SDK absorbs the post-create segment-assignment lag inside produce.
        let first = client
            .produce("produce-route", b"k", b"rec-0".to_vec(), 1)
            .await
            .expect("first produce acked");
        let second = client
            .produce("produce-route", b"k", b"rec-1".to_vec(), 1)
            .await
            .expect("second produce acked");
        assert!(
            second > first,
            "entry ids must advance within a range: {first} -> {second}"
        );
        Ok(())
    });

    sim.run()
}

/// Concurrent requests on one client all get the right reply — the read loop demuxes
/// by request id, so no produce or describe receives another's response.
#[test]
#[serial_test::serial]
fn client_multiplexes_concurrent_requests() -> turmoil::Result {
    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, sim_cluster);

    sim.client("test-client", async {
        let client = Client::connect(client_seeds()).expect("client connects");
        client.create_topic("mux", policy()).await.expect("create");
        // Warm the cache + segment assignment so the burst below routes directly.
        let warm = client
            .produce("mux", b"k", b"warm".to_vec(), 1)
            .await
            .expect("warmup produce");

        // Four produces + two describes concurrently over the shared connection.
        let (a, b, c, d, e, f) = tokio::join!(
            client.produce("mux", b"k", b"p1".to_vec(), 1),
            client.produce("mux", b"k", b"p2".to_vec(), 1),
            client.produce("mux", b"k", b"p3".to_vec(), 1),
            client.produce("mux", b"k", b"p4".to_vec(), 1),
            client.resolve_topic("mux"),
            client.resolve_topic("mux"),
        );

        let mut ids = [
            a.expect("p1"),
            b.expect("p2"),
            c.expect("p3"),
            d.expect("p4"),
        ];
        e.expect("describe 1 demuxed correctly");
        f.expect("describe 2 demuxed correctly");

        // Distinct ids ⇒ every ack reached its own waiter (no cross-delivery).
        ids.sort_unstable();
        for pair in ids.windows(2) {
            assert_ne!(pair[0], pair[1], "produce acks must be distinct");
        }
        assert!(ids.iter().all(|&id| id != warm));
        Ok(())
    });

    sim.run()
}

/// Resilience after a node crash: with connections warmed to every node, one is
/// crashed and the client still resolves — the pool drops the dead conn, the loop
/// bounces to a survivor.
#[test]
#[serial_test::serial]
fn client_reconnects_after_node_crash() -> turmoil::Result {
    let mut sim = build_sim(200);
    host_cluster(&mut sim, &NODES, sim_cluster);

    // Client names the victim once warmed; the driver crashes it and runs the sim out.
    let victim = Arc::new(Mutex::new(None::<String>));
    let victim_w = victim.clone();

    sim.client("test-client", async move {
        let client = Client::connect(client_seeds()).expect("client connects");
        client
            .create_topic("reconnect", policy())
            .await
            .expect("create");
        client
            .produce("reconnect", b"k", b"rec-0".to_vec(), 1)
            .await
            .expect("produce");

        // A few describes so the pool opens a connection to every node (incl. the victim).
        for _ in 0..NODES.len() * 2 {
            let _ = client.resolve_topic("reconnect").await;
        }

        *victim_w.lock().unwrap() = Some("node-1".to_string());

        // After the crash + SWIM convergence, one describe still resolves — the retry
        // drops the dead conn and re-resolves to a survivor, no caller-side loop.
        tokio::time::sleep(Duration::from_secs(10)).await;
        client
            .resolve_topic("reconnect")
            .await
            .expect("client resolves the topic after a node crash");
        Ok(())
    });

    // Driver: step until the client names the victim, crash it, run to completion.
    while victim.lock().unwrap().is_none() {
        assert!(
            sim.elapsed() < Duration::from_secs(150),
            "client never signaled readiness"
        );
        sim.step()?;
    }
    let victim_node = victim.lock().unwrap().clone().unwrap();
    tracing::info!(
        "[RECONNECT] crashing {victim_node} at {}s",
        sim.elapsed().as_secs()
    );
    sim.crash(victim_node);

    sim.run()
}

/// Stale-cache correction (doc item 5): a wrong-but-live cached leader is fixed by one
/// produce — `NotWriteLeader` is followed in one hop, the stale entry dropped, and the
/// next produce routes straight to the real leader.
#[test]
#[serial_test::serial]
fn client_corrects_stale_write_leader() -> turmoil::Result {
    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, sim_cluster);

    sim.client("test-client", async {
        let client = Client::connect(client_seeds()).expect("client connects");
        client
            .create_topic("stale", policy())
            .await
            .expect("create");
        // Seed the cache via `describe` (a produce could be redirected and invalidate it).
        client
            .resolve_topic("stale")
            .await
            .expect("describe seeds the routing cache");

        let real = client
            .cached_write_leader("stale", b"k")
            .expect("write leader cached after describe");
        // Poison the cache: point at a different live node (a replica, not the leader).
        let wrong = client_seeds()
            .into_iter()
            .find(|a| *a != real)
            .expect("another node");
        client.poison_cache("stale", wrong);
        assert_eq!(
            client.cached_write_leader("stale", b"k"),
            Some(wrong),
            "cache is now stale"
        );

        // Starts at `wrong`, gets NotWriteLeader{real}, follows once, commits.
        client
            .produce("stale", b"k", b"rec-1".to_vec(), 1)
            .await
            .expect("produce self-corrects through the redirect");

        // The redirect invalidated the stale entry; the next produce re-resolves.
        assert!(
            client.cached_write_leader("stale", b"k").is_none(),
            "stale entry dropped after the corrected produce"
        );
        client
            .produce("stale", b"k", b"rec-2".to_vec(), 1)
            .await
            .expect("produce after re-resolve");
        assert_eq!(
            client.cached_write_leader("stale", b"k"),
            Some(real),
            "re-resolved back to the real write leader"
        );
        Ok(())
    });

    sim.run()
}

/// Unreachable cluster: a call returns `Timeout` at the deadline rather than hanging
/// (guards the per-attempt deadline bound).
#[test]
#[serial_test::serial]
fn client_call_times_out_when_unreachable() -> turmoil::Result {
    let mut sim = build_sim(60);
    host_cluster(&mut sim, &NODES, sim_cluster); // hosts exist (so lookup resolves), but we seed a dead port

    sim.client("test-client", async {
        // node-1 is up, but nothing listens on 9999 — every attempt fails to connect.
        let bogus = SocketAddr::new(turmoil::lookup("node-1"), 9999);
        let policy = RetryPolicy {
            deadline: Duration::from_secs(2),
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
        };
        let client = Client::connect_with(vec![bogus], policy).expect("client connects");

        let start = tokio::time::Instant::now();
        let err = client
            .resolve_topic("nope")
            .await
            .expect_err("an unreachable cluster cannot resolve");
        assert!(
            matches!(err, ClientError::Timeout { .. }),
            "expected Timeout, got {err:?}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "call returned near the deadline, not hung"
        );
        Ok(())
    });

    sim.run()
}

/// Node discovery: a client bootstrapped from a single seed grows its contact pool as
/// it learns the rest of the cluster from redirects and describe replica sets.
#[test]
#[serial_test::serial]
fn client_discovers_nodes_beyond_the_seed() -> turmoil::Result {
    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, sim_cluster);

    sim.client("test-client", async {
        // Bootstrap from one node only.
        let one = SocketAddr::new(turmoil::lookup(NODES[0].0), NODES[0].1);
        let client = Client::connect(vec![one]).expect("client connects");
        assert_eq!(client.known_node_count(), 1, "starts with just the seed");

        client
            .create_topic("discover", policy())
            .await
            .expect("create");
        // Resolve until SWIM has filled in every replica address (a describe that runs
        // before convergence may omit a not-yet-known node).
        for _ in 0..10 {
            client.resolve_topic("discover").await.expect("resolve");
            if client.known_node_count() == NODES.len() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        assert_eq!(
            client.known_node_count(),
            NODES.len(),
            "discovered the whole cluster from one seed"
        );
        Ok(())
    });

    sim.run()
}

/// Producer uses Lz4 compression and the server stores the compressed batch byte-for-byte.
#[test]
#[serial_test::serial]
fn producer_compression_lz4_end_to_end() -> turmoil::Result {
    use crate::connections::protocol::{
        ClientDataPlaneRequest, DataPlaneResponse, FetchByIdRequest,
    };

    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, sim_cluster);

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));
        client
            .create_topic("compressed-topic", policy())
            .await
            .expect("create topic");

        // Warm the cache
        client
            .resolve_topic("compressed-topic")
            .await
            .expect("resolve");

        let producer = crate::client::Producer::new(
            client.clone(),
            "compressed-topic".to_string(),
            crate::client::ProducerConfig {
                buffer: crate::client::BufferConfig {
                    linger: Duration::from_millis(50),
                    max_batch_bytes: 1024 * 1024,
                    max_batch_records: 100,
                },
                codec: crate::client::CompressionCodec::Lz4,
            },
        );

        // Send 2 records concurrently so they get batched and compressed
        let (res1, res2) = tokio::join!(
            producer.send(b"ckey", b"cval1".to_vec()),
            producer.send(b"ckey", b"cval2".to_vec()),
        );

        let entry_id = res1.expect("send 1");
        let entry_id2 = res2.expect("send 2");
        assert_eq!(entry_id, entry_id2);

        // Resolve topic to get topic_id and range_id
        let detail = client
            .resolve_topic("compressed-topic")
            .await
            .expect("resolve");
        let topic_id = detail.topic_id;
        let range = &detail.ranges[0];
        let range_id = range.range_id;
        let leader_addr = range.active_segment.as_ref().unwrap().replica_set[0].client_addr;

        // Perform a raw FetchById to retrieve the exact stored payload from the leader
        let fetch_req = FetchByIdRequest {
            topic_id,
            range_id,
            entry_id,
            max_bytes: 65536,
        };

        let served = client
            .call(leader_addr, ClientDataPlaneRequest::FetchById(fetch_req))
            .await
            .expect("fetch");
        if let crate::connections::protocol::ClientResponse::DataPlane(
            DataPlaneResponse::Fetched { entries, .. },
        ) = served.response
        {
            assert_eq!(entries.len(), 1);
            let entry = &entries[0];
            assert_eq!(entry.entry_id, entry_id);
            assert_eq!(entry.record_count, 2);

            // Verify that the first byte of the stored payload is indeed the Lz4 codec tag (1)
            assert_eq!(
                entry.data[0],
                crate::client::CompressionCodec::Lz4 as u8,
                "Stored payload must lead with Lz4 tag"
            );

            // Decompress and decode records from the retrieved payload
            let decoded_records =
                crate::client::CompressionCodec::decode_payload(&entry.data, entry.record_count)
                    .expect("Decompress and decode batch");

            assert_eq!(decoded_records.len(), 2);
            assert_eq!(decoded_records[0].key, b"ckey");
            assert_eq!(decoded_records[0].value, b"cval1");
            assert_eq!(decoded_records[1].key, b"ckey");
            assert_eq!(decoded_records[1].value, b"cval2");
        } else {
            panic!("Expected Fetched response, got {:?}", served.response);
        }

        Ok(())
    });

    sim.run()
}

/// Producer uses Zstd compression and the server stores the compressed batch byte-for-byte.
#[test]
#[serial_test::serial]
fn producer_compression_zstd_end_to_end() -> turmoil::Result {
    use crate::connections::protocol::{
        ClientDataPlaneRequest, DataPlaneResponse, FetchByIdRequest,
    };

    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, sim_cluster);

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));
        client
            .create_topic("zstd-topic", policy())
            .await
            .expect("create topic");

        // Warm the cache
        client.resolve_topic("zstd-topic").await.expect("resolve");

        let producer = crate::client::Producer::new(
            client.clone(),
            "zstd-topic".to_string(),
            crate::client::ProducerConfig {
                buffer: crate::client::BufferConfig {
                    linger: Duration::from_millis(50),
                    max_batch_bytes: 1024 * 1024,
                    max_batch_records: 100,
                },
                codec: crate::client::CompressionCodec::Zstd,
            },
        );

        // Send 2 records concurrently so they get batched and compressed
        let (res1, res2) = tokio::join!(
            producer.send(b"zkey", b"zval1".to_vec()),
            producer.send(b"zkey", b"zval2".to_vec()),
        );

        let entry_id = res1.expect("send 1");
        let entry_id2 = res2.expect("send 2");
        assert_eq!(entry_id, entry_id2);

        // Resolve topic to get topic_id and range_id
        let detail = client.resolve_topic("zstd-topic").await.expect("resolve");
        let topic_id = detail.topic_id;
        let range = &detail.ranges[0];
        let range_id = range.range_id;
        let leader_addr = range.active_segment.as_ref().unwrap().replica_set[0].client_addr;

        // Perform a raw FetchById to retrieve the exact stored payload from the leader
        let fetch_req = FetchByIdRequest {
            topic_id,
            range_id,
            entry_id,
            max_bytes: 65536,
        };

        let served = client
            .call(leader_addr, ClientDataPlaneRequest::FetchById(fetch_req))
            .await
            .expect("fetch");
        if let crate::connections::protocol::ClientResponse::DataPlane(
            DataPlaneResponse::Fetched { entries, .. },
        ) = served.response
        {
            assert_eq!(entries.len(), 1);
            let entry = &entries[0];
            assert_eq!(entry.entry_id, entry_id);
            assert_eq!(entry.record_count, 2);

            // Verify that the first byte of the stored payload is indeed the Zstd codec tag (2)
            assert_eq!(
                entry.data[0],
                crate::client::CompressionCodec::Zstd as u8,
                "Stored payload must lead with Zstd tag"
            );

            // Decompress and decode records from the retrieved payload
            let decoded_records =
                crate::client::CompressionCodec::decode_payload(&entry.data, entry.record_count)
                    .expect("Decompress and decode batch");

            assert_eq!(decoded_records.len(), 2);
            assert_eq!(decoded_records[0].key, b"zkey");
            assert_eq!(decoded_records[0].value, b"zval1");
            assert_eq!(decoded_records[1].key, b"zkey");
            assert_eq!(decoded_records[1].value, b"zval2");
        } else {
            panic!("Expected Fetched response, got {:?}", served.response);
        }

        Ok(())
    });

    sim.run()
}

/// Stress: 50 concurrent tasks firing sends independently.
/// Verifies thread-safety, lock-free sequencing, and robust batching under load.
#[test]
#[serial_test::serial]
fn producer_concurrency_stress() -> turmoil::Result {
    use std::collections::HashSet;

    let mut sim = build_sim(120);
    host_cluster(&mut sim, &NODES, sim_cluster);

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));
        client
            .create_topic("prod-stress", policy())
            .await
            .expect("create topic");

        // Warm the cache
        client.resolve_topic("prod-stress").await.expect("resolve");

        let producer = Arc::new(crate::client::Producer::new(
            client.clone(),
            "prod-stress".to_string(),
            crate::client::ProducerConfig {
                buffer: crate::client::BufferConfig {
                    linger: Duration::from_millis(30),
                    max_batch_bytes: 1024 * 1024,
                    max_batch_records: 100,
                },
                codec: crate::client::CompressionCodec::None,
            },
        ));

        const TASKS: usize = 50;
        const RECORDS_PER_TASK: usize = 10;

        let mut join_set = tokio::task::JoinSet::new();

        for t in 0..TASKS {
            let producer = producer.clone();
            join_set.spawn(async move {
                let mut ids = Vec::with_capacity(RECORDS_PER_TASK);
                for r in 0..RECORDS_PER_TASK {
                    // Alternate keys to write to different ranges or share them
                    let key = format!("key-{}", (t * RECORDS_PER_TASK + r) % 5).into_bytes();
                    let val = format!("val-{}-{}", t, r).into_bytes();
                    let id = producer.send(&key, val).await?;
                    ids.push(id);
                }
                Ok::<_, ClientError>(ids)
            });
        }

        let mut all_ids = Vec::with_capacity(TASKS * RECORDS_PER_TASK);
        while let Some(res) = join_set.join_next().await {
            let ids = res.expect("Task did not panic").expect("Send successful");
            all_ids.extend(ids);
        }

        assert_eq!(
            all_ids.len(),
            TASKS * RECORDS_PER_TASK,
            "All records must be sent"
        );

        // Assert that batching actually happened: the number of unique entry IDs
        // must be significantly less than the total number of records (500).
        let unique_ids: HashSet<u64> = all_ids.into_iter().collect();
        assert!(
            unique_ids.len() < (TASKS * RECORDS_PER_TASK) / 2,
            "Records must be batched; unique entry IDs: {}, total: {}",
            unique_ids.len(),
            TASKS * RECORDS_PER_TASK
        );

        Ok(())
    });

    sim.run()
}
