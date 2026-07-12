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

use super::host_cluster;
use crate::client::{
    BufferConfig, Client, ClientError, CompressionCodec, Consumer, ConsumerConfig, KeyInterest,
    PartitionStrategy, Producer, ProducerConfig, RetryPolicy, StartPolicy, StoragePolicy,
    TopicDetail,
};
use crate::config::Environment;
use crate::connections::protocol::ClientResponse;
use crate::control_plane::metadata::{EntryId, RangeId};
use crate::it::e2e::{NODES, NODES_4};

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

fn client_seeds_4() -> Vec<SocketAddr> {
    NODES_4
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

        let producer = Producer::new(
            client.clone(),
            "compressed-topic".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(50),
                    max_batch_bytes: 1024 * 1024,
                    max_batch_records: 100,
                },
                codec: CompressionCodec::Lz4,
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
        if let ClientResponse::DataPlane(DataPlaneResponse::Fetched { entries, .. }) =
            served.response
        {
            assert_eq!(entries.len(), 1);
            let entry = &entries[0];
            assert_eq!(entry.entry_id, entry_id);
            assert_eq!(entry.record_count, 2);

            // Verify that the first byte of the stored payload is indeed the Lz4 codec tag (1)
            assert_eq!(
                entry.data[0],
                CompressionCodec::Lz4 as u8,
                "Stored payload must lead with Lz4 tag"
            );

            // Decompress and decode records from the retrieved payload
            let decoded_records = CompressionCodec::decode_payload(&entry.data, entry.record_count)
                .expect("Decompress and decode batch");

            assert_eq!(decoded_records.len(), 2);
            assert_eq!(decoded_records[0].0, b"ckey");
            assert_eq!(decoded_records[0].1, b"cval1");
            assert_eq!(decoded_records[1].0, b"ckey");
            assert_eq!(decoded_records[1].1, b"cval2");
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

        let producer = Producer::new(
            client.clone(),
            "zstd-topic".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(50),
                    max_batch_bytes: 1024 * 1024,
                    max_batch_records: 100,
                },
                codec: CompressionCodec::Zstd,
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
        if let ClientResponse::DataPlane(DataPlaneResponse::Fetched { entries, .. }) =
            served.response
        {
            assert_eq!(entries.len(), 1);
            let entry = &entries[0];
            assert_eq!(entry.entry_id, entry_id);
            assert_eq!(entry.record_count, 2);

            // Verify that the first byte of the stored payload is indeed the Zstd codec tag (2)
            assert_eq!(
                entry.data[0],
                CompressionCodec::Zstd as u8,
                "Stored payload must lead with Zstd tag"
            );

            // Decompress and decode records from the retrieved payload
            let decoded_records = CompressionCodec::decode_payload(&entry.data, entry.record_count)
                .expect("Decompress and decode batch");

            assert_eq!(decoded_records.len(), 2);
            assert_eq!(decoded_records[0].0, b"zkey");
            assert_eq!(decoded_records[0].1, b"zval1");
            assert_eq!(decoded_records[1].0, b"zkey");
            assert_eq!(decoded_records[1].1, b"zval2");
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

        let producer = Arc::new(Producer::new(
            client.clone(),
            "prod-stress".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(30),
                    max_batch_bytes: 1024 * 1024,
                    max_batch_records: 100,
                },
                codec: CompressionCodec::None,
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
        let unique_ids: HashSet<EntryId> = all_ids.into_iter().collect();
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

/// Exercises the exact overlapping linger task scenario:
/// 1) Record 1 is sent -> Linger 1 spawned (100ms).
/// 2) Wait 40ms. Send enough records to trigger a size/count threshold flush.
///    This flushes the buffer immediately and resets spawned_linger to false.
/// 3) Send Record 3 (does not hit threshold). Since spawned_linger is false,
///    it sets spawned_linger = true and spawns Linger 2 (100ms).
/// 4) Wait 80ms (virtual time 120ms, past Linger 1's 100ms expiration).
///    Linger 1 wakes up, calls take(), finds Record 3, and flushes it early.
///    This resets spawned_linger to false.
/// 5) Wait another 80ms (virtual time 200ms, past Linger 2's 100ms expiration).
///    Linger 2 wakes up, calls take(), finds the buffer empty, and safely no-ops.
#[test]
#[serial_test::serial]
fn producer_overlapping_linger_scenario() -> turmoil::Result {
    fn is_future_ready<F: std::future::Future>(f: F) -> bool {
        use std::task::{Context, Poll, Waker};
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut f = std::pin::pin!(f);
        matches!(f.as_mut().poll(&mut cx), Poll::Ready(_))
    }

    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, sim_cluster);

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));
        client
            .create_topic("linger-overlap", policy())
            .await
            .expect("create topic");

        // Warm the cache
        client
            .resolve_topic("linger-overlap")
            .await
            .expect("resolve");

        let producer = Producer::new(
            client.clone(),
            "linger-overlap".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(100),
                    // Threshold is 2 records
                    max_batch_bytes: 1024 * 1024,
                    max_batch_records: 2,
                },
                codec: CompressionCodec::None,
            },
        );

        // 1) Send Record 1 -> spawns Linger 1 (100ms)
        let f1 = producer.send(b"key-1", b"val-1".to_vec());

        // 2) Wait 40ms
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Send Record 2 -> hits threshold (max_batch_records: 2) -> flushes immediately!
        // Record 1 and Record 2 are flushed. spawned_linger becomes false.
        let f2 = producer.send(b"key-1", b"val-2".to_vec());

        // Wait for them to complete (they were flushed immediately)
        let (id1, id2) = tokio::try_join!(f1, f2).expect("first batch flushed");
        assert_eq!(id1, id2, "Should be in the same batch");

        // 3) Send Record 3 -> does not hit threshold.
        // Since spawned_linger is false, this spawns Linger 2 (100ms).
        let mut f3 = std::pin::pin!(producer.send(b"key-1", b"val-3".to_vec()));

        // 4) Wait 80ms (total elapsed time: 120ms since start).
        // Linger 1 (spawned at 0ms, fires at 100ms) wakes up (with seq 1).
        // It calls take(..., 1). Since the current batch seq is 2, it no-ops!
        // Therefore, Record 3 must NOT be flushed early.
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Verify that Record 3 is indeed NOT flushed yet.
        assert!(
            !is_future_ready(f3.as_mut()),
            "Record 3 must not be flushed early by Linger 1"
        );

        // 5) Wait another 40ms (total elapsed time: 160ms since start).
        // Linger 2 (spawned at 40ms, fires at 140ms) wakes up (with seq 2).
        // It calls take(..., 2), finds Record 3, flushes it, and resets spawned_linger.
        tokio::time::sleep(Duration::from_millis(40)).await;

        let id3 = f3
            .as_mut()
            .await
            .expect("Record 3 should be flushed by Linger 2");

        // 6) Wait another 40ms (total elapsed time: 200ms since start).
        // Linger 2's timer has already fired. No other active linger timers remain.
        tokio::time::sleep(Duration::from_millis(40)).await;

        // Verify that we can still write new records and they behave normally.
        let id4 = producer
            .send(b"key-1", b"val-4".to_vec())
            .await
            .expect("Record 4");
        assert_ne!(id3, id4);

        Ok(())
    });

    sim.run()
}

/// E2E Consumer Test 1: Basic Consume.
/// Produce 5 records, consume them with StartPolicy::Earliest and KeyInterest::AllKeys,
/// and assert exact ordered delivery.
/// Then roll the segment (making them sealed history) and verify that StartPolicy::Latest
/// skips them and only reads new records produced afterward.
#[test]
#[serial_test::serial]
fn consumer_basic_consume_earliest() -> turmoil::Result {
    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, |env| {
        sim_cluster(env);
        // Force fast size-based rolls
        env.segment_size_limit_bytes = 10;
    });

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));
        let custom_policy = policy();
        client
            .create_topic("basic-consume", custom_policy)
            .await
            .expect("create topic");

        // Warm cache
        client
            .resolve_topic("basic-consume")
            .await
            .expect("resolve");

        // Produce 5 records
        let producer = Producer::new(
            client.clone(),
            "basic-consume".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(10),
                    max_batch_bytes: 1024,
                    max_batch_records: 5,
                },
                codec: CompressionCodec::None,
            },
        );

        for i in 0..5 {
            let key = format!("key-{}", i);
            let val = format!("val-{}", i).into_bytes();
            producer
                .send(key.as_bytes(), val)
                .await
                .expect("produce record");
        }

        // Consume them with Earliest
        let consumer = Consumer::new(
            client.clone(),
            "basic-consume".to_string(),
            KeyInterest::AllKeys,
            ConsumerConfig::new(StartPolicy::Earliest),
        )
        .await
        .expect("create consumer");

        let mut received = Vec::new();
        for _ in 0..5 {
            let rec = consumer.next_record().await.expect("read record");
            let rec = rec.expect("should have record");
            received.push(rec);
        }

        // Verify keys and values
        for (i, rec) in received.iter().enumerate().take(5) {
            assert_eq!(rec.key, format!("key-{}", i).into_bytes());
            assert_eq!(rec.value, format!("val-{}", i).into_bytes());
        }

        // Now, wait for the segment to roll and seal, making these 5 records "sealed history"
        wait_for_segment_roll(&client, "basic-consume", 0, 1).await;

        // Start a consumer with StartPolicy::Latest. It should skip the sealed segment 0
        // and start at segment 1 (offset 5).
        let consumer_latest = Consumer::new(
            client.clone(),
            "basic-consume".to_string(),
            KeyInterest::AllKeys,
            ConsumerConfig::new(StartPolicy::Latest),
        )
        .await
        .expect("create latest consumer");

        // Send 2 new records
        producer
            .send(b"key-5", b"val-5".to_vec())
            .await
            .expect("produce record");
        producer
            .send(b"key-6", b"val-6".to_vec())
            .await
            .expect("produce record");

        // The latest consumer should only receive keys 5 and 6
        let rec5 = consumer_latest
            .next_record()
            .await
            .expect("read")
            .expect("rec 5");
        assert_eq!(rec5.key, b"key-5");
        let rec6 = consumer_latest
            .next_record()
            .await
            .expect("read")
            .expect("rec 6");
        assert_eq!(rec6.key, b"key-6");

        // Verify no other record is returned
        let timeout_res =
            tokio::time::timeout(Duration::from_millis(500), consumer_latest.next_record()).await;
        assert!(
            timeout_res.is_err(),
            "Latest consumer should not receive historical records"
        );

        Ok(())
    });

    sim.run()
}

/// E2E Consumer Test 1.5: Latest Skips History (Active Segment)
/// StartPolicy::Latest should skip existing records in an active segment without needing a roll.
#[test]
#[serial_test::serial]
fn consumer_latest_starts_at_end_of_active_segment() -> turmoil::Result {
    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, |env| {
        sim_cluster(env);
    });

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));
        let custom_policy = policy();
        client
            .create_topic("basic-consume-latest", custom_policy)
            .await
            .expect("create topic");

        // Warm cache
        client
            .resolve_topic("basic-consume-latest")
            .await
            .expect("resolve");

        let producer = Producer::new(
            client.clone(),
            "basic-consume-latest".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(10),
                    max_batch_bytes: 1024,
                    max_batch_records: 5,
                },
                codec: CompressionCodec::None,
            },
        );

        // Produce 5 records into the active segment (no roll)
        for i in 0..5 {
            let key = format!("key-{}", i);
            let val = format!("val-{}", i).into_bytes();
            producer
                .send(key.as_bytes(), val)
                .await
                .expect("produce record");
        }

        // Wait a tiny bit to ensure they hit the server
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Start a Latest consumer. Because it's Latest, it should skip all 5 existing
        // records in the active segment and wait at offset 5.
        let consumer_latest = Consumer::new(
            client.clone(),
            "basic-consume-latest".to_string(),
            KeyInterest::AllKeys,
            ConsumerConfig::new(StartPolicy::Latest),
        )
        .await
        .expect("create latest consumer");

        // Produce 2 NEW records
        producer.send(b"key-5", b"val-5".to_vec()).await.unwrap();
        producer.send(b"key-6", b"val-6".to_vec()).await.unwrap();

        // Consumer should read exactly key-5 and key-6
        let rec5 = consumer_latest.next_record().await.unwrap().unwrap();
        assert_eq!(rec5.key, b"key-5");

        let rec6 = consumer_latest.next_record().await.unwrap().unwrap();
        assert_eq!(rec6.key, b"key-6");

        // Verify no historical records are returned
        let timeout_res =
            tokio::time::timeout(Duration::from_millis(500), consumer_latest.next_record()).await;
        assert!(
            timeout_res.is_err(),
            "Latest consumer should not receive historical records from the active segment"
        );

        Ok(())
    });

    sim.run()
}

/// E2E Consumer Test 2: Key Filtering.
/// Configure a topic with two ranges (via automatic split), consume with KeyInterest::KeySpan
/// matching only the right child range, and verify that only records from that range are received.
#[test]
#[serial_test::serial]
fn consumer_key_filtering_multi_range() -> turmoil::Result {
    let mut sim = build_sim(120);
    host_cluster(&mut sim, &NODES, |env| {
        sim_cluster(env);
        // Force fast size-based rolls to trigger automatic split
        env.segment_size_limit_bytes = 10;
    });

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));
        client
            .create_topic("key-filter-multi", policy())
            .await
            .expect("create topic");

        // Warm cache
        client
            .resolve_topic("key-filter-multi")
            .await
            .expect("resolve");

        // Produce a record, then wait for roll, 3 times to trigger split
        let producer = Producer::new(
            client.clone(),
            "key-filter-multi".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(10),
                    max_batch_bytes: 1024,
                    max_batch_records: 1,
                },
                codec: CompressionCodec::None,
            },
        );

        for i in 0..3 {
            producer
                .send(b"k", format!("p-{}", i).into_bytes())
                .await
                .expect("send parent");
            wait_for_segment_roll(&client, "key-filter-multi", 0, (i + 1) as u64).await;
        }

        // Wait for split to propagate and commit
        let detail = wait_for_split(&client, "key-filter-multi").await;
        assert_eq!(
            detail.ranges.len(),
            3,
            "Topic must have split into 3 ranges total"
        );

        // Create a consumer with KeyInterest for the right child only ([144, 149))
        // Since midpoint is 127, the right child is [127, 255), which overlaps with [144, 149).
        // The left child is [0, 127), which does not overlap with [144, 149).
        let consumer = Consumer::new(
            client.clone(),
            "key-filter-multi".to_string(),
            KeyInterest::KeySpan {
                start: vec![0x90],
                end: vec![0x95],
            },
            ConsumerConfig::new(StartPolicy::Latest),
        )
        .await
        .expect("create consumer");

        // Produce a record to the left child range (key b"a" = 97)
        producer
            .send(b"a", b"left-child".to_vec())
            .await
            .expect("send to left child");

        // Produce a record to the right child range (key b"\x90" = 144)
        producer
            .send(b"\x90", b"right-child".to_vec())
            .await
            .expect("send to right child");

        // The consumer should receive the right child record
        let rec = consumer
            .next_record()
            .await
            .expect("read record")
            .expect("should have record");
        assert_eq!(rec.key, vec![0x90]);
        assert_eq!(rec.value, b"right-child");

        // Sleep a bit and check that no other record is received (since left child is filtered out)
        let timeout_res =
            tokio::time::timeout(Duration::from_millis(500), consumer.next_record()).await;
        assert!(timeout_res.is_err(), "Should not receive any other record");

        Ok(())
    });

    sim.run()
}

/// E2E Consumer Test 3: Range Split Consume.
/// Configure a topic with AutoSplit and trigger segment rolls. Produce continuously,
/// verify an automatic split occurs, and assert the consumer walks the lineage split
/// and receives all records from both parent and child ranges exactly once.
#[test]
#[serial_test::serial]
fn consumer_range_split_consume() -> turmoil::Result {
    let mut sim = build_sim(120);
    host_cluster(&mut sim, &NODES, |env| {
        sim_cluster(env);
        // Force fast size-based rolls
        env.segment_size_limit_bytes = 10;
    });

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));
        client
            .create_topic("split-consume", policy())
            .await
            .expect("create topic");

        // Warm cache
        client
            .resolve_topic("split-consume")
            .await
            .expect("resolve");

        let producer = Producer::new(
            client.clone(),
            "split-consume".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(10),
                    max_batch_bytes: 1024,
                    max_batch_records: 1,
                },
                codec: CompressionCodec::None,
            },
        );

        // 1. Produce 3 records to parent range, waiting for segment rolls in between
        for i in 0..3 {
            producer
                .send(b"k", format!("parent-{}", i + 1).into_bytes())
                .await
                .expect("send");
            wait_for_segment_roll(&client, "split-consume", 0, (i + 1) as u64).await;
        }

        // Wait for split to propagate and commit
        let detail = wait_for_split(&client, "split-consume").await;
        assert_eq!(detail.ranges.len(), 3, "Topic must have split");

        // 2. Produce records to children ranges
        // Left child (midpoint is 127): key b"a" = 97
        producer
            .send(b"a", b"child-left".to_vec())
            .await
            .expect("send left");
        // Right child: key b"\x90" = 144
        producer
            .send(b"\x90", b"child-right".to_vec())
            .await
            .expect("send right");

        // 3. Consume all records starting from Earliest
        let consumer = Consumer::new(
            client.clone(),
            "split-consume".to_string(),
            KeyInterest::AllKeys,
            ConsumerConfig::new(StartPolicy::Earliest),
        )
        .await
        .expect("create consumer");

        let mut received = Vec::new();
        for _ in 0..5 {
            let rec = consumer
                .next_record()
                .await
                .expect("read record")
                .expect("should have record");
            received.push(rec);
        }

        // Verify we got exactly 5 records: 3 parent, 2 children
        let mut values: Vec<String> = received
            .into_iter()
            .map(|r| String::from_utf8(r.value).unwrap())
            .collect();
        values.sort();

        let expected = vec![
            "child-left".to_string(),
            "child-right".to_string(),
            "parent-1".to_string(),
            "parent-2".to_string(),
            "parent-3".to_string(),
        ];
        assert_eq!(values, expected);

        Ok(())
    });

    sim.run()
}

/// E2E Consumer Test 4: Retention Recovery.
/// Produce records, wait for them to age out and be automatically deleted by retention on the server.
/// Assert that a consumer starting at Earliest (offset 0) receives EntryIdOutOfRange,
/// automatically recovers by querying ListOffsets, skips forward to the surviving offset, and continues.
#[test]
#[serial_test::serial]
fn consumer_retention_recovery() -> turmoil::Result {
    let mut sim = build_sim(120);
    host_cluster(&mut sim, &NODES, |env| {
        sim_cluster(env);
        // Force fast size-based rolls
        env.segment_size_limit_bytes = 10;
    });

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));

        // Topic policy: 0 second retention! This means sealed segments are immediately expired.
        let mut retention_policy = policy();
        retention_policy.retention_ms = Some(0);

        client
            .create_topic("retention-recover", retention_policy)
            .await
            .expect("create topic");

        // Warm cache
        client
            .resolve_topic("retention-recover")
            .await
            .expect("resolve");

        let producer = Producer::new(
            client.clone(),
            "retention-recover".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(10),
                    max_batch_bytes: 1024,
                    max_batch_records: 1,
                },
                codec: CompressionCodec::None,
            },
        );

        // 1. Produce record 1 (goes to segment 0, entry 0)
        producer
            .send(b"k", b"expired-rec".to_vec())
            .await
            .expect("send");

        // 2. Wait for the segment to roll and seal
        wait_for_segment_roll(&client, "retention-recover", 0, 1).await;

        // 3. Wait for the sealed segment to exceed retention and be deleted
        wait_for_retention_deletion(&client, "retention-recover", 0).await;

        // 4. Produce record 2 (goes to segment 1, entry 1)
        producer
            .send(b"k", b"surviving-rec".to_vec())
            .await
            .expect("send");

        // 5. Start consumer at Earliest (tries to read offset 0)
        let consumer = Consumer::new(
            client.clone(),
            "retention-recover".to_string(),
            KeyInterest::AllKeys,
            ConsumerConfig::new(StartPolicy::Earliest),
        )
        .await
        .expect("create consumer");

        // 6. Read the record. The consumer should automatically skip offset 0 and return surviving-rec at offset 1
        let rec = consumer
            .next_record()
            .await
            .expect("read record")
            .expect("should have record");
        assert_eq!(rec.position.batch_offset, 0);
        assert_eq!(rec.position.entry_id, 1.into());
        assert_eq!(rec.value, b"surviving-rec");

        // Ensure no other record is returned
        let timeout_res =
            tokio::time::timeout(Duration::from_millis(500), consumer.next_record()).await;
        assert!(timeout_res.is_err(), "Should not receive expired-rec");

        Ok(())
    });

    sim.run()
}

/// E2E Consumer Test 5: Prefetching.
/// Produce records across multiple segments (forcing rolls), and consume them.
/// Verify that historical reads cross segment boundaries smoothly, utilizing
/// the client-side speculative one-ahead prefetching of the next sealed segment.
#[test]
#[serial_test::serial]
fn consumer_prefetch_sealed_segments() -> turmoil::Result {
    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, |env| {
        sim_cluster(env);
        // Force fast size-based rolls
        env.segment_size_limit_bytes = 10;
    });

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));
        client
            .create_topic("prefetch-topic", policy())
            .await
            .expect("create topic");

        // Warm cache
        client
            .resolve_topic("prefetch-topic")
            .await
            .expect("resolve");

        let producer = Producer::new(
            client.clone(),
            "prefetch-topic".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(10),
                    max_batch_bytes: 1024,
                    max_batch_records: 1,
                },
                codec: CompressionCodec::None,
            },
        );

        // Produce 3 records across 3 different segments by rolling them
        producer
            .send(b"k", b"rec-seg-0".to_vec())
            .await
            .expect("send");
        wait_for_segment_roll(&client, "prefetch-topic", 0, 1).await;

        producer
            .send(b"k", b"rec-seg-1".to_vec())
            .await
            .expect("send");
        wait_for_segment_roll(&client, "prefetch-topic", 0, 2).await;

        producer
            .send(b"k", b"rec-seg-2".to_vec())
            .await
            .expect("send");

        // Now we have sealed segment 0, sealed segment 1, and active segment 2.
        // Start consumer at Earliest to read them.
        let consumer = Consumer::new(
            client.clone(),
            "prefetch-topic".to_string(),
            KeyInterest::AllKeys,
            ConsumerConfig::new(StartPolicy::Earliest),
        )
        .await
        .expect("create consumer");

        // Consume all 3 records. The historical reads will cross segment boundaries.
        // Speculative prefetching of segment 1 is triggered when reading segment 0,
        // and prefetching of segment 2 is triggered when reading segment 1.
        let rec0 = consumer
            .next_record()
            .await
            .expect("read")
            .expect("rec-seg-0");
        assert_eq!(rec0.value, b"rec-seg-0");

        let rec1 = consumer
            .next_record()
            .await
            .expect("read")
            .expect("rec-seg-1");
        assert_eq!(rec1.value, b"rec-seg-1");

        let rec2 = consumer
            .next_record()
            .await
            .expect("read")
            .expect("rec-seg-2");
        assert_eq!(rec2.value, b"rec-seg-2");

        Ok(())
    });

    sim.run()
}

#[test]
#[serial_test::serial]
fn consumer_pause_seek_resume_live_range() -> turmoil::Result {
    let mut sim = build_sim(120);
    host_cluster(&mut sim, &NODES, sim_cluster);

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));
        client
            .create_topic("pause-seek-resume", policy())
            .await
            .expect("create topic");
        client
            .resolve_topic("pause-seek-resume")
            .await
            .expect("warm routing cache");

        let producer = Producer::new(
            client.clone(),
            "pause-seek-resume".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(50),
                    max_batch_bytes: 1024 * 1024,
                    max_batch_records: 3,
                },
                codec: CompressionCodec::None,
            },
        );

        let (rec0, rec1, rec2) = tokio::join!(
            producer.send(b"k", b"rec-0".to_vec()),
            producer.send(b"k", b"rec-1".to_vec()),
            producer.send(b"k", b"rec-2".to_vec()),
        );
        let rec0 = rec0.expect("rec-0");
        assert_eq!(rec0, rec1.expect("rec-1"), "first records batch together");
        assert_eq!(rec0, rec2.expect("rec-2"), "first records batch together");

        let consumer = Consumer::new(
            client.clone(),
            "pause-seek-resume".to_string(),
            KeyInterest::AllKeys,
            ConsumerConfig::new(StartPolicy::Latest),
        )
        .await
        .expect("create consumer");

        consumer.pause_range(RangeId(0)).await.expect("pause range");
        tokio::time::sleep(Duration::from_millis(500)).await;

        producer.send(b"k", b"rec-3".to_vec()).await.expect("rec-3");
        let paused = tokio::time::timeout(Duration::from_millis(500), consumer.next_record()).await;
        assert!(paused.is_err(), "paused consumer must not fetch rec-3");

        consumer
            .seek_range(RangeId(0), 1)
            .await
            .expect("seek range");
        consumer
            .resume_range(RangeId(0))
            .await
            .expect("resume range");

        let mut values = Vec::new();
        for _ in 0..3 {
            let rec = consumer.next_record().await.expect("read").expect("record");
            values.push(rec.value);
        }
        assert_eq!(
            values,
            [b"rec-1".to_vec(), b"rec-2".to_vec(), b"rec-3".to_vec()]
        );

        Ok(())
    });

    sim.run()
}

#[test]
#[serial_test::serial]
fn consumer_seek_resume_after_sealed_segment_reassignment() -> turmoil::Result {
    let mut sim = build_sim(360);
    host_cluster(&mut sim, &NODES_4, |env| {
        sim_cluster(env);
        env.segment_size_limit_bytes = 2048;
    });

    let victim = Arc::new(Mutex::new(None::<String>));
    let victim_w = victim.clone();

    sim.client("test-client", async move {
        let client = Arc::new(
            Client::connect_with(
                client_seeds_4(),
                RetryPolicy {
                    deadline: Duration::from_secs(120),
                    initial_backoff: Duration::from_millis(50),
                    max_backoff: Duration::from_secs(1),
                },
            )
            .expect("client connects"),
        );
        tokio::time::sleep(Duration::from_secs(10)).await;
        client
            .create_topic("pause-repair-seek", policy())
            .await
            .expect("create topic");

        let producer = Producer::new(
            client.clone(),
            "pause-repair-seek".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(10),
                    max_batch_bytes: 1024 * 1024,
                    max_batch_records: 1,
                },
                codec: CompressionCodec::None,
            },
        );

        let record = |idx: usize| {
            let mut value = format!("sealed-rec-{idx}").into_bytes();
            value.resize(1024, b'-');
            value
        };

        for idx in 0..3 {
            producer
                .send(b"k", record(idx))
                .await
                .expect("produce sealed record");
        }
        let detail = wait_for_segment_roll(&client, "pause-repair-seek", 0, 1).await;
        let sealed_replicas = detail
            .ranges
            .iter()
            .find(|range| range.range_id == RangeId(0))
            .and_then(|range| range.sealed_segments.first())
            .map(|segment| {
                segment
                    .replica_set
                    .iter()
                    .map(|replica| replica.node_id.clone())
                    .collect::<Vec<_>>()
            })
            .expect("sealed segment replica set");
        assert_eq!(sealed_replicas.len(), 3, "RF=3 leaves one spare");

        let victim_node = NODES_4
            .iter()
            .map(|(name, _, _)| *name)
            .find(|name| sealed_replicas.iter().any(|id| id.starts_with(name)))
            .expect("sealed segment replica to crash");

        let consumer = Consumer::new(
            client,
            "pause-repair-seek".to_string(),
            KeyInterest::AllKeys,
            ConsumerConfig::new(StartPolicy::Latest),
        )
        .await
        .expect("create consumer");
        consumer
            .pause_range(RangeId(0))
            .await
            .expect("pause before repair");
        tokio::time::sleep(Duration::from_millis(500)).await;

        *victim_w.lock().unwrap() = Some(victim_node.to_string());

        tokio::time::sleep(Duration::from_secs(90)).await;
        consumer
            .seek_range(RangeId(0), 0)
            .await
            .expect("seek into repaired sealed segment");
        consumer
            .resume_range(RangeId(0))
            .await
            .expect("resume after repair");

        let first = consumer
            .next_record()
            .await
            .expect("read repaired sealed record")
            .expect("record");
        assert_eq!(first.position.entry_id, EntryId(0));
        assert_eq!(&first.value[..12], b"sealed-rec-0");

        Ok(())
    });

    while victim.lock().unwrap().is_none() {
        assert!(
            sim.elapsed() < Duration::from_secs(180),
            "client never chose a sealed replica to crash"
        );
        sim.step()?;
    }
    let victim_node = victim.lock().unwrap().clone().unwrap();
    tracing::info!(
        "[CONSUMER-REPAIR] crashing {victim_node} at {}s",
        sim.elapsed().as_secs()
    );
    sim.crash(victim_node);

    sim.run()
}

/// Verify that producer batching with linger_ms > 0 advances the physical entry IDs
/// by the record_count of each batch, and that consumer reads/resumes work correctly.
// ! Current it fails: "Error: "Ran for duration: 90s steps: 902 without completing"
#[test]
#[serial_test::serial]
fn consumer_linger_batching_end_to_end() -> turmoil::Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("[new consumer]=info")
        .with_test_writer()
        .try_init();

    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, |env| {
        sim_cluster(env);
    });

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));
        let custom_policy = policy();
        client
            .create_topic("linger-batching", custom_policy)
            .await
            .expect("create topic");

        client
            .resolve_topic("linger-batching")
            .await
            .expect("resolve");

        let producer = Producer::new(
            client.clone(),
            "linger-batching".to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_millis(50),
                    max_batch_bytes: 1024 * 1024,
                    max_batch_records: 10,
                },
                codec: CompressionCodec::Lz4,
            },
        );

        // Send 3 records concurrently so they get batched together as Entry 0
        let (res1, res2, res3) = tokio::join!(
            producer.send(b"key1", b"val1".to_vec()),
            producer.send(b"key1", b"val2".to_vec()),
            producer.send(b"key1", b"val3".to_vec()),
        );
        let e0_1 = res1.expect("send 1");
        let e0_2 = res2.expect("send 2");
        let e0_3 = res3.expect("send 3");
        assert_eq!(e0_1, EntryId(0));
        assert_eq!(e0_2, EntryId(0));
        assert_eq!(e0_3, EntryId(0));

        // Sleep to let the next batch start
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Send 2 records concurrently so they get batched together as Entry 1
        let (res4, res5) = tokio::join!(
            producer.send(b"key1", b"val4".to_vec()),
            producer.send(b"key1", b"val5".to_vec()),
        );
        let e1_1 = res4.expect("send 4");
        let e1_2 = res5.expect("send 5");
        // Since Entry 0 had 3 records, next_entry_id should have advanced to EntryId(3)
        assert_eq!(e1_1, EntryId(1));
        assert_eq!(e1_2, EntryId(1));

        // Consume the records
        let mut config = ConsumerConfig::new(StartPolicy::Earliest);
        config.group_id = Some("linger-batching-group".into()); // Set group id 

        let consumer = Consumer::new(
            client.clone(),
            "linger-batching".to_string(),
            KeyInterest::AllKeys,
            config.clone(),
        )
        .await
        .expect("create consumer");

        // Read first 3 records (from Entry 0)
        let r1 = consumer.next_record().await.expect("read 1").expect("r1");
        assert_eq!(r1.position.batch_offset, 0);
        assert_eq!(r1.position.entry_id, EntryId(0));
        assert_eq!(r1.position.absolute_offset, 0);
        assert_eq!(r1.value, b"val1");

        let r2 = consumer.next_record().await.expect("read 2").expect("r2");
        assert_eq!(r2.position.batch_offset, 1);
        assert_eq!(r2.position.entry_id, EntryId(0));
        assert_eq!(r2.position.absolute_offset, 1);
        assert_eq!(r2.value, b"val2");

        let r3 = consumer.next_record().await.expect("read 3").expect("r3");
        assert_eq!(r3.position.batch_offset, 2);
        assert_eq!(r3.position.entry_id, EntryId(0));
        assert_eq!(r3.position.absolute_offset, 2);
        assert_eq!(r3.value, b"val3");

        // Read next 2 records (from Entry 3)
        let r4 = consumer.next_record().await.expect("read 4").expect("r4");
        assert_eq!(r4.position.batch_offset, 0);
        assert_eq!(r4.position.entry_id, EntryId(1));
        assert_eq!(r4.position.absolute_offset, 3);
        assert_eq!(r4.value, b"val4");

        let r5 = consumer.next_record().await.expect("read 5").expect("r5");
        assert_eq!(r5.position.batch_offset, 1);
        assert_eq!(r5.position.entry_id, EntryId(1));
        assert_eq!(r5.position.absolute_offset, 4);
        assert_eq!(r5.value, b"val5");

        // Commit progress at offset 4
        consumer.ack(&r1).expect("ack 1");
        consumer.ack(&r2).expect("ack 2");
        consumer.ack(&r3).expect("ack 3");
        consumer.ack(&r4).expect("ack 4");
        consumer.ack(&r5).expect("ack 5");
        consumer.commit().await.expect("commit");

        // Close consumer and create a new one to verify resuming / skipping works
        drop(consumer);

        let span = tracing::info_span!("new consumer");

        let _guard = span.enter();
        let consumer_resume = Consumer::new(
            client,
            "linger-batching".to_string(),
            KeyInterest::AllKeys,
            config,
        )
        .await
        .expect("create consumer resume");

        tokio::time::sleep(Duration::from_millis(60)).await;

        // Produce a 6th record (should start at offset 0 / EntryId 2)
        let res6 = producer.send(b"key1", b"val6".to_vec()).await;

        let e2 = res6.expect("send 6");
        assert_eq!(e2, EntryId(2));

        // Read the 6th record and verify offsets/entry_ids
        let r6 = consumer_resume
            .next_record()
            .await
            .expect("read 6")
            .expect("r6");

        drop(_guard);
        assert_eq!(r6.position.batch_offset, 0);
        assert_eq!(r6.value, b"val6");
        assert_eq!(r6.position.entry_id, EntryId(2));
        assert_eq!(r6.position.absolute_offset, 5);

        Ok(())
    });

    sim.run()
}

// ── Polling helper functions for robust E2E testing ───────────────────────

async fn wait_for_segment_roll(
    client: &Client,
    topic: &str,
    range_id: u64,
    expected_segment_id: u64,
) -> TopicDetail {
    for _ in 0..240 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if let Ok(detail) = client.resolve_topic(topic).await
            && let Some(range) = detail
                .ranges
                .iter()
                .find(|r| r.range_id == RangeId(range_id))
        {
            if range.state == crate::control_plane::metadata::RangeState::Sealed {
                return detail;
            }
            if let Some(seg) = &range.active_segment
                && *seg.segment_id >= expected_segment_id
                && !range.sealed_segments.is_empty()
            {
                return detail;
            }
        }
    }
    panic!(
        "Timed out waiting for segment roll to {} on range {}",
        expected_segment_id, range_id
    );
}

async fn wait_for_split(client: &Client, topic: &str) -> TopicDetail {
    for _ in 0..240 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if let Ok(detail) = client.resolve_topic(topic).await
            && detail.ranges.len() == 3
        {
            return detail;
        }
    }
    panic!("Timed out waiting for split on topic {}", topic);
}

async fn wait_for_retention_deletion(client: &Client, topic: &str, range_id: u64) {
    for _ in 0..240 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        std::thread::sleep(std::time::Duration::from_millis(10));
        if let Ok(detail) = client.resolve_topic(topic).await
            && let Some(range) = detail
                .ranges
                .iter()
                .find(|r| r.range_id == RangeId(range_id))
            && range.sealed_segments.is_empty()
            && let Some(seg) = &range.active_segment
            && seg.start_entry_id > 0.into()
        {
            return;
        }
    }
    panic!(
        "Timed out waiting for retention deletion on range {}",
        range_id
    );
}

#[test]
#[serial_test::serial]
fn producer_split_fence_retry() -> turmoil::Result {
    let mut sim = build_sim(90);
    host_cluster(&mut sim, &NODES, |env| {
        sim_cluster(env);
        env.segment_size_limit_bytes = 10;
    });

    sim.client("test-client", async {
        let client_admin = Arc::new(Client::connect(client_seeds()).expect("admin connects"));
        let topic = "split-fence";

        let mut policy = policy();
        policy.partition_strategy = PartitionStrategy::AutoSplit;
        client_admin
            .create_topic(topic, policy)
            .await
            .expect("create topic");

        let client1 = Arc::new(Client::connect(client_seeds()).expect("client1 connects"));
        let detail_initial = client1
            .resolve_topic(topic)
            .await
            .expect("resolve topic initial");
        assert_eq!(detail_initial.ranges.len(), 1, "Must start with 1 range");

        let producer = Arc::new(Producer::new(
            client1.clone(),
            topic.to_string(),
            ProducerConfig {
                buffer: BufferConfig {
                    linger: Duration::from_secs(60),
                    max_batch_bytes: 1024 * 1024,
                    max_batch_records: 1000,
                },
                codec: CompressionCodec::None,
            },
        ));

        // Both keys enter the parent batch before the range splits.
        let left = {
            let producer = producer.clone();
            tokio::spawn(async move { producer.send(&[0x10], b"left".to_vec()).await })
        };
        let right = {
            let producer = producer.clone();
            tokio::spawn(async move { producer.send(&[0xF0], b"right".to_vec()).await })
        };
        tokio::task::yield_now().await;

        for i in 0..3 {
            client_admin
                .produce(topic, b"k", b"longer_than_ten_bytes".to_vec(), 1)
                .await
                .expect("produce trigger record");
            wait_for_segment_roll(&client_admin, topic, 0, (i + 1) as u64).await;
        }

        let detail_split = wait_for_split(&client_admin, topic).await;
        assert_eq!(
            detail_split.ranges.len(),
            3,
            "Topic must have split into 3 ranges"
        );

        producer.flush().await;
        assert!(left.await.expect("left sender completes").is_ok());
        assert!(right.await.expect("right sender completes").is_ok());

        let refreshed = client1.resolve_topic(topic).await.expect("refresh routing");
        let left_range = refreshed
            .ranges
            .iter()
            .filter(|r| {
                r.state == crate::control_plane::metadata::RangeState::Active
                    && r.keyspace_start.as_slice() <= [0x10].as_slice()
            })
            .max_by_key(|r| &r.keyspace_start)
            .expect("left child");
        let right_range = refreshed
            .ranges
            .iter()
            .filter(|r| {
                r.state == crate::control_plane::metadata::RangeState::Active
                    && r.keyspace_start.as_slice() <= [0xF0].as_slice()
            })
            .max_by_key(|r| &r.keyspace_start)
            .expect("right child");
        assert_ne!(left_range.range_id, right_range.range_id);

        Ok(())
    });

    sim.run()
}
