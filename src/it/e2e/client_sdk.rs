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

/// Stress: a burst of concurrent produces over one multiplexed connection. Distinct
/// entry ids ⇒ every ack reached its own waiter (no demux race under load).
#[test]
#[serial_test::serial]
fn client_sdk_stress_concurrent_produces() -> turmoil::Result {
    use std::collections::HashSet;
    use std::sync::Arc;

    let mut sim = build_sim(120);
    host_cluster(&mut sim, &NODES, sim_cluster);

    sim.client("test-client", async {
        let client = Arc::new(Client::connect(client_seeds()).expect("client connects"));
        client
            .create_topic("stress", policy())
            .await
            .expect("create");
        // Warm cache + segment assignment so the burst routes straight to the leader.
        client
            .produce("stress", b"k", b"warm".to_vec(), 1)
            .await
            .expect("warmup produce");

        const N: usize = 100;
        let mut tasks = tokio::task::JoinSet::new();
        for i in 0..N {
            let client = client.clone();
            tasks.spawn(async move {
                client
                    .produce("stress", b"k", format!("rec-{i}").into_bytes(), 1)
                    .await
            });
        }

        let mut ids = HashSet::new();
        while let Some(joined) = tasks.join_next().await {
            let entry_id = joined.expect("task did not panic").expect("produce acked");
            assert!(
                ids.insert(entry_id),
                "entry_id {entry_id} returned twice — demux delivered an ack to the wrong waiter"
            );
        }
        assert_eq!(ids.len(), N, "every concurrent produce got a distinct ack");
        Ok(())
    });

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
