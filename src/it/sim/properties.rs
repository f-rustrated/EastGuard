use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use turmoil::Builder;

use crate::StartUp;
use crate::it::helpers::{check_alive_count, check_dead_or_not_exist, default_env};
use crate::it::sim::invariants::{
    assert_leader_converges, assert_membership_converged, assert_single_leader,
    assert_topic_visible_on_quorum, query_shard_info, query_shard_leader,
};
use crate::it::sim::scenario::{
    client_port, cluster_port, make_create_topic_req, node_name, try_propose,
};

/// One seed per run for both the turmoil rng and every node's `StartUp` rng
/// (#133): a failing run prints it, and `EG_SIM_SEED=<seed>` replays the exact
/// schedule, latencies, fault interleaving — and, because the seed is threaded
/// into `spawn_node`, the same per-node election-jitter sequences. Previously
/// every node in every run got `rng_seed = 0`, so the jitter sequences were
/// identical across an entire stress sweep and only network latency varied.
/// Timestamps tracing output with turmoil's simulated clock so replay logs
/// read in sim seconds instead of (compressed) wall time. Lines emitted
/// outside a sim context (e.g. the driver thread) print `--`.
struct SimTime;

impl tracing_subscriber::fmt::time::FormatTime for SimTime {
    fn format_time(
        &self,
        w: &mut tracing_subscriber::fmt::format::Writer<'_>,
    ) -> std::fmt::Result {
        match turmoil::sim_elapsed() {
            Some(t) => write!(w, "[sim {:>9.4}s]", t.as_secs_f64()),
            None => write!(w, "[sim        --]"),
        }
    }
}

fn sim_seed() -> u64 {
    let seed: u64 = std::env::var("EG_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        });
    println!("turmoil seed: {seed} (replay with EG_SIM_SEED={seed})");
    seed
}

fn three_node_sim(simulation_secs: u64, seed: u64) -> turmoil::Sim<'static> {
    Builder::new()
        .tick_duration(Duration::from_millis(100))
        .simulation_duration(Duration::from_secs(simulation_secs))
        .tcp_capacity(4096)
        .rng_seed(seed)
        .build()
}

fn spawn_node(sim: &mut turmoil::Sim<'static>, i: u8, total: u8, seed: u64) {
    let cp = client_port(i);
    let rp = cluster_port(i);
    sim.host(node_name(i), move || async move {
        let name = node_name(i);
        let me = turmoil::lookup(name.as_str());
        let seeds: Vec<String> = (1..=total)
            .filter(|&j| j != i)
            .map(|j| {
                format!(
                    "{}:{}",
                    turmoil::lookup(node_name(j).as_str()),
                    cluster_port(j)
                )
            })
            .collect();
        let mut env = default_env(i as u32, name.clone(), cp, rp);
        env.advertise_host = Some(me.to_string());
        env.join_seed_nodes = seeds;
        env.vnodes_per_node = 3;
        StartUp::with_env(env, seed).run().await?;
        Ok(())
    });
}

/// After `CreateTopic` is acked, all alive nodes must eventually expose the topic via `GetTopics`.
#[test]
#[serial_test::serial]
fn metadata_visible() -> turmoil::Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_timer(SimTime)
        .try_init();
    let seed = sim_seed();
    let mut sim = three_node_sim(90, seed);
    for i in 1..=3u8 {
        spawn_node(&mut sim, i, 3, seed);
    }

    sim.client("checker", async {
        // Retry until a leader is elected and the propose is acked (up to 30s).
        let req = make_create_topic_req("visible-test");
        let mut acked = false;
        for _ in 0..30u32 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            for i in 1..=3u8 {
                if let Some(crate::connections::protocol::ControlPlaneResponse::TopicCreated) =
                    try_propose(&node_name(i), client_port(i), &req).await
                {
                    acked = true;
                    break;
                }
            }
            if acked {
                break;
            }
        }
        assert!(acked, "CreateTopic was not acked by any node within 30s");

        let nodes: &[(&str, u16)] = &[("node-1", 8081), ("node-2", 8082), ("node-3", 8083)];
        assert_topic_visible_on_quorum(nodes, "visible-test", 60, Duration::from_millis(100))
            .await?;
        Ok(())
    });

    sim.run()
}

/// After the **current leader** of the topic's shard group is crashed, the
/// surviving nodes elect a replacement and converge on it (no split-brain).
///
/// Rewritten for #133. The previous version had three structural problems:
///
/// 1. Vacuous in ~2/3 of runs: it crashed a fixed `node-1` at a fixed t=20s
///    and accepted any leader `!starts_with("node-1")` — already true *before*
///    the crash whenever node-2/3 won the initial election, so the checker
///    completed without testing re-election at all.
/// 2. Iteration-bounded polling: the helpers' inner connect/read timeouts
///    (2s/3-5s) stretched the nominal "30s" loops far past their budget, so
///    the sim hit its duration ceiling before any assert fired — producing
///    bare `Ran for duration` failures with no diagnostic.
/// 3. Time-gated crash: t=20s raced slow bootstraps, sometimes killing the
///    node mid-initial-election — a different scenario than intended.
///
/// Structure now (every phase is bounded by a *sim-time deadline* and fails
/// with its own labeled message; the 180s envelope is only a backstop):
///
/// - Phase 1 (≤30s): topology resolved and an initial leader agreed by ≥2
///   nodes; its host index is published to the driver. A `CreateTopic` is
///   also proposed to exercise the write path, but not asserted on (a lost
///   ack makes the duplicate retry rejectable; `metadata_visible` covers
///   creation semantics).
/// - Driver: steps until the victim is published, allows 3s of steady state,
///   crashes that host, then signals the checker.
/// - Phase 2 (≤40s from the crash): a survivor reports a leader different
///   from the crashed one. Budget: SWIM death detection (~6-7s) + election
///   timeout (5-7s base+jitter) + a few pessimal split-vote rounds.
/// - Phase 3 (≤10s): both survivors converge on the same replacement.
///   `assert_leader_converges` absorbs *stale previous-term views*, which
///   `assert_single_leader(.., Duration::ZERO)` would misread as split-brain.
#[test]
#[serial_test::serial]
fn leader_elects_after_kill() -> turmoil::Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_timer(SimTime)
        .try_init();
    let seed = sim_seed();
    let mut sim = three_node_sim(180, seed);
    for i in 1..=3u8 {
        spawn_node(&mut sim, i, 3, seed);
    }

    // Checker → driver: host index (1..=3) of the discovered leader; 0 = unset.
    let victim = Arc::new(AtomicU8::new(0));
    // Driver → checker: set once the crash has been performed.
    let crashed = Arc::new(AtomicBool::new(false));

    let victim_tx = victim.clone();
    let crashed_rx = crashed.clone();
    sim.client("checker", async move {
        let t0 = tokio::time::Instant::now();

        // ── Phase 1: topology + an initial leader agreed by ≥2 nodes ──
        let req = make_create_topic_req("leader-kill-test");
        let phase1_deadline = t0 + Duration::from_secs(30);
        let mut shard_group_id: Option<u64> = None;
        let mut topic_acked = false;
        let mut agreed_leader: Option<String> = None;
        let mut last_round: Vec<Option<String>> = vec![None, None, None];
        while tokio::time::Instant::now() < phase1_deadline {
            if shard_group_id.is_none() {
                shard_group_id =
                    query_shard_info(&node_name(2), client_port(2), b"leader-kill-test")
                        .await
                        .ok()
                        .flatten()
                        .map(|info| info.shard_group_id);
            }
            if !topic_acked {
                for i in 1..=3u8 {
                    if let Some(crate::connections::protocol::ControlPlaneResponse::TopicCreated) =
                        try_propose(&node_name(i), client_port(i), &req).await
                    {
                        topic_acked = true;
                        break;
                    }
                }
            }
            if let Some(gid) = shard_group_id {
                // Act only on a leader reported identically by at least two
                // nodes, so one node's transient view can't pick the victim.
                for (slot, i) in (1..=3u8).enumerate() {
                    last_round[slot] = query_shard_leader(&node_name(i), client_port(i), gid)
                        .await
                        .ok()
                        .flatten();
                }
                let mut views: Vec<String> = last_round.iter().flatten().cloned().collect();
                views.sort();
                agreed_leader = views
                    .windows(2)
                    .find(|w| w[0] == w[1])
                    .map(|w| w[0].clone());
            }
            if agreed_leader.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let shard_group_id =
            shard_group_id.unwrap_or_else(|| panic!("phase1: topology not populated within 30s"));
        let initial_leader = agreed_leader.unwrap_or_else(|| {
            panic!(
                "phase1: no quorum-agreed leader within 30s \
                 (shard {shard_group_id}, topic_acked: {topic_acked}, \
                 last views from node-1..3: {last_round:?})"
            )
        });
        let victim_idx = (1..=3u8)
            .find(|i| initial_leader.starts_with(node_name(*i).as_str()))
            .unwrap_or_else(|| panic!("leader id {initial_leader:?} maps to no known host"));
        tracing::info!(
            elapsed = ?t0.elapsed(),
            shard_group_id,
            %initial_leader,
            victim_idx,
            topic_acked,
            "phase1 done"
        );
        victim_tx.store(victim_idx, Ordering::SeqCst);

        // Wait for the driver to perform the crash (it gates on the store above).
        let crash_signal_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while !crashed_rx.load(Ordering::SeqCst) {
            assert!(
                tokio::time::Instant::now() < crash_signal_deadline,
                "driver did not signal the crash within 15s"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let survivors: Vec<u8> = (1..=3u8).filter(|i| *i != victim_idx).collect();
        let survivor_names: Vec<String> = survivors.iter().map(|i| node_name(*i)).collect();

        // ── Phase 2: a survivor reports a replacement leader ──
        let phase2_deadline = tokio::time::Instant::now() + Duration::from_secs(40);
        let mut last_views: Vec<Option<String>> = vec![None; survivors.len()];
        let mut replacement: Option<String> = None;
        'phase2: while tokio::time::Instant::now() < phase2_deadline {
            for (slot, (i, name)) in survivors.iter().zip(survivor_names.iter()).enumerate() {
                let view = query_shard_leader(name, client_port(*i), shard_group_id)
                    .await
                    .ok()
                    .flatten();
                if let Some(l) = &view
                    && l != &initial_leader
                {
                    replacement = Some(l.clone());
                    break 'phase2;
                }
                last_views[slot] = view;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let replacement = replacement.unwrap_or_else(|| {
            panic!(
                "phase2: no replacement leader within 40s after crashing {initial_leader:?} \
                 (last views from {survivor_names:?}: {last_views:?})"
            )
        });
        assert!(
            survivor_names
                .iter()
                .any(|n| replacement.starts_with(n.as_str())),
            "phase2: replacement leader {replacement:?} is not a survivor {survivor_names:?}"
        );
        tracing::info!(elapsed = ?t0.elapsed(), %replacement, "phase2 done");

        // ── Phase 3: survivors converge on the replacement ──
        let nodes: Vec<(&str, u16)> = survivors
            .iter()
            .zip(survivor_names.iter())
            .map(|(i, n)| (n.as_str(), client_port(*i)))
            .collect();
        assert_leader_converges(&nodes, shard_group_id, &initial_leader, Duration::from_secs(10))
            .await?;
        // Converged views also satisfy the split-brain invariant by definition.
        assert_single_leader(&nodes, shard_group_id, Duration::ZERO).await?;
        Ok(())
    });

    // ── Driver: phase-gate the crash on the checker's discovery ──
    while victim.load(Ordering::SeqCst) == 0 {
        assert!(
            sim.elapsed() < Duration::from_secs(45),
            "driver: checker published no leader within 45s of sim time"
        );
        sim.step()?;
    }
    let victim_idx = victim.load(Ordering::SeqCst);
    // A few seconds of steady state so the kill hits an established leader.
    let crash_at = sim.elapsed() + Duration::from_secs(3);
    while sim.elapsed() < crash_at {
        sim.step()?;
    }
    tracing::info!(elapsed = ?sim.elapsed(), host = %node_name(victim_idx), "crashing leader");
    sim.crash(node_name(victim_idx).as_str());
    crashed.store(true, Ordering::SeqCst);

    sim.run()
}

/// After crashing then restarting a node, all surviving nodes eventually see it as `Alive`.
#[test]
#[serial_test::serial]
fn membership_converges_after_rejoin() -> turmoil::Result {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_timer(SimTime)
        .try_init();
    let seed = sim_seed();
    let mut sim = three_node_sim(120, seed);
    for i in 1..=3u8 {
        spawn_node(&mut sim, i, 3, seed);
    }

    sim.client("checker", async {
        tokio::time::sleep(Duration::from_secs(10)).await;
        check_alive_count("node-2", 8082, 3).await?;
        check_alive_count("node-3", 8083, 3).await?;

        // Wait for crash + death detection (crash at t≈15s, detectable within ~20s).
        tokio::time::sleep(Duration::from_secs(25)).await; // now at t≈35s
        assert!(
            check_dead_or_not_exist("node-2", 8082, "node-1").await,
            "node-2 should see node-1 as Dead"
        );
        assert!(
            check_dead_or_not_exist("node-3", 8083, "node-1").await,
            "node-3 should see node-1 as Dead"
        );

        // node-1 is bounced at t≈40s. Wait for rejoin + membership convergence.
        tokio::time::sleep(Duration::from_secs(30)).await; // now at t≈65s

        let all_nodes: &[(&str, u16)] = &[("node-1", 8081), ("node-2", 8082), ("node-3", 8083)];
        assert_membership_converged(all_nodes, 30, Duration::from_secs(1)).await?;
        Ok(())
    });

    while sim.elapsed() < Duration::from_secs(15) {
        sim.step()?;
    }
    sim.crash("node-1");

    while sim.elapsed() < Duration::from_secs(40) {
        sim.step()?;
    }
    sim.bounce("node-1");

    sim.run()
}
