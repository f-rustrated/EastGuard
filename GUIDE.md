# DST Integration Guide

## What is DST?

Deterministic Simulation Testing (DST) is a testing technique designed to gain confidence in distributed systems by
shaking out edge cases and reliably reproducing bugs. Unlike standard unit tests, DST adopts a whole-of-system mindset.
It works by synthesizing chaotic interactions over an extended period of simulated time, where the system randomly
charts a path through a huge state space. Crucially, despite this randomness, every choice (network delays, thread
scheduling) flows from a single RNG seed, meaning any failure can be perfectly replayed.

### Why DST for EastGuard

- **Reliable Reproducibility:** Turns rare heisenbugs into reproducible scenarios. Use the failing seed to replay the
  exact execution path locally.
- **Defense-in-Depth:** Verifies invariants (e.g. ACID-like membership consistency) and crashes loudly when they are
  violated.
- **Unhappy Path Testing:** Easily injects hardships like network latency, packet loss, and node crashes — difficult to
  do in a real environment.
- **Early Detection:** Catches complex issues (deadlocks, gossip divergence, race conditions) on every PR before they
  reach production.

---

## EastGuard-Specific Context

### Architecture

Three concurrent actors, all `tokio::spawn`'d:

```
StartUp
├── SwimTransportActor  — real tokio::net::UdpSocket (non-deterministic I/O)
├── SwimActor           — SWIM gossip state machine (mostly pure logic)
└── run_scheduling_actor — tokio::time::interval (controllable time)
```

### Non-Determinism Sources to Eliminate

| # | Location                                    | Issue                                                                                             |
|---|---------------------------------------------|---------------------------------------------------------------------------------------------------|
| 1 | `src/clusters/swims/livenode_tracker.rs:24` | `StdRng::from_entropy()` called on every `add()`                                                  |
| 2 | `src/schedulers/ticker.rs:12`               | `timers: HashMap<u32, T>` — expiry order non-deterministic when multiple timers fire on same tick |
| 3 | `src/clusters/swims/swim.rs:62`             | `members: HashMap<NodeId, SwimNode>` — gossip selection iterates this map                         |
| 4 | `src/clusters/swims/swim.rs:77`             | `pending_join_seqs: HashMap<u32, PendingJoin>`                                                    |
| 5 | `src/clusters/swims/topology.rs:33`         | `pnodes: HashMap<NodeId, ...>`                                                                    |

> **Note:** `uuid` in `config.rs` is only for persisting node identity to disk, not part of SWIM logic — safe to leave
> as-is. Node IDs are injected explicitly in DST tests.

### Existing Test Harness

`src/clusters/tests.rs` already has a `NetworkBridge` that routes packets in-memory. This shows the actor model is
well-separated. Turmoil replaces `NetworkBridge` entirely. Two DST-hostile patterns to remove from the existing tests:

```rust
// real time — non-deterministic
tokio::time::sleep(Duration::from_millis(10)).await;

// non-deterministic routing order
routes: HashMap<SocketAddr, mpsc::Sender<SwimCommand> >,
```

---

## Implementation Plan

### Phase 1 — Eliminate Non-Determinism

This phase makes the application itself deterministic before the simulation layer is added.

#### 1a. Fix `LiveNodeTracker` (`src/clusters/swims/livenode_tracker.rs`)

Hold a seeded `StdRng` as a field instead of calling `from_entropy()` on every `add()`:

```rust
pub(super) struct LiveNodeTracker {
    nodes: VecDeque<NodeId>,
    rng: StdRng,   // seeded at construction
}
```

Add `LiveNodeTracker::new(seed: u64)`. The seed flows down from `Swim::new()`.

#### 1b. Fix `Ticker` (`src/schedulers/ticker.rs`)

```rust
// before
timers: HashMap<u32, T>,

// after
timers: BTreeMap<u32, T>,  // ordered by seq — deterministic expiry order
```

#### 1c. Fix `Swim` member maps (`src/clusters/swims/swim.rs`)

```rust
// before
members: HashMap<NodeId, SwimNode>,
last_suspected_seqs: HashMap<NodeId, u32>,
pending_join_seqs: HashMap<u32, PendingJoin>,

// after
members: BTreeMap<NodeId, SwimNode>,
last_suspected_seqs: BTreeMap<NodeId, u32>,
pending_join_seqs: BTreeMap<u32, PendingJoin>,
```

#### 1d. Thread the seed

`Swim::new()` takes a `u64` seed, constructs the `StdRng`, and passes it to `LiveNodeTracker`.

#### 1e. The "Meta Test"

Write a test that runs the 3-node cluster formation scenario with the same seed twice and asserts log output is
byte-for-byte identical. This is the determinism proof before fault injection is added.

```rust
#[test]
fn meta_test_same_seed_same_output() {
    let log_a = run_scenario_collect_logs(seed: 42);
    let log_b = run_scenario_collect_logs(seed: 42);
    assert_eq!(log_a, log_b);
}
```

---

### Phase 2 — Transport Abstraction

This phase makes the network stack injectable with a simulated socket.

#### 2a. Define `UdpTransport` trait (`src/clusters/transport.rs`)

```rust
pub trait UdpTransport: Send + 'static {
    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize>;
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)>;
    fn local_addr(&self) -> io::Result<SocketAddr>;
}
```

#### 2b. Implement for `tokio::net::UdpSocket`

Production path. One forwarding impl, zero behavioral change.

#### 2c. Make `SwimTransportActor` generic

```rust
pub struct SwimTransportActor<S: UdpTransport> {
    socket: S,
    to_actor: mpsc::Sender<SwimCommand>,
    from_actor: mpsc::Receiver<OutboundPacket>,
}
```

#### 2d. Update `StartUp::run()`

```rust
SwimTransportActor::<tokio::net::UdpSocket>::new(...)
```

No visible change to production startup behavior.

---

### Phase 3 — Write the DST Harness

#### 3a. Add Turmoil to `Cargo.toml`

```toml
[dev-dependencies]
turmoil = { version = "0.6", features = ["udp"] }
```

#### 3b. Implement `UdpTransport` for `turmoil::net::UdpSocket`

In `tests/dst/transport.rs`, gated on `#[cfg(test)]`.

#### 3c. Create a `run_node()` helper

Takes a seed and `JoinConfig`. Runs the full EastGuard actor stack (SwimTransportActor + SwimActor +
run_scheduling_actor) using the simulated socket. This becomes the Turmoil host closure.

#### 3d. Create `tests/dst.rs` with the first scenario

```rust
#[test]
fn cluster_formation() {
    for seed in 0..100 {
        let mut sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(60))
            .build();

        for node in ["node-a", "node-b", "node-c"] {
            sim.host(node, || run_node(seed, join_config()));
        }

        sim.run().unwrap();
        // assert: all nodes see 3 members in topology
    }
}
```

This is the PR smoke-test: 100 seeds, fast, catches regressions early.

---

### Phase 4 — Fault Injection Scenarios

Once Phase 3 is stable, add these scenarios using Turmoil's fault injection API:

| Scenario                         | What it validates                                     |
|----------------------------------|-------------------------------------------------------|
| Node crash mid-join              | Join retry logic, `max_attempts` behavior             |
| Network partition + recovery     | Suspect/Dead transitions, refutation after reconnect  |
| Packet loss during gossip        | Gossip convergence rate, `gossip_buffer` under stress |
| Concurrent joins                 | Race between multiple nodes joining simultaneously    |
| Slow network (latency injection) | Timeout thresholds are correctly tuned                |

```rust
// Example: partition then repair
sim.partition("node-a", "node-b");
sim.step(); // advance simulated time
sim.repair("node-a", "node-b");
```

---

## Verification Strategy

**The "Meta Test":** Run any scenario with the same seed twice. Compare `TRACE`-level logs down to the last byte. They
must be identical to confirm the simulation is truly deterministic.

**On every PR:** Run the cluster formation scenario with 100 seeds. Print the failing seed on failure so it can be
reproduced locally with a single seed.

**Nightly:** Run a much larger seed sweep (e.g. 10,000 seeds) across all fault injection scenarios.

---

## References

- https://s2.dev/blog/dst
- https://github.com/tursodatabase/turso/blob/main/testing/simulator/README.md
- https://github.com/tokio-rs/turmoil