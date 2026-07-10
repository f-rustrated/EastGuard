# it/sim — Deterministic Simulation Tests

Property-based integration tests using [`turmoil`](https://github.com/tokio-rs/turmoil), a virtual network simulator. Each test runs a real EastGuard cluster on a fake clock and network, injecting faults and asserting system-wide invariants.

## Module Map

```
it/sim/
├── scenario.rs    — scenario types, seeded generator, turmoil executor
├── invariants.rs  — reusable cross-node assertions (membership, leader, metadata)
├── properties.rs  — concrete named property tests (#[test])
└── bugbase.rs     — failure shrinker + on-disk regression suite
```

## How the Pieces Fit Together

### `scenario.rs` — Core Engine

Defines `SimScenario` (node count, duration, fault events, command events) and two entry points:

- **Seeded generation** — deterministically produces the same topology, commands,
  and fault profile from the same seed. The first five seed classes deliberately
  cover steady state, loss of a sealed data replica, partition/heal, an active
  data-replica flap, and current metadata-leader partition/heal. The simulator
  RNG, node identities, and node-local election randomness all derive from the
  scenario seed, so replay preserves the network and election schedule as well
  as the generated events.
- **Scenario execution** — builds a simulated cluster, issues commands at their
  scheduled timestamps, produces acknowledged records, forces a size-based
  segment roll, applies faults on the virtual clock, and checks the surviving
  cluster after its convergence window. The replica-loss profile verifies death
  classification and acknowledged-record durability. Dedicated deterministic
  repair scenarios verify replacement and catch-up completion. The flap profile
  sends another record while the selected data replica is isolated, exercising
  bounded client retries through the recovery window.

Also provides shared helpers (`try_propose`, `make_create_topic_req`, `node_name`, `client_port`, `cluster_port`) used by both the checker task and `properties.rs`.

### `invariants.rs` — Cross-Node Assertion Library

Reusable async functions that talk to live node APIs through the virtual network. Not tests themselves — building blocks for both `scenario.rs` and `properties.rs`.

| Function | What it checks |
|---|---|
| `assert_membership_converged` | All alive nodes return the same sorted set of `Alive` member IDs. Retries up to N times. |
| `assert_single_leader` | All nodes that know a leader for a given shard group agree on the same one (no split-brain). |
| Topic durability | After a create acknowledgment, an original shard quorum reports the topic whenever that quorum survives the scenario. |
| Single-leader safety | Surviving shard members never report conflicting leaders. |
| Record durability | Every acknowledged entry remains readable after the generated fault. |
| Offset continuity | A size-triggered roll exposes a sealed end immediately followed by the successor start. |
| Repair completion | A killed sealed replica is replaced, and the replacement serves the old entry. |
| Client retry bound | Production during a replica flap must receive an acknowledgment within the recovery budget. |

### `properties.rs` — Concrete Property Tests

Handwritten deterministic scenarios, each a `#[test]`. Imports invariant checkers from `invariants.rs` and propose helpers from `scenario.rs`.

| Test | Property |
|---|---|
| `metadata_visible` | A committed `CreateTopic` is eventually visible on all alive nodes. |
| `leader_elects_after_kill` | Killing the shard leader causes a new leader to be elected within 30s; survivors agree on exactly one. |
| `membership_converges_after_rejoin` | A crashed-then-restarted node is eventually seen as `Alive` by all survivors. |

### `bugbase.rs` — Shrinking and Regression Storage

Two responsibilities:

- **`shrink_scenario`** — given a failing `SimScenario`, finds the smallest one that still fails. Strips faults one at a time, then commands, then reduces node count toward 3, then binary-searches `simulation_secs` toward 30.
- **`record_failure`** — serializes the shrunk scenario to `bugbase/<crc32>.json`.
- **`bugbase_regression` test** — re-runs every `.json` in `bugbase/` as a permanent regression suite. Files stay on disk until manually removed after a fix is confirmed.

## End-to-End Flow

```
SimScenario::from_seed(seed)
        │
        ▼
run_for_scenario(&scenario)          ← scenario.rs
        │
        ├── turmoil::Sim
        │       └── N EastGuard nodes (real code, virtual network + clock)
        │
        └── checker client task
                ├── try_propose(...)                ← scenario.rs
                ├── assert_membership_converged     ← invariants.rs
                └── assert_topic_visible_on_quorum            ← invariants.rs

on Err ──► shrink_scenario(failing)  ← bugbase.rs
                └── record_failure → bugbase/<hash>.json
                        └── bugbase_regression re-runs on next CI
```

## Running

```sh
# Run all property tests
cargo test -p east-guard --test '*' it::sim

# Run the per-change campaign (10 seeds by default)
cargo test sim_campaign -- --nocapture --test-threads=1

# Run a larger local campaign
EG_SIM_SEED_COUNT=500 cargo test sim_campaign -- --nocapture --test-threads=1

# Replay exactly one generated seed
EG_SIM_SEED=73 cargo test sim_campaign -- --nocapture --test-threads=1

# Replay a saved bug scenario
cargo test bugbase_regression
```

## Data Plane Coverage

The data plane (`DataPlane<W>`, `WalStorage`, `SegmentTracker`, checkpoint, cold read) follows the same sync state-machine pattern as Raft and SWIM, so it is fully simulatable under turmoil. DST can prove correctness in three areas:

| Property | What to assert |
|---|---|
| **WAL recovery** | After a node crash + restart, all previously acked `Produce` records survive and are readable. |
| **Produce/consume round-trip** | After `Produce` is acked, a consumer read against the same segment returns the same records in the same order. |
| **Segment seal → checkpoint handoff** | When `MetadataStateMachine` commits a `RollSegment`, the data plane checkpoints the sealed segment and the new segment accepts subsequent writes without a gap. |

The generated campaign covers cross-node replication, rolling, replica loss,
sealed repair, and production during a replica flap. Targeted deterministic
scenarios complement it with the longer multi-phase paths that are poor fits for
random generation: active-leader crash boundary recovery, coordinator loss during
repair, disk reuse after restart, range split consumption, and broker restart
during consumer-group scale-out. The latter commits the pre-rebalance offsets and
rejects any replay while the restarted group consumes the next batch.

## TODO

### Campaign runner
- A standalone command could make corpus browsing more convenient, but replay and
  bounded/extended execution are already available through the test entry point
  and environment variables above.

## Constraints

- Use `tokio::time::sleep` — `turmoil` owns the virtual clock; `std::thread::sleep` breaks it.
- Every turmoil test requires `#[serial_test::serial]` — turmoil is process-global.
- Invariant assertions belong in `sim.client("checker", ...)`, never inside node tasks.
