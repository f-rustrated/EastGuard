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

- **`SimScenario::from_seed(u64)`** — deterministically generates a scenario from a seed using `StdRng`. Same seed always produces the same topology, faults, and commands.
- **`run_for_scenario(&SimScenario)`** — builds a `turmoil::Sim`, spawns one EastGuard process per node slot, and runs a `checker` client task that issues commands at their scheduled timestamps and calls invariant checkers at the end. Fault injection (node kills) is applied by stepping the virtual clock to `fault.at_secs` and calling `sim.crash()`.

Also provides shared helpers (`try_propose`, `make_create_topic_req`, `node_name`, `client_port`, `cluster_port`) used by both the checker task and `properties.rs`.

### `invariants.rs` — Cross-Node Assertion Library

Reusable async functions that talk to live node APIs through the virtual network. Not tests themselves — building blocks for both `scenario.rs` and `properties.rs`.

| Function | What it checks |
|---|---|
| `assert_membership_converged` | All alive nodes return the same sorted set of `Alive` member IDs. Retries up to N times. |
| `assert_single_leader` | All nodes that know a leader for a given shard group agree on the same one (no split-brain). |
| `assert_topic_visible` | After a `CreateTopic` ack, all alive nodes eventually return the topic via `GetTopics`. |

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
                └── assert_topic_visible            ← invariants.rs

on Err ──► shrink_scenario(failing)  ← bugbase.rs
                └── record_failure → bugbase/<hash>.json
                        └── bugbase_regression re-runs on next CI
```

## Running

```sh
# Run all property tests
cargo test -p east-guard --test '*' it::sim

# Run the 100-seed loop (slow, ignored by default)
cargo test sim_loop_100 -- --ignored

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

Cross-node durability (no record lost across leader failure) requires replication (`TODO replication(D2)`) to be wired first. Until then, the three properties above are testable today and cover the highest-risk single-node data plane behaviors.

## TODO

### Phase 1 — Unblock the loop + add partitions
- Fix the root cause of `sim_loop_100` failures (topic not visible after startup delay) and remove `#[ignore]`.
- Implement `FaultKind::PartitionNode` / `HealNode` in `run_for_scenario` using `sim.partition()` / `sim.repair()`. Wire them into `SimScenario::from_seed` so the loop exercises partition faults. This covers split-brain scenarios that node kills cannot reach.

### Phase 3 — Targeted fault sequences
- Add `FaultKind::PartitionLeader` — resolves the current leader via `GetShardLeader` at fault time, then partitions it. Untargeted partition faults are much weaker.
- Add `FaultKind::FlappingPartition { node, heal_after_secs }` — partition then heal within the same scenario, forcing SWIM death detection followed by rejoin. The hardest convergence case.

### Phase 4 — Campaign runner
- Promote the sim loop to a standalone binary (`src/bin/sim.rs`) with subcommands: `run`, `loop --n <N>`, `replay <hash>`, `list`.
- Add a `--profile` flag to parameterize fault rates and scenario density without code changes.
- Run `loop --n 500` nightly in CI and post failures to Slack.

## Constraints

- Use `tokio::time::sleep` — `turmoil` owns the virtual clock; `std::thread::sleep` breaks it.
- Every turmoil test requires `#[serial_test::serial]` — turmoil is process-global.
- Invariant assertions belong in `sim.client("checker", ...)`, never inside node tasks.
