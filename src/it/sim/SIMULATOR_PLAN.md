# Simulation Testing Plan

Deterministic simulator on top of `turmoil`. Each phase is self-contained.

**What already fires automatically** — `TAssertInvariant` is called inside every state-mutating method on `Swim`, `Raft`, and `MetadataStateMachine`. Per-node structural invariants are free; the sim adds cross-node agreement checks.

---

## Module Structure

```
src/it/sim/
├── mod.rs
├── invariants.rs   (Phase 1)
├── scenario.rs     (Phase 2)
├── properties.rs   (Phase 3)
└── bugbase.rs      (Phase 4)
```

Add `mod sim;` to `src/it/mod.rs`.

---

## Phase 1 — Cross-Node Invariant Checkers (`invariants.rs`)

Reusable async functions called from a turmoil `checker` task. Not tests themselves.

**1a. Membership convergence** — query `GetMembers` from every alive node; all must return the same sorted set of `Alive` node IDs.

**1b. Single leader per shard** — add `QueryCommand::GetShardLeader { shard_group_id: u64 }` to `src/connections/request.rs` and wire through `src/connections/clients.rs`. Assert no two nodes claim leadership of the same shard in the same term. *(Can defer to Phase 3 if too invasive.)*

**1c. Metadata agreement** — add `QueryCommand::GetTopics` returning `Vec<TopicSummary>` from the local `MetadataStateMachine`. After a `CreateTopic` ack, all alive nodes must eventually return the topic (retry up to 30 × 100ms ticks).

Shared helpers (`src/it/helpers.rs`): `get_members`, `check_alive_count`, `default_env`, `send_propose` — already exist, import from there.

---

## Phase 2 — Seeded Scenario Generator (`scenario.rs`)

```rust
// dev-dependencies: rand_chacha = "0.3"
pub struct SimScenario {
    pub seed: u64,
    pub node_count: u8,        // 3..=5
    pub simulation_secs: u64,  // 30..=120
    pub faults: Vec<FaultEvent>,
    pub commands: Vec<CommandEvent>,
}

pub enum FaultKind { KillNode(u8), PartitionNode(u8), HealNode(u8) }
pub enum CommandKind { CreateTopic { name: String }, DeleteTopic { name: String } }
```

`SimScenario::from_seed(seed)` uses `ChaCha8Rng` to deterministically populate all fields. The checker task dispatches events in tick order, then calls `assert_cluster_invariants` after a settle period.

**Loop test in `mod.rs`:**
```rust
#[test]
fn sim_loop_100() {
    for seed in 0..100 {
        run_scenario(seed).unwrap_or_else(|e| panic!("seed={seed} failed: {e}"));
    }
}
```

---

## Phase 3 — Property Library (`properties.rs`)

Each property: setup → workload (with interleaved faults) → assertion.

| Property | Assert |
|---|---|
| `MetadataVisible` | After `CreateTopic` ack + convergence timeout, all alive nodes return the topic |
| `LeaderElectsAfterKill` | After killing the leader, exactly one node reports a higher-term leader within 30 ticks |
| `MembershipConvergesAfterRejoin` | After kill + restart, all surviving nodes see the rejoined node as `Alive` |
| `NoDataLossAfterLeaderSwitch` | All committed records readable after leader switch *(skip if DataPlane protocol not wired)* |

---

## Phase 4 — Shrinking + BugBase (`bugbase.rs`)

When `run_scenario` returns `Err`, call `shrink_scenario(failing_seed)`:
- Strip fault events one by one
- Strip command events one by one
- Reduce node count and simulation duration
- Return the smallest `SimScenario` that still fails

Serialize the result to `bugbase/<hash>.json` (add `serde_json = "1"` to dev-dependencies).

Regression suite re-runs every `bugbase/*.json` entry in CI.

---

## Constraints

- Use `tokio::time::sleep`, never `thread::sleep` — turmoil owns the virtual clock.
- Every turmoil test needs `#[serial_test::serial]` — turmoil is process-global.
- Invariant checks only in `sim.client("checker", ...)`, not inside node tasks.
- Node config must use `uuid::Uuid::new_v4()` for data/meta dirs (`default_env` in `src/it/helpers.rs` already does this).
- Do not proceed to the next phase until `cargo test` is green.

---

## TODO: Tests to Remove Once sim/ Is In Place

- **After Phase 3** — `src/it/swim/membership.rs::dead_node_rejoin_after_process_restart` (exact duplicate of `MembershipConvergesAfterRejoin`)
- **After Phase 3 matures** — `src/it/swim/membership.rs::cluster_setup` and `src/it/e2e/lifecycle.rs::e2e_swim_raft_cluster_lifecycle`
- **Keep permanently** — `partition_gossip`, `forwarded_request_not_forwarded_again`, `three_node_raft_elects_leader`, `node_death/join_triggers_*`, `leader_election_emits_leader_change_event`
