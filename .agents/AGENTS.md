# east-guard
EastGuard = zero-controller messaging system for flexible scalability + high operability. Inspired by LinkedIn's Northguard architecture.

# 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

# 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

# 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

# 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

# Two replica sets (don't conflate)

EastGuard has two distinct "replica sets", chosen by different mechanisms and **decoupled** — they drift apart and must not be confused:

- **Raft replica set** — a shard group's *consensus peers* (`Raft::peers`: voters + learners). Who replicates the group's *metadata log*. Changes via committed `AddPeer`/`RemovePeer` (see `raft.md`).
- **Data replica set** — a *segment's* `replica_set` (`SegmentMeta.replica_set`). Where the segment *bytes* live. Changes via committed `RollSegment`/`ReassignSegment`, decided by the owning Raft group's leader (see `metadata-state-machine.md`, `raft-actor.md`).

A node can be in one without the other. "Is node N a data replica of segment S?" is answered only by S's owning Raft group's committed `replica_set` — **never** by whether N hosts that group or is one of its Raft peers. A node that holds S's bytes on disk but isn't in S's data replica set is a stray, regardless of its Raft membership.

# Code Quality

## Clippy
After every code change, run clippy + fix all errors before task done:
```sh
cargo clippy --all-targets --all-features -- -D warnings
```
All warnings = errors (`-D warnings`). No `#[allow(...)]` to suppress legitimate warnings — fix underlying issue. Use `#[allow(dead_code)]` only for code intentionally kept for future use or used only in test targets.

## Invariants vs. Rules

Before calling something an invariant, make sure it is one — don't reach for "invariant" to dignify an ordinary behavioral guarantee.

- **Invariant** — a *structural* property: a predicate over current state that is **always** true and checkable on a snapshot, with no notion of time or order. This is exactly what `assert_invariants()` asserts. E.g. "each active range has exactly one active segment"; "`last_applied ≤ commit_index ≤ stabled_index`". If you can't write it as an assertion over present state, it isn't an invariant.
- **Rule** — everything else: behavioral, liveness, and ordering guarantees about what the system *does over time*. E.g. "every event is followed by a flush"; "a seal's end is recovered before the roll"; "catch-up is re-driven until the replica confirms". These have no checkable snapshot — they're enforced by construction or protocol and live in `.claude/rules/`. They do **not** go in `assert_invariants()`.

The confirmation gate below is for **invariants** — they ride in every test via `assert_invariants`, so adding one touches every contributor. Rules are ordinary design: document them, don't gate on them, and don't dress them up as invariants.

## Invariant Checking
State machines (e.g., `MetadataStateMachine`, `Raft`, `Topology`, `MetadataStorage`) have `#[cfg(test)] fn assert_invariants(&self)` methods that verify documented invariants at runtime. These are called automatically after every state-changing operation.

When adding or modifying a state-changing method on any state machine:
1. Ensure the method calls `assert_invariants()` (guarded by `#[cfg(test)]`) after mutation
2. If a new invariant is introduced, add its check to the component's `assert_invariants()`
3. If an invariant is documented in `.claude/rules/` but not checked in `assert_invariants()`, add it

This makes every existing and future test an invariant test automatically — no separate invariant tests needed.

If a code change introduces a new invariant (or modifies an existing one), **always ask the user for confirmation before proceeding** — regardless of the permission mode (auto, plan, default, etc.). Invariants are system-level contracts; adding or changing one affects every test and every future contributor.

## Testing: the data plane under turmoil

turmoil runs SWIM + DS-RSM as cooperative tasks on **one thread under a virtual clock it controls** — the basis of deterministic e2e. In **production** the data plane runs on its **own OS thread** (`DataPlaneActor` → `thread::Builder` + a dedicated current-thread runtime; likewise the cold-read pool and checkpoint worker), doing blocking I/O (WAL/fsync). A real OS thread runs in real wall-time outside turmoil's clock + scheduling — so e2e assertions on its effects would race the virtual clock (the sim fast-forwards virtual time while the real thread still needs real seconds).

So **in `#[cfg(test)]` builds the data-plane workers run as tasks on turmoil's runtime** — the spawn is `#[cfg]`'d (prod = OS thread + own runtime; test = `tokio::spawn`) — sharing the sim's virtual clock and cooperative scheduling, which makes their effects deterministic. Requirements:

1. **Async dispatch, never `blocking_send`.** tokio's `blocking_send` panics inside a runtime, so the worker's flush awaits (`send_batch` / `send_timer_batch`). The worker mailbox is `flume` — sync `recv()` on the prod thread, `recv_async()` on the test task (one channel type).
2. **Every real OS thread in the tested path gets the same `#[cfg]` treatment.** The data-plane worker and the cold-read pool (serves cold/sealed fetches *and* the catch-up source read) both run as tasks under test; a leftover real thread reintroduces the race.
3. **turmoil virtualizes `tokio::time`, NOT `std::time`.** A data-plane timer that must be deterministic uses `tokio::time::Instant` (virtualized on the sim runtime; the seal-retry timeout does). `std::time` stays real wall-time even on the sim runtime — segment `created_at` still uses it — so **drive tests by size-based seal (`size_bytes >= limit`, checked on commit) and event-driven catch-up, never by age** (`created_at.elapsed()` is real-time).
4. Data-plane *logic* is also covered by synchronous `DataPlane` state-machine tests (`src/data_plane/state.rs`) — no clock, no threads, fully deterministic.
5. **A multi-node e2e must pin every nondeterministic input** or the sim isn't reproducible: the turmoil RNG (`Builder::rng_seed` — randomly seeded by default) and node ids (`node_id_suffix` — the default is `{prefix}::{Uuid::new_v4()}`, OS-random, which shuffles the hash ring). Beware per-process `HashMap` order (Rust's `RandomState` is process-random and can't be pinned) — keep order-dependent logic out of the asserted path. **`vnodes_per_node` is now a speed knob, not a correctness one:** low (e.g. 16) means fewer Raft groups → faster sim. At 256 the ~1000 groups pressure SWIM's one-shot shard-leader gossip, so a coordinator can momentarily resolve `MAP-EMPTY` — but the data-plane **topology fallback** (`SendToCoordinator` on `None` → broadcast to the ring's members; the Raft leader acts, followers no-op) recovers it, so repair still completes (the former #135 gap). `sealed_repair_survives_coordinator_crash` runs at 256 deliberately to guard that fallback; the other repair e2es run at 16 for speed.

Also: `crate::net` is `turmoil::net` under `#[cfg(test)]`, so an in-binary `#[tokio::test]` gets simulated sockets — a real-socket harness would need `tests/` (lib links non-test → real `tokio::net`).

---

# Subsystem Rule Routing

This project enforces strict subsystem-level architectural rules and invariants. Because these rules are kept in individual files to keep the prompt size manageable, you **MUST** read the relevant rule file(s) from [.agents/rules/](file:///Users/migolee/rust_codes/EastGuard/.agents/rules/) using the `view_file` tool before making any changes if your task touches any of the following subsystems or components:

| Subsystem / Topic | Rule File to Read | Trigger Components / Keywords |
| :--- | :--- | :--- |
| **Coding Conventions** | [code-convention.md](file:///Users/migolee/rust_codes/EastGuard/.agents/rules/code-convention.md) | Enforced for *all* code changes. Covers encapsulation, enum struct pattern, Box vs. Vec, error propagation, and batching. |
| **DS-RSM Core** | [ds-rsm.md](file:///Users/migolee/rust_codes/EastGuard/.agents/rules/ds-rsm.md) | Shard group consensus, SWIM authority, sync-first actor design, flushing output buffers, unique timer seqs, single-topic operations. |
| **Metadata State Machine** | [metadata-state-machine.md](file:///Users/migolee/rust_codes/EastGuard/.agents/rules/metadata-state-machine.md) | `MetadataStateMachine`, `TopicMeta`, `RangeMeta`, `SegmentMeta`, split/merge operations, keyspace coverage, offset continuity. |
| **Raft Actor Wrapper** | [raft-actor.md](file:///Users/migolee/rust_codes/EastGuard/.agents/rules/raft-actor.md) | `RaftActor`, `MultiRaftActor`, actor boundary, message loops, leader-election reconciliation. |
| **Raft Consensus** | [raft.md](file:///Users/migolee/rust_codes/EastGuard/.agents/rules/raft.md) | `Raft` state machine, log replication, terms, votes, peers, learners, membership change log flows. |
| **Raft Transport** | [raft-transport.md](file:///Users/migolee/rust_codes/EastGuard/.agents/rules/raft-transport.md) | Outbound/inbound packet routing, UDP, socket connections, node ID to address resolution. |
| **Scheduler & Timers** | [scheduler.md](file:///Users/migolee/rust_codes/EastGuard/.agents/rules/scheduler.md) | `Ticker`, `RaftTimerTokenGenerator`, timeout dispatch, timer sequence management. |
| **Storage Layout** | [storage-layout.md](file:///Users/migolee/rust_codes/EastGuard/.agents/rules/storage-layout.md) | RocksDB key structure, log entries, hard states, group prefixes, atomic flushes, `stabled_index`. |
| **SWIM Membership** | [swim.md](file:///Users/migolee/rust_codes/EastGuard/.agents/rules/swim.md) | `Swim`, `SwimActor`, failure detection, gossip dissemination, incarnation, suspect/dead states. |
| **Topology & Ring** | [topology.md](file:///Users/migolee/rust_codes/EastGuard/.agents/rules/topology.md) | `Topology`, consistent hash ring, virtual nodes, routing, shard leader tracking. |

## Instructions for the Agent
1. When starting a task, identify which subsystems or components are involved.
2. Proactively read the corresponding rule file(s) listed above before proposing or writing any code.
3. Treat the rules and invariants in those files as strict system contracts.
