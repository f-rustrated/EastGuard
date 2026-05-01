# EastGuard Metadata Management Roadmap

SWIM + MultiRaft infrastructure is drafted. This roadmap covers the path from the current consensus/membership layer to a full metadata management system — topics, ranges, segments — following the DS-RSM (Dynamically-Sharded Replicated State Machine) architecture inspired by Northguard.

---

## Phase 1: Data Model + MetadataStateMachine

**Goal:** Define metadata types and a pure in-memory state machine.



### Types (`types.rs`)

```
TopicId(u64), RangeId(u64), SegmentId(u64)

TopicState  { Active, Sealed, Deleted }
RangeState  { Active, Sealed, Deleting }
SegmentState { Active, Sealed, Reassigning { from, to }, Deleting }

StoragePolicy { retention_ms, replication_factor }

TopicMeta {
    topic_id, name, state, storage_policy,
    active_ranges: Vec<RangeId>,              // keyspace coverage for write routing
    ranges: HashMap<RangeId, RangeMeta>,      // all ranges including sealed
    next_range_id: u64,
}

RangeMeta {
    range_id, keyspace: [start, end),
    state, active_segment,
    segments: HashMap<SegmentId, SegmentMeta>,
    next_segment_id: u64,
    next_offset: u64,
    split_into, merged_into, merged_from
}

SegmentMeta {
    segment_id, state,
    replica_set, size_bytes,
    start_offset: u64, end_offset: Option<u64>,
    created_at, sealed_at
}
```

ID counters scoped to parent: `next_range_id` in TopicMeta, `next_segment_id` in RangeMeta. No back-references — ownership expressed by nesting.

### State Machine (`state_machine.rs`)

```rust
struct MetadataStateMachine {
    topics:           HashMap<TopicId, TopicMeta>,
    topic_name_index: HashMap<String, TopicId>,
    next_topic_id:    u64,
}
```

Flat maps for ranges/segments removed — they live nested inside `TopicMeta` and `RangeMeta` respectively.

Pure functions: `apply_create_topic()`, `apply_split_range()`, `apply_roll_segment()`. No I/O, no async.

See `diagrams/metadata-management/data-model.md` for full entity relationships, state transitions, cascading effects, and invariants.

**Depends on:** Nothing.
**Scope:** ~300-400 lines + unit tests.

---

## Phase 2: Extend RaftCommand ✅

**Goal:** Grow `RaftCommand` from internal-only to include metadata operations.

### File: `src/clusters/raft/messages/command.rs`

**`RaftCommand` wraps `MetadataCommand`:**

```rust
enum RaftCommand {
    Noop,
    Metadata(MetadataCommand),
}
```

`MetadataCommand` is a separate enum in `src/clusters/metadata/command.rs` with typed structs per variant:

```rust
enum MetadataCommand {
    CreateTopic(CreateTopic),
    RollSegment(RollSegment),
    SplitRange(SplitRange),
    MergeRange(MergeRange),
    DeleteTopic(DeleteTopic),
}
```

Each struct carries its own fields (e.g., `CreateTopic { name, storage_policy, replica_set, created_at }`). `DeleteTopic` uses `name: String` — resolved to `TopicId` at apply time via `topic_name_index`.

No `AssignRange` — ranges emerge from `CreateTopic` (initial full-keyspace range) or `SplitRange`/`MergeRange` (lifecycle). See `diagrams/metadata-management/data-model.md` § "How Range ID and Keyspace Are Determined".

**`ProposeError`:**

```rust
enum ProposeError {
    NotLeader,
    ShardNotFound,
}
```

`NotLeader(Option<NodeId>)` added in Phase 5 — carries leader hint. Epoch cancelled (not needed).

**Depends on:** Phase 1 (needs `TopicId`, `StoragePolicy`, etc.).
**Scope:** Mostly type changes + match arm updates.

---

## Phase 3: Application State Machine Dispatch

**Goal:** `Raft` owns `MetadataStateMachine` and applies committed metadata commands inline.

### 3a. Embed `MetadataStateMachine` inside `Raft`

Raft = log machine + state machine. EastGuard is a metadata system, not a generic Raft library. Only one state machine type ever exists. Direct ownership, no trait, no generic.

```rust
// In state.rs:
pub struct Raft {
    // ... existing consensus fields ...
    state_machine: MetadataStateMachine,
}
```

Initialized to `MetadataStateMachine::default()` in `Raft::new()`.

### 3b. Inline apply in `apply_committed_entries()`

```rust
fn apply_committed_entries(&mut self) {
    while self.last_applied_index < self.commit_index.min(self.stabled_index) {
        self.last_applied_index += 1;
        let entry = self.log_get(self.last_applied_index).clone();
        match entry.command {
            RaftCommand::Noop => {}
            RaftCommand::Metadata(cmd) => {
                match self.state_machine.apply(cmd) {
                    Ok(result)  => tracing::debug!("Applied: {:?}", result),
                    Err(e)      => tracing::error!("Apply error: {:?}", e),
                }
            }
        }
    }
}
```

No buffer-drain pattern. No `take_applied_entries()`. Apply happens at consensus boundary, inside `Raft`, guarded by `commit_index.min(stabled_index)`.

Membership changes (`AddPeer`/`RemovePeer`) are handled separately via `MultiRaft` — SWIM is membership authority, not the Raft log.

### 3c. Read-only accessor

```rust
pub(crate) fn state_machine(&self) -> &MetadataStateMachine {
    &self.state_machine
}
```

For tests to query applied state.

---

## Phase 4: Client → Raft Propose Pathway

**Goal:** Wire end-to-end path: Client → `MultiRaftActor` → hash(key) → propose to local Raft → commit → apply → respond.

`MultiRaftActor` handles routing internally. No separate broker/routing layer needed.

### 4a. Redesign `MultiRaftActorCommand::Propose`

Current (dormant, zero callers):
```rust
Propose {
    shard_group_id: ShardGroupId,  // caller must know the shard
    command: RaftCommand,
    reply: oneshot::Sender<Result<(), ProposeError>>,
}
```

New:
```rust
Propose {
    resource_key: Vec<u8>,         // MultiRaft hashes internally
    command: RaftCommand,
    reply: oneshot::Sender<Result<(), ProposeError>>,
}
```

Inside `MultiRaft::propose()`:
```rust
fn propose(&mut self, resource_key: &[u8], command: RaftCommand) -> Result<(), ProposeError> {
    let shard_id = ShardGroupId::new(resource_key);
    match self.groups.get_mut(&shard_id) {
        Some(shard) if shard.raft.is_leader() => {
            shard.raft.propose(command)
            // dirty tracking, etc.
        }
        Some(shard) => Err(ProposeError::NotLeaderHint(
            shard.raft.current_leader().cloned()
        )),
        None => Err(ProposeError::NotLeader),
    }
}
```

**Why MultiRaftActor routes, not a separate layer:**
- Already has `HashMap<ShardGroupId, Raft>` — checks `is_leader()` directly
- `ShardGroupId::new(key)` is a pure hash — no topology query needed
- Avoids async round-trip to SWIM for routing
- Client handler in `lib.rs` stays thin

### 4b. Extend client protocol

**File:** `src/connections/request.rs`

```rust
enum ConnectionRequests {
    // Existing
    Discovery,
    Connection(ConnectionRequest),
    Query(QueryCommand),
    // New
    Propose(ProposeRequest),
}

struct ProposeRequest {
    resource_key: Vec<u8>,
    command: ClientCommand,
}

enum ClientCommand {
    CreateTopic { name: String, storage_policy: StoragePolicy },
}

enum ProposeResponse {
    Success,
    NotLeader(Option<NodeId>),
    ShardNotFound,
    Error(String),
}
```

### 4c. Thread `raft_tx` to client handler

In `lib.rs`, `raft_tx` is currently moved into `SwimActor::run()`. Clone before that, pass clone to `receive_client_streams`. Client handler translates `ClientCommand` → `RaftCommand`, sends `MultiRaftActorCommand::Propose { resource_key, command, reply }`.

### 4d. Commit-completion tracking

Current `raft.propose()` returns `Ok(())` on log append — does NOT wait for commit. For client API, must wait for commit + apply.

```rust
// In MultiRaft:
pending_proposals: HashMap<(ShardGroupId, u64 /* log_index */), oneshot::Sender<ProposeResult>>,
```

On `propose()`: store `(shard_id, log_index) → reply`.
On `take_applied_entries()`: resolve reply with result.
On leader stepdown: drain all pending, send `NotLeader`.

### 4e. Full propose flow

```
Client                 lib.rs              MultiRaftActor            Raft
  │── CreateTopic ──────▶│                       │                     │
  │   ("blue")           │── Propose ───────────▶│                     │
  │                      │   key="blue"          │── hash("blue")      │
  │                      │   cmd=CreateTopic     │   = shard #45       │
  │                      │                       │── groups[#45]       │
  │                      │                       │   .is_leader()? yes │
  │                      │                       │── raft.propose() ──▶│
  │                      │                       │                     │
  │                      │                       │  ... replication ...│
  │                      │                       │                     │
  │                      │                       │◀── committed ───────│
  │                      │                       │   apply to SM       │
  │                      │                       │   resolve reply     │
  │◀── Success ──────────│◀──────────────────────│                     │
```

**Depends on:** Phases 1-3.
**Scope:** 3-4 PRs.

---

## Phase 5: Leader Forwarding + Shard Discovery 

**Goal:** Non-leader nodes forward proposals transparently; clients can discover shard → leader mappings.

### 5a. Leader forwarding 

`ProposeError::NotLeader(Option<NodeId>)` carries leader hint. Non-leader nodes transparently forward proposals to the leader via TCP. Two-strategy forwarding:
1. **Fast path:** Use leader hint + `ResolveAddress` → `NodeAddress.client_addr` (SWIM membership, converges in ~2-3s)
2. **Slow path:** Fall back to `ResolveShardLeader` → shard leader gossip

Max 1 hop — `ProposeRequest.forwarded: bool` prevents re-forwarding.

### 5b. Epoch validation — cancelled ❌

Originally planned to track `conf_ver` per shard group. Cancelled because:
- Clients never send replica sets — server resolves from SWIM topology
- SWIM convergence (~2-3s) makes staleness window near-zero
- `NotLeader` hint + forwarding already handles leader changes
- Even stale replica sets are self-healing on subsequent operations

Revisit only if client-side routing cache bypasses server-side routing.

### 5c. Shard discovery query 

```rust
QueryCommand::GetShardInfo { key: Vec<u8> }
// Returns: Option<ShardInfoResponse { shard_group_id, leader_node_id, leader_addr }>
```

Single actor round-trip via `SwimQueryCommand::GetShardInfo` — resolves shard group + leader in one hop.

### 5d. Client-side routing cache (backlog)

Client SDK concern. Server-side signals available:
- `ProposeResponse::Error(NotLeader(Some(leader_id)))` — cache miss
- `GetShardInfo` — cache population
- Error-driven invalidation (no TTL, no polling)

### Architecture changes in Phase 5

- **`NodeAddress { cluster_addr, client_addr }`** — first-class value object used in `SwimNode.addr`, `ShardLeaderInfo`, `ShardLeaderEntry`, `Swim.self_addr`, `RaftWriters.addr_cache`. Derives `Copy`.
- **`SwimSender` / `RaftSender`** — typed wrappers encapsulating channel + oneshot request-reply pattern. Clean client handler code in `clients.rs`.
- **`port` → `client_port`** — explicit naming (`--client-port`, `EASTGUARD_CLIENT_PORT`). Port collision check in `init()`.

**Depends on:** Phase 4.
**Scope:** Completed in 4 PRs (epoch cancelled).

---

## Phase 6: Hot Range Detection + Auto-Split/Merge 

**Goal:** MetadataStateMachine detects hot/cold ranges and proposes `SplitRange`/`MergeRange` automatically. Simplest viable approach — no probe protocol, no key histograms.

### Core Insight

`MetadataStateMachine` already sees every `RollSegment` commit. A segment seals when it reaches ~1GB. If a range's segments seal frequently, that range is hot. No external metrics pipeline needed — the Raft log IS the signal.

### 6a. Per-Range Seal Tracker

`RangeSealHistory` on `RangeMeta`:

```rust
struct RangeSealHistory {
    seal_timestamps: Vec<u64>,         // recent seal times within measurement window
    created_by_split_at: Option<u64>,  // cooldown anchor for child ranges
}
```

Lives inside `RangeMeta` — no separate tracker map needed. Naturally scoped to the range, cleaned up when range is deleted.

Updated inside `RangeMeta::roll_segment()` via `record_seal()` — pure, deterministic, replicated on every node. Old timestamps pruned on each seal (sliding window).

### 6b. Split Decision Logic

`RangeMeta::roll_segment()` returns `should_split` bool. `MetadataStateMachine::roll_segment()` checks the return value (leader-only evaluation via `pending_proposals` buffer):

```rust
// RangeSealHistory::should_split():
seal_count >= SPLIT_SEAL_THRESHOLD
    && (no split cooldown OR cooldown expired)

// RangeMeta::roll_segment() returns should_split
// MetadataStateMachine checks: can_split && should_split && valid_split_point(midpoint)
```

Split point = midpoint of keyspace (`RangeMeta::compute_midpoint()`). `valid_split_point()` guards against ranges too narrow to split (integer truncation makes midpoint equal to start).

**Constants:**

| Constant | Value | Meaning |
|---|---|---|
| `SPLIT_SEAL_THRESHOLD` | 3 | Seals within window to trigger split |
| `MEASUREMENT_WINDOW_MS` | 300_000 (5 min) | Sliding window for counting seals |
| `SPLIT_COOLDOWN_MS` | 300_000 (5 min) | No re-split after recent split |
| `MERGE_SEAL_THRESHOLD` | 0 | Both ranges must be idle (no seals in window) |

### 6c. Merge Decision Logic

Periodic check by leader via `MergeCheckTimeout` timer (not per-event):

```rust
// MetadataStateMachine::evaluate_merges(now):
// For each topic with AutoSplit strategy and 2+ active ranges:
//   Walk adjacent pairs in active_ranges (sorted by keyspace_start)
//   If both ranges have recent_seal_count(now) == MERGE_SEAL_THRESHOLD:
//     Propose MergeRange, break (one merge per topic per evaluation)
```

`RangeMeta::mergeable_with()` checks both ranges are cold. Hysteresis built in: split at 3 seals/window, merge at 0. No oscillation.

### 6d. Proposal Path — Lazy Execution

Auto-proposals flow through a two-level buffer:

1. `MetadataStateMachine::pending_proposals` — populated during `apply()` (deterministic on all replicas, but only leader drains them)
2. `Raft::pending_proposals` — leader wraps in `RaftCommand::Metadata(...)`, drained by `MultiRaft::flush()`

**Lazy execution:** `MultiRaft::flush()` drains `pending_proposals` at the **start** of the next flush cycle, proposing them as regular entries. They are persisted in the same batch — no second persist pass. ~100ms latency (one tick) is negligible for decisions based on 5-minute windows.

**Anti-recursion:** `apply_committed_entries()` never calls `propose()`. Auto-proposals are buffered, not acted on inline. `SplitRange` apply does not generate further proposals (children start with empty seal history). Max depth: 1 level.

Stale proposals (e.g., merge proposed but range split before commit) are safe — `apply_split_range()` / `apply_merge_range()` precondition checks reject them. `RollSegment` with wrong segment ID is a no-op (stale seal).

### 6e. MergeCheck Timer

`RaftTimerKind::MergeCheck` variant, `MERGE_CHECK_INTERVAL_TICKS = 6000` (10 min at 100ms/tick). Set on `become_leader()`, cancelled on stepdown via `cancel_all_timers()`. On timeout, leader calls `evaluate_merges()` with wall-clock time and reschedules.

### 6f. Key Design Decisions

- **`RangeSealHistory` uses timestamp Vec, not counter+window.** Pruning on each seal keeps the Vec bounded. `recent_seal_count(now)` re-evaluates against current time for merge checks.
- **Split logic lives on `RangeMeta`.** `roll_segment()`, `split()`, `merge()`, `compute_midpoint()`, `mergeable_with()` are methods on `RangeMeta` — state machine dispatches, range owns the logic.
- **`TimerSeqs` struct groups timer seq values.** Avoids too-many-arguments on `Raft::new()`.
- **Child ranges get `created_by_split_at` via `with_split_origin()` builder.** Enforces cooldown without adding parameters to `RangeMeta::new()`.
- **Stale `RollSegment` is no-op, not error.** If `active_seg_id != cmd.segment_id`, returns `Ok(())` — expected during leader transitions, not worth error logging.

**Depends on:** Phases 1-4 (needs working propose pathway + state machine dispatch). Phase 5 not required.
**Status:** Implemented.

---

## Phase Dependency Graph

```
Phase 1 (Data Model + MetadataStateMachine)
    │
    ▼
Phase 2 (Extend RaftCommand)
    │
    ▼
Phase 3 (Application State Machine Dispatch)
    │
    ├──────────────────────────┐
    ▼                          ▼
Phase 4 (Propose Pathway)   Phase 6 (Hot Range Detection)
    │                          (needs Phase 4 for end-to-end,
    ▼                           but core logic testable after Phase 3)
Phase 5 (Leader Forwarding + Shard Discovery) ✅
```

---

## Backlog (Out of Scope)

### Hot Range Detection Optimizations

Not in Phase 6 — add only when midpoint splitting proves insufficient:

- **Key histogram / percentile-based split points** — requires sampling infrastructure on data plane nodes, probe protocol from MetadataStateMachine, memory budget management. Only valuable for highly skewed workloads where midpoint splits don't divide load evenly.
- **Write throughput metrics** — counting writes/sec rather than seal frequency. More granular but requires data plane → MetadataStateMachine reporting pipeline.
- **Adaptive thresholds** — per-topic or per-range thresholds based on historical patterns instead of global constants.
- **Predictive splitting** — split before hotspot causes problems, based on trend detection. Requires time-series analysis.

### Consumer Protocol for Range Lifecycle

Not in metadata management scope — data plane concern. But depends on metadata providing:
- `end_offset` on sealed segments (consumer knows "done with this range")
- `merged_into` / `split_into` lineage (consumer follows range transitions)
- `active_ranges` list (consumer discovers current write targets)

Consumer behavior on merge:
- **Single consumer, both ranges:** drain both sealed ranges to completion, then switch to merged range at offset 0.
- **Two consumers (one per range):** one takes merged range ownership, other → hot standby for next split.

Consumer behavior on split:
- **Single consumer:** switches to consuming both child ranges (may spawn second consumer).
- **Consumer group:** rebalance — assign each child range to a consumer.

### RocksDB Storage Integration

Durable storage per `storage_implementation_plan.md`. Can begin after Phase 3.

Covers: RocksDB dependency, `MultiRaft` → `MultiRaftStore` refactor, `WriteBatch` flush, startup recovery, snapshots, log compaction, InstallSnapshot RPC. See `storage_implementation_plan.md` for full design.

---

## Key Design Decisions

**1. MultiRaftActor handles routing — no separate broker layer.**
`MultiRaftActorCommand::Propose` takes `resource_key`, not `shard_group_id`. MultiRaft hashes the key to find the shard group, checks leadership, and proposes — all internally. Client handler in `lib.rs` stays thin (just forwards). No async topology queries needed because `ShardGroupId::new(key)` is a pure hash function.

**2. MetadataStateMachine lives per shard group, inside MultiRaft(Store).**
Not inside `Raft`. Raft remains a pure consensus state machine — no knowledge of topics, ranges, segments.

**3. Application commands use buffer-drain pattern.**
Consistent with `take_outbound()`, `take_timer_commands()`, `take_log_mutations()`. New: `take_applied_entries()`.

**4. Commit completion tracked via pending_proposals map.**
`raft.propose()` returns immediately (entry appended). Actual commit notification comes asynchronously. `MultiRaft` tracks `(shard_id, log_index) → oneshot::Sender` to resolve reply on commit+apply. Leader stepdown drains all pending with `NotLeader`.

**5. Client redirect first, transparent forwarding second.**
Phase 4 returns `NotLeader(leader_hint)` — client redirects. Phase 5 adds transparent forwarding. Both coexist in production.

**6. Nested ownership — no flat maps, no back-references.**
Ranges nested inside `TopicMeta`, segments nested inside `RangeMeta`. ID counters (`next_range_id`, `next_segment_id`) scoped to parent entity. Eliminates `topic_id` back-ref on Range, `range_id` back-ref on Segment. Ownership expressed by structure. `DeleteTopic` = drop the `TopicMeta`. `RaftCommand` variants carry `topic_id` (and `range_id` for segment ops) to navigate the tree.

**7. Offsets on segments enable consumer position tracking.**
`start_offset` / `end_offset` on `SegmentMeta`, `next_offset` on `RangeMeta`. Consumer knows "done with sealed range" when position reaches last segment's `end_offset`. On split/merge, child ranges start at offset 0 — clean break. Consumer protocol (backlog) uses these fields plus `merged_into`/`split_into` lineage to navigate range transitions.

**8. Seal frequency as hot range signal — no external metrics pipeline.**
`RollSegment` commits are already in the Raft log. Counting seal frequency per range gives a load signal for free. No probe protocol, no data plane → MetadataStateMachine reporting, no key histograms. Midpoint split. Accuracy is good enough for Phase 6 — optimization is backlog.

---

## Risk Areas

| Risk | Mitigation |
|---|---|
| Pending proposals leak on leader stepdown | Drain and error all pending in `step_down()` |
| Hash ring divergence between client/broker | Server resolves routing from SWIM topology (~2-3s convergence). Leader forwarding handles misrouted proposals. Epoch cancelled — not needed. |
| Bincode format change on new `RaftCommand` variants | In-memory only for now; version byte on `LogEntry` when RocksDB lands |
| Midpoint split doesn't balance skewed workloads | Acceptable for Phase 6 — percentile-based split in backlog |
| Seal tracker grows unbounded for long-lived ranges | Sliding window bounded; entries removed on range seal/delete |
