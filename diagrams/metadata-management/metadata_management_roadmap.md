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

Pure functions: `apply_create_topic()`, `apply_split_range()`, `apply_seal_segment()`. No I/O, no async.

See `diagrams/metadata-management/data-model.md` for full entity relationships, state transitions, cascading effects, and invariants.

**Depends on:** Nothing.
**Scope:** ~300-400 lines + unit tests.

---

## Phase 2: Extend RaftCommand

**Goal:** Grow `RaftCommand` from internal-only to include metadata operations.

### File: `src/clusters/raft/messages/command.rs`

**Extend `RaftCommand`:**

```rust
enum RaftCommand {
    // Existing
    Noop,
    // New
    CreateTopic { name: String, storage_policy: StoragePolicy },
    SealSegment { topic_id: TopicId, range_id: RangeId, segment_id: SegmentId },
    SplitRange { topic_id: TopicId, range_id: RangeId, split_point: Vec<u8> },
    MergeRange { topic_id: TopicId, range_id_1: RangeId, range_id_2: RangeId },
    DeleteTopic { topic_id: TopicId },
    // Future: MoveShard, ReassignSegment
}
```

No `AssignRange` — ranges are never created standalone. They emerge from `CreateTopic` (initial full-keyspace range) or `SplitRange`/`MergeRange` (lifecycle). See `diagrams/metadata-management/data-model.md` § "How Range ID and Keyspace Are Determined".

**Extend `ProposeError`:**

```rust
enum ProposeError {
    NotLeader,
    ShardNotFound,
    EpochNotMatch { current: ShardEpoch },
    NotLeaderHint(Option<NodeId>),
}
```

**Remove** the manual `serialize()` method (lines 18-26) — `bincode::Encode`/`Decode` handles serialization.

**Depends on:** Phase 1 (needs `TopicId`, `StoragePolicy`, etc.).
**Scope:** Mostly type changes + match arm updates.

---

## Phase 3: Application State Machine Dispatch

**Goal:** Split `apply_committed_entries()` — ConfChange stays in `Raft`, application commands dispatched to `MetadataStateMachine`.

This is the TODO at `state.rs:569`:
> "Phase 4 will extend this with application state machine dispatch."

### 3a. Refactor `apply_committed_entries()` in `Raft`

ConfChange handling (`AddPeer`/`RemovePeer`) stays inside `Raft` — it modifies Raft-internal peer state.

Application commands buffered for caller:

```rust
// In Raft:
pending_applied: Vec<LogEntry>,

fn apply_committed_entries(&mut self) {
    for entry in last_applied+1..=commit_index:
        match entry.command:
            Noop             => {}
            RemovePeer(id)   => self.peers.remove(id); ...
            AddPeer(id)      => self.peers.insert(id); ...
            _                => self.pending_applied.push(entry)  // new
}

fn take_applied_entries(&mut self) -> Vec<LogEntry>  // new drain method
```

### 3b. Embed `MetadataStateMachine` per shard group in `MultiRaft`

```rust
// In multi_raft.rs:
struct ShardGroupState {
    raft: Raft,
    state_machine: MetadataStateMachine,
}

groups: HashMap<ShardGroupId, ShardGroupState>,
```

### 3c. Dispatch in `flush()`

```rust
for id in dirty:
    for entry in shard.raft.take_applied_entries():
        match entry.command:
            CreateTopic { .. }  => shard.state_machine.apply_create_topic(..)
            SplitRange { .. }   => shard.state_machine.apply_split_range(..)
            SealSegment { .. }  => shard.state_machine.apply_seal_segment(..)
```

Matches `storage_implementation_plan.md` § "State machine application" exactly.

**Depends on:** Phases 1 + 2.
**Scope:** 1-2 PRs.

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

## Phase 5: Leader Forwarding + Epoch Validation

**Goal:** Non-leader nodes forward proposals transparently; stale routing detected via epochs.

### 5a. Leader forwarding

When `MultiRaft::propose()` returns `NotLeaderHint(Some(leader_id))`:
1. Resolve leader address via `SwimQueryCommand::ResolveAddress`
2. Forward `ProposeRequest` over TCP to leader's node
3. Relay response back to client

Falls back to `NotLeader(None)` if leader unknown (election in progress) — client retries with backoff.

### 5b. Epoch validation

```rust
struct ShardEpoch {
    conf_ver: u64,  // incremented on AddPeer/RemovePeer commit
    version: u64,   // incremented on split/merge (future)
}
```

Every `Propose` includes client's cached epoch. Shard group validates before accepting:
- `client_epoch < current_epoch` → `EpochNotMatch { current }` → client refreshes routing cache

### 5c. Shard discovery query

```rust
// New QueryCommand variant:
GetShardInfo { key: Vec<u8> }
// Returns: { shard_group_id, leader_node_id, leader_addr, epoch }
```

**Depends on:** Phase 4.
**Scope:** 2-3 PRs.

---

## Phase 6: Hot Range Detection + Auto-Split/Merge

**Goal:** MetadataStateMachine detects hot/cold ranges and proposes `SplitRange`/`MergeRange` automatically. Simplest viable approach — no probe protocol, no key histograms.

### Core Insight

`MetadataStateMachine` already sees every `SealSegment` commit. A segment seals when it reaches ~1GB. If a range's segments seal frequently, that range is hot. No external metrics pipeline needed — the Raft log IS the signal.

### 6a. Per-Range Seal Tracker

Add to `RangeMeta`:

```rust
struct RangeSealHistory {
    last_seal_at: u64,          // monotonic timestamp of most recent seal
    seal_count_in_window: u32,  // seals within sliding window
    window_start: u64,          // start of current measurement window
    cooldown_until: u64,        // no split before this time (post-split cooldown)
}

// In RangeMeta:
seal_history: RangeSealHistory,
```

Lives inside `RangeMeta` — no separate tracker map needed. Naturally scoped to the range, cleaned up when range is deleted.

Updated inside `apply_seal_segment()` — pure, deterministic, replicated on every node.

### 6b. Split Decision Logic

After each `apply_seal_segment()`, MetadataStateMachine (leader only) evaluates:

```rust
fn should_split(topic: &TopicMeta, range_id: RangeId, now: u64) -> bool {
    let range = topic.ranges.get(&range_id);

    range.state == Active
        && now > range.seal_history.cooldown_until
        && range.keyspace_width() > MIN_RANGE_WIDTH
        && range.seal_history.seal_count_in_window >= SPLIT_SEAL_THRESHOLD
}
```

Split point = midpoint of keyspace. Simple, no key distribution data needed.

**Constants (initial, tunable):**

| Constant | Value | Meaning |
|---|---|---|
| `SPLIT_SEAL_THRESHOLD` | 3 | Seals within window to trigger split |
| `MEASUREMENT_WINDOW_MS` | 300_000 (5 min) | Sliding window for counting seals |
| `SPLIT_COOLDOWN_MS` | 300_000 (5 min) | No re-split after recent split |
| `MIN_RANGE_WIDTH` | 256 bytes | Minimum keyspace to prevent fragmentation |

### 6c. Merge Decision Logic

Periodic check by MetadataStateMachine leader (on a timer, not per-event):

```rust
fn should_merge(topic: &TopicMeta, r1: RangeId, r2: RangeId, now: u64) -> bool {
    let range1 = topic.ranges.get(&r1);
    let range2 = topic.ranges.get(&r2);

    are_buddies(range1, range2)
        && range1.state == Active && range2.state == Active
        && range1.seal_history.seal_count_in_window <= MERGE_SEAL_THRESHOLD
        && range2.seal_history.seal_count_in_window <= MERGE_SEAL_THRESHOLD
        && now > range1.seal_history.cooldown_until
        && now > range2.seal_history.cooldown_until
}
```

| Constant | Value | Meaning |
|---|---|---|
| `MERGE_SEAL_THRESHOLD` | 0 | Both ranges must be idle (no seals in window) |
| `MERGE_CHECK_INTERVAL_MS` | 600_000 (10 min) | How often leader scans for merge candidates |

Hysteresis built in: split at 3 seals/window, merge at 0. No oscillation.

### 6d. Proposal Path

Split/merge decisions happen in `MultiRaftActor` after applying entries (Phase 3 dispatch):

```rust
// After apply_seal_segment():
if should_split(&topic, range_id, now) {
    let mid = topic.ranges.get(&range_id).midpoint();
    shard.raft.propose(RaftCommand::SplitRange { topic_id, range_id, split_point: mid });
}

// On periodic timer:
for (topic_id, r1, r2) in shard.state_machine.merge_candidates(now) {
    shard.raft.propose(RaftCommand::MergeRange { topic_id, range_id_1: r1, range_id_2: r2 });
}
```

Leader-only — followers apply committed decisions but never propose. Stale proposals rejected by `apply_split_range()` / `apply_merge_range()` precondition checks.

### 6e. New Timer

Add `MergeCheck` variant to `RaftTimer` for periodic merge scanning. Fires every `MERGE_CHECK_INTERVAL_MS`. Leader-only — set on `become_leader()`, cancelled on stepdown.

**Depends on:** Phases 1-4 (needs working propose pathway + state machine dispatch). Phase 5 not required.
**Scope:** 2-3 PRs.

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
Phase 5 (Leader Forwarding + Epoch)
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
`SealSegment` commits are already in the Raft log. Counting seal frequency per range gives a load signal for free. No probe protocol, no data plane → MetadataStateMachine reporting, no key histograms. Midpoint split. Accuracy is good enough for Phase 6 — optimization is backlog.

---

## Risk Areas

| Risk | Mitigation |
|---|---|
| Pending proposals leak on leader stepdown | Drain and error all pending in `step_down()` |
| Hash ring divergence between client/broker | Epoch validation catches stale routing (Phase 5) |
| Bincode format change on new `RaftCommand` variants | In-memory only for now; version byte on `LogEntry` when RocksDB lands |
| Midpoint split doesn't balance skewed workloads | Acceptable for Phase 6 — percentile-based split in backlog |
| Seal tracker grows unbounded for long-lived ranges | Sliding window bounded; entries removed on range seal/delete |
