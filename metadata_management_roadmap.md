# EastGuard Metadata Management Roadmap

SWIM + MultiRaft infrastructure is drafted. This roadmap covers the path from the current consensus/membership layer to a full metadata management system — topics, ranges, segments — following the DS-RSM (Dynamically-Sharded Replicated State Machine) architecture inspired by Northguard.

---

## Phase 1: Data Model + MetadataStateMachine

**Goal:** Define metadata types and a pure in-memory state machine.

**New module:** `src/clusters/coordinator/`

### Types (`types.rs`)

```
TopicId(u64), RangeId(u64), SegmentId(u64)

TopicState  { Active, Sealed, Deleted }
RangeState  { Active, Sealed, Deleting }
SegmentState { Active, Sealed, Reassigning { from, to }, Deleting }

StoragePolicy { retention_ms, replication_factor, partition_strategy }
PartitionStrategy { AutoSplit, Fixed }

TopicMeta {
    topic_id, name, state, storage_policy,
    ranges: Vec<RangeId>
}

RangeMeta {
    range_id, topic_id, keyspace: [start, end),
    state, segments, active_segment,
    split_into, merged_into, merged_from
}

SegmentMeta {
    segment_id, range_id, topic_id, state,
    replica_set, size_bytes, created_at, sealed_at
}
```

All types derive `Encode`, `Decode`, `Clone`, `Debug`.

### State Machine (`state_machine.rs`)

```rust
struct MetadataStateMachine {
    topics:           HashMap<TopicId, TopicMeta>,
    ranges:           HashMap<RangeId, RangeMeta>,
    segments:         HashMap<SegmentId, SegmentMeta>,
    topic_name_index: HashMap<String, TopicId>,
    next_topic_id:    u64,
    next_range_id:    u64,
    next_segment_id:  u64,
}
```

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
    RemovePeer(NodeId),
    AddPeer(NodeId),
    // New
    CreateTopic { name: String, storage_policy: StoragePolicy },
    SealSegment { segment_id: SegmentId },
    SplitRange { range_id: RangeId, split_point: Vec<u8> },
    MergeRange { range_id_1: RangeId, range_id_2: RangeId },
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
    ▼
Phase 4 (Propose Pathway)
    │
    ▼
Phase 5 (Leader Forwarding + Epoch)
```

---

## Backlog (Out of Scope)

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

---

## Risk Areas

| Risk | Mitigation |
|---|---|
| Pending proposals leak on leader stepdown | Drain and error all pending in `step_down()` |
| Hash ring divergence between client/broker | Epoch validation catches stale routing (Phase 5) |
| Bincode format change on new `RaftCommand` variants | In-memory only for now; version byte on `LogEntry` when RocksDB lands |
