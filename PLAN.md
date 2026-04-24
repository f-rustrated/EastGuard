# Phases 1–3: Data Model → RaftCommand → State Machine Dispatch

Implements the first three phases of `metadata_management_roadmap.md`. Phases 4–5 (propose
pathway, leader forwarding) follow after these land.

---

## Current State

| What | Where | Status |
|---|---|---|
| `RaftCommand` | `messages/command.rs:12` | Only `Noop`, `RemovePeer`, `AddPeer`. No application commands. |
| `ProposeError` | `messages/command.rs:31` | Only `NotLeader`. Missing `ShardNotFound`, `EpochNotMatch`, `NotLeaderHint`. |
| `MetadataStateMachine` | — | Does not exist. |
| `TopicId` / `RangeId` / `SegmentId` | — | Do not exist. |
| `apply_committed_entries()` | `state.rs:593` | Handles ConfChange only. No application dispatch. Private. |
| `pending_applied` buffer | — | Does not exist. |
| `ShardGroupState` | `multi_raft.rs` | `groups: HashMap<ShardGroupId, Raft>`. No embedded state machine. |
| Manual `serialize()` on `RaftCommand` | `command.rs:18` | Redundant — `bincode::Encode` already derived. |

---

## Phase 1 — Data Model + MetadataStateMachine

### 1a. Types

**New file:** `src/clusters/raft/types.rs`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct TopicId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct RangeId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct SegmentId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum TopicState { Active, Sealed, Deleted }

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum RangeState { Active, Sealed, Deleting }

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum SegmentState {
    Active,
    Sealed,
    Reassigning { from: NodeId, to: NodeId },
    Deleting,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum PartitionStrategy { AutoSplit, Fixed }

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct StoragePolicy {
    pub retention_ms: u64,
    pub replication_factor: u8,
    pub partition_strategy: PartitionStrategy,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct TopicMeta {
    pub topic_id: TopicId,
    pub name: String,
    pub state: TopicState,
    pub storage_policy: StoragePolicy,
    pub ranges: Vec<RangeId>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct RangeMeta {
    pub range_id: RangeId,
    pub topic_id: TopicId,
    pub keyspace_start: Vec<u8>,
    pub keyspace_end: Vec<u8>,
    pub state: RangeState,
    pub segments: Vec<SegmentId>,
    pub active_segment: Option<SegmentId>,
    pub split_into: Option<[RangeId; 2]>,
    pub merged_into: Option<RangeId>,
    pub merged_from: Option<[RangeId; 2]>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct SegmentMeta {
    pub segment_id: SegmentId,
    pub range_id: RangeId,
    pub topic_id: TopicId,
    pub state: SegmentState,
    pub replica_set: Vec<NodeId>,
    pub size_bytes: u64,
    pub created_at: u64,
    pub sealed_at: Option<u64>,
}
```

### 1b. MetadataStateMachine

**New file:** `src/clusters/raft/state_machine.rs`

Pure in-memory. No I/O, no async. All logic in apply functions.

```rust
pub(crate) struct MetadataStateMachine {
    topics:           HashMap<TopicId, TopicMeta>,
    ranges:           HashMap<RangeId, RangeMeta>,
    segments:         HashMap<SegmentId, SegmentMeta>,
    topic_name_index: HashMap<String, TopicId>,
    next_topic_id:    u64,
    next_range_id:    u64,
    next_segment_id:  u64,
}

impl MetadataStateMachine {
    pub(crate) fn apply_create_topic(&mut self, name: String, policy: StoragePolicy) { ... }
    pub(crate) fn apply_split_range(&mut self, range_id: RangeId, split_point: Vec<u8>) { ... }
    pub(crate) fn apply_seal_segment(&mut self, segment_id: SegmentId) { ... }
    pub(crate) fn apply_merge_range(&mut self, r1: RangeId, r2: RangeId) { ... }
    pub(crate) fn apply_delete_topic(&mut self, topic_id: TopicId) { ... }
}
```

`apply_create_topic` allocates the initial full-keyspace range and first segment.
Ranges are never created standalone — they always emerge from `CreateTopic` or `SplitRange`/`MergeRange`.

**Depends on:** Nothing.
**Scope:** ~300-400 lines + unit tests.

---

## Phase 2 — Extend RaftCommand

**File:** `src/clusters/raft/messages/command.rs`

### 2a. Add application variants

```rust
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum RaftCommand {
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
}
```

No `AssignRange` — ranges are never created standalone (emerge from `CreateTopic`/`SplitRange`/`MergeRange`).
`MoveShard` / `ReassignSegment` deferred to future phase.

### 2b. Remove manual `serialize()`

Delete lines 18-26 (`fn serialize()`). `bincode::Encode` already derived — manual method is redundant and will diverge.

### 2c. Extend ProposeError

```rust
#[derive(Debug, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader,
    ShardNotFound,
    EpochNotMatch { current: ShardEpoch },
    NotLeaderHint(Option<NodeId>),
}
```

`EpochNotMatch` and `NotLeaderHint` are used by Phase 5 (leader forwarding + epoch validation).
Adding them now keeps the type stable.

**Depends on:** Phase 1 (needs `TopicId`, `SegmentId`, `StoragePolicy`, etc.).
**Scope:** Mostly type changes + match arm updates.

---

## Phase 3 — Application State Machine Dispatch

Splits `apply_committed_entries()`: ConfChange stays in `Raft`, application commands dispatched
to `MetadataStateMachine` embedded per shard group.

### 3a. Buffer application entries in `Raft`

**File:** `src/clusters/raft/state.rs`

1. Add field:
   ```rust
   pending_applied: Vec<LogEntry>,
   ```

2. In `apply_committed_entries()`, push application commands instead of ignoring:
   ```rust
   RaftCommand::CreateTopic { .. }
   | RaftCommand::SealSegment { .. }
   | RaftCommand::SplitRange { .. }
   | RaftCommand::MergeRange { .. }
   | RaftCommand::DeleteTopic { .. } => {
       self.pending_applied.push(entry.clone());
   }
   ```
   ConfChange arms (`AddPeer`, `RemovePeer`) and `Noop` remain unchanged.

3. Add drain method:
   ```rust
   pub(crate) fn take_applied_entries(&mut self) -> Vec<LogEntry> {
       std::mem::take(&mut self.pending_applied)
   }
   ```

4. Make `apply_committed_entries()` `pub(crate)` so `MultiRaft::flush()` can call it.

### 3b. Introduce `ShardGroupState` in `MultiRaft`

**File:** `src/clusters/raft/multi_raft.rs`

Replace `groups: HashMap<ShardGroupId, Raft>` with:

```rust
struct ShardGroupState {
    raft: Raft,
    state_machine: MetadataStateMachine,
}

groups: HashMap<ShardGroupId, ShardGroupState>,
```

`MetadataStateMachine` is per shard group. `Raft` remains a pure consensus state machine with
no knowledge of topics, ranges, or segments.

### 3c. Dispatch in `flush()`

**File:** `src/clusters/raft/multi_raft.rs`

After the existing log-persistence step, drain and dispatch applied entries:

```rust
for id in &self.dirty {
    let shard = self.groups.get_mut(id).unwrap();
    shard.raft.apply_committed_entries();

    for entry in shard.raft.take_applied_entries() {
        match entry.command {
            RaftCommand::CreateTopic { name, storage_policy } =>
                shard.state_machine.apply_create_topic(name, storage_policy),
            RaftCommand::SplitRange { range_id, split_point } =>
                shard.state_machine.apply_split_range(range_id, split_point),
            RaftCommand::SealSegment { segment_id } =>
                shard.state_machine.apply_seal_segment(segment_id),
            RaftCommand::MergeRange { range_id_1, range_id_2 } =>
                shard.state_machine.apply_merge_range(range_id_1, range_id_2),
            RaftCommand::DeleteTopic { topic_id } =>
                shard.state_machine.apply_delete_topic(topic_id),
            _ => {}
        }
    }
}
```

### 3d. Add `pending_votes` buffer

**File:** `src/clusters/raft/multi_raft.rs`

Prevents RPCs for not-yet-created shard groups from being silently dropped.

Add field:
```rust
pending_votes: HashMap<ShardGroupId, Vec<(NodeId, RaftRpc)>>,
```

In `step()`: buffer instead of dropping when group not found:
```rust
fn step(&mut self, shard_id: ShardGroupId, from: NodeId, rpc: RaftRpc) {
    if let Some(shard) = self.groups.get_mut(&shard_id) {
        shard.raft.step(from, rpc);
        self.dirty.insert(shard_id);
    } else {
        self.pending_votes
            .entry(shard_id)
            .or_default()
            .push((from, rpc));
    }
}
```

In `add_group()`, drain buffer after inserting:
```rust
if let Some(buffered) = self.pending_votes.remove(&group_id) {
    for (from, rpc) in buffered {
        shard.raft.step(from, rpc);
    }
}
```

Cap buffer per shard group to 32 entries — drop oldest on overflow.

**Depends on:** Phases 1 + 2.
**Scope:** 1-2 PRs.

---

## Ordering Invariants to Preserve

- **`apply_committed_entries()` gated by `min(commit_index, stabled_index)`.**
  Must be called after `flush()` advances `stabled_index`, not before.

- **Application commands dispatched in log order.**
  `take_applied_entries()` drains in append order. `MetadataStateMachine::apply_*` must not
  be called out of order across entries from the same shard group.

- **`MetadataStateMachine` is not persistent in these phases.**
  RocksDB integration is Backlog (see `metadata_management_roadmap.md`). State is rebuilt from
  the Raft log on restart. Persistence (WriteBatch for `AppliedIndex` + `Epoch`) comes after.

---

## Test Plan

| Test | Phase | What it verifies |
|---|---|---|
| `apply_create_topic_initializes_range` | 1 | `MetadataStateMachine::apply_create_topic` creates topic + initial full-keyspace range |
| `apply_split_range_produces_two_ranges` | 1 | `apply_split_range` removes original range, inserts two children |
| `create_topic_command_roundtrips` | 2 | `RaftCommand::CreateTopic` encodes and decodes without manual serialize |
| `propose_error_variants_exist` | 2 | `ProposeError::ShardNotFound` / `EpochNotMatch` / `NotLeaderHint` compile |
| `create_topic_dispatched_to_state_machine` | 3 | After propose + flush, `ShardGroupState::state_machine` reflects the new topic |
| `pending_applied_drained_per_flush` | 3 | `take_applied_entries()` returns entries produced since last drain |
| `pending_votes_drained_on_ensure_group` | 3 | `PacketReceived` before `EnsureGroup` is buffered and processed once group is created |
| `pending_votes_cap_enforced` | 3 | Overflow beyond 32 drops oldest, does not panic |
