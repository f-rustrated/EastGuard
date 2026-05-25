# Phase D3: Segment Lifecycle Integration

**Goal:** Connect data plane storage to metadata consensus. Three classes of integration: (1) seal triggers — size, age, failure, node death — all converge to `RollSegment` via the coordinator, (2) lifecycle event propagation — `CreateTopic`, `RollSegment`, `SplitRange`, `MergeRange` commit → `SegmentAssignment` to data plane, (3) coordinator routing — segment leader resolves and reaches the coordinator.

**Depends on:** Phase D2 (segment replication), metadata control plane (shard leader gossip via SWIM).

---

## Coordinator

The **coordinator** is the Raft leader for the shard group that owns the topic. Not a separate component — it's a role that any vnode member can hold by winning Raft election. The coordinator proposes metadata changes (`CreateTopic`, `RollSegment`, `SplitRange`, `MergeRange`) via Raft and sends lifecycle notifications to the data plane after commit.

**Routing** — how the segment leader locates the coordinator:

```
SegmentTracker.shard_group_id          ← routing metadata from SegmentAssignment
    │
    ▼
Topology::shard_leader(shard_group_id)
    → ShardLeaderEntry { leader_node_id, leader_addr, term }
    │
    ▼
DataTransportActor::send(leader_addr, SealRequest { ... })
```

The shard leader map is maintained by SWIM gossip (`ShardLeaderInfo` piggybacked on protocol messages), term-monotonic (stale entries rejected), and NOT cleared on node death (overwritten by next election).

**Stale routing is safe.** A deposed leader rejects the proposal (`ProposeError::NotLeader`). The segment leader refreshes from `Topology` and retries via `SealRequestTimeout`. May take multiple retries until SWIM gossip propagates the new leader — convergence depends on cluster size and probe interval. No correctness risk — only latency.

---

## Seal Triggers

Four triggers, one response: seal the current segment and open a new one via `RollSegment`. No retry, no ISR, no reconciliation.

| Trigger | Source | Detection | Initiator |
|---|---|---|---|
| Replication failure | Follower timeout | Sub-second (`ReplicationTimeout`) | Segment leader sends `SealRequest` |
| Segment size | ~1GB threshold | On each `flush_batch()` | Segment leader sends `SealRequest` |
| Segment age | Configurable max age | Periodic ticker (~60s) | Segment leader sends `SealRequest` |
| Node death | SWIM protocol | ~6-7s | Coordinator proposes `RollSegment` directly |

### Trigger 1: Replication Failure (D2)

Already implemented in D2. `ReplicationTimeout` fires when any follower fails to ack. The segment leader emits `ReplicationTimedOut`, dispatched as a `SealRequest` to the coordinator. D3 completes the flow by adding coordinator routing (currently `targets = vec![]`).

### Trigger 2: Segment Size

When `SegmentTracker::size_bytes` approaches 1GB, the segment leader sends a `SealRequest`. Planned transition — no failed nodes, no uncommitted tail complications.

```
flush_batch() completes
    │
    ├── tracker.size_limit_reached()?
    │       └── yes → emit SealRequest {
    │                   segment_key,
    │                   failed_nodes: vec![],
    │                   end_entry_id: committed_entry_id
    │                 }
    └── no → continue
```

`end_entry_id` is set to the current `committed_entry_id`, which may lag `write_cursor` by in-flight entries — those are replayed into the new segment via `handle_seal_response()`. This replay relies on the coordinator preserving the SealRequest sender as `new_replica_set[0]` — the sender is alive (it just sent the request) and holds the uncommitted tail. The size check runs after every `flush_batch()`, not on a timer.

### Trigger 3: Segment Age

When a segment has been active longer than the configured max age (e.g., 24 hours), the segment leader sends a `SealRequest`. Bounds segment lifetime for low-traffic topics — prevents stale long-lived segments, simplifies retention and WAL management.

```
Periodic age ticker fires (~60s)
    │
    ├── for each active segment where tracker.role() == Leader:
    │       tracker.age_limit_reached(max_segment_age)?
    │           └── yes → emit SealRequest { segment_key, failed_nodes: vec![], end_entry_id: committed_entry_id }
    └── no → continue
```

Unlike size and replication triggers, the age check is driven by a periodic ticker, not by `flush_batch()`. This is the only trigger that fires without a write — necessary for idle segments.

### Trigger 4: SWIM Node Death

SWIM detects node death after ~6-7 seconds. The coordinator scans for active segments with the dead node in their `replica_set` and proposes `RollSegment` for each. No `SealRequest` needed — the coordinator has direct access to MetadataStateMachine. See [HandleNodeDeath Integration](#handleNodeDeath-integration) for full flow.

---

## Seal Flow: SealRequest → RollSegment → SealResponse

The full round-trip connecting data plane to metadata consensus.

### Message Flow

```
Segment Leader (node D)           Coordinator (node A)           Raft [A, B, C]
        │                               │                            │
  trigger fires                         │                            │
        │                               │                            │
  resolve coordinator:                  │                            │
  Topology::shard_leader(sgid)          │                            │
        │                               │                            │
        │── SealRequest ──────────────► │                            │
        │   { segment_key,              │                            │
        │     failed_nodes,          receive SealRequest             │
        │     end_entry_id }         via DataTransportActor          │
        │                               │                            │
        │                          dedup check (pending_seal_keys)   │
        │                          build RollSegment                 │
        │                          store pending context             │
        │                               │                            │
        │                               │── propose RollSegment ───► │
        │                               │                            │ commit
        │                               │◄─── commit notification ───│
        │                               │                            │
        │                          ALWAYS: SegmentAssignment ──► new_replica_set[0]
        │                               │                            │
        │◄── SealResponse ──────────────│  (best-effort, if          │
        │   { old_segment_key,          │   PendingRollContext       │
        │     new_segment_id,           │   exists)                  │
        │     new_replica_set }         │                            │
        │                               │                            │
  handle_seal_response()                │                            │
  open new segment                      │                            │
  replay uncommitted tail               │                            │
  resume produce (~100ms)               │                            │
```

### Types

```rust
// Sent by DataPlaneActor to MultiRaftActor via coordinator_tx
enum CoordinatorCommand {
    SealRequest {
        requester: NodeId,         // segment leader — SealResponse target
        segment_key: SegmentKey,
        failed_nodes: Vec<NodeId>,
        end_entry_id: u64,
    },
}

// Proposed via Raft
pub struct RollSegment {
    pub topic_id: TopicId,
    pub range_id: RangeId,
    pub segment_id: SegmentId,
    pub sealed_at: u64,
    pub new_replica_set: Vec<NodeId>,
    pub end_entry_id: u64,             // from SealRequest.end_entry_id; 0 for node-death seals
}

// Stored on propose, resolved on commit
struct PendingRollContext {
    requester: NodeId,
    old_segment_key: SegmentKey,
}

// In MultiRaftActor:
pending_rolls: HashMap<(ShardGroupId, u64 /* log_index */), PendingRollContext>,
pending_seal_keys: HashSet<SegmentKey>,  // dedup: skip duplicate SealRequests
```

`requester` is the NodeId of the segment leader that sent the SealRequest, extracted from the TCP connection's peer identity (handshake NodeId in DataTransportActor). For `HandleNodeDeath`-initiated rolls, `requester` is `None` — no SealResponse needed.

### Channel Wiring

```
                          DataTransportActor (tokio, data_port TCP)
                         /         │         \
                inbound messages   │   outbound messages
                        │          │          ▲
                        ▼          │          │
                   DataPlaneActor  │   data_transport_tx
                   (OS thread)     │   (new channel)
                        │          │          │
                coordinator_tx     │          │
                (new channel)      │          │
                        ▼          │          │
                   MultiRaftActor (tokio)
```

| Channel | Type | Direction | Purpose |
|---|---|---|---|
| `coordinator_tx` | `tokio::mpsc::Sender<Box<[CoordinatorCommand]>>` | DataPlaneActor → MultiRaftActor | Forward SealRequests for Raft proposal |
| `data_transport_tx` | `tokio::mpsc::Sender<Box<[DataTransportCommand]>>` | MultiRaftActor → DataTransportActor | Send SealResponse, SegmentAssignment |

Both channels send `Box<[T]>` per the project-wide batching convention (see `code-convention.md`). The sender accumulates into `Vec<T>` during its event loop, then `.into_boxed_slice()` before sending.

### DataPlaneActor: SealRequest Forwarding

On inbound `SealRequest` from data_port: convert to `CoordinatorCommand::SealRequest`, accumulate in batch, flush via `coordinator_tx.try_send(batch)` at end of event loop. Uses `try_send`, not `blocking_send` — the OS thread must not block on tokio. Dropped batches are retriable via `SealRequestTimeout`.

### MultiRaftActor: SealRequest Handling

```
on CoordinatorCommand::SealRequest { requester, segment_key, failed_nodes, end_entry_id }:
    if segment_key ∈ pending_seal_keys → skip (dedup)
    compute new_replica_set from current replica_set − failed_nodes + healthy nodes
    build RollSegment { topic_id, range_id, segment_id, sealed_at, new_replica_set, end_entry_id }
    propose via Raft:
        Ok(log_index) → insert pending_seal_keys + pending_rolls
        NotLeader → send error to requester (they retry)
```

### DataPlane: SealRequest Dispatch and Timeout

**`dispatch_events()` additions:** On `ReplicationTimedOut`, after `flush_batch()` size check, and on periodic age ticker — resolve coordinator via `Topology::shard_leader(tracker.shard_group_id)` and send `SealRequest` to coordinator via data_port. Replaces `let targets = vec![];` with actual coordinator resolution.

**SealRequest timeout (~5s):** On SealRequest send, set `SealRequestTimeout` timer. On expiry, refresh coordinator from `Topology` and retry. For size/age-triggered seals, this is the only recovery path — `ReplicationTimeout` won't fire since replication is healthy.

### apply_roll_segment()

Seals the active segment with `end_offset = cmd.end_entry_id`, creates the new segment with `start_offset = cmd.end_entry_id + 1`. This aligns the metadata view with the actual data boundary reported by the segment leader.

**`end_entry_id = 0` for node-death seals:** Placeholder — corrected during sealed segment repair (D5). Temporarily violates MetadataStateMachine invariant 8 (offset continuity) — holds after D5 repair.

### RollSegment Idempotency

Both write-path timeout (D2) and SWIM node death may fire for the same failure. This is safe: `apply_roll_segment()` checks that the segment is still active. If already sealed, the duplicate returns `Err(StaleSegment)` — a no-op (DS-RSM invariant 9).

**Exception: end_offset correction.** If a death-triggered `RollSegment` (with `end_entry_id = 0`) commits first, and the segment leader's `SealRequest` arrives later with the correct `end_entry_id`, `apply_roll_segment()` does not return `StaleSegment`. Instead, it updates the sealed segment's `end_offset` from 0 to the correct value. Guard: only when current `end_offset == 0` and new `end_entry_id > 0`.

---

## Lifecycle Event Propagation

After Raft commits a metadata command that creates new segments, the coordinator sends `SegmentAssignment` to `replica_set[0]`. Followers self-authorize from the leader's first `ReplicaAppend` (D2).

| Metadata Event | Segments Created | SegmentAssignment Target |
|---|---|---|
| `CreateTopic` | 1 (initial segment for initial range) | `replica_set[0]` |
| `RollSegment` | 1 (new segment replacing sealed one) | `new_replica_set[0]` |
| `SplitRange` | 2 (one per child range) | Each child's `replica_set[0]` |
| `MergeRange` | 1 (for the merged range) | `merged_replica_set[0]` |

### ApplyResult

```rust
enum ApplyResult {
    TopicCreated {
        topic_id: TopicId,
        range_id: RangeId,
        segment_id: SegmentId,
        replica_set: Vec<NodeId>,
    },
    SegmentRolled {
        topic_id: TopicId,
        range_id: RangeId,
        new_segment_id: SegmentId,
        new_replica_set: Vec<NodeId>,
        end_entry_id: u64,
    },
    RangeSplit {
        children: [(RangeId, SegmentId, Vec<NodeId>); 2],
    },
    RangeMerged {
        merged_range_id: RangeId,
        segment_id: SegmentId,
        replica_set: Vec<NodeId>,
    },
    TopicDeleted,
}
```

`SegmentRolled` carries all fields needed to construct SegmentAssignment independently of `PendingRollContext`. This ensures SegmentAssignment is always sent — even when a coordinator leadership change loses the pending context.

### SegmentAssignment

```rust
struct SegmentAssignment {
    segment_key: SegmentKey,      // (topic_id, range_id, segment_id)
    shard_group_id: ShardGroupId, // routing metadata — cached by SegmentTracker for coordinator resolution
    replica_set: Vec<NodeId>,
    start_entry_id: u64,          // new segments: 0, rolled: prev.end_entry_id + 1
}
```

### RaftEvent Extension

```rust
enum RaftEvent {
    // ... existing variants ...
    MetadataApplied {                     // NEW
        shard_group_id: ShardGroupId,
        result: ApplyResult,
        log_index: u64,
    },
}
```

Emitted from `apply_metadata_entry()` after successful apply. All replicas emit the event; only the leader dispatches notifications.

### Event Dispatch

On `RaftEvent::MetadataApplied`, leader-only dispatch:

```
SegmentRolled { topic_id, range_id, new_segment_id, new_replica_set, end_entry_id }:
    ALWAYS: send SegmentAssignment to new_replica_set[0] (start_entry_id = end_entry_id + 1)
    if PendingRollContext exists:
        remove old_segment_key from pending_seal_keys
        send SealResponse to requester (best-effort)

TopicCreated { topic_id, range_id, segment_id, replica_set }:
    send SegmentAssignment to replica_set[0] (start_entry_id = 0)

RangeSplit, RangeMerged:
    send SegmentAssignment to each new segment's replica_set[0]
```

`ApplyResult` is produced on ALL replicas (deterministic apply). Only the Raft leader dispatches. `SegmentAssignment` is always sent for `SegmentRolled` — the result carries full context independent of `PendingRollContext`.

### MetadataStateMachine Extension

New method: `active_segments_for_node(node_id) → Vec<(TopicId, RangeId, SegmentId, Vec<NodeId>)>`. Full scan: topics → active ranges → active segment, filtered by `replica_set.contains(node_id)`.

---

## HandleNodeDeath Integration

Extends `MultiRaft::remove_node()` to also propose `RollSegment` for affected segments. The coordinator uses a full scan (not a reverse index) to find affected segments — node death is rare, the scan takes microseconds for typical segment counts. If the count grows past thousands, add a reverse index then.

```
remove_node(dead_node_id):
    for each (group_id, raft) in self.groups:
        // Changed (D3): propose RemovePeer via Raft (was direct remove_peer)
        if raft.has_peer(dead_node_id) && raft.is_leader():
            propose RemovePeer(dead_node_id)

        // NEW (D3): propose RollSegment for affected segments
        if !raft.is_leader(): continue
        for each active segment where dead_node_id ∈ replica_set:
            new_replica_set = replace dead_node_id with healthy node
            propose RollSegment { ..., end_entry_id: 0 }  // unknown — resolved in D5
```

**Segment leader preservation:** When a follower dies, the coordinator keeps the existing leader at `replica_set[0]`. When the leader itself dies, a surviving follower is promoted. Replacement nodes selected from healthy cluster members not already in the set, tie-breaking by node ID.

---

## Wire Protocol

Updated message catalog (additions from D3 in bold):

| Message | Direction | Key fields | Phase |
|---|---|---|---|
| `ReplicaAppend` | Leader → followers | `segment_key`, `replica_set`, `data`, `record_count`, `entry_id` | D2 |
| `ReplicaAck` | Follower → leader | `segment_key`, `entry_id`, `from` | D2 |
| `CommitAdvance` | Leader → followers | `segment_key`, `committed_entry_id` | D2 |
| `SealRequest` | Leader → coordinator | `segment_key`, `failed_nodes`, `end_entry_id`, **`from`** | D2 |
| `SealResponse` | Coordinator → leader | `old_segment_key`, `new_segment_id`, `new_replica_set` | D2 |
| `SegmentSealed` | Leader → old followers | `segment_key` | D2 |
| **`SegmentAssignment`** | **Coordinator → leader** | **`segment_key`, `shard_group_id`, `replica_set`, `start_entry_id`** | **D3** |

`SealRequest.from` is explicit (was inferred from TCP handshake). `SegmentAssignment` was defined in D2 but not sent; D3 implements the send path.

---

## Examples

### CreateTopic End-to-End

```
Client                 Coordinator A          Raft [A,B,C]         Broker D
  │                         │                      │                  │
  │── CreateTopic ────────► │                      │                  │
  │                         │── propose ──────────►│                  │
  │                         │                      │ commit           │
  │                         │◄─────────────────────│                  │
  │◄─── Ok(TopicId(0)) ─────│                      │                  │
  │                         │── SegmentAssignment ───────────────────►│
  │                         │   { key: (0,0,0),    │          create tracker
  │                         │     rs: [D,E,F],     │          (Leader)
  │                         │     start_entry: 0 } │     ready for produce
```

### Size-Based Seal

```
Broker D (segment leader)       Coordinator A        Raft [A,B,C]
   │                                  │                    │
   │ size_bytes exceeds 1GB           │                    │
   │── SealRequest { seg 7,           │                    │
   │   failed: [],                    │                    │
   │   end_entry_id: 42000 }─────────►│                    │
   │                                  │── propose ────────►│
   │                                  │                    │ commit
   │                                  │◄───────────────────│
   │◄─── SealResponse { seg 8 } ──────│                    │
   │                                  │                    │
   │ open seg 8 (start: 42001)        │                    │
   │ replay uncommitted tail          │                    │
   │ resume produce                   │                    │
```

---

## Invariants

1. **Coordinator is sole proposer for segment lifecycle.** All `RollSegment` proposals come from the coordinator — either on behalf of a SealRequest or directly on SWIM node death. DataPlaneActor never proposes Raft commands.

2. **SegmentAssignment sent only by coordinator, only after Raft commit.** No speculative assignments — the segment exists in MetadataStateMachine before any data node learns about it.

3. **SealResponse is best-effort.** Lost if coordinator changes between propose and commit (no `PendingRollContext` on new leader). Segment leader has a `SealRequestTimeout` to detect and retry.

4. **ApplyResult emitted on all replicas, dispatched only by leader.** Followers apply but don't notify. `SegmentAssignment` always sent on `SegmentRolled` (full context in ApplyResult). `SealResponse` only if `PendingRollContext` exists.

5. **SealRequest routing is retryable.** Rejected proposals (not leader) trigger retry via `SealRequestTimeout` after refreshing shard leader view from SWIM gossip. May take multiple rounds until gossip converges — safe, only affects latency.

6. **`end_entry_id` handoff is exact for segment-leader-initiated seals.** Equals `tracker.committed_entry_id`. Carried through `RollSegment` → `SegmentMeta.end_offset`. New segment starts at `end_entry_id + 1`.

7. **`end_entry_id = 0` is a placeholder for node-death seals.** Corrected during sealed segment repair (D5). Temporarily violates MetadataStateMachine invariant 8 (offset continuity) — holds after D5 repair. A subsequent SealRequest with the correct `end_entry_id` updates `end_offset` from 0 (see RollSegment Idempotency).

8. **Size-based and age-based seals reuse the failure path.** Same flow, `failed_nodes` empty, replica set preserved.

9. **No SegmentAssignment needed for followers.** Followers self-authorize from the leader's first `ReplicaAppend` (D2 invariant 7).
