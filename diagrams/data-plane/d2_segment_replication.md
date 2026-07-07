# Phase D2: Segment Replication

**Goal:** Primary-backup replication — all replicas fsync before producer ACK. Seal-on-failure. `DataTransportActor` for `data_port` TCP.

**Depends on:** Phase D1 (storage engine).

---

## Replication Protocol

### Leader Write Path

```
Produce arrives (one per producer, pre-serialized EntryPayload)
    │
    ▼
tracker.staged_entries.push(StagedEntry)
    │
    │ flush trigger fires (10ms / 10MB)
    ▼
╔════════════════════════════════════════════════════════════════╗
║ WAL: for each staged entry, write routing header + blob       ║
║      → flat interleaved write → BatchEnd → fsync              ║
║      assign entry_id to each entry                            ║
╚════════════════════════╤═══════════════════════════════════════╝
                         │ ◄── durability point (local)
                         ▼
   ┌─────────── for each dirty segment ──────────────┐
   │ tracker.publish_staged(lsn)                     │
   │   for each staged entry:                        │
   │     → entry_id = next_entry_id++                │
   │     → Arc::new(CachedEntry) → ring buffer       │
   └─────────────────────┬───────────────────────────┘
                         │ ◄── in cache, not yet visible to consumers
                         ▼
   emit BatchPublished { lsn, segment_batches }
                         │
                   actor event loop
                         │
                         ▼
   ┌─────────── fan-out to followers ────────────┐
   │ for each segment with followers:            │
   │   send ReplicaAppend to all followers       │
   │ set ReplicationTimeout timer per segment    │
   └─────────────────────┬──────────────────────-┘
                         │
                         ▼
   ┌── per segment, independently as acks arrive ─┐
   │ DataPlaneCommand::ReplicaAck arrives         │
   │ update pending_acks for that segment         │
   │ when segment's ALL followers acked:          │
   │   read_cursor += 1                           │  ◄── durability point (segment)
   │   notify.notify_waiters()                    │  ◄── leader consumers can read
   │   CommitAdvance to followers                 │
   │   drain segment replies with Ok              │  ◄── ACK producers
   └─────────────────────┬───────────────────────-┘
                         │
                         ▼ (background)
   checkpoint worker       size_bytes → RollSegment at ~1GB
```

WAL fsync is the sole disk write on the leader's critical path, same as D1. The replication fan-out is network I/O (async, non-blocking). Produce latency = `local_fsync + max(follower_fsyncs)` — one local fsync plus one replication RTT in the common case.

**No-follower fast path:** When `tracker.followers()` is empty (replication_factor=1), `flush_batch()` checks `tracker.followers().is_empty()` after publish. If true: immediately call `commit_entry` for each published entry, drain all pending replies with `Ok`, no `ReplicaAppend`, no timer, no `PendingBatch`. Degrades to D1 behavior.

### Follower Write Path

```
ReplicaAppend arrives (one entry: EntryPayload + entry_id)
    │
    ├── segment unknown? validate & create SegmentTracker (see self-authorization)
    │
    ▼
tracker.stage_entry_from_replica(data, record_count, entry_id)
    │
    │ needs_flush = true → flush_batch() fires immediately
    ▼
╔═══════════════════════════════════════════════════════════╗
║ WAL: for each staged entry, write routing header + blob   ║
║    → single write → BatchEnd → fsync                      ║
╚════════════════════════════╤══════════════════════════════╝
                             │ ◄── durability point (all entries in this flush)
                             ▼
       per follower segment:
         publish_staged(lsn) → ReplicaAck { entry_id } to leader
       per leader segment:
         publish_staged(lsn) → BatchPublished (replication fan-out)

CommitAdvance { committed_entry_id } arrives later from leader
    │
    ▼
tracker.commit_entry(committed_entry_id)
  → read_cursor += 1, notify.notify_waiters()    ◄── follower consumers can read
```

Records arrive pre-batched in `ReplicaAppend` — no accumulation on the follower side. Both leader and follower segments buffer into `tracker.staged_entries`. The unified flush drains all trackers in a single WAL write + fsync + `BatchEnd`, enabling uniform crash recovery (D5).

**Unified WAL Flush.** A node writes to one WAL. All segments buffer into `tracker.staged_entries` — the buffer is embedded in `SegmentTracker`, co-located with `role`, `cache`, and `committed_entry_id`. `flush_batch()` iterates dirty segments, drains each tracker's `staged_entries`, and writes everything in a single WAL write + fsync + `BatchEnd`. Post-flush, `tracker.role` determines events: leader → `BatchPublished` (replication fan-out), follower → `ReplicaAck` via `InterNodeCommandQueued`. Single lookup per segment, no separate buffer map.

Flush fires when either a leader flush trigger trips (10ms / 10MB) OR the actor finishes draining the mailbox with `needs_flush == true`. Multiple `ReplicaAppend`s from different leaders that arrive in the same mailbox drain share one fsync. Leader entries ride along for free if a follower triggers the flush, and vice versa.

Follower-to-leader promotion (F2) requires no buffer migration: the new segment's `SegmentTracker` is created with `role = Leader` and an empty `staged_entries`. `flush_batch()` handles it like any other leader segment.

### Replica Self-Authorization

The first `ReplicaAppend` for an unknown segment is self-authorizing — no prior `SegmentAssignment` notification needed for followers. `ReplicaAppend` carries `replica_set` and batch data. Follower validates:
1. Am I in `replica_set`? → if not, reject
2. Is sender `replica_set[0]` (segment leader)? → if not, reject
3. Valid → create `SegmentTracker` with `SegmentRole::Follower`, create segment file, accept

Only the segment leader requires explicit `SegmentAssignment` from the coordinator (D3). Followers learn about new segments from the leader's first `ReplicaAppend` — no gossip or coordinator notification required on the follower data path.

### Commit Notification

After the leader advances `read_cursor` (all replicas ACKed), it sends `CommitAdvance { segment_key, committed_entry_id }` to all followers. Followers advance their own `read_cursor` on receipt — follower consumers can then read the committed entry.

`CommitAdvance` carries the `entry_id`, not the ring buffer slot position. Follower on receipt: `tracker.commit_entry(committed_entry_id)` — internally advances `read_cursor` by 1, sets `committed_entry_id`, and notifies consumers. Decouples the wire protocol from ring buffer internals. Lightweight message (`segment_key` + `u64`), sent once per commit per follower. TCP ordering guarantees it arrives before the next `ReplicaAppend`. Follower visibility lags the leader by ~0.5 RTT (one network hop), not by one flush interval.

**Validation:** `debug_assert!(cached_entry.entry_id == committed_entry_id)`. Mismatch indicates a bug (TCP guarantees ordering, so message reordering is impossible). In release: log error, disconnect from leader — follower can't trust subsequent messages. Leader's `ReplicationTimeout` fires and seals.

---

## Component Changes

### SegmentTracker

```
SegmentTracker (D1 fields unchanged)
├── cache: SegmentRingBuffer, size_bytes, checkpoint_lsn, segment_file_path
├── role: Leader | Follower                     ← NEW
├── replica_set: [leader, follower, follower]   ← NEW
├── committed_entry_id: u64                     ← NEW (init 0)
├── next_entry_id: u64                          ← NEW (broker-assigned per entry)
└── staged_entries: Vec<StagedEntry>            ← NEW (accumulated produce entries)
```

**`next_entry_id` vs `size_bytes`:** `next_entry_id` tracks the next entry ID to assign on publish (incremented per entry, not per record). `size_bytes` tracks cumulative payload bytes for the 1GB segment limit. `SegmentAssignment` carries `start_entry_id` (D3 — for initial segments: 0, rolled segments: `prev.end_entry_id + 1`).

**Two-phase publish:** `stage_to_wal(wal_buf)` encodes each staged entry into WAL buffer (one WAL record per entry, with routing header). `publish_staged(lsn)` drains `staged_entries`, assigns `entry_id` to each, publishes to ring buffer. Commit is separate: `commit_entry(entry_id)` (`read_cursor` += 1, updates `committed_entry_id`, notifies consumers). Also adds `followers() → &[NodeId]` returning `replica_set[1..]`.

**Entry types:** `StagedEntry` (`EntryPayload` + `record_count` + `SegmentKey`) lives in `states/segment/record.rs` — the opaque blob from the producer. `WalRecord` (CRC + type + payload) lives in `wal.rs` — the WAL serialization format. `CachedEntry` (in `cache.rs`) stores `EntryPayload` (opaque blob, broker never parses). WAL owns the serialization buffer (`WalStorage::buf()`); trackers encode into it via `stage_to_wal`.

**Two position domains — do not conflate:**
- `SegmentRingBuffer.read_cursor` — ring buffer slot position. Always advances by 1 per committed entry.
- `SegmentTracker.committed_entry_id` — the `entry_id` of the last committed entry. Carried in `SealRequest.end_entry_id`, `CommitAdvance`.

**Checkpoint change:** `drain_for_checkpoint()` bounds to `eviction_frontier..read_cursor` — uncommitted entries may be replayed into a different segment on seal.

### DataPlane State Machine

**New fields:**
- `node_id: NodeId` — role determination and self-authorization
- `needs_flush: bool` — set by `process_replica_append()` and `handle_seal_response()`, checked by actor after mailbox drain, reset by `flush_batch()`
- `buffer_byte_count: usize` — payload bytes across all leader staged entries, drives 10MB flush threshold
- `dirty_segments: Vec<SegmentKey>` — tracks segments with non-empty `staged_entries`; `flush_batch()` only visits these
- `replication: ReplicationState` — reply tracking and in-flight batch management (see ReplicationState below)

**New methods:**

| Method | Role | Effect |
|---|---|---|
| `process_replica_append()` | Follower | Validate sender (self-authorization), stage entry via `stage_entry_from_replica()`, set `needs_flush` |
| `commit_segment(key, entry_id)` | Both | `tracker.commit_entry(entry_id)`, send `CommitAdvance` to followers via `InterNodeCommandQueued` |
| `handle_seal_response(...)` | Leader | Create new tracker (`start_entry_id = old.committed_entry_id + 1`), replay uncommitted tail via `uncommitted_entries()`, call `replication.segment_handoff()`, send `SegmentSealed` to old followers via `InterNodeCommandQueued` |

**`flush_batch()` changes (D1 → D2):** Three phases: (1) `stage_to_wal(wal.buf())` for each dirty segment — encodes each staged entry into WAL with routing header, (2) `wal.flush_batch()` — single write+fsync, returns LSN, (3) `publish_staged(lsn)` for each dirty segment — drains `staged_entries`, assigns `entry_id` to each, publishes to ring buffer. Per entry per segment, `tracker.role` determines event: Leader with followers → `BatchPublished`, Leader without followers → immediate `commit_entry` (no-follower fast path), Follower → `ReplicaAck` via `InterNodeCommandQueued`. Triggers: `BatchFlushDeadline` timer (10ms) OR byte threshold (10MB) OR `needs_flush` after mailbox drain.

**Other changes:** `handle_segment_assignment()` stores `replica_set`, sets role (leader-only in D2 — followers self-authorize). Followers reject `Produce` with error.

### DataPlaneActor

**Reply lifecycle:**

```
Pending ──────────► Replicating ──────► Resolved
  Produce            BatchPublished     All acks → Ok
  → pending_replies   → PendingBatch     Timeout  → seal + replay
                                         WAL fail → Err
```

**ReplicationState** (lives in `states/replication.rs`, owned by `DataPlane`):

```rust
struct ReplicationState {
    pending_replies: HashMap<SegmentKey, Vec<oneshot::Sender<ProduceAck>>>,
    in_flight: HashMap<SegmentKey, VecDeque<PendingBatch>>,
    timer_seqs: HashMap<SegmentKey, u64>,
    next_timer_seq: u64,
}

struct PendingBatch {
    replies: Vec<oneshot::Sender<ProduceAck>>,
    pending_acks: HashSet<NodeId>,
    entry_id: EntryId,
}
```

Per-segment `VecDeque` — each segment's entries commit in order independently. `entry_id` in `ReplicaAck` correlates to `PendingBatch.entry_id` — entry IDs are monotonic per segment. Late acks for sealed segments are silently dropped.

Key methods: `enqueue_reply()` (Pending phase), `begin_replication()` (moves replies to `PendingBatch`, returns timer seq if first in-flight), `process_ack()` (removes node from `pending_acks`, returns `AckCommitted` with replies when all acked), `segment_handoff()` (migrates replies from old to new segment on seal), `fail_all()` (drains all replies with `Err` on WAL failure). Timer seq gating via `is_active_timer()` prevents stale timeout callbacks from firing after a timer has been reset by `process_ack()`.

### DataPlane Command Catalog

Top-level `DataPlaneCommand` has four variants. Inter-node commands arrive via `DataTransportActor` as `DataPlaneInterNodeCommand`, wrapped in `DataPlaneCommand::InterNode`.

| Command | Variant | Source | Key fields |
|---|---|---|---|
| `Produce` | top-level | Client | `segment_key`, `data: EntryPayload`, `record_count`, `reply` |
| `CheckpointComplete` | top-level | Checkpoint worker | `segment_key`, `checkpointed_lsn` |
| `Timeout` | top-level | Ticker | `DataPlaneTimeoutCallback` |
| `SegmentAssignment` | inter-node | Coordinator (D3) | `segment_key`, `replica_set`, `start_entry_id` |
| `ReplicaAppend` | inter-node | Leader via transport | `segment_key`, `replica_set`, `data: EntryPayload`, `record_count`, `entry_id` |
| `ReplicaAck` | inter-node | Follower via transport | `segment_key`, `entry_id`, `from` |
| `CommitAdvance` | inter-node | Leader via transport | `segment_key`, `committed_entry_id` |
| `SealRequest` | inter-node | Leader via transport | `segment_key`, `failed_nodes`, `end_entry_id` |
| `SealResponse` | inter-node | Coordinator via transport | `old_segment_key`, `new_segment_id`, `new_replica_set` |
| `SegmentSealed` | inter-node | Leader via transport | `segment_key` |

`ReplicaAppend` carries `entry_id` (broker-assigned) + `data: EntryPayload` (opaque blob, zero-copy via `Bytes` refcount). `ReplicaAck` includes `from: NodeId` so the leader can identify which follower acked. `SealRequest` routing to coordinator is deferred to D3 (currently placeholder).

**SegmentSealed follower handling:** Submit final checkpoint, then drop `SegmentTracker`. No further `ReplicaAppend` accepted for that segment.

### DataPlane Event Catalog

| Event | Role | Key fields |
|---|---|---|
| `CheckpointRequired` | Both | `CheckpointJob` |
| `BatchFlushTimerScheduled` | Leader | `TimerCommand<BatchFlushTimer>` |
| `BatchPublished` | Leader | `lsn`, `segment_batches: Vec<PendingReplicationBatch>` |
| `ReplicaAckReceived` | Leader | `segment_key`, `entry_id`, `from` |
| `InterNodeCommandQueued` | Both | `targets: Vec<NodeId>`, `message: DataPlaneInterNodeCommand` |
| `ReplicationTimedOut` | Leader | `segment_key`, `committed_entry_id` |

`PendingReplicationBatch` is a struct: `{ segment_key, entry: Arc<CachedEntry>, replica_set: Vec<NodeId>, followers: Vec<NodeId> }`. Provides `into_replica_append()` to convert into `(targets, ReplicaAppend)` — `EntryPayload` is cloned via `Bytes` refcount (zero-copy).

`InterNodeCommandQueued` is a generic wrapper for all outbound inter-node messages (replaces individual `Send*` events from the original design). Used for `CommitAdvance`, `ReplicaAck`, `SealRequest`, and `SegmentSealed` — all dispatched uniformly through `DataTransportCommand::send()`.

Reply lifecycle events (`enqueue_reply`, `begin_replication`, `process_ack`, `segment_handoff`, `fail_all`) are handled directly by `ReplicationState` methods, not as `DataPlaneEvent` variants.

**WAL failure:** On `wal.flush_batch()` error, `replication.fail_all()` drains all pending and in-flight replies with `Err`. Leader segments lose their buffered records. Follower segments send no `ReplicaAck` — leader's `ReplicationTimeout` fires and seals.

### DataPlaneTimer

D2 uses two separate timer types, each implementing `TTimer`. No single `DataPlaneTimer` — the two types have different callback semantics and run on separate ticker instances. Both use rolling seq counters (like SWIM). `DataPlaneTimeoutCallback` is the unified callback enum that both convert into.

| Timer type | Callback | Trigger | Effect |
|---|---|---|---|
| `BatchFlushTimer` | `BatchFlushDeadline` | 10ms (1 tick) after first buffered record | Flush inline in `handle_timeout` |
| `ReplicationTimer` | `ReplicationTimeout { seq, segment_key }` | Per-segment, first in-flight batch | Seal + replay |

`BatchFlushDeadline`: set when the first record is buffered (leader produce). Uses WAL's `next_lsn` as seq — ticker rejects duplicate seqs, so re-registering before a flush is a no-op. After flush, `next_lsn` advances and the next produce registers a new timer. Fires once — not periodic. Ensures bounded produce latency even under low throughput.

`ReplicationTimeout`: one timer per segment. Set when the first `PendingBatch` enters `in_flight` via `begin_replication()`. Timer seq allocated from `ReplicationState.next_timer_seq` (wrapping counter). On `process_ack()` commit, if more batches remain in-flight, a new seq is allocated and old is invalidated — stale firings silently dropped via `is_active_timer()`. In-flight state is intentionally NOT cleared on timeout — a late ack may still commit the batch while `SealRequest` is in transit. Safe because `apply_roll_segment()` is idempotent (DS-RSM invariant 9). Timeout: 1s (100 ticks × 10ms).

---

## DataTransportActor

Same architectural pattern as `RaftTransportActor` (see `raft-transport.md`) — `HashMap<NodeId, OwnedWriteHalf>`, lower-`NodeId`-wins conflict resolution, length-prefixed bincode frames, identical connection lifecycle (outbound resolve, inbound accept, disconnect on SWIM death, 300s cleanup).

**Differences from RaftTransportActor:**

| | RaftTransportActor | DataTransportActor |
|---|---|---|
| Port | `raft_port` | `data_port` (2923) |
| Frame size cap | 4MB | 64MB (record batches up to 10MB per segment) |
| Runtime | tokio | tokio |

**Threading note:** `DataTransportActor` runs on tokio (async TCP I/O, no blocking calls). `DataPlaneActor` runs on a dedicated OS thread (blocking WAL fsync) — a pinned thread keeps hot data (write buffers, segment trackers, WAL write buffer) in L1/L2 cache across batches.

---

## Data Port Message Catalog

All `data_port` wire messages, consolidated across phases:

| Message | Direction | Key fields | Phase |
|---|---|---|---|
| `ReplicaAppend` | Leader → followers | `segment_key`, `replica_set`, `data: EntryPayload`, `record_count`, `entry_id` | D2 |
| `ReplicaAck` | Follower → leader | `segment_key`, `entry_id`, `from` | D2 |
| `CommitAdvance` | Leader → followers | `segment_key`, `committed_entry_id` | D2 |
| `SealRequest` | Leader → coordinator | `segment_key`, `failed_nodes: Vec<NodeId>`, `end_entry_id` | D2 |
| `SealResponse` | Coordinator → leader | `old_segment_key`, `new_segment_id`, `new_replica_set` | D2/D3 |
| `SegmentSealed` | Leader → old followers | `segment_key` | D2 |
| `SegmentAssignment` | Coordinator → leader | `segment_key`, `replica_set`, `start_entry_id` | D3 |
| `CatchUpAssignment` | Coordinator → replacement | `segment_key`, `replica_set` | D2 |
| `CatchUpRequest` | Replacement → healthy replica | `segment_key`, `local_end_offset` | D2 |
| `CatchUpResponse` | Healthy replica → replacement | `segment_key`, `records`, `start/end_offset` | D2 |

**SealRequest routing:** Segment leader resolves coordinator address via shard leader map (SWIM gossip), sends SealRequest over `data_port`. Coordinator-side receive path (`DataTransportActor` → routing to `MultiRaftActor.propose(RollSegment)`) is defined in D3.

---

## Failure Model

All failures converge to one response: **seal the segment, open a new one.** No retry, no ISR, no reconciliation.

**Entry ID handoff:** `SealRequest` carries `end_entry_id = tracker.committed_entry_id` (last entry_id ACKed by all replicas). The coordinator passes it through `RollSegment` → `apply_roll_segment()` seals with this value, new segment starts at `end_entry_id + 1`.

**Uncommitted tail replay:** Entries between `read_cursor` and `write_cursor` at seal time are not part of the sealed segment — but they are not dropped. On `SealResponse`, the leader reads uncommitted entries from the old segment's cache via `tracker.uncommitted_entries()`, stages them into the new segment's `tracker.staged_entries` (re-WAL'd with new routing headers on next `flush_batch()`), and migrates the pending reply channels via `replication.segment_handoff()`. When the replayed entries are committed in the new segment with the new replica set, the original producers receive `Ok` — they never see a failure, just higher latency (~50-100ms seal overhead). No producer retry, no duplicates from the seal mechanism itself.

Old WAL records (under the sealed segment's routing headers) are cleaned up by normal WAL file deletion once the checkpoint watermark advances past them.

**RollSegment idempotency:** Scenarios F1 and F2 may race for the same failure. `apply_roll_segment()` checks preconditions — if already sealed, subsequent proposals are no-ops (DS-RSM invariant 9).

### F1: Follower Failure

Detected by segment leader via `ReplicationTimeout` (~500ms–1s). Leader preserves its position at `replica_set[0]`.

```
Leader D      Coordinator A     Raft [A,B,C]    Follower E
   │               │                 │               │
   │ ReplicationTimeout fires        │               │
   │ stop new produces for seg 7     │               │
   │               │                 │               │
   │─SealRequest──►│                 │               │
   │ {end_entry_id}│─RollSegment────►│               │
   │               │ {new_rs:[D,E,G]}│ commit        │
   │               │◄────────────────│               │
   │◄SealResponse──│                 │               │
   │ {seg 8,[D,E,G]}                 │               │
   │               │                 │               │
   │ open seg 8, replay uncommitted  │               │
   │ resume produce (~100ms)         │               │
   │─SegmentSealed──────────────────────────────────►│
   │               │                 │        close seg 7
```

### F2: Leader Failure

Detected by SWIM node death (~6-7s). A surviving follower is promoted to `replica_set[0]`.

```
SWIM        Coordinator A    Raft [A,B,C]   E (promoted)  Producer
  │              │                │              │              │
  │ Leader D dead│                │              │              │
  │─NodeDeath───►│                │              │              │
  │              │─RollSegment───►│              │              │
  │              │ {new_rs:[E,G]} │ commit       │              │
  │              │◄───────────────│              │              │
  │              │─SegmentAssignment────────────►│              │
  │              │ {seg 8,[E,G]}  │  open seg 8  │              │
  │              │                │              │◄─meta query──│
  │              │                │              │─leader info─►│
  │              │                │              │◄─Produce─────│
```

### F3: Sealed Segment Repair

After seal, the old segment may be under-replicated. Coordinator assigns a replacement and triggers async repair. Only sealed (immutable) segments need catch-up — active segments never do (seal-on-failure guarantees fresh start).

```
Coordinator A    Raft [A,B,C]    Healthy E     Replacement H
   │                  │              │               │
   │─ReassignSegment─►│              │               │
   │ {[D,E,F]→[D,E,H]}│ commit       │               │
   │◄─────────────────│              │               │
   │─CatchUpAssignment──────────────────────────────►│
   │                  │              │  check local  │
   │                  │              │◄─CatchUpReq───│
   │                  │              │ {end_entry_id}│
   │                  │              │─CatchUpResp──►│
   │                  │              │ stream delta  │
   │                  │              │        verify + complete
```

Nodes get new NodeIds on restart (UUID regenerated). Local disk may still have data from a previous lifecycle — `CatchUpRequest` advertises `local_end_offset`, healthy replica streams only the delta.

---

## Example Scenario: Seal-on-Failure

Metadata group: vnodes [A, B, C]. Data replicas: brokers [D, E, F] for segment 7.

```
Client              Coordinator A       Raft [A,B,C]        Broker D
  │                      │                   │                  │
  │──CreateTopic────────►│                   │                  │
  │                      │──propose─────────►│                  │
  │                      │                   │ commit           │
  │                      │◄──────────────────│                  │
  │                      │──SegmentAssignment──────────────────►│
  │                      │  {seg 7,[D,E,F]}  │       create tracker (Leader)
  │◄─────────────────────│                   │       D = replica_set[0]
```

**Step 1: Normal produce flow**
```
Producer ──> Broker D (segment leader, DataPlaneActor)
               │── batch trigger → WAL append + fsync
               │── publish to SegmentRingBuffer (write_cursor advances, read_cursor stays)
               │── ReplicaAppend ──> E (WAL fsync, ack) ✓
               │── ReplicaAppend ──> F (WAL fsync, ack) ✓
               │── all acks received → read_cursor = write_cursor, notify consumers
               │── ACK producer
               │── async: checkpoint worker → segment file + sparse index
```

**Steps 2-3: F fails → seal (follows F1 flow above)**

F times out on ReplicaAppend. D fires ReplicationTimeout, keeps pending replies alive, sends SealRequest to coordinator A. Coordinator proposes `RollSegment { old: 7, new_rs: [D, E, G] }`, Raft commits, returns SealResponse `{ seg 8, [D, E, G] }` to D.

**Step 4: D replays uncommitted tail, resumes (~100ms total downtime)**
```
Broker D              Broker E              Broker G              Broker F (dead)
   │                     │                     │                     ✗
   │ open seg 8 (Leader) │                     │
   │ read uncommitted batches from seg 7 cache │
   │ replay uncommitted entries into seg 8 tracker.staged_entries
   │ migrate pending reply channels to seg 8   │
   │──SegmentSealed─────►│                     │
   │                  close seg 7              │
   │                     │                     │
   │ flush_batch() (replayed + any new records)│
   │──ReplicaAppend(seg 8)──►E ✓               │
   │──ReplicaAppend(seg 8)────────────────────►│
   │                     │          validate replica_set (self-auth)
   │                     │          create tracker (Follower)
   │                     │                     │
   │ all acks → commit → ACK original producers (Ok)
   │ resume produce      │                     │
```

**Step 5: Sealed segment repair (async)**
```
Coordinator A       Raft [A,B,C]       Broker E           Broker H (replacement)
   │                    │                  │                    │
   │ seg 7 under-replicated (F missing)    │                    │
   │──ReassignSegment──►│                  │                    │
   │  {[D,E,F]→[D,E,H]}│ commit            │                    │
   │◄───────────────────│                  │                    │
   │                    │                  │◄──CatchUpRequest───│
   │                    │                  │ {local_end_offset} │
   │                    │                  │──CatchUpResponse──►│
   │                    │                  │  stream delta      │
   │                    │                  │              verify + complete
```

### Safety Between Seal and Notification

Between Raft commit (step 3) and `data_port` notifications reaching participants (step 4), there's a brief window (~milliseconds over TCP). This is safe:

- **Segment leader D** already stopped writing to segment 7 (D initiated the seal). No new data enters segment 7.
- **Follower E** learns via `SegmentSealed` from D over `data_port`. Not waiting for gossip.
- **New replica G** learns from D's first `ReplicaAppend` for segment 8 (self-authorizing).
- **Failed node F** is unreachable. On recovery with new NodeId, it queries the coordinator.
- **No legitimate produce traffic** targets segment 7 — the active segment is now 8.
- **Consumer reads** of segment 7 are fine — sealed segments are immutable.

---

## Invariants

1. **ALL-replica ack before producer ACK.** Producer receives ACK only after the leader's WAL fsync AND all followers' WAL fsyncs are confirmed. No partial ack, no majority ack.

2. **`read_cursor` ≤ `write_cursor` with replication gap.** In D2, `read_cursor` lags `write_cursor` by one replication RTT. `write_cursor` advances on cache publish (after local WAL fsync). `read_cursor` advances after all replicas ACK. Consumers only read up to `read_cursor`. Extends D1 invariant 15 — in D1, `read_cursor == write_cursor`; in D2, `read_cursor ≤ write_cursor` with the gap closed by replication.

3. **Sealed segment `end_entry_id` = last committed entry_id.** On seal, `end_entry_id = tracker.committed_entry_id` — the entry_id of the last entry ACKed by all replicas. Entries past this (published to cache but uncommitted) are replayed into the new segment (see invariant 10). Same as D1 invariant 15.

4. **Single writer per segment.** Only `replica_set[0]` (the segment leader) accepts `Produce` commands and initiates `ReplicaAppend`. Followers reject `Produce` commands for segments where `role == Follower`.

5. **Follower `committed_entry_id` ≤ leader `committed_entry_id`.** Followers advance `read_cursor` and update `committed_entry_id` on `CommitAdvance` from the leader, which arrives ~0.5 RTT after the leader commits. The follower's committed range is always ≤ the leader's.

6. **Replication is per-segment.** Each segment has its own `replica_set` and independent `VecDeque<PendingBatch>`. One WAL flush can produce batches for multiple segments; each is replicated independently. Commit and producer ACK happen per-segment. No cross-segment dependency.

7. **Self-authorization is sufficient for follower onboarding.** No prior `SegmentAssignment` required for followers — see "Replica Self-Authorization" above.

8. **ReplicationTimeout triggers seal, not retry.** A slow or failed replica is treated the same — seal the segment, replace the replica. No retry, no ISR shrink/expand.

9. **Seal is leader-initiated for follower failures, coordinator-initiated for leader failures.** The segment leader detects follower failure (timeout) and sends `SealRequest`. The coordinator detects leader failure via SWIM `HandleNodeDeath` and proposes `RollSegment` directly.

10. **Uncommitted tail replayed, not dropped.** Entries between `read_cursor` and `write_cursor` at seal time are read from the old segment's cache via `uncommitted_entries()` and staged into the new segment's `tracker.staged_entries`. Pending reply channels migrate via `replication.segment_handoff()`. Producers receive `Ok` after the replayed entries commit in the new segment — no retry, no duplicates from the seal mechanism.

11. **Segment leader preserved on follower failure.** `RollSegment` keeps the previous leader at `replica_set[0]` when only a follower failed — preserving cache locality and active producer connections. Only when the leader itself failed does a new node take position 0.

12. **TCP ordering is a protocol assumption.** All `data_port` messages between a pair of nodes are delivered in send order. The protocol relies on this for: `CommitAdvance` arriving after its corresponding `ReplicaAppend`, `ReplicaAck` correlation to the front of the per-segment `VecDeque`, and `SegmentSealed` arriving after all `CommitAdvance`s for that segment.