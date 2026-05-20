# Phase D2: Segment Replication

**Goal:** Primary-backup replication — all replicas fsync before producer ACK. Seal-on-failure. `DataTransportActor` for `data_port` TCP.

**Depends on:** Phase D1 (storage engine).

---

## Replication Protocol

### Leader Write Path

```
records arrive
    │
    ▼
tracker.write_buffer.push(record)
    │
    │ batch trigger fires (10ms / 20k records / 10MB)
    ▼
╔═══════════════════════════════════════════════════════╗
║ WAL: serialize all buffers with routing headers       ║
║      → flat interleaved write → fsync                 ║
║      assign LSN to each record                        ║
╚════════════════════════╤══════════════════════════════╝
                         │ ◄── durability point (local)
                         ▼
   ┌─────────── for each segment_key ────────────┐
   │ Vec<Record> ──move──▶ SegmentRecordBatch    │
   │ Arc::new() → store at batches[tail % CAP]   │
   │ tail.store(tail + 1, Release)               │
   └─────────────────────┬──────────────────────-┘
                         │ ◄── in cache, not yet visible to consumers
                         ▼
   emit WalBatchComplete { lsn, segment_batches }
                         │
                   actor event loop
                         │
                         ▼
   ┌─────────── fan-out to followers ────────────┐
   │ for each segment with followers:            │
   │   send ReplicaAppend to all followers       │
   │   via DataTransportActor                    │
   │ set ReplicationTimeout timer per segment    │
   └─────────────────────┬──────────────────────-┘
                         │
                         ▼
   ┌── per segment, independently as acks arrive ─┐
   │ DataPlaneCommand::ReplicaAck arrives         │
   │ update pending_acks for that segment         │
   │ when segment's ALL followers acked:          │
   │   commit_offset += 1                         │  ◄── durability point (segment)
   │   notify.notify_waiters()                    │  ◄── leader consumers can read
   │   CommitAdvance to followers                 │
   │   drain segment replies with Ok              │  ◄── ACK producers
   └─────────────────────┬───────────────────────-┘
                         │
                         ▼ (background)
   checkpoint worker       size_bytes → RollSegment at ~1GB
```

WAL fsync is the sole disk write on the leader's critical path, same as D1. The replication fan-out is network I/O handled by `DataTransportActor` (tokio). Produce latency = `local_fsync + max(follower_fsyncs)` — one local fsync plus one replication RTT in the common case.

**No-follower fast path:** When `tracker.followers()` is empty (replication_factor=1), the flow degrades to D1 behavior — `WalBatchComplete` drains `pending_replies` immediately, no replication tracking needed.

### Follower Write Path

```
ReplicaAppend arrives
    │
    ├── segment unknown? validate & create SegmentTracker (see self-authorization)
    │
    ▼
tracker.write_buffer.push(records)
    │
    │ flush_batch() fires (unified — see below)
    ▼
╔═══════════════════════════════════════════════════════╗
║ WAL: drain each tracker.write_buffer (all segments)    ║
║    → single write → fsync → BatchEnd                  ║
╚════════════════════════════╤══════════════════════════╝
                             │ ◄── durability point (all records in this flush)
                             ▼
       per follower segment:
         publish_uncommitted(batch) → ReplicaAck to leader
       per leader segment:
         publish_uncommitted(batch) → WalBatchComplete (replication fan-out)

CommitAdvance { committed_end_offset } arrives later from leader
    │
    ▼
commit_offset += 1, notify.notify_waiters()    ◄── follower consumers can read
tracker.committed_end_offset = committed_end_offset
```

Records arrive pre-batched in `ReplicaAppend` — no accumulation on the follower side. Both leader and follower segments buffer into `tracker.write_buffer`. The unified flush drains all trackers in a single WAL write + fsync + `BatchEnd`, enabling uniform crash recovery (D5).

**Unified WAL Flush.** A node writes to one WAL. All segments buffer into `tracker.write_buffer` — the buffer is embedded in `SegmentTracker`, co-located with `role`, `cache`, and `committed_end_offset`. `flush_batch()` iterates `segments`, drains each tracker's `write_buffer`, and writes everything in a single WAL write + fsync + `BatchEnd`. Post-flush, `tracker.role` determines events: leader → WalBatchComplete (replication fan-out), follower → ReplicaAckReady. Single lookup per segment, no separate buffer map.

Flush fires when either a leader batch trigger trips (10ms / 20k / 10MB) OR the actor finishes draining the mailbox with `needs_flush == true`. Multiple `ReplicaAppend`s from different leaders that arrive in the same mailbox batch share one fsync. Leader records ride along for free if a follower triggers the flush, and vice versa.

Follower-to-leader promotion (F2) requires no buffer migration: the new segment's `SegmentTracker` is created with `role = Leader` and an empty `write_buffer`. `flush_batch()` handles it like any other leader segment.

### Replica Self-Authorization

The first `ReplicaAppend` for an unknown segment is self-authorizing — no prior `SegmentAssignment` notification needed for followers. `ReplicaAppend` carries `replica_set` and batch data. Follower validates:
1. Am I in `replica_set`? → if not, reject
2. Is sender `replica_set[0]` (segment leader)? → if not, reject
3. Valid → create `SegmentTracker` with `SegmentRole::Follower`, create segment file, accept

Only the segment leader requires explicit `SegmentAssignment` from the coordinator (D3). Followers learn about new segments from the leader's first `ReplicaAppend` — no gossip or coordinator notification required on the follower data path.

### Commit Notification

After the leader advances `commit_offset` (all replicas ACKed), it sends `CommitAdvance { segment_key, committed_end_offset }` to all followers. Followers advance their own `commit_offset` on receipt — follower consumers can then read the committed batch.

`CommitAdvance` carries the **logical record offset** (`committed_end_offset`), not the ring buffer batch position. Follower on receipt: `cache.commit(cache.load_commit_offset() + 1)` (ring buffer always advances by 1), then sets `tracker.committed_end_offset` directly from the message — no batch read needed. Can validate: `next_batch.end_offset == received_offset`. Decouples the wire protocol from ring buffer internals. Lightweight message (`segment_key` + `u64`), sent once per commit per follower. TCP ordering guarantees it arrives before the next `ReplicaAppend`. Follower visibility lags the leader by ~0.5 RTT (one network hop), not by one batch interval.

---

## Component Changes

### SegmentTracker

D2 adds `role: SegmentRole` (Leader | Follower) and `replica_set: Vec<NodeId>` (`replica_set[0]` = leader).

```
SegmentTracker (D1 fields unchanged)
├── cache, size_bytes, checkpoint_lsn, segment_file_path
├── role: Leader | Follower                     ← NEW
├── replica_set: [leader, follower, follower]   ← NEW
├── committed_end_offset: u64                   ← NEW (logical offset, not ring buffer position)
└── write_buffer: Vec<Record>                   ← NEW (moved from separate map into tracker)
```

**Publish split:** D1's `publish()` auto-commits. D2 splits into `publish_uncommitted()` (advances `tail` only) and `commit(batch_end_offset)` (advances ring buffer `commit_offset` by 1, updates `committed_end_offset` from the batch's logical offset, notifies consumers). Also adds `followers() → &[NodeId]` returning `replica_set[1..]`.

**Two offset domains — do not conflate:**
- `SegmentCache.commit_offset` — ring buffer batch position (e.g., 5 = "5 batches committed"). Each `flush_batch()` produces exactly one `SegmentRecordBatch` per segment, so commit always advances by exactly 1. No stored ring position needed — `commit()` calls `cache.commit(cache.load_commit_offset() + 1)` internally.
- `SegmentTracker.committed_end_offset` — logical record offset within the segment (e.g., 42000 = "records through offset 42000 are committed"). Updated from the batch's `end_offset` field on each commit. This is the value carried in `SealRequest.end_offset` and feeds MetadataStateMachine invariant 8 (offset continuity).

- **Leader:** `publish_uncommitted()` during flush → `commit(batch_end_offset)` after all replicas ACK
- **Follower:** `publish_uncommitted()` on `ReplicaAppend` → `commit()` on `CommitAdvance` from leader (~0.5 RTT after leader commits)

### DataPlane State Machine

**New field:** `node_id: NodeId` — needed for role determination and self-authorization.

**New methods:**

- `process_replica_append()` — follower path. Validates sender, buffers records in `tracker.write_buffer`, sets `needs_flush`. No WAL write here — deferred to `flush_batch()`. Does NOT advance `commit_offset` — that happens on `CommitAdvance`.
- `commit_segment(batch_end_offset)` — advances ring buffer `commit_offset` by 1 (always +1, each batch = one ring slot), updates `committed_end_offset` from the logical offset. Called by actor when a segment's acks complete within a flight.
- `handle_seal_response(old_segment_key, new_segment_id, new_replica_set)` — creates new `SegmentTracker` with `role = Leader` and the new `replica_set`. Reads uncommitted batches from old segment's cache (`commit_offset` to `tail - 1`), injects their records into `the new segment's `tracker.write_buffer``. Emits `SendSegmentSealed` to notify old followers. Old segment's tracker is removed. Reply channels for the replayed records are migrated by the actor (see "Uncommitted tail replay").

**`ProducePending` change:** D1 emits `ProducePending(reply)`. D2 emits `ProducePending { segment_key, reply }` — the segment_key lets the actor partition replies per-segment for independent ack/failure handling.

**`flush_batch()` changes:**

```
D1:  WAL write+fsync → tracker.publish(batch)       ← auto-commits
                      → emit WalBatchComplete

D2:  for each segment, drain tracker.write_buffer
     → single WAL write+fsync+BatchEnd
     → per segment: publish_uncommitted(batch)
       tracker.role == Leader  → collect for WalBatchComplete (replication fan-out)
       tracker.role == Follower → emit ReplicaAckReady
     → reset needs_flush
```

Flush triggers: leader batch trigger (10ms / 20k / 10MB) OR `needs_flush == true` after mailbox drain. Either trigger flushes everything — leader and follower buffers combined.

**`handle_segment_assignment()` changes:** D1 ignores `replica_set`. D2 stores it and sets `role = Leader` if `replica_set[0] == self.node_id`, else `Follower`.

**`process()` dispatch additions:** Two new arms — `ReplicaAppend` dispatches to `process_replica_append()`, `SealResponse` dispatches to `handle_seal_response()`.

**Produce rejection for followers:** If a `Produce` targets a segment where `role == Follower`, reply with `ProduceAck::Err("not leader for segment")`. Stale routing — producer retries after metadata refresh.

### DataPlaneActor

**Reply lifecycle:**

```
Pending ─────────────────► Replicating ──────────────► Resolved
  Produce arrives            WalBatchComplete            All acks → drain with Ok
  → push to                  → build SegmentFlights      Timeout → seal + replay
    pending_replies            from pending_replies         (replies migrate to new segment)
                               + segment_batches         WAL fail → drain with Err
```

Two phases, two transitions. `pending_replies` accumulates pre-flush. `WalBatchComplete` carries the segment batches and moves replies directly into `SegmentFlight`s — no intermediate buffer. On timeout, replies are not drained with Err — they migrate to the new segment via `handle_seal_response()` and resolve with `Ok` after the replayed records commit.

**Actor state:**

```rust
struct ReplicationState {
    // Pending — accumulates between produces, drained on WalBatchComplete
    // D2: per-segment (was flat Vec in D1)
    pending_replies: HashMap<SegmentKey, Vec<oneshot::Sender<ProduceAck>>>,

    // Replicating — in-flight, built directly from pending_replies + WalBatchComplete
    replicating: VecDeque<ReplicationFlight>,
}

struct ReplicationFlight {
    batch_id: u64,
    segments: HashMap<SegmentKey, SegmentFlight>,
}

// Per-segment within a flight — completes independently
struct SegmentFlight {
    replies: Vec<oneshot::Sender<ProduceAck>>,
    pending_acks: HashSet<NodeId>,
    batch_end_offset: u64,  // extracted from batch.end_offset at construction
}
```

Each segment's replication completes independently — no cross-segment dependency within a flight. When s1's followers all ack, s1's producers are ACKed immediately even if s2 is still pending.

**`spawn()` additions:** Two new params — `node_id: NodeId` (passed to `DataPlane::new()`) and `data_transport_tx: tokio_mpsc::Sender<OutboundDataMessage>` (data_port transport).

**D2 event additions:**

```rust
pub(crate) enum DataPlaneEvent {
    // ... D1 variants unchanged ...

    ProducePending { segment_key: SegmentKey, reply: oneshot::Sender<ProduceAck> },
    WalBatchComplete {
        lsn: u64,
        segment_batches: Vec<(SegmentKey, Vec<NodeId>, Arc<SegmentRecordBatch>)>,
    },

    // follower → leader
    ReplicaAckReady { leader: NodeId, segment_key: SegmentKey, batch_id: u64 },
    // leader → followers
    SendCommitAdvance { segment_key: SegmentKey, committed_end_offset: u64, followers: Vec<NodeId> },
    SendSealRequest { segment_key: SegmentKey, failed_node: NodeId, end_offset: u64 },
    SendSegmentSealed { segment_key: SegmentKey, followers: Vec<NodeId> },
}
```

**Actor dispatch (sketch):**

```rust
match event {
    ProducePending { .. }        => // push reply to pending_replies[segment_key]
    WalBatchComplete { .. }      => // per segment: no followers → commit + ACK immediately
                                    //              has followers → build SegmentFlight, fan-out
                                    //                              ReplicaAppend, set timeout,
                                    //                              push to replicating
    WalBatchFailed(..)           => // drain ALL pending_replies with Err
    ReplicaAckReady { .. }
    | SendCommitAdvance { .. }
    | SendSegmentSealed { .. }   => // forward to DataTransportActor
}

match command {
    ReplicaAck { .. }            => // remove ack, segment complete → commit + ACK
                                    //   + SendCommitAdvance { committed_end_offset }
                                    // flight empty → remove from replicating
}
```

### DataPlaneTimer

New variant: `ReplicationTimeout { segment_key, batch_id }`. Set when replication starts for a segment batch. Cancelled when all acks received. If it fires → stop accepting produces for this segment, emit `SendSealRequest`. Reply channels kept alive — migrated to the new segment on `SealResponse` (see "Uncommitted tail replay").

**Timeout tuning:** 500ms–1s. Conservative to avoid seal-on-transient-blip. A slow replica that consistently misses the timeout triggers seal — this is intentional (see roadmap "Performance Implications of ALL-Replica Ack").

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
| `ReplicaAppend` | Leader → followers | `segment_key`, `replica_set`, `records`, `start/end_offset`, `batch_id` | D2 |
| `ReplicaAck` | Follower → leader | `segment_key`, `batch_id` | D2 |
| `CommitAdvance` | Leader → followers | `segment_key`, `committed_end_offset` | D2 |
| `SealRequest` | Leader → coordinator | `shard_group_id`, `range_id`, `segment_id`, `failed_node`, `end_offset` | D2 |
| `SealResponse` | Coordinator → leader | `old_segment_key`, `new_segment_id`, `new_replica_set` | D2/D3 |
| `SegmentSealed` | Leader → old followers | `segment_key` | D2 |
| `SegmentAssignment` | Coordinator → leader | `segment_key`, `replica_set` | D3 |
| `CatchUpRequest` | Replacement → healthy replica | `segment_key`, `local_end_offset` | D2 |
| `CatchUpResponse` | Healthy replica → replacement | `segment_key`, `records`, `start/end_offset` | D2 |

---

## Failure Model

All failures converge to one response: **seal the segment, open a new one.** No retry, no ISR, no reconciliation.

**Offset handoff:** `SealRequest` carries `end_offset = tracker.committed_end_offset` (logical offset of last record ACKed by all replicas). The coordinator passes it through `RollSegment` → `apply_roll_segment()` seals with this value, new segment starts at `end_offset + 1`. Maintains MetadataStateMachine invariant 8 (offset continuity).

**Uncommitted tail replay:** Records between `commit_offset` and `tail` at seal time are not part of the sealed segment — but they are not dropped. On `SealResponse`, the leader reads uncommitted batches from the old segment's cache, injects their records into `the new segment's `tracker.write_buffer`` (re-WAL'd with new routing headers on next `flush_batch()`), and migrates the pending reply channels to the new segment. When the replayed records are committed in the new segment with the new replica set, the original producers receive `Ok` — they never see a failure, just higher latency (~50-100ms seal overhead). No producer retry, no duplicates from the seal mechanism itself.

Old WAL records (under the sealed segment's routing headers) are cleaned up by normal WAL file deletion once the checkpoint watermark advances past them.

**RollSegment idempotency:** Scenarios F1 and F2 may race for the same failure. `apply_roll_segment()` checks preconditions — if already sealed, subsequent proposals are no-ops (DS-RSM invariant 9).

### F1: Follower Failure

Detected by segment leader via `ReplicationTimeout` (~500ms–1s). Leader preserves its position at `replica_set[0]`.

```
Leader D         Coordinator A        Raft [A,B,C]       Follower E
   │                  │                    │                  │
   │ ReplicationTimeout fires              │                  │
   │ stop accepting new produces for seg 7 │                  │
   │                  │                    │                  │
   │──SealRequest────►│                    │                  │
   │  {end_offset}    │──RollSegment──────►│                  │
   │                  │  {new_rs:[D,E,G]}  │ commit           │
   │                  │◄───────────────────│                  │
   │◄─SealResponse────│                    │                  │
   │  {seg 8,[D,E,G]} │                    │                  │
   │                  │                    │                  │
   │ open seg 8 tracker (Leader)           │                  │
   │ resume produce (~100ms downtime)      │                  │
   │──SegmentSealed──────────────────────────────────────────►│
   │                  │                    │          close seg 7
```

### F2: Leader Failure

Detected by SWIM node death (~6-7s). A surviving follower is promoted to `replica_set[0]`.

```
SWIM              Coordinator A       Raft [A,B,C]     Follower E (promoted)    Producer
  │                    │                   │                  │                    │
  │ Leader D dead      │                   │                  │                    │
  │──HandleNodeDeath──►│                   │                  │                    │
  │                    │──RollSegment─────►│                  │                    │
  │                    │  {new_rs:[E,G]}   │ commit           │                    │
  │                    │◄──────────────────│                  │                    │
  │                    │──SegmentAssignment──────────────────►│                    │
  │                    │  {seg 8,[E,G]}    │       open seg 8 (Leader)             │
  │                    │                   │                  │◄──metadata query───│
  │                    │                   │                  │──new leader info──►│
  │                    │                   │                  │◄──Produce──────────│
```

### F3: Sealed Segment Repair

After seal, the old segment may be under-replicated. Coordinator assigns a replacement and triggers async repair. Only sealed (immutable) segments need catch-up — active segments never do (seal-on-failure guarantees fresh start).

```
Coordinator A       Raft [A,B,C]       Healthy E          Replacement H
   │                    │                  │                    │
   │──ReassignSegment──►│                  │                    │
   │  {[D,E,F]→[D,E,H]} │ commit           │                    │
   │◄───────────────────│                  │                    │
   │──CatchUpAssignment────────────────────────────────────────► │
   │                    │                  │check local inventory
   │                    │                  │◄───CatchUpRequest──│
   │                    │                  │ {local_end_offset} │ 
   │                    │                  │──CatchUpResponse──►│
   │                    │                  │  stream delta      │
   │                    │                  │               verify CRC
   │                    │                  │               repair complete
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
               │── publish to SegmentCache (tail advances, commit_offset stays)
               │── ReplicaAppend ──> E (WAL fsync, ack) ✓
               │── ReplicaAppend ──> F (WAL fsync, ack) ✓
               │── all acks received → commit_offset = tail, notify consumers
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
   │ inject records into seg 8 tracker.write_buffer
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

2. **`commit_offset` ≤ `tail` with replication gap.** In D2, `commit_offset` lags `tail` by one replication RTT. `tail` advances on cache publish (after local WAL fsync). `commit_offset` advances after all replicas ACK. Consumers only read up to `commit_offset`. Extends D1 invariant 15 — in D1, `commit_offset == tail`; in D2, `commit_offset ≤ tail` with the gap closed by replication.

3. **Sealed segment `end_offset` = last committed logical offset.** On seal, `end_offset = tracker.committed_end_offset` — the logical offset of the last record ACKed by all replicas. Records past this (published to cache but uncommitted) are replayed into the new segment (see invariant 10). Same as D1 invariant 16.

4. **Single writer per segment.** Only `replica_set[0]` (the segment leader) accepts `Produce` commands and initiates `ReplicaAppend`. Followers reject `Produce` commands for segments where `role == Follower`.

5. **Follower `committed_end_offset` ≤ leader `committed_end_offset`.** Followers advance `commit_offset` and update `committed_end_offset` on `CommitAdvance` from the leader, which arrives ~0.5 RTT after the leader commits. The follower's committed range is always ≤ the leader's.

6. **Replication is per-segment.** Each segment has its own `replica_set`. One WAL batch can produce batches for multiple segments; each segment's batch is replicated independently to that segment's followers. Within a `ReplicationFlight`, each `SegmentFlight` completes independently — commit and producer ACK happen per-segment, not per-flight. No cross-segment dependency.

7. **Self-authorization is sufficient for follower onboarding.** No prior `SegmentAssignment` required for followers — see "Replica Self-Authorization" above.

8. **ReplicationTimeout triggers seal, not retry.** A slow or failed replica is treated the same — seal the segment, replace the replica. No retry, no ISR shrink/expand.

9. **Seal is leader-initiated for follower failures, coordinator-initiated for leader failures.** The segment leader detects follower failure (timeout) and sends `SealRequest`. The coordinator detects leader failure via SWIM `HandleNodeDeath` and proposes `RollSegment` directly.

10. **Uncommitted tail replayed, not dropped.** Records between `commit_offset` and `tail` at seal time are read from the old segment's cache and injected into the new segment's `tracker.write_buffer`. Pending reply channels migrate with them. Producers receive `Ok` after the replayed records commit in the new segment — no retry, no duplicates from the seal mechanism.

11. **DataTransportActor and RaftTransportActor are independent.** Separate ports, separate connections, separate actors. Data backpressure never stalls Raft heartbeats.

12. **Unified WAL flush across roles.** All segments buffer into `tracker.write_buffer` and share one per-node WAL. `flush_batch()` iterates `segments`, drains each tracker's buffer into a single WAL write + fsync + `BatchEnd`, then uses `tracker.role` for post-flush events. One fsync per flush, not one per role per segment. WAL replay on crash recovery (D5) handles all records uniformly via routing headers.

13. **`ReplicaAck` carries no data.** Only `(segment_key, batch_id)`. `batch_id` is an opaque correlation token (derived from the leader's WAL LSN, but not interpreted as a WAL position by the follower). The leader uses it to match acks to in-flight `ReplicationFlight`s.

14. **Segment leader preserved on follower failure.** `RollSegment` keeps the previous leader at `replica_set[0]` when only a follower failed — preserving cache locality and active producer connections. Only when the leader itself failed does a new node take position 0.