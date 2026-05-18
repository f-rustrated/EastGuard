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
accumulation_buffers[segment_key].push(record)
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
   emit ReplicateSegmentBatch per segment with followers
   emit WalBatchComplete { lsn }
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
   ┌─────── wait ALL follower acks ──────────────┐
   │ DataPlaneCommand::ReplicaAck arrives         │
   │ update pending_acks tracking                 │
   │ when all segments × all followers acked:     │
   └─────────────────────┬──────────────────────-┘
                         │ ◄── durability point (all replicas)
                         ▼
   per segment, as each completes independently:
     commit_offset += 1, notify.notify_waiters()  ◄── leader consumers can read
     send CommitAdvance { segment_key, commit_offset } to followers
     ACK that segment's producers (drain segment replies with Ok)
                         │
                         ▼ (background)
   checkpoint worker       size_bytes → RollSegment at ~1GB
```

WAL fsync is the sole disk write on the leader's critical path, same as D1. The replication fan-out is network I/O handled by `DataTransportActor` (tokio). Produce latency = `local_fsync + max(follower_fsyncs)` — one local fsync plus one replication RTT in the common case.

**No-follower fast path:** When `tracker.followers()` is empty (replication_factor=1), the flow degrades to D1 behavior — `WalBatchComplete` drains `pending_replies` immediately, no replication tracking needed.

### Follower Write Path

```
DataTransportActor receives ReplicaAppend on data_port
    │
    ▼
DataPlaneCommand::ReplicaAppend forwarded to DataPlaneActor
    │
    ▼
DataPlane.process_replica_append()
    │
    ├── segment unknown? validate & create SegmentTracker (see self-authorization)
    │
    ├── WAL: encode records with routing headers → write → fsync
    │        │ ◄── follower durability point
    │
    ├── SegmentRecordBatch → publish to SegmentCache (tail advances)
    │
    └── emit ReplicaAckReady { leader, segment_key, batch_id }
            │
            ▼
      actor sends ReplicaAck to leader via DataTransportActor

CommitAdvance { segment_key, commit_offset } arrives later from leader
    │
    ▼
    commit_offset += 1, notify.notify_waiters()  ◄── follower consumers can read
```

Followers reuse WAL and `SegmentCache` infrastructure from D1. No accumulation buffers — records arrive pre-batched in `ReplicaAppend`. One WAL write + fsync per `ReplicaAppend`. Follower WAL enables uniform crash recovery (D5) and maintains durability guarantees even if SWIM's detection window leaves a follower briefly active after seal.

### Replica Self-Authorization

The first `ReplicaAppend` for an unknown segment is self-authorizing — no prior `SegmentAssignment` notification needed for followers. `ReplicaAppend` carries `replica_set` and batch data. Follower validates:
1. Am I in `replica_set`? → if not, reject
2. Is sender `replica_set[0]` (segment leader)? → if not, reject
3. Valid → create `SegmentTracker` with `SegmentRole::Follower`, create segment file, accept

Only the segment leader requires explicit `SegmentAssignment` from the coordinator (D3). Followers learn about new segments from the leader's first `ReplicaAppend` — no gossip or coordinator notification required on the follower data path.

### Commit Notification

After the leader advances `commit_offset` (all replicas ACKed), it sends `CommitAdvance { segment_key, commit_offset }` to all followers. Followers advance their own `commit_offset` on receipt — follower consumers can then read the committed batch.

`CommitAdvance` is a lightweight message (`segment_key` + `u64`), sent once per commit per follower. TCP ordering guarantees it arrives before the next `ReplicaAppend`. Follower visibility lags the leader by ~0.5 RTT (one network hop), not by one batch interval.

---

## Component Changes

### SegmentTracker

D2 adds `role: SegmentRole` (Leader | Follower) and `replica_set: Vec<NodeId>` (`replica_set[0]` = leader).

```
SegmentTracker (D1 fields unchanged)
├── cache, size_bytes, checkpoint_lsn, segment_file_path
├── role: Leader | Follower                     ← NEW
├── replica_set: [leader, follower, follower]   ← NEW
└── committed_end_offset: u64                   ← NEW (logical offset, not ring buffer position)
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

- `process_replica_append()` — follower path. Validates sender, WAL write + fsync, publishes to cache, emits `ReplicaAckReady`. Does NOT advance `commit_offset` — that happens on `CommitAdvance`.
- `commit_segment(batch_end_offset)` — advances ring buffer `commit_offset` by 1 (always +1, each batch = one ring slot), updates `committed_end_offset` from the logical offset. Called by actor when a segment's acks complete within a flight.

**`ProducePending` change:** D1 emits `ProducePending(reply)`. D2 emits `ProducePending { segment_key, reply }` — the segment_key lets the actor partition replies per-segment for independent ack/failure handling.

**`flush_batch()` changes:**

```
D1:  WAL write+fsync → tracker.publish(batch)       ← auto-commits
                      → emit WalBatchComplete

D2:  WAL write+fsync → publish_uncommitted(batch)    ← tail only, no commit
                      → emit ReplicateSegmentBatch { followers, batch, batch_end_offset }
                        (skipped if no followers — commit immediately, degrade to D1)
                      → emit WalBatchComplete
```

**`handle_segment_assignment()` changes:** D1 ignores `replica_set`. D2 stores it and sets `role = Leader` if `replica_set[0] == self.node_id`, else `Follower`.

**`process()` dispatch additions:** Two new arms — `ReplicaAppend` dispatches to `process_replica_append()`, `SealResponse` dispatches to `handle_seal_response()`.

**Produce rejection for followers:** If a `Produce` targets a segment where `role == Follower`, reply with `ProduceAck::Err("not leader for segment")`. Stale routing — producer retries after metadata refresh.

### DataPlaneActor

**New actor state:**

```
pending_replies: HashMap<SegmentKey, Vec<Sender<ProduceAck>>>   ← D2: per-segment (was flat Vec in D1)

replication_targets: Vec<ReplicationTarget>      ← buffered per ReplicateSegmentBatch
replicating: VecDeque<ReplicationFlight>         ← in-flight replication, keyed by batch_id

ReplicationTarget
├── segment_key, followers, batch, batch_end_offset

ReplicationFlight
├── batch_id
└── segments: HashMap<SegmentKey, SegmentFlight>

SegmentFlight
├── replies: Vec<Sender<ProduceAck>>
├── pending_acks: HashSet<NodeId>
└── batch_end_offset: u64          ← logical offset, for committed_end_offset update
```

Each segment's replication completes independently — no cross-segment dependency within a flight. When s1's followers all ack, s1's producers are ACKed immediately even if s2 is still pending.

**`spawn()` additions:** Two new params — `node_id: NodeId` (passed to `DataPlane::new()`) and `data_transport_tx: tokio_mpsc::Sender<OutboundDataMessage>` (data_port transport).

**Event handling changes:**

```
ProducePending { segment_key, reply }
                               → push to pending_replies[segment_key] (D2: per-segment)
ReplicateSegmentBatch { .. }   → buffer in replication_targets (NEW)
WalBatchComplete { lsn }      → for segments with no followers:
                                   commit_segment(batch_end_offset), drain replies, send Ok
                                 for segments with followers:
                                   build SegmentFlight per segment from targets + replies
                                   → fan-out ReplicaAppend via transport
                                   → set ReplicationTimeout per segment
                                   → push ReplicationFlight to replicating queue
ReplicaAckReady { .. }         → forward ReplicaAck to leader via transport (NEW)
SendCommitAdvance { .. }       → send CommitAdvance to followers via transport (NEW)
WalBatchFailed(e)              → drain ALL pending_replies with Err (unchanged)
```

**ReplicaAck processing:**

```
ReplicaAck { from, segment_key, batch_id } arrives
    │
    ├── find flight → remove from from segments[segment_key].pending_acks
    ├── cancel ReplicationTimeout if segment fully acked
    ├── segment complete → commit_segment(segment_key, batch_end_offset)
    │                    → emit SendCommitAdvance to followers
    │                    → drain segment_flight.replies with Ok
    └── flight empty (all segments done) → remove flight from replicating
```

**Multiple flights in flight:** Volume triggers can flush a second batch before the first replication completes. Flights complete independently. A flight is removed from `replicating` when all its `SegmentFlight`s are drained.

### New Commands and Events

**DataPlaneCommand additions (D2):**

| Variant | Direction | Key fields |
|---|---|---|
| `ReplicaAppend` | leader → follower | `from`, `segment_key`, `replica_set`, `records`, `start/end_offset`, `batch_id` |
| `ReplicaAck` | follower → leader | `from`, `segment_key`, `batch_id` |
| `CommitAdvance` | leader → followers | `segment_key`, `commit_offset` |
| `SealResponse` | coordinator → leader | `old_segment_key`, `new_segment_id`, `new_replica_set` |

**DataPlaneEvent additions (D2):**

| Event | Emitted by | Purpose |
|---|---|---|
| `ReplicateSegmentBatch` | leader, during `flush_batch()` | carries `segment_key`, `followers`, `batch`, `batch_end_offset` |
| `ReplicaAckReady` | follower, after `process_replica_append()` | carries `leader`, `segment_key`, `batch_id` |
| `SendCommitAdvance` | leader, after `commit_segment()` | carries `segment_key`, `commit_offset`, `followers` |
| `SendSealRequest` | leader, on replication timeout | carries `segment_key`, `failed_node`, `end_offset` |
| `SendSegmentSealed` | leader, after seal completes | carries `segment_key`, `followers` |

### DataPlaneTimer

New variant: `ReplicationTimeout { segment_key, batch_id }`. Set when replication starts for a segment batch. Cancelled when all acks received. If it fires → seal the segment (emits `SendSealRequest`).

**Timeout tuning:** 500ms–1s. Conservative to avoid seal-on-transient-blip. A slow replica that consistently misses the timeout triggers seal — this is intentional (see roadmap "Performance Implications of ALL-Replica Ack").

---

## DataTransportActor

TCP transport for `data_port` (2923). Persistent bidirectional connections. Same architectural pattern as `RaftTransportActor` — `HashMap<NodeId, OwnedWriteHalf>`, lower-`NodeId`-wins conflict resolution, length-prefixed bincode frames.

```
DataTransportActor
├── listener: TcpListener              (data_port, 2923)
├── DataWriters
│   ├── writers: HashMap<NodeId, OwnedWriteHalf>
│   ├── addr_cache: HashMap<NodeId, NodeAddress>
│   └── dead_peers: HashSet<NodeId>
└── cleanup_interval: 300s
```

### Wire Protocol

Length-prefixed bincode frames (same as `RaftTransportActor`):
1. **Handshake**: `[len: u32][NodeId: bincode]`
2. **Messages**: `[len: u32][DataPortMessage: bincode]`

Size guards: NodeId frames capped at 1KB, message frames at 64MB (larger than Raft's 4MB — record batches can be up to 10MB per segment).

### Channel Integration

```
DataPlaneActor (dedicated OS thread)
    │
    │  data_transport_tx: tokio_mpsc::Sender<OutboundDataMessage>
    │  (blocking_send from dedicated thread, same pattern as scheduler_tx)
    ▼
DataTransportActor (tokio task)
    │  data_plane_tx: crossbeam::channel::Sender<DataPlaneCommand>
    │  (feeds inbound messages back to DataPlane mailbox)
    ▼
TCP connections to peers
```

`DataTransportActor` converts inbound wire messages to `DataPlaneCommand` variants and sends via `data_plane_tx` (the same crossbeam channel returned by `DataPlaneActor::spawn()`). Outbound messages arrive via `data_transport_tx` and are dispatched to the appropriate peer connection.

### Connection Lifecycle

Same as `RaftTransportActor`:
1. **Outbound**: On first message to peer, resolve `NodeId → SocketAddr` via SWIM `ResolveAddress`, connect, handshake, send, spawn reader task.
2. **Inbound**: Accept, read handshake `NodeId`, register writer, spawn reader task.
3. **Disconnect**: On `DisconnectPeer` (SWIM node death), remove writer + addr cache, add to `dead_peers`.
4. **Cleanup**: Every 300s, clear `dead_peers`.

### Separation from Raft Transport

| Property | `RaftTransportActor` | `DataTransportActor` |
|---|---|---|
| Port | `raft_port` (2922 TCP) | `data_port` (2923 TCP) |
| Traffic | Low throughput (heartbeats, log entries) | High throughput (record batches) |
| Frame limit | 4MB | 64MB |
| Backpressure | Must never stall (Raft liveness) | Can stall (produces backpressure to producers) |

---

## Data Port Message Catalog

All `data_port` wire messages, consolidated across phases:

| Message | Direction | Key fields | Phase |
|---|---|---|---|
| `ReplicaAppend` | Leader → followers | `segment_key`, `replica_set`, `records`, `start/end_offset`, `batch_id` | D2 |
| `ReplicaAck` | Follower → leader | `segment_key`, `batch_id` | D2 |
| `CommitAdvance` | Leader → followers | `segment_key`, `commit_offset` | D2 |
| `SealRequest` | Leader → coordinator | `shard_group_id`, `range_id`, `segment_id`, `failed_node`, `end_offset` | D2 |
| `SealResponse` | Coordinator → leader | `old_segment_key`, `new_segment_id`, `new_replica_set` | D2/D3 |
| `SegmentSealed` | Leader → old followers | `segment_key` | D2 |
| `SegmentAssignment` | Coordinator → leader | `segment_key`, `replica_set` | D3 |
| `CatchUpRequest` | Replacement → healthy replica | `segment_key`, `local_end_offset` | D2 |
| `CatchUpResponse` | Healthy replica → replacement | `segment_key`, `records`, `start/end_offset` | D2 |

---

## Failure Response

### Follower Failure (detected by segment leader)

When a follower fails to ack within timeout (`DataPlaneTimeoutCallback::ReplicationTimeout`):

1. Segment leader marks the segment as sealing — rejects new `Produce` commands for this segment with `ProduceAck::Err("segment sealing")`
2. Leader drains all `SegmentFlight.replies` for this segment across all in-flight flights with `ProduceAck::Err("replication failed")` — producers retry. Other segments' replies in the same flights are unaffected.
3. Leader emits `SendSealRequest { segment_key, failed_node, end_offset }` where `end_offset = tracker.committed_end_offset` (logical offset of last record ACKed by all replicas before the failure)
4. Actor sends `SealRequest` to coordinator via `DataTransportActor`
5. Coordinator proposes `RollSegment` via Raft with `new_replica_set` excluding failed node. Previous segment leader preserved at `replica_set[0]` (cache locality, active producer connections)
6. Raft commits → MetadataStateMachine seals old segment (sets `end_offset`), creates new segment (`start_offset = end_offset + 1`)
7. Coordinator sends `SealResponse` back via `DataTransportActor`
8. Leader receives `DataPlaneCommand::SealResponse` → creates new `SegmentTracker` (Leader, `new_replica_set`), resumes produce
9. Leader emits `SendSegmentSealed` → notifies old followers to close the segment

**Produce downtime:** ~100ms (one Raft round-trip for `RollSegment`). Producers whose records were in the failed flight retry against the new segment — they never received an ACK.

### Leader Failure (detected by SWIM)

Detected via SWIM node death (~6-7s). The coordinator (vnode leader) receives `HandleNodeDeath` and seals all affected active segments.

1. SWIM detects leader (D) as dead → coordinator receives `HandleNodeDeath(D)`
2. Coordinator proposes `RollSegment` via Raft with `new_replica_set` excluding D. A surviving follower is promoted to `replica_set[0]` (new segment leader)
3. Raft commits → MetadataStateMachine seals old segment, creates new segment
4. Coordinator sends `SegmentAssignment` to new segment leader via `data_port`
5. New leader opens segment tracker, ready for produce
6. Producer discovers new leader via metadata query (D6)

**RollSegment idempotency:** Both paths may fire for the same failure — write-path timeout and SWIM detection can race. `apply_roll_segment()` checks preconditions; if the segment is already sealed, subsequent proposals are no-ops (DS-RSM invariant 9).

### Offset Handoff: Data Plane → Metadata Plane

MetadataStateMachine does not see individual records. The `DataPlane` state machine is the authoritative offset tracker during normal operation. On seal, the offset is handed off via `SealRequest` — which carries `end_offset` (= `tracker.committed_end_offset`, the logical offset of the last record ACKed by all replicas). The coordinator carries `end_offset` through to the `RollSegment` Raft command. `apply_roll_segment()` uses it to seal the segment and derive the new segment's `start_offset = end_offset + 1`. This maintains MetadataStateMachine invariant 8 (offset continuity) while keeping per-record offset tracking in the data plane.

### Orphaned Records Past end_offset

The leader's WAL (and healthy followers' WALs) may contain records past `end_offset` — published to cache (between `commit_offset` and `tail`) and fsynced locally, but never committed because the failed follower never ACKed. These records are:

- **Not part of the sealed segment.** Consumers respect `end_offset` and never read past it.
- **Not replayed into the new segment.** The leader drops uncommitted batches from cache on seal.
- **Retried by producers.** Producers whose records were in the uncommitted window never received an ACK and retry against the new segment.
- **Cleaned up.** WAL rotation eventually deletes the files containing orphaned records. Checkpoint never processes them (they're above `commit_offset`, in the uncommitted zone).

No duplicate records arise from seal-on-failure — orphaned records are dropped, and producers retry once.

---

## Sealed Segment Repair

When a sealed segment is under-replicated (replica count < `replication_factor`), the coordinator assigns a replacement node, updates `replica_set` via Raft (`ReassignSegment`), and triggers repair:

1. Coordinator selects replacement node H (least-loaded, not already in `replica_set`)
2. Coordinator proposes `ReassignSegment` via Raft: `[D, E, F]` → `[D, E, H]`
3. After Raft commit, coordinator sends `CatchUpRequest` assignment to H via `data_port`
4. H checks local segment inventory (see "Local Data Reuse on Recovery" in roadmap). If H has data from a previous lifecycle, it advertises `local_end_offset`
5. Healthy replica (D or E) streams delta (offsets beyond `local_end_offset`) via `CatchUpResponse`
6. Once H has all data (verified by offset and CRC), repair is complete

**`CatchUpRequest` is the only catch-up mechanism.** In the seal-on-failure model, active segments never need catch-up — any replica failure seals the segment, and the new segment starts fresh with all replicas in sync from `start_offset`. Only sealed segments, which are immutable, need repair.

Nodes get new NodeIds on restart (UUID regenerated) — from the cluster's perspective, a recovered node is a new member. However, its local disk may still contain segment data from a previous lifecycle. On `CatchUpRequest`, the replacement node checks its local segment inventory and advertises `local_end_offset` — the healthy replica streams only the delta, or nothing if the data is already complete.

---

## Replication vs Consensus

| Property | Raft (metadata) | Segment replication (data) |
|---|---|---|
| Model | Consensus (leader election, log ordering) | Primary-backup (leader-driven, seal-on-failure) |
| Ack | Majority quorum | ALL replicas fsync |
| Failure handling | Re-election, log reconciliation | Seal segment, open new one. No reconciliation. |
| Ordering | Total order (log index) | Per-segment order (offset) |
| Transport | `raft_port` (TCP) | `data_port` (TCP) |
| Actor | `MultiRaftActor` (tokio) | `DataPlaneActor` (dedicated OS thread) |

---

## Example Scenario: Seal-on-Failure

```
Metadata group (Raft): vnodes [A, B, C]    ← manage topic "orders" metadata
Data replicas:         brokers [D, E, F]    ← store segment 7 of range 0

Topic "orders" created via:
  Client → ProposeRequest(CreateTopic) → vnode leader A
  A proposes via Raft, commits on [A, B, C]
  Coordinator assigns replica_set = [D, E, F] (based on storage policy)
  A sends SegmentAssignment to D via data_port     ← D creates SegmentTracker (Leader)
  D = replica_set[0] = segment leader
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

**Step 2: F fails (disk failure, crash, partition)**
```
Producer ──> Broker D
               │── WAL append + fsync
               │── publish to SegmentCache
               │── ReplicaAppend ──> E ✓ ack
               │── ReplicaAppend ──> F ✗ timeout (500ms–1s)
               │
               D: ReplicationTimeout fires
               D: fails pending_replies with Err("replication failed")
               D: emits SendSealRequest { end_offset = commit_offset }
```

**Step 3: D requests seal from coordinator (RPC via data_port)**
```
D ──(SealRequest)──> vnode A (coordinator for shard group)
                            │
                            A proposes RollSegment {
                              topic: "orders", range: 0,
                              old_segment: 7, end_offset,
                              new_replica_set: [D, E, G]
                            }
                            │
                            Raft commits on [A, B, C]
                            MetadataStateMachine: segment 7 = Sealed
                                                  segment 8 = Active
                            │
D <──(SealResponse)─────────│
      { new_segment_id: 8, new_replica_set: [D, E, G] }
```

**Step 4: D resumes, notifies participants via data_port**
```
Broker D: creates SegmentTracker for segment 8 (Leader, [D, E, G])
          resumes produce on segment 8 (~100ms total downtime)

D notifies via data_port:

Broker E: receives SegmentSealed from D. Closes segment 7 writes.
           Segment 7 data on E is complete (caught up before seal).

Broker G: receives first ReplicaAppend for segment 8 from D (self-authorizing).
           Validates replica_set, creates SegmentTracker (Follower), starts accepting.

Broker F: (if recovered, with new NodeId) is a new cluster member.
           NOT in segment 8's replica_set. Old data on disk from previous lifecycle.
```

**Step 5: Sealed segment repair (async)**
```
Coordinator detects: segment 7 under-replicated (F's copy incomplete)
Assigns replacement: broker H

H → CatchUpRequest to healthy replica E
E → CatchUpResponse: streams records from 0 to end_offset
H now has complete copy. Segment 7 fully replicated.
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

3. **Sealed segment `end_offset` = last committed logical offset.** On seal, `end_offset = tracker.committed_end_offset` — the logical offset of the last record ACKed by all replicas. Records past this (published to cache but uncommitted) are dropped. Same as D1 invariant 16.

4. **Single writer per segment.** Only `replica_set[0]` (the segment leader) accepts `Produce` commands and initiates `ReplicaAppend`. Followers reject `Produce` commands for segments where `role == Follower`.

5. **Follower `commit_offset` ≤ leader `commit_offset`.** Followers advance `commit_offset` on `CommitAdvance` from the leader, which arrives ~0.5 RTT after the leader commits. The follower's committed range is always ≤ the leader's.

6. **Replication is per-segment.** Each segment has its own `replica_set`. One WAL batch can produce batches for multiple segments; each segment's batch is replicated independently to that segment's followers. Within a `ReplicationFlight`, each `SegmentFlight` completes independently — commit and producer ACK happen per-segment, not per-flight. No cross-segment dependency.

7. **Self-authorization is sufficient for follower onboarding.** A follower creates a `SegmentTracker` on first `ReplicaAppend` if `self.node_id ∈ replica_set` and `sender == replica_set[0]`. No prior `SegmentAssignment` required.

8. **ReplicationTimeout triggers seal, not retry.** A slow or failed replica is treated the same — seal the segment, replace the replica. No retry, no ISR shrink/expand.

9. **Seal is leader-initiated for follower failures, coordinator-initiated for leader failures.** The segment leader detects follower failure (timeout) and sends `SealRequest`. The coordinator detects leader failure via SWIM `HandleNodeDeath` and proposes `RollSegment` directly.

10. **Orphaned records never committed.** Records between `commit_offset` and `tail` at seal time are dropped by the leader, not replayed into the new segment. Producers retry. No duplicates from the seal mechanism itself.

11. **DataTransportActor and RaftTransportActor are independent.** Separate ports, separate connections, separate actors. Data backpressure never stalls Raft heartbeats.

12. **Follower WAL reuses leader's WAL infrastructure.** Followers write to the shared per-node WAL with routing headers. One WAL fsync per `ReplicaAppend`. WAL replay on crash recovery (D5) handles both leader and follower records uniformly.

13. **`ReplicaAck` carries no data.** Only `(segment_key, batch_id)`. `batch_id` is an opaque correlation token (derived from the leader's WAL LSN, but not interpreted as a WAL position by the follower). The leader uses it to match acks to in-flight `ReplicationFlight`s.

14. **Segment leader preserved on follower failure.** `RollSegment` keeps the previous leader at `replica_set[0]` when only a follower failed — preserving cache locality and active producer connections. Only when the leader itself failed does a new node take position 0.