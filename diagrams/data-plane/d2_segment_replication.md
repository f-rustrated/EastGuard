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
╔═══════════════════════════════════════════════════════════╗
║ WAL: serialize all buffers with routing headers           ║
║      → flat interleaved write → fsync                     ║
║      assign LSN to each record                            ║
╚═══════════════════════════════╤═══════════════════════════╝
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
   for each segment_key:
     commit_offset.store(tail, Release)
     notify.notify_waiters()  ◄── consumers can read
                         │
                         ▼
   ACK producer (drain pending_replies with Ok)
                         │
                         ▼ (background)
   checkpoint worker       size_bytes → RollSegment at ~1GB
```

WAL fsync is the sole disk write on the leader's critical path, same as D1. The replication fan-out is network I/O handled by `DataTransportActor` (tokio). Produce latency = `local_fsync + max(follower_fsyncs)` — one local fsync plus one replication RTT in the common case.

**Optimization note:** `local_fsync` and replication fan-out could run in parallel — emit `ReplicateSegmentBatch` before fsync, letting `DataTransportActor` send while the dedicated thread blocks on fsync. Since `network_rtt + remote_fsync > local_fsync` in practice, this hides local fsync behind replication latency. Deferred — serial order is simpler and adds only ~1ms.

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
    ├── commit_offset.store(leader_commit_offset, Release)
    │   notify.notify_waiters()  ◄── follower consumers can read committed data
    │
    └── emit ReplicaAckReady { leader, segment_key, lsn }
            │
            ▼
      actor sends ReplicaAck to leader via DataTransportActor
```

Followers reuse WAL and `SegmentCache` infrastructure from D1. No accumulation buffers — records arrive pre-batched in `ReplicaAppend`. One WAL write + fsync per `ReplicaAppend`. Follower WAL enables uniform crash recovery (D5) and maintains durability guarantees even if SWIM's detection window leaves a follower briefly active after seal.

### Replica Self-Authorization

The first `ReplicaAppend` for an unknown segment is self-authorizing — no prior `SegmentAssignment` notification needed for followers:

```rust
ReplicaAppend {
    segment_key: SegmentKey,
    replica_set: [D, E, G],
    records, start_offset, end_offset, lsn,
    leader_commit_offset: u64,
}
```

Follower validates:
1. Am I in `replica_set`? → if not, reject
2. Is sender `replica_set[0]` (segment leader)? → if not, reject
3. Valid → create `SegmentTracker` with `SegmentRole::Follower`, create segment file, accept

Only the segment leader requires explicit `SegmentAssignment` from the coordinator (D3). Followers learn about new segments from the leader's first `ReplicaAppend` — no gossip or coordinator notification required on the follower data path.

### Commit Offset Piggybacking

Every `ReplicaAppend` carries `leader_commit_offset` — the leader's current `commit_offset` for this segment. Followers use this to advance their own `commit_offset`, enabling follower reads: consumers can read from any replica up to `commit_offset`.

The leader's `commit_offset` always reflects the *previous* committed position — the current batch being replicated is not yet committed. Once all followers ACK, the leader advances `commit_offset` and the next `ReplicaAppend` carries the new value.

---

## Component Changes

### SegmentTracker

```rust
struct SegmentTracker {
    cache: Arc<SegmentCache>,
    size_bytes: u64,
    checkpoint_lsn: u64,
    segment_file_path: PathBuf,
    role: SegmentRole,           // NEW
    replica_set: Vec<NodeId>,    // NEW: replica_set[0] = leader
}

enum SegmentRole {
    Leader,
    Follower,
}
```

**Publish changes:** D1's `SegmentTracker::publish()` auto-commits (`commit_offset = tail`) in one step. D2 splits into two operations:

```rust
impl SegmentTracker {
    // Publish batch to cache. Advances tail. Does NOT advance commit_offset.
    fn publish_uncommitted(&self, batch: SegmentRecordBatch) {
        self.cache.publish(Arc::new(batch));
    }

    // Advance commit_offset. Consumers can now read up to this position.
    fn commit(&self, offset: u64) {
        self.cache.commit(offset);
    }

    // Followers for this segment (replica_set[1..]).
    fn followers(&self) -> &[NodeId] {
        &self.replica_set[1..]
    }
}
```

Leader path: `publish_uncommitted()` during flush, `commit()` after all replicas ACK.
Follower path: `publish_uncommitted()` on `ReplicaAppend`, `commit(leader_commit_offset)` from message.

### DataPlane State Machine

**New field:**

```rust
struct DataPlane<W: WalStorage> {
    node_id: NodeId,    // NEW: needed for role determination and self-authorization
    wal: W,
    segments: HashMap<SegmentKey, SegmentTracker>,
    accumulation_buffers: HashMap<SegmentKey, Vec<BufferedRecord>>,
    pending_events: Vec<DataPlaneEvent>,
    buffer_record_count: usize,
    buffer_byte_count: usize,
    data_dir: PathBuf,
}
```

**Constructor change:**

```rust
pub fn new(node_id: NodeId, wal: W, data_dir: PathBuf) -> Self
```

**New methods:**

```rust
impl<W: WalStorage> DataPlane<W> {
    // D2: process incoming ReplicaAppend (follower path).
    // Validates sender, writes to WAL, publishes to cache, advances commit_offset,
    // emits ReplicaAckReady.
    fn process_replica_append(
        &mut self,
        from: NodeId,
        segment_key: SegmentKey,
        replica_set: Vec<NodeId>,
        records: Vec<Record>,
        start_offset: u64,
        end_offset: u64,
        lsn: u64,
        leader_commit_offset: u64,
    );

    // D2: advance commit_offset on a segment's cache after all replicas ACK.
    // Called by actor when a ReplicationFlight completes.
    fn commit_segment(&mut self, segment_key: SegmentKey, offset: u64);
}
```

**`flush_batch()` changes (D1 → D2):**

D1 calls `tracker.publish(batch)` which auto-commits. D2:

1. Calls `tracker.publish_uncommitted(batch)` — tail advances, `commit_offset` does not
2. Captures `tail` value after publish: `let commit_up_to = tracker.cache.load_tail()`
3. If `tracker.followers().is_empty()`, proceeds to `WalBatchComplete` as D1 (no replication needed)
4. If followers exist, emits `ReplicateSegmentBatch` per segment before `WalBatchComplete`

```
D1 flush_batch():
    ... WAL write + fsync ...
    tracker.publish(batch);              ← auto-commits
    emit WalBatchComplete { lsn }

D2 flush_batch():
    ... WAL write + fsync ...
    tracker.publish_uncommitted(batch);  ← tail advances, commit_offset stays
    let commit_up_to = tracker.cache.load_tail();
    emit ReplicateSegmentBatch { segment_key, followers, batch, commit_up_to }
    ...
    emit WalBatchComplete { lsn }
```

**`handle_segment_assignment()` changes:**

D1 ignores `replica_set`. D2 stores it and determines role:

```rust
fn handle_segment_assignment(&mut self, segment_key: SegmentKey, replica_set: Vec<NodeId>) {
    let role = if replica_set.first() == Some(&self.node_id) {
        SegmentRole::Leader
    } else {
        SegmentRole::Follower
    };
    let tracker = SegmentTracker::new(seg_path, role, replica_set);
    self.segments.insert(segment_key, tracker);
}
```

**`process()` dispatch additions:**

```rust
pub fn process(&mut self, cmd: DataPlaneCommand) {
    match cmd {
        // D1 variants (unchanged except SegmentAssignment uses replica_set)
        DataPlaneCommand::Produce { .. } => { .. },
        DataPlaneCommand::SegmentAssignment { segment_key, replica_set } => {
            self.handle_segment_assignment(segment_key, replica_set);
        },
        DataPlaneCommand::SealSegment { .. } => { .. },
        DataPlaneCommand::CheckpointComplete(..) => { .. },
        DataPlaneCommand::Timeout(..) => { .. },

        // D2 variants (new)
        DataPlaneCommand::ReplicaAppend { .. } => self.process_replica_append(..),
        DataPlaneCommand::SealResponse { .. } => self.handle_seal_response(..),
    }
}
```

**Produce rejection for followers:** If a `Produce` command targets a segment where this node is `SegmentRole::Follower`, reply with `ProduceAck::Err("not leader for segment")`. Stale routing — producer retries after metadata refresh.

### DataPlaneActor

**New state (held on the dedicated thread alongside `pending_replies`):**

```rust
// Buffered by ReplicateSegmentBatch events, consumed by WalBatchComplete
replication_targets: Vec<ReplicationTarget>,

// Per-WAL-batch tracking: replies waiting for replication to complete
replicating: VecDeque<ReplicationFlight>,

struct ReplicationTarget {
    segment_key: SegmentKey,
    followers: Vec<NodeId>,
    batch: Arc<SegmentRecordBatch>,
    commit_up_to: u64,             // tail value to commit to when done
}

struct ReplicationFlight {
    lsn: u64,
    replies: Vec<oneshot::Sender<ProduceAck>>,
    pending_acks: HashMap<SegmentKey, PendingSegmentAcks>,
}

struct PendingSegmentAcks {
    remaining: HashSet<NodeId>,
    commit_up_to: u64,
}
```

**`spawn()` signature change:**

```rust
pub fn spawn(
    node_id: NodeId,                    // NEW: passed to DataPlane::new()
    data_dir: PathBuf,
    checkpoint_tx: Sender<CheckpointJob>,
    scheduler_tx: tokio_mpsc::Sender<TickerCommand<DataPlaneTimer>>,
    data_transport_tx: tokio_mpsc::Sender<OutboundDataMessage>,  // NEW: data_port transport
) -> Sender<DataPlaneCommand>
```

**Changed event handling:**

```
DataPlaneEvent::ProducePending(reply) →
    push to pending_replies (no change from D1)

DataPlaneEvent::ReplicateSegmentBatch { .. } →  (NEW)
    buffer in replication_targets

DataPlaneEvent::WalBatchComplete { lsn } →
    if replication_targets.is_empty() {
        // No replicas (replication_factor=1), ACK immediately (D1 behavior)
        drain pending_replies, send Ok
    } else {
        // Build ReplicationFlight
        let flight = ReplicationFlight {
            lsn,
            replies: pending_replies.drain(..).collect(),
            pending_acks: build from replication_targets,
        };
        // Send ReplicaAppend to each follower
        for target in replication_targets.drain(..) {
            data_transport_tx.blocking_send(ReplicaAppend { .. });
        }
        // Set ReplicationTimeout timer per segment
        for segment in flight.pending_acks.keys() {
            scheduler_tx.blocking_send(SetSchedule { timer: ReplicationTimeout { .. } });
        }
        replicating.push_back(flight);
    }

DataPlaneEvent::ReplicaAckReady { leader, segment_key, lsn } →  (NEW)
    data_transport_tx.blocking_send(ReplicaAck { segment_key, lsn })

DataPlaneEvent::WalBatchFailed(e) →
    drain pending_replies with Err(e) (no change from D1)
```

**New command handling (ReplicaAck processing in actor):**

```
DataPlaneCommand::ReplicaAck { from, segment_key, lsn } →
    find flight in replicating where pending_acks contains (segment_key, from)
    remove from from pending_acks[segment_key].remaining
    cancel ReplicationTimeout timer for this segment if remaining empty
    if segment complete → DataPlane.commit_segment(segment_key, commit_up_to)
    if all segments in flight complete →
        drain flight.replies with ProduceAck::Ok
        remove flight from replicating
```

**D2 batch-reply mapping — multiple flights in flight simultaneously:**

Volume triggers can flush a batch inline (mid-produce), then more produces arrive before the first batch's replication completes. This produces multiple `ReplicationFlight`s in the `replicating` queue. Flights complete independently. Cumulative draining: when a flight completes, also drain any earlier flights that happen to be complete (ordered by insertion, drained front-to-back, but each checked independently).

```
Time 0: Produce batch 1 → LSN=1, flight 1 starts replication
Time 1: Produce batch 2 → LSN=2, flight 2 starts replication
Time 2: Flight 1 all acks → commit segments, drain flight 1 replies
Time 3: Flight 2 all acks → commit segments, drain flight 2 replies
```

### New Commands and Events

**DataPlaneCommand additions:**

```rust
pub enum DataPlaneCommand {
    // ... existing D1 variants ...

    // D2: follower receives from leader via DataTransportActor
    ReplicaAppend {
        from: NodeId,
        segment_key: SegmentKey,
        replica_set: Vec<NodeId>,
        records: Vec<Record>,
        start_offset: u64,
        end_offset: u64,
        lsn: u64,
        leader_commit_offset: u64,
    },

    // D2: leader receives from follower via DataTransportActor
    ReplicaAck {
        from: NodeId,
        segment_key: SegmentKey,
        lsn: u64,
    },

    // D2: leader receives from coordinator after SealRequest
    SealResponse {
        old_segment_key: SegmentKey,
        new_segment_id: SegmentId,
        new_replica_set: Vec<NodeId>,
    },
}
```

**DataPlaneEvent additions:**

```rust
pub(crate) enum DataPlaneEvent {
    // ... existing D1 variants ...

    // D2: leader emits per segment during flush_batch() when followers exist
    ReplicateSegmentBatch {
        segment_key: SegmentKey,
        followers: Vec<NodeId>,
        batch: Arc<SegmentRecordBatch>,
        commit_up_to: u64,              // tail value after publish
    },

    // D2: follower emits after processing ReplicaAppend
    ReplicaAckReady {
        leader: NodeId,
        segment_key: SegmentKey,
        lsn: u64,
    },

    // D2: leader emits on replication timeout (follower failed)
    SendSealRequest {
        segment_key: SegmentKey,
        failed_node: NodeId,
        end_offset: u64,
    },

    // D2: leader emits to notify old followers of seal
    SendSegmentSealed {
        segment_key: SegmentKey,
        followers: Vec<NodeId>,
    },
}
```

### DataPlaneTimer

New timeout variant for replication:

```rust
#[derive(Debug, Default)]
pub enum DataPlaneTimeoutCallback {
    #[default]
    PeriodicTick,
    ReplicationTimeout {          // NEW
        segment_key: SegmentKey,
        lsn: u64,
    },
}
```

Set when replication starts for a segment batch. Cancelled when all acks received for that segment. If it fires, the leader seals the segment (emits `SendSealRequest`).

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

```rust
enum DataPortMessage {
    // D2: Replication
    ReplicaAppend {
        segment_key: SegmentKey,
        replica_set: Vec<NodeId>,
        records: Vec<Record>,
        start_offset: u64,
        end_offset: u64,
        lsn: u64,
        leader_commit_offset: u64,
    },
    ReplicaAck {
        segment_key: SegmentKey,
        lsn: u64,
    },

    // D2/D3: Seal
    SealRequest {
        shard_group_id: ShardGroupId,
        range_id: RangeId,
        segment_id: SegmentId,
        failed_node: NodeId,
        end_offset: u64,
    },
    SealResponse {
        old_segment_key: SegmentKey,
        new_segment_id: SegmentId,
        new_replica_set: Vec<NodeId>,
    },
    SegmentSealed {
        segment_key: SegmentKey,
    },

    // D3: Segment lifecycle
    SegmentAssignment {
        segment_key: SegmentKey,
        replica_set: Vec<NodeId>,
    },

    // D2: Sealed segment repair
    CatchUpRequest {
        segment_key: SegmentKey,
        local_end_offset: u64,
    },
    CatchUpResponse {
        segment_key: SegmentKey,
        records: Vec<Record>,
        start_offset: u64,
        end_offset: u64,
    },
}
```

| Message | Direction | Purpose | Phase |
|---|---|---|---|
| `ReplicaAppend` | Segment leader → followers | Replicate batch + `commit_offset` | D2 |
| `ReplicaAck` | Follower → segment leader | Confirm WAL fsync for a batch | D2 |
| `SealRequest` | Segment leader → coordinator | Request segment seal (carries `end_offset`) | D2 |
| `SealResponse` | Coordinator → segment leader | New segment ID + replica set | D2/D3 |
| `SegmentSealed` | Segment leader → old followers | Notify seal, stop writes | D2 |
| `SegmentAssignment` | Coordinator → segment leader | Assign new/rolled segment | D3 |
| `CatchUpRequest` | Replacement → healthy replica | Request sealed segment data | D2 |
| `CatchUpResponse` | Healthy replica → replacement | Stream segment data (delta) | D2 |

---

## Failure Response

### Follower Failure (detected by segment leader)

When a follower fails to ack within timeout (`DataPlaneTimeoutCallback::ReplicationTimeout`):

1. Segment leader marks the segment as sealing — rejects new `Produce` commands for this segment with `ProduceAck::Err("segment sealing")`
2. Leader drains all `ReplicationFlight` replies associated with this segment across all in-flight flights with `ProduceAck::Err("replication failed")` — producers retry
3. Leader emits `SendSealRequest { segment_key, failed_node, end_offset }` where `end_offset = commit_offset` (last offset ACKed by all replicas before the failure)
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

MetadataStateMachine does not see individual records. The `DataPlane` state machine is the authoritative offset tracker during normal operation. On seal, the offset is handed off via `SealRequest`:

```
SealRequest {
    shard_group_id,
    range_id,
    segment_id,
    failed_node: NodeId,
    end_offset: u64,       ← commit_offset at seal time, from DataPlane
}
```

The coordinator carries `end_offset` through to the `RollSegment` Raft command. `apply_roll_segment()` uses it to seal the segment and derive the new segment's `start_offset = end_offset + 1`. This maintains MetadataStateMachine invariant 8 (offset continuity) while keeping per-record offset tracking in the data plane.

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

3. **Sealed segment `end_offset` = last committed offset.** On seal, `end_offset = commit_offset` at seal time. Batches between `commit_offset` and `tail` are uncommitted and dropped. Same as D1 invariant 16.

4. **Single writer per segment.** Only `replica_set[0]` (the segment leader) accepts `Produce` commands and initiates `ReplicaAppend`. Followers reject `Produce` commands for segments where `role == Follower`.

5. **Follower `commit_offset` ≤ leader `commit_offset`.** Followers advance `commit_offset` from `leader_commit_offset` in `ReplicaAppend`, which reflects the leader's *previous* committed position. The follower's committed range is always ≤ the leader's.

6. **Replication is per-segment.** Each segment has its own `replica_set`. One WAL batch can produce batches for multiple segments; each segment's batch is replicated independently to that segment's followers. A `ReplicationFlight` tracks all segments' acks for a single WAL LSN.

7. **Self-authorization is sufficient for follower onboarding.** A follower creates a `SegmentTracker` on first `ReplicaAppend` if `self.node_id ∈ replica_set` and `sender == replica_set[0]`. No prior `SegmentAssignment` required.

8. **ReplicationTimeout triggers seal, not retry.** A slow or failed replica is treated the same — seal the segment, replace the replica. No retry, no ISR shrink/expand.

9. **Seal is leader-initiated for follower failures, coordinator-initiated for leader failures.** The segment leader detects follower failure (timeout) and sends `SealRequest`. The coordinator detects leader failure via SWIM `HandleNodeDeath` and proposes `RollSegment` directly.

10. **Orphaned records never committed.** Records between `commit_offset` and `tail` at seal time are dropped by the leader, not replayed into the new segment. Producers retry. No duplicates from the seal mechanism itself.

11. **DataTransportActor and RaftTransportActor are independent.** Separate ports, separate connections, separate actors. Data backpressure never stalls Raft heartbeats.

12. **Follower WAL reuses leader's WAL infrastructure.** Followers write to the shared per-node WAL with routing headers. One WAL fsync per `ReplicaAppend`. WAL replay on crash recovery (D5) handles both leader and follower records uniformly.

13. **`ReplicaAck` carries no data.** Only `(segment_key, lsn)`. The leader uses `lsn` to correlate acks to in-flight `ReplicationFlight`s. The leader already has the data — follower confirmation is boolean.

14. **Segment leader preserved on follower failure.** `RollSegment` keeps the previous leader at `replica_set[0]` when only a follower failed — preserving cache locality and active producer connections. Only when the leader itself failed does a new node take position 0.
