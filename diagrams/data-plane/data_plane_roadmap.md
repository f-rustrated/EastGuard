# EastGuard Data Plane Roadmap

The metadata control plane is complete — SWIM membership, MultiRaft consensus, MetadataStateMachine, RocksDB persistence, client propose/query/forwarding, hot range detection. This roadmap covers the **data plane**: producing and consuming actual messages through the metadata-managed topic/range/segment hierarchy.

---

## Core Design Principles

### 1. Storage Engine: WAL + Segment Files + Sparse Index

Following Northguard's storage engine design. Three components per node:

```
Write path:
  Records batched (10ms / 20k records / 10MB)
    → WAL (sequential append, fsync)         ← durability point
    → ACK producer                           ← immediately after WAL fsync
    → Segment file(s) (append, fsync)        ← async, for serving reads
    → Sparse index in RocksDB (update)       ← async, for offset lookup

Read path:
  Offset → sparse index (nearest entry) → seek segment file → scan forward
```

Producer latency is bounded by WAL fsync only — one sequential write to one file. Segment file writes and index updates are off the critical path. If the node crashes before they complete, WAL replay recovers them.

**WAL (Write-Ahead Log):**
- Single append-only file per node (or per shard group). ALL writes go here first.
- Provides truly sequential writes at the disk level regardless of how many active segments exist on this node.
- fsynced before any other writes — the durability guarantee.
- On crash recovery: replay WAL to rebuild any segment file / index entries that were lost.

**Segment files:**
- One file per segment. Written after WAL. fsynced as part of the batch.
- Serve consumer reads — consumers read directly from segment files, not the WAL.
- O_DIRECT for both writes and reads — bypasses OS page cache to avoid double-buffering (WAL already has the data) and to prevent cold consumer reads from evicting hot pages.

**Sparse index in RocksDB:**
- NOT every offset indexed. Sparse — e.g., one entry per N records or per batch.
- Consumer seek: find nearest indexed offset ≤ target, then scan forward in the segment file.
- Reduces RocksDB write amplification and memory overhead vs full index.
- Separate RocksDB instance from metadata store.

**Application-level read cache:**
- Since O_DIRECT bypasses page cache, the application manages its own cache.
- Cache is consume-stream-aware: knows which consumers are active and pre-populates cache for their read positions.
- Hot tail reads (consumer chasing the write head) served from cache.
- Cold reads (old sealed segments) go directly to disk — no cache pollution.

### 2. Primary-Backup Replication with Seal-on-Failure

Follows Northguard's replication model. NOT Kafka ISR. NOT BookKeeper quorum.

**Ack model:** Producer connects only to the segment leader. The leader replicates to ALL followers internally, waits for fsync ack from ALL replicas, then ACKs the producer. The producer has no knowledge of replicas or replication — it sees one connection to one node.

**Failure handling:** When any replica fails, **seal the segment and open a new one** with a healthy replica set. No ISR shrink/expand. No ensemble changes. The sealed segment is immutable and gets repaired asynchronously.

**Why seal-on-failure instead of ISR:**

- **Simpler.** No ISR set tracking, no shrink/expand protocol, no high watermark management. The invariant is trivial: active segment has ALL replicas healthy, or it gets sealed.
- **Faster recovery.** Sealed segments are immutable — replication is just "consume protocol between brokers" (read and copy bytes). No divergent state to reconcile.
- **Matches metadata machinery.** `RollSegment` already seals current segment and creates a new one with a new `replica_set`. The mechanism exists.

### Why ALL-Replica, Not Majority or ISR

Three replication models compared:

| | ALL-replica + seal (Northguard/EastGuard) | Majority quorum (Raft-style) | ISR (Kafka) |
|---|---|---|---|
| Ack condition | ALL replicas fsync | Majority fsync | All ISR members ack |
| On replica failure | Seal segment, open new one with replacement node | Continue — majority still works | Remove from ISR, continue with smaller set |
| Replication factor after failure | **Restored immediately** (new node joins replica set) | Maintained (quorum still holds) | **Degraded** until node recovers |
| Segments created per failure | 1 (new segment) | 0 | 0 |
| Lagging followers | Not allowed — seal and replace | Allowed if not needed for majority | Allowed — removed from ISR |
| Complexity | Low (no ISR tracking, no quorum math) | Medium (quorum calculation) | High (ISR shrink/expand, high watermark) |
| Tail latency | Bounded by slowest replica, then seal | Bounded by majority | Bounded by ISR members |

**On failure — replica set is replaced, not shrunk:**

```
Kafka ISR:
  Segment [A, B, C]  →  C fails  →  ISR shrinks to [A, B]
                                     Same segment, fewer replicas
                                     Replication factor degraded until C recovers

EastGuard:
  Segment [A, B, C]  →  C fails  →  Seal segment
                                     New segment [A, B, D]   ← D is a healthy node
                                     Full replication factor restored immediately
```

Kafka ISR degrades the replication factor and hopes C comes back. EastGuard maintains it by picking a replacement from the topology. The old sealed segment with only [A, B] is repaired asynchronously — a new node copies the data in the background.

**The cost is more segments.** Each failure creates a new one. But segments are the natural unit of GC anyway (retention-based deletion), sealed segments are immutable and trivially copyable, and sealing is cheap (~100ms for one Raft round-trip). More segments is operationally visible but not structurally expensive.

### Performance Implications of ALL-Replica Ack

**In steady state (all replicas healthy), ALL-replica and ISR/majority perform identically.** All models send to all followers. The difference is only in what you wait for. The costs show during degradation and failures:

**1. Tail latency — slowest replica determines produce latency.**

With 3 replicas, produce latency = `max(fsync_A, fsync_B, fsync_C)`. With majority (2 of 3), it would be `median(fsync_A, fsync_B, fsync_C)` — insulated from the slowest. If each replica's P99 fsync is 15ms, the ALL-replica combined P99 is worse because you're taking the max of 3 random variables.

Seal-on-failure handles **chronic** slowness (slow replica gets replaced). But **transient** spikes — a single 50ms fsync on one replica — don't trigger a seal. They add 50ms to that batch's latency. Majority quorum absorbs these spikes silently.

**2. Producer stall during seal transition.**

Each seal causes ~100ms of producer blocking (Raft round-trip for `RollSegment`). Additionally:
- New replication TCP connections to replacement node (handshake latency)
- Replacement node starts with cold page cache for this segment — consumer tail reads see a cache miss at the transition point
- Un-acked records in the producer's pipeline must be retried against the new segment

Under flaky network conditions, frequent seals could cause repeated stalls. Mitigation: ack timeout tuned conservatively (500ms–1s) so transient blips don't trigger seals.

**3. Segment count growth under sustained failures.**

Each failure creates a new segment — more file handles, more offset index entries, more metadata entries in MetadataStateMachine. Under sustained instability (e.g., rolling restarts of a large cluster), segment count grows faster than with ISR/majority models. Mitigation: sealed segment GC (retention-based deletion) bounds the total.

**4. Where ALL-replica wins on performance.**

Consumer reads are simpler and faster: ANY replica has ALL committed data. No high watermark check, no risk of reading un-committed data from a lagging follower. Consumer can read from the nearest/fastest replica unconditionally.

### 3. Write Path: WAL-first, O_DIRECT, Batched fsync

Records are batched in memory (10ms / 20k records / 10MB, whichever threshold hit first). When a batch is ready:

1. **Write to WAL** (sequential append, O_DIRECT, fsync) — the durability point
2. **ACK producer** — immediately after WAL fsync on all replicas
3. **Append to segment file(s)** (O_DIRECT, fsync) — async, for serving consumer reads
4. **Update sparse index** in RocksDB — async, for offset lookup

Steps 3-4 are off the critical path. Producer latency = WAL fsync latency only. If the node crashes before segment files or index are written, WAL replay recovers them.

**Why WAL-first:**

- **Minimal producer latency.** One sequential write to one file. Segment file writes (potentially to multiple files) and RocksDB index updates don't block the producer.
- **True sequential writes.** The WAL is one file. All records from all active segments on this node go through it sequentially. No random I/O regardless of segment count.
- **Crash safety.** WAL is the source of truth. Segment files and index are derived and rebuildable.
- **Batching amortizes fsync cost.** One WAL fsync per batch covers all segments in that batch. 10ms batch window → ~100 fsyncs/sec total, not per-segment.

**Why O_DIRECT:**

- **No double buffering.** Without O_DIRECT, data exists in three places: application buffer, page cache, disk. With O_DIRECT, it's two: application buffer, disk. Halves memory overhead.
- **Predictable fsync latency.** O_DIRECT writes go directly to disk (or disk controller cache). fsync latency reflects actual disk performance, not page cache flush behavior.
- **No cache pollution from cold reads.** Consumers reading old sealed segments don't evict hot pages. Application-level cache handles hot reads explicitly.

---

## Replication Failure Detection

Two detection paths catch different failure modes. Together, they cover all cases.

### Path 1: Write-Path Timeout (fast, per-segment)

The segment leader sends `ReplicaAppend` to each follower. Every follower must ack (with fsync) within a configurable timeout. If any follower fails to ack:

```
Segment leader (broker D)                    Vnode leader (A)
   |                                              |
   |── ReplicaAppend ──> E  ✓ ack                 |
   |── ReplicaAppend ──> F  ✗ timeout             |
   |                                              |
   D detects: F failed for segment 7              |
   |                                              |
   D ──(SealRequest RPC)─────────────────────────> A
   |    { shard_group_id, range_id, segment_id,   |
   |      failed_node: F }                        |
   |                                              A proposes RollSegment via Raft
   |                                              Raft commits on [A, B, C]
   |                                              |
   D <──(SealResponse)────────────────────────────|
   |    { new_segment_id: 8,                      |
   |      new_replica_set: [D, E, G] }            |
   |                                              |
   D opens segment 8, resumes produce  (~100ms)   |
   Producer retries un-acked records              |
                                                  |
                                    Meanwhile (async, ~2-3s):
                                    SWIM gossip → E, G, F learn about seal
```

The segment leader (broker) cannot propose Raft commands — it's not in the vnode Raft group. It sends a `SealRequest` RPC to the vnode leader, which proposes `RollSegment` on its behalf and replies with the new segment info after Raft commit. Same request-reply pattern as client `ProposeRequest`/`ProposeResponse`.

The initiator (D) learns the result via RPC response (~100ms total). Other brokers (E, G, recovered F) learn via SWIM gossip (~2-3s). Gossip is the notification path for non-initiators, not for the node that requested the seal.

**What this catches:**
- Follower crash (TCP connection drops)
- Follower disk failure (fsync hangs or fails)
- Follower slow disk (fsync exceeds timeout — treated as failure)
- Network partition between leader and follower

**Detection latency:** Sub-second (replication ack timeout).

**Key property:** A slow replica is as bad as a dead replica. If one node's fsync takes 500ms instead of 10ms, every produce to that segment is blocked waiting for it. Seal and move on — the new segment gets a healthy replica set.

### Path 2: SWIM Node Death (slower, node-level)

SWIM detects node death after multiple probe rounds (~5-10 seconds). This catches segments that **weren't actively being written to** when the node died.

```
SWIM: Node X is Dead
SwimActor ──> MultiRaftActor: HandleNodeDeath(X)

For each shard group where this node is leader:
  For each active segment where X is in replica_set:
    Leader proposes RollSegment { new_replica_set: exclude X }
```

**What this catches:**
- Failed replica on an idle segment (no produce traffic → write-path detection never triggers)
- Node-level failure affecting multiple segments simultaneously (single SWIM event seals all affected segments)

**Detection latency:** ~5-10 seconds (SWIM protocol convergence).

### Coverage Matrix

| Failure Mode | Write-Path | SWIM | Notes |
|---|---|---|---|
| Follower crash | ✓ (sub-second) | ✓ (5-10s) | Write-path catches first if segment is active |
| Follower disk failure | ✓ (sub-second) | ✗ | SWIM pings are UDP (network), not disk. Only write-path catches disk failures. |
| Follower slow disk | ✓ (sub-second) | ✗ | Treated as failure — seal and move on |
| Network partition | ✓ (sub-second) | ✓ (5-10s) | Write-path catches first for active segments |
| Idle segment, node dead | ✗ | ✓ (5-10s) | No produce traffic → write-path never triggers |
| Idle segment, disk dead | ✗ | ✗ | Gap — caught on next produce to that segment |

**The gap: idle segment with dead disk.** Node alive (passes SWIM health checks), disk dead (can't fsync). No produce traffic, so write-path timeout never triggers. This is caught when the next produce arrives and the follower fails to ack. Acceptable tradeoff — adding a storage health heartbeat between replicas is a future optimization (backlog).

### Sealed Segment Repair

After sealing, the old segment may be under-replicated (missing the failed node's copy). The coordinator detects this and triggers **sealed segment replication** — literally the consume protocol between brokers:

```
Healthy Replica ── read segment records ──> New Replica (write to local segment file)
```

No special protocol. Sealed segments are immutable. Replication = read + write. Uses the same consume read path and the same append write path. Self-healing.

---

## Metadata and Data Placement Are Independent

Metadata nodes (vnode Raft group) and data nodes (segment `replica_set`) are **independent**. The nodes that run Raft consensus for a topic's metadata are not necessarily the same nodes that store the topic's segment data. Following Northguard's architecture.

```
                          Client
                         /      \
                        /        \
            metadata ops          data ops
            (CreateTopic,         (Produce, Consume)
             GetShardInfo)              |
                  |                     |
                  v                     v
           Any Node               replica_set[0] (produce)
           (vnode leader           or Any Replica (consume)
            for shard group)            |
                  |                     v
                  v               Storage Engine
           MetadataStateMachine   (WAL + segment files + sparse index)
           (Raft consensus,
            RocksDB log store)
```

**Metadata path** — strong consensus via Raft. Total ordering. Low throughput (topic/range lifecycle events). Runs on vnode members (determined by hash ring). Already built.

**Data path** — primary-backup replication. Per-segment ordering. High throughput (producer payloads). Runs on `replica_set` members (assigned by coordinator based on storage policy and broker attributes). This roadmap.

### How Data Nodes Learn About Metadata Decisions

Since data nodes are not in the Raft group, they don't apply Raft log entries directly. Metadata changes (segment sealed, new segment created, range split) are propagated to data nodes via **SWIM gossip** — the same mechanism already used for shard leader announcements (`ShardLeaderEvent`).

New gossip event types:
- `SegmentSealEvent { shard_group_id, range_id, segment_id }` — segment sealed, stop writing
- `SegmentActiveEvent { shard_group_id, range_id, segment_id, replica_set }` — new active segment, start accepting replication

All brokers maintain a local copy of gossipped segment state. Gossip convergence: ~2-3 seconds.

### Example Scenario: Seal-on-Failure with Separate Metadata and Data Nodes

```
Metadata group (Raft): vnodes [A, B, C]    ← manage topic "orders" metadata
Data replicas:         brokers [D, E, F]    ← store segment 7 of range 0

Topic "orders" created via:
  hash("orders") → shard group #45 → vnode members [A, B, C]
  Coordinator assigns replica_set = [D, E, F] (based on storage policy)
  D = replica_set[0] = segment leader
```

**Step 1: Normal produce flow**
```
Producer ──> Broker D (segment leader)
               |── WAL append + fsync
               |── ReplicaAppend ──> E (WAL fsync, ack) ✓
               |── ReplicaAppend ──> F (WAL fsync, ack) ✓
               |── ACK producer
               |── async: segment file + index
```

**Step 2: F fails (disk failure, crash, partition)**
```
Producer ──> Broker D
               |── WAL append + fsync
               |── ReplicaAppend ──> E ✓ ack
               |── ReplicaAppend ──> F ✗ timeout
               |
               D detects: F failed for segment 7
```

**Step 3: D requests seal from metadata coordinator (RPC)**
```
D ──(SealRequest RPC)──> vnode A (Raft leader for shard group #45)
                                |
                                A proposes RollSegment {
                                  topic: "orders",
                                  range: 0,
                                  old_segment: 7,
                                  new_replica_set: [D, E, G]  ← F replaced with G
                                }
                                |
                                Raft commits on [A, B, C]
                                MetadataStateMachine: segment 7 = Sealed
                                                      segment 8 = Active, replica_set = [D, E, G]
                                |
D <──(SealResponse)────────────|
      { new_segment_id: 8, new_replica_set: [D, E, G] }
```

Broker D learns the result immediately via RPC response (~100ms). No gossip wait.

**Step 4: D resumes, gossip propagates to other brokers**
```
Broker D: opens segment 8 file, resumes produce on segment 8 with [D, E, G].
          (~100ms total downtime)

Meanwhile, SWIM gossip propagates seal event (~2-3s):

Broker E: learns via gossip. Closes segment 7 for writes.
           Segment 7 data on E is complete (was caught up before seal).

Broker G: learns via gossip. Opens segment 8 file.
           Starts receiving ReplicaAppend from D.

Broker F: (if recovered) learns via gossip. Segment 7 is sealed.
           Closes segment 7. F is NOT in segment 8's replica_set.
           F's copy of segment 7 may be incomplete — repaired async.
```

**Step 5: Sealed segment repair (async)**
```
Coordinator detects: segment 7 under-replicated (F's copy incomplete)
Assigns new replica: broker H

Broker E ──(consume protocol)──> Broker H
  Read segment 7 records → write to H's segment 7 file
  H now has complete copy. Segment 7 fully replicated.
```

### Safety During Gossip Window

Between Raft commit (step 3) and gossip convergence (step 4), there's a ~2-3 second window where some brokers don't know the segment is sealed. This is safe:

- **Segment leader D** already stopped writing to segment 7 (D initiated the seal). No new data enters segment 7.
- **Follower E** might not know segment 7 is sealed for ~2-3s, but D stopped sending ReplicaAppend for segment 7. E has nothing to write.
- **Failed node F** is unreachable. When it recovers and learns via gossip, segment 7 is sealed. F never writes stale data.
- **No legitimate produce traffic** targets segment 7. Producers connect to segment leader of the **active** segment (segment 8 on D). 
- **Consumer reads** of segment 7 are fine — sealed segments are immutable and readable from any replica that has the data.

---

## Phase D1: Storage Engine

**Goal:** On-disk format for a single node — WAL, segment files, sparse index, application-level cache. No replication, no client API.

### Filesystem Layout

```
{data_dir}/
  wal.log                                # per-node WAL, sequential append, O_DIRECT
  {shard_group_id}/
    {range_id}/
      {segment_id}.seg                   # append-only, O_DIRECT, ≤1GB
```

WAL is per-node (single sequential write point). Segment files organized by ownership hierarchy. Removing a shard group = `rm -rf {data_dir}/{shard_group_id}/`.

### Record Format

```
[crc32: 4 bytes][type: 1 byte][length: 4 bytes][payload: N bytes][length: 4 bytes]
                                                                   ^ trailing length
```

Trailing length enables O(1) backward scan from EOF — read last 4 bytes, jump back `length + 9` (header size), verify CRC. Used in both WAL and segment files.

WAL records additionally carry `(shard_group_id, range_id, segment_id)` so replay knows which segment file to write to.

Record types: `Data` (user payload), `BatchEnd` (marks flush boundary).

### Sparse Offset Index

Separate RocksDB instance (not the metadata RocksDB). Sparse — not every offset indexed:

```
Key:   [shard_group_id: 8B][range_id: 8B][segment_id: 8B][offset: 8B]
Value: [byte_position: 8B]
```

One entry per batch (e.g., every ~1000 records or every batch boundary). Consumer seek: RocksDB `seek_for_prev(target_offset)` → nearest indexed offset ≤ target → `pread()` segment file at that position → scan forward to exact offset.

Sparse index trades one short sequential scan on read for dramatically fewer index writes. Rebuilt from segment files on crash — not a source of truth.

### Write Path (single node)

1. Accumulate records in application buffer until batch trigger (10ms / 20k records / 10MB)
2. Write batch to WAL (O_DIRECT, aligned), fsync WAL — durability point, ACK producer here
3. Append records to segment file(s) (O_DIRECT, aligned) — async, for serving reads
4. Update sparse index in RocksDB (batch boundary entries only) — async
5. Track `size_bytes` — when approaching 1GB, signal metadata layer to propose `RollSegment`

### Read Path

1. Check application-level cache for requested offset range
2. Cache miss: resolve `(range_id, segment_id, offset)` → nearest `byte_position` via sparse index (`seek_for_prev`)
3. `pread()` on segment file (O_DIRECT) at `byte_position`, scan forward to exact offset
4. Populate application cache with fetched records
5. Stream records forward until requested `max_bytes` or EOF

### Application-Level Read Cache

Since O_DIRECT bypasses the OS page cache, the application manages its own read cache:

- **Consume-stream-aware.** Tracks active consumer sessions and their read positions. Pre-fetches ahead of active consumers.
- **Hot tail reads served from cache.** Consumer chasing the write head reads data that was just written — served from the write buffer or cache, zero disk I/O.
- **Cold reads bypass cache.** Consumers reading old sealed segments go directly to disk. No eviction of hot data.

**Depends on:** Nothing.

---

## Phase D2: Produce/Consume Client API

**Goal:** End-to-end path from client produce/consume through to local segment files. Sessionized streaming protocol with pipelining and windowing.

### Produce Protocol

Sessionized and streaming (following Northguard's approach):

1. Producer opens TCP connection, sends handshake with stream ID
2. Broker responds with initial window size (flow control)
3. Producer sends `Append { stream_id, seq, records }` — multiple in-flight without waiting for ack (pipelining)
4. Broker acks committed records: `Ack { ack_seq, updated_window }` — M acks for N appends (batched)
5. Broker only acks records that have been fsync'd (and replicated, once D3 is implemented)

Windowing prevents producer from overwhelming the broker. Broker adjusts window based on backpressure from disk/replication.

### New Actor: DataActor

Follows the existing actor pattern — `DataSender` typed wrapper (like `RaftSender`, `SwimSender`), tokio task, mailbox-driven event loop.

### Produce Routing

1. `hash(topic_name)` → shard group (metadata ownership)
2. Query MetadataStateMachine: `topic_name_index` → `TopicMeta` → `active_ranges` → find range whose keyspace contains `key`
3. Range's `active_segment` → `SegmentMeta.replica_set` → segment leader
4. If this node is segment leader: write locally via `DataActor`
5. If not: forward to segment leader (1 hop max, same pattern as metadata propose forwarding)

### Consume

Client → any replica in segment's `replica_set` → read from segment file.

Response includes:
- Records (batch of payloads)
- `has_more: bool` (more records available at higher offsets)
- `sealed: bool` (segment/range is sealed — no more writes)
- If sealed: `end_offset`, `split_into` or `merged_into` lineage pointers

### Metadata Query for Routing

DataActor needs read access to MetadataStateMachine state (topic → range → segment mapping). Two options:

- **Option A:** DataActor queries MultiRaftActor via channel (request-reply, like existing `GetLeader`)
- **Option B:** MetadataStateMachine state replicated to DataActor via applied-entry events

Option A is simpler for Phase D2. Option B may be needed later for performance (avoid per-produce round-trip to Raft actor).

**Depends on:** Phase D1 (storage engine).

---

## Phase D3: Segment Replication

**Goal:** Primary-backup replication. fsync on ALL replicas before producer ACK. Seal-on-failure.

### Replication Protocol

```
Producer
   |
   v
Segment Leader
   |── batch records in memory
   |
   |── ┌── local WAL write + fsync ──────┐
   |── └── fan-out ReplicaAppend ─────────┘  in parallel
   |        (followers WAL fsync before ack)
   |
   |── wait for ALL: local fsync + all follower acks
   |── ACK to producer (records committed)
   |
   v (async, off critical path)
   |── append to segment file (O_DIRECT)
   |── update sparse index
```

Local WAL fsync and follower fan-out happen in parallel. Since `network_rtt + remote_fsync > local_fsync` in practice, the local fsync is hidden behind replication latency. Produce latency = `max(local_fsync, max(follower_fsyncs))`.

Followers write to their own WAL and fsync before acking. Their segment file writes and index updates are also async.

### Replica Authorization on ReplicaAppend

When a segment leader starts replicating a new segment (e.g., after seal-and-replace), followers may not have received the gossip yet. The first `ReplicaAppend` for a new segment carries the segment metadata — followers validate and act without waiting for gossip:

```
ReplicaAppend {
    shard_group_id,
    range_id,
    segment_id,
    replica_set: [D, E, G],    // from SealResponse / metadata
    records,
}
```

Follower validates:
1. Am I in `replica_set`? → if not, reject
2. Is sender `replica_set[0]` (segment leader)? → if not, reject
3. Valid → create segment file, write, ack

The replication message is self-authorizing — no gossip required for the data path. SWIM gossip confirms the state for nodes not in the replica_set and handles edge cases (e.g., the old segment is sealed, cleanup). This eliminates the ~2-3s gossip wait from the replication critical path.

No quorum. No ISR. ALL replicas must ack. If any fails → seal segment, open new one.

### Failure Response

When a follower fails to ack within timeout:

1. Segment leader (broker) stops accepting new produces for this segment
2. Segment leader sends `SealRequest` to vnode leader for this shard group
3. Vnode leader proposes `RollSegment` via Raft with `new_replica_set` excluding failed node
4. Raft commits → MetadataStateMachine seals old segment, creates new segment
5. SWIM gossip propagates seal event to all brokers
6. Segment leader opens new segment file with healthy replica set
7. Blocked producer streams resume against new segment
8. Sealed old segment queued for under-replication repair (async)

### Transport

`DataTransportActor` — TCP listener on `data_port`. Persistent bidirectional connections between nodes (same pattern as `RaftTransportActor` — `HashMap<NodeId, OwnedWriteHalf>`, lower-NodeId-wins conflict resolution, length-prefixed bincode frames).

Separate from Raft TCP transport (`raft_port`) because:
- Different traffic patterns (high throughput data vs low throughput consensus)
- Independent backpressure (slow data replication should not delay Raft heartbeats)
- Can be tuned independently (buffer sizes, TCP_NODELAY, SO_SNDBUF)

### Replication vs Consensus

| Property | Raft (metadata) | Segment replication (data) |
|---|---|---|
| Model | Consensus (leader election, log ordering) | Primary-backup (leader-driven, seal-on-failure) |
| Ack | Majority quorum | ALL replicas fsync |
| Failure handling | Raft re-election, log reconciliation | Seal segment, open new one. No reconciliation. |
| Ordering | Total order (log index) | Per-segment order (offset) |
| Transport | raft_port (TCP) | data_port (TCP) |

### Sealed Segment Replication (self-healing)

When a sealed segment is under-replicated (replica count < `replication_factor`), the coordinator assigns a new node and triggers repair. The repair protocol is the consume protocol between brokers — read from healthy replica, write to new replica. No special protocol needed.

**Depends on:** Phase D2 (produce API for end-to-end testing).

---

## Phase D4: Segment Roll Integration

**Goal:** Connect storage engine lifecycle to metadata lifecycle. Three triggers for segment seal.

### Seal Triggers

| Trigger | Source | Mechanism |
|---|---|---|
| Size limit (~1GB) | DataActor monitoring `size_bytes` | Segment leader sends `SealRequest` to vnode leader → vnode proposes `RollSegment` |
| Replica failure | Write-path timeout on `ReplicaAppend` | Segment leader sends `SealRequest` to vnode leader → vnode proposes `RollSegment` with updated `replica_set` |
| Time limit (1 hour) | DataActor monitoring segment age | Segment leader sends `SealRequest` to vnode leader → vnode proposes `RollSegment` |
| Node death | SWIM `NodeDead` event | Vnode leader proposes `RollSegment` for all affected active segments (vnode-initiated, no broker request needed) |

All triggers result in the same Raft command (`RollSegment`). MetadataStateMachine applies it identically regardless of trigger. First three are broker-initiated (via `SealRequest`). Last one is vnode-initiated (SWIM event processed directly by vnode leader).

### Gossip-Based Event Propagation

Metadata changes propagated to data nodes via SWIM gossip (not local channels — metadata and data may be on different nodes):

- `SegmentSealEvent { shard_group_id, range_id, segment_id }` — broker closes segment file for writes
- `SegmentActiveEvent { shard_group_id, range_id, segment_id, replica_set }` — broker in `replica_set` opens new segment file
- `RangeSplitEvent { topic_id, parent_range_id, child_range_ids }` — brokers open files for new child ranges
- `RangeMergeEvent { topic_id, source_range_ids, merged_range_id }` — brokers open file for merged range
- `TopicDeleteEvent { topic_id }` — brokers close all segment files, mark for GC

Piggybacked on existing SWIM dissemination buffer, same mechanism as `ShardLeaderEvent`.

### Segment Leader Determination

Segment leader = `replica_set[0]`. Deterministic from metadata — any node reading `SegmentMeta` knows the leader without election or extra state.

`replica_set` ordering is set at segment creation time by the Raft leader proposing `CreateTopic` or `RollSegment`. The Raft leader chooses `replica_set[0]` based on load distribution — it can spread data write leadership across nodes by varying which node is placed first. This is independent of Raft leadership: the Raft leader handles metadata consensus (small writes), while `replica_set[0]` handles data writes (high throughput, fan-out replication). Different concerns, potentially different nodes.

On seal-and-replace, the new segment gets a new `replica_set` with a potentially different `[0]` — the Raft leader can rebalance data write leadership at each segment transition.

Producer routing: client discovers the segment leader via `GetShardInfo` or produce-forwarding (same 1-hop pattern as metadata propose). Shard leader gossip (piggybacked on SWIM) can be extended to include segment leader info for faster routing without per-produce metadata queries.

**Depends on:** Phase D3 (replication), existing metadata Phase 6 (auto-split/merge).

---

## Phase D5: Consumer Range Tracking

**Goal:** Consumers follow range lifecycle transitions (split, merge, seal) without losing messages.

### Consumer Discovery Flow

```
Consumer                          EastGuard Node
   |                                     |
   |── ListActiveRanges(topic) ────────>|
   |<── [range_id=0, keyspace=full] ────|
   |                                     |
   |── Consume(topic, range=0, off=0) ─>|
   |<── records, has_more=true ─────────|
   |   ...                               |
   |── Consume(topic, range=0, off=N) ─>|
   |<── records=[], sealed=true,         |
   |    end_offset=M,                    |
   |    split_into=[range1, range2]  ────|
   |                                     |
   |── Consume(topic, range=1, off=0) ─>|  (left child)
   |── Consume(topic, range=2, off=0) ─>|  (right child)
```

### Response Enrichment

Consume response for sealed/split/merged ranges includes enough metadata for the consumer to navigate the transition without a separate metadata query:

- `sealed: true` + `end_offset` — consumer knows there are no more records
- `split_into: [RangeId, RangeId]` — consumer follows to child ranges
- `merged_into: RangeId` — consumer follows to merged range
- Child ranges start at offset 0 — clean break, no offset mapping

### Metadata Queries

- `ListActiveRanges { topic }` — returns current write-target ranges with keyspace boundaries
- `GetRangeInfo { topic, range_id }` — returns full range state, segment list, lineage, replica set

These query MetadataStateMachine state (read-only, no Raft proposal needed). Any node can serve them by querying its local Raft state machine (follower reads are stale but eventually consistent — acceptable for consumer discovery).

**Depends on:** Phase D4 (segment roll working, ranges actually transition in production).

---

## Phase D6: Crash Recovery

**Goal:** Recover data plane state after node crash. Distinct from Raft log recovery (which already works).

### Recovery Procedure

1. **Scan WAL forward from last checkpoint.** Find all committed batches (CRC-verified). For each batch, identify target `(shard_group_id, range_id, segment_id)`.
2. **Replay to segment files.** For each WAL entry, check if the corresponding segment file has the record. If not (crash happened between WAL fsync and segment file fsync), append it.
3. **Rebuild sparse index.** For any segment files with records not in the index, scan forward and rebuild index entries.
4. **Truncate partial writes.** WAL and segment files may have partial records at tail (crash mid-write). Backward scan using trailing length + CRC to find last valid record, truncate.
5. **Truncate WAL.** After replay is complete and all segment files are consistent, truncate WAL up to the last replayed position.
6. **Peer catch-up.** If this node's segment is behind the committed offset (was a follower that crashed), read missing records from leader. Same consume protocol.

### Properties

- WAL is the source of truth for crash recovery — segment files and index are derived
- Recovery is parallelizable per shard group (each group's segments are independent)
- Sparse index is treated as a cache — rebuilt from segment files, not authoritative
- `DataActor` startup runs recovery before accepting produce/consume commands

### Data Loss Semantics

With fsync-on-all-replicas before ACK:
- **ACKed records** are fsynced on ALL replicas. Survive any single-node crash. Survive any single-disk failure. Only total cluster failure loses data.
- **Un-ACKed records** were never confirmed to the producer. Producer retries. No data loss from producer's perspective.

No "un-fsynced but acked" window — this is the key difference from Kafka's deferred-fsync model.

**Depends on:** Phase D1 (storage engine format), Phase D3 (replication for peer catch-up).

---

## Phase Dependency Graph

```
D1 (Storage Engine)
 |
 ├──────────────────┐
 v                  v
D2 (Client API)   D6 (Crash Recovery)
 |                  (also needs D3 for peer catch-up)
 v
D3 (Replication)
 |
 v
D4 (Segment Roll Integration)
 |
 v
D5 (Consumer Range Tracking)
```

---

## Integration with Existing Components

| Existing Component | Data Plane Interaction |
|---|---|
| `MetadataStateMachine` | Coordinator (vnode leader) manages segment lifecycle. DataActor on broker requests `RollSegment` on size threshold or replica failure. |
| `MultiRaftActor` | Vnode-side only. Commits segment lifecycle changes. Not directly connected to DataActor — changes propagated via gossip. |
| `Topology` (hash ring) | Determines vnode membership (metadata placement). Segment `replica_set` assigned independently by coordinator based on storage policy. |
| `SwimActor` | Address resolution (`ResolveAddress`). Node death triggers segment seal. **Gossip propagates segment state changes** (`SegmentSealEvent`, `SegmentActiveEvent`) to all brokers. |
| `StoragePolicy` | `replication_factor` determines replica set size. Broker attributes + constraint expressions determine replica placement. `retention_ms` drives future GC. |
| `RocksDB` (metadata) | Untouched. Data plane uses separate RocksDB instance for sparse offset index. |

---

## Backlog (Out of Scope)

### Exactly-Once Semantics
Producer idempotency keys, deduplication at segment leader. Requires producer session tracking.

### Segment GC / Retention
Deleting sealed segments past `retention_ms`. Triggered by periodic ticker, cascades `Deleting` state through metadata.

### Consumer Groups
Multiple consumers sharing work across ranges. Offset commit tracking. Rebalancing on consumer join/leave.

### Batching / Compression
Batch multiple records into a single write. Compress batches (LZ4/Snappy). Reduces I/O and network.

### Zero-Copy Reads
`sendfile()` / `splice()` from segment file directly to TCP socket. Eliminates kernel-to-userspace copy on consume path.

### Dynamic Segment Leader Rebalancing
Actively rebalance `replica_set[0]` across nodes based on real-time load metrics rather than static assignment at segment creation. Requires load reporting + rebalancing heuristics.

### Tiered Storage
Cold segments offloaded to object storage (S3). Hot segments on local disk. Consumer transparently reads from either tier.

### Storage Health Heartbeat
Periodic fsync-and-ack between segment replicas. Catches disk failure on idle segments (the one gap in the current failure detection model — see coverage matrix above).

---

## Risk Areas

| Risk | Mitigation |
|---|---|
| WAL replay on crash | WAL is sequential and CRC-framed. Replay is fast (forward scan). Segment files rebuilt from WAL if needed. |
| ALL-replica ack latency (slowest replica determines produce latency) | Seal-on-failure handles chronic slowness. Batch fsync (10ms window) amortizes per-record overhead. Tail latency spikes from transient slowness are bounded by ack timeout. |
| Frequent segment seals under flaky network | Ack timeout tuned conservatively (e.g. 500ms–1s). Transient network blips don't trigger seal. SWIM convergence (~5-10s) handles true node death. Sealed segment count is observable — alert if excessive. |
| Application cache complexity | O_DIRECT bypasses page cache — application must manage its own read cache. Consume-stream-aware caching provides better eviction policy than OS page cache, but adds implementation complexity. |
| Sparse index RocksDB grows large | Index is sparse (one entry per batch). Segment deletion triggers range-delete in index. Rebuilt from segment files on crash — not authoritative. |
| Segment leader failover latency | SWIM detects node death in ~5-10s. Raft re-election in ~5s. New leader catches up from replicas. Total failover: ~10-15s. Acceptable for metadata system; may need optimization for latency-sensitive produce. |
| Data transport competing with Raft transport | Separate ports, separate TCP connections, separate actors. Raft heartbeats (1s interval, small packets) unaffected by data throughput. |
