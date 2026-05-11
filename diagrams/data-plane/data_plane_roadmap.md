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
    → Cache (in-memory)                      ← serves reads immediately
    → Segment file(s) (checkpoint flush)     ← background, driven by checkpoint
    → Sparse index in RocksDB (update)       ← after segment file flush

Read path:
  Check cache (hot tail reads)
    → hit: serve from memory, zero disk I/O
    → miss: sparse index → seek segment file → scan forward (cold reads)
```

Producer latency is bounded by WAL fsync only — one sequential write to the active WAL file. After ACK, records enter the in-memory cache where they serve consumer reads immediately. Segment file writes happen in the background, driven by checkpoint logic. WAL is the sole disk I/O on the critical path — no contention between WAL and segment file writes.

**Post-ACK failure handling:** After WAL fsync + producer ACK, cache population and segment file writes must eventually complete. WAL is the source of truth; everything else is derived.

- **Node crash before segment file flush:** WAL replay rebuilds segment files and repopulates cache. Each WAL record carries its assigned offset. On replay, records with offset ≤ segment's `end_offset` are skipped (already written), records beyond are appended — making replay idempotent.
- **Segment file write fails at runtime** (disk full, I/O error): Retry from cache/WAL. If retries fail (persistent disk error), the disk is likely dead — WAL fsync also fails on the same disk → produce path fails → replication ack timeouts on peers → seal triggered via write-path detection (1a or 1b). No explicit "mark unhealthy" mechanism needed — the write-path failure detection handles it naturally. Consumer reads from this node fail; consumers retry from other replicas.
- **Index update fails:** Sparse index is rebuilt from segment files on demand. Non-critical.

**WAL (Write-Ahead Log):**
- Segmented log per node — a sequence of fixed-size files (~64MB each), rotated on size limit. ALL writes go to the single active WAL file first.
- Truly sequential writes — all records from all segments append to one active file. WAL is the sole disk write on the critical path. Segment file writes are background checkpoint flushes, never competing with WAL fsync for disk I/O on the produce path.
- fsynced before any other writes — the durability guarantee. Batching amortizes fsync cost: one WAL fsync per batch covers all segments in that batch. 10ms batch window → ~100 fsyncs/sec total, not per-segment.
- Old WAL files deleted (file-level `unlink`) once all their records are durably in segment files. Deletion gated by a **checkpoint position** watermark: a WAL file is only unlinked when ALL its records have been flushed to their segment files (segment's `end_offset` ≥ WAL file's last record offset). This makes the WAL→segment transition idempotent regardless of crash timing.
- On crash recovery: replay from oldest un-deleted WAL file to rebuild cache, segment files, and index entries. Replay is idempotent — records already in segment files are skipped by offset comparison.

**Segment files:**
- One file per segment. Written by background checkpoint from cache, not directly from WAL.
- Serve cold consumer reads (cache misses). Hot tail reads served from cache without touching segment files.
- O_DIRECT for both writes and reads. WAL uses standard buffered I/O + fsync (write-once-read-never in normal path — O_DIRECT alignment constraints add unnecessary complexity with no benefit). O_DIRECT for segment files because: (1) no double buffering — data exists in cache + disk, not also in page cache; (2) predictable fsync latency reflecting actual disk performance; (3) no cache pollution from cold consumer reads evicting hot data.

**Sparse index in RocksDB:**
- NOT every offset indexed. Sparse — e.g., one entry per N records or per batch.
- Maps message offset → file position within segment files. For efficient seeking, NOT for consumer offset tracking (consumer progress tracking is a separate concern — see Consumer Groups in backlog).
- Consumer seek: find nearest indexed offset ≤ target, then scan forward in the segment file.
- Reduces RocksDB write amplification and memory overhead vs full index.
- Separate RocksDB instance from metadata store.

**Application-level cache (write staging + read serving):**
- Serves dual purpose: (1) staging area for records between WAL and segment files, and (2) read-serving layer for consumer requests. After WAL fsync + producer ACK, records enter the cache immediately and are readable by consumers before the checkpoint flushes them to segment files.
- Cache is consume-stream-aware: tracks active consume connections and their read positions (learned from incoming consume requests, not from committed consumer offsets). Pre-populates cache ahead of active readers.
- Hot tail reads (consumer chasing the write head) served from cache — zero disk I/O for the most common consumer pattern.
- Cold reads (old sealed segments, already flushed and evicted from cache) go directly to segment files via O_DIRECT — no cache pollution.
- **Checkpoint:** Background process flushes cached records to segment files. Checkpoint frequency balances WAL retention (unflushed records keep WAL files alive) against disk I/O scheduling (larger, less frequent flushes are more efficient). After checkpoint flush + fsync, the corresponding WAL files become eligible for deletion and cache entries for those records become evictable.

### 2. Primary-Backup Replication with Seal-on-Failure

Follows Northguard's replication model. NOT Kafka ISR. NOT BookKeeper quorum.

**Ack model:** Producer connects only to the segment leader. The leader replicates to ALL followers internally, waits for fsync ack from ALL replicas, then ACKs the producer. The producer has no knowledge of replicas or replication — it sees one connection to one node.

**Failure handling:** When any replica fails, **seal the segment and open a new one** with a healthy replica set. No ISR shrink/expand. No ensemble changes. The sealed segment is immutable — its data never changes. The missing replica is restored asynchronously by copying data from a healthy replica to a replacement node.

**Why seal-on-failure instead of ISR:**

- **Simpler.** No ISR set tracking, no shrink/expand protocol, no high watermark management. The invariant is trivial: active segment has ALL replicas healthy, or it gets sealed.
- **Faster recovery.** Sealed segments are immutable — replication is just "consume protocol between brokers" (read and copy bytes). No divergent state to reconcile.
- **Matches metadata machinery.** `RollSegment` already seals current segment and creates a new one with a new `replica_set`. The mechanism exists.

### Performance Implications of ALL-Replica Ack

**In steady state (all replicas healthy), ALL-replica and ISR/majority perform identically.** All models send to all followers. The difference is only in what you wait for. The costs show during degradation and failures:

**1. Tail latency — slowest replica determines produce latency.**

With 3 replicas, produce latency = `max(fsync_A, fsync_B, fsync_C)`. With majority (2 of 3), it would be `median(fsync_A, fsync_B, fsync_C)` — insulated from the slowest. If each replica's P99 fsync is 15ms, the ALL-replica combined P99 is worse because you're taking the max of 3 random variables.

Seal-on-failure handles **chronic** slowness (slow replica gets replaced). But **transient** spikes — a single 50ms fsync on one replica — don't trigger a seal. They add 50ms to that batch's latency. Majority quorum absorbs these spikes silently.

**2. Producer stall during seal transition.**

Each seal causes ~100ms of producer blocking (Raft round-trip for `RollSegment`). Additionally:
- New replication TCP connections to replacement node if not already connected (handshake latency). Nodes likely already have `data_port` connections for other segments — overhead is only for the first segment between two nodes.
- Replacement node starts with cold cache for this segment — consumer tail reads see a cache miss at the transition point
- Un-acked records in the producer's pipeline must be retried against the new segment

Under flaky network conditions, frequent seals could cause repeated stalls. Mitigation: ack timeout tuned conservatively (500ms–1s) so transient blips don't trigger seals.

**3. Segment count growth under sustained failures.**

Each failure creates a new segment — more file handles, more offset index entries, more metadata entries in MetadataStateMachine. Under sustained instability (e.g., rolling restarts of a large cluster), segment count grows faster than with ISR/majority models. Mitigation: retention-based deletion continuously removes segments older than `retention_ms`. Total segment count bounded by `(write_rate × retention_period) / avg_segment_size + active_segments`. Extra segments from failures are typically short-lived (sealed quickly) and get GC'd first.

**4. Where ALL-replica wins on performance.**

Consumer reads are simpler and faster: ANY replica has ALL committed data. Followers track a `commit_offset` piggybacked on each `ReplicaAppend` from the leader — one extra `u64` per replication message. Followers serve reads only up to `commit_offset`, preventing the brief window where a follower has fsynced records that the leader hasn't confirmed committed. No ISR tracking, no watermark advancement protocol. Consumer can read from the nearest/fastest replica. (Follower reads are standard in modern systems: Kafka KIP-392, Pulsar bookie reads, Northguard.)

---

## Replication Failure Detection

**Coordinator** = the vnode leader (Raft group leader) for the shard group that owns the topic. Not a separate component — it's a role. Any vnode member can become coordinator by winning Raft election. The coordinator has authority to propose metadata changes (`CreateTopic`, `RollSegment`, `SplitRange`, etc.) via Raft.

Two detection paths catch different failure modes. Together, they cover leader and follower failures.

### Path 1: Write-Path Timeout (fast, per-segment)

**1a. Leader detects follower failure.**

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
                                    Meanwhile (async):
                                    data_port notifications → E, G learn
```

The segment leader (broker) cannot propose Raft commands — it's not in the vnode Raft group. It sends a `SealRequest` RPC to the coordinator, which proposes `RollSegment` on its behalf and replies with the new segment info after Raft commit. Same request-reply pattern as client `ProposeRequest`/`ProposeResponse`.

The initiator (D) learns the result via RPC response (~100ms total). Other brokers learn via `data_port`: E receives `SegmentSealed` from D, G receives self-authorizing `ReplicaAppend` for the new segment from D. When F recovers (with a new NodeId), it queries the coordinator to learn that segment 7 is sealed and it is not in segment 8's `replica_set`. F's partial copy of segment 7 is orphaned — eligible for local GC.

**Segment leader preference on roll:** The coordinator preserves the previous segment leader at `replica_set[0]` when possible. Only when the leader itself failed does a new node take position 0 — preserving cache locality and active producer connections.

**What this catches:**
- Follower crash (TCP connection drops)
- Follower disk failure (fsync hangs or fails)
- Follower slow disk (fsync exceeds timeout — treated as failure)
- Network partition between leader and follower

**Detection latency:** Sub-second (replication ack timeout).

**Key property:** A slow replica is as bad as a dead replica. If one node's fsync takes 500ms instead of 10ms, every produce to that segment is blocked waiting for it. Seal and move on — the new segment gets a healthy replica set.

**1b. Follower detects leader failure.**

Followers detect leader failure via **TCP connection drop** — the replication connection between leader and follower is persistent, so a leader crash or network partition causes an immediate TCP RST or timeout. ReplicaAppend is NOT periodic (only sent when there's produce traffic), so "no ReplicaAppend" alone is not a failure signal — it may just mean the segment is idle.

On TCP drop, the follower sends `SealRequest` to the coordinator — same RPC, same `RollSegment` proposal path. Any node in the `replica_set` can send `SealRequest`, not just the segment leader.

```
Segment follower (broker E)                  Coordinator (vnode leader A)
   |                                              |
   |  TCP connection to D dropped                 |
   |                                              |
   E ──(SealRequest RPC)─────────────────────────> A
   |    { shard_group_id, range_id, segment_id,   |
   |      failed_node: D }                        |
   |                                              A proposes RollSegment via Raft
   |                                              new_replica_set: [E, F, G]
   |                                              (D excluded, E promoted to leader)
   |                                              |
   E <──(SealResponse)────────────────────────────|
   |    { new_segment_id: 8,                      |
   |      new_replica_set: [E, F, G] }            |
   |                                              |
   E opens segment 8 as leader, accepts produce   |
   Producer discovers new leader via metadata     |
```

**SealRequest idempotency:** Multiple followers may detect the same leader failure and send concurrent SealRequests. This is safe — the coordinator may propose multiple `RollSegment` commands, but MetadataStateMachine's `apply_roll_segment()` checks preconditions: if the segment is already sealed, subsequent proposals are no-ops. Same guarantee as DS-RSM invariant 9 ("stale proposals are safe").

**What this catches:**
- Leader crash (TCP connection drops)
- Leader disk failure (leader can't fsync WAL, stops replicating, connection drops)
- Network partition between followers and leader

**Detection latency:** Sub-second (TCP connection drop detection).

This is the complement of 1a — together they cover both leader and follower failures at the write-path level, without waiting for SWIM. For the case where the leader hangs without TCP dropping (partial failure), SWIM eventually catches it (6-7s).

### Path 2: SWIM Node Death (slower, node-level)

SWIM detects node death after multiple probe rounds (~6-7 seconds worst case: probe_interval 1s + direct_ack_timeout 300ms + indirect_ack_timeout 600ms + suspect_timeout 5s). This catches segments that **weren't actively being written to** when the node died.

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

**Detection latency:** ~6-7 seconds worst case (SWIM protocol convergence).

### Coverage Matrix

| Failure Mode | Write-Path (1a: leader detects) | Write-Path (1b: follower detects) | SWIM | Notes |
|---|---|---|---|---|
| Follower crash | ✓ (sub-second) | — | ✓ (6-7s) | Leader detects via ReplicaAppend timeout |
| Follower disk failure | ✓ (sub-second) | — | ✗ | SWIM pings are UDP (network), not disk |
| Follower slow disk | ✓ (sub-second) | — | ✗ | Treated as failure — seal and move on |
| Leader crash | — | ✓ (sub-second) | ✓ (6-7s) | Follower detects via TCP connection drop |
| Leader disk failure | — | ✓ (sub-second) | ✗ | Leader connection drops, followers detect |
| Network partition | ✓ (sub-second) | ✓ (sub-second) | ✓ (6-7s) | Write-path catches first for active segments |
| Idle segment, node dead | ✗ | ✗ | ✓ (6-7s) | No produce traffic → write-path never triggers |
| Idle segment, disk dead | ✗ | ✗ | ✗ | Gap — caught on next produce to that segment |

**The gap: idle segment with dead disk.** Node alive (passes SWIM health checks), disk dead (can't fsync). No produce traffic, so write-path timeout never triggers. This is caught when the next produce arrives and the replica fails to ack. Acceptable tradeoff — adding a storage health heartbeat between replicas is a future optimization (backlog).

### Sealed Segment Repair

After sealing, the old segment may be under-replicated — e.g., segment 7 had `replica_set = [D, E, F]`, F died, now only D and E have copies. The desired state: `replication_factor` healthy nodes each holding a complete copy.

**Detection:** The coordinator knows immediately. When it commits `RollSegment` (which sealed the segment because F failed), the sealed segment's `replica_set` still lists F. The coordinator marks the sealed segment as under-replicated as part of the same apply — no separate detection step.

For SWIM-triggered seals (`HandleNodeDeath`): the coordinator scans MetadataStateMachine for sealed segments where the dead node is in `replica_set` (see Under-Replication Detection below).

**Repair flow:**
1. Coordinator selects a replacement node H (least-loaded, not already in `replica_set`)
2. Coordinator proposes a `ReassignSegment` command via Raft, updating the sealed segment's `replica_set` from `[D, E, F]` to `[D, E, H]`
3. After Raft commit, coordinator sends `CatchUpRequest` assignment to H via `data_port`
4. H sends `CatchUpRequest { segment_id, from_offset: 0, to_offset: end_offset }` to D or E
5. Healthy replica streams the full sealed segment to H — same consume read path, same append write path
6. Once H has all data (verified by offset), repair is complete

**Orphaned copies:** The failed node F, if it recovers (with a new NodeId), discovers via metadata query that it is no longer in the `replica_set`. F's local copy of segment 7 is orphaned and eligible for GC.

No special protocol. Sealed segments are immutable. Replication = read + write. Self-healing.

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
           Coordinator            Segment Leader
           (vnode leader           (replica_set[0])
            for shard group)       or Any Replica (consume)
                  |                     |
                  |<--- SealRequest ----|  (failure-triggered seal, via data_port)
                  |-- SegmentAssignment-->|  (new segment creation, via data_port)
                  |                     |
                  v                     v
           MetadataStateMachine   Storage Engine
           (Raft consensus,       (WAL + segment files + sparse index)
            RocksDB log store)
```

**Metadata path** — strong consensus via Raft. Total ordering. Low throughput (topic/range lifecycle events). Runs on vnode members (determined by hash ring). Already built.

**Data path** — primary-backup replication. Per-segment ordering. High throughput (producer payloads). Runs on `replica_set` members (assigned by coordinator based on storage policy and broker attributes). This roadmap.

**Data↔Metadata path** — segment lifecycle coordination. Segment leader or follower sends `SealRequest` to coordinator on failure detection. Coordinator sends `SegmentAssignment` to segment leader on segment creation/roll. All via `data_port` (TCP).

### How Data Nodes Learn About Metadata Decisions

All segment lifecycle communication goes through `data_port` — NOT SWIM gossip. SWIM stays coarse-grained (membership + shard leaders only). This avoids byte budget pressure from potentially thousands of segment events.

**1. Direct notification via `data_port` (initial assignment, seal result)**

When the coordinator creates or rolls a segment, it directly notifies the segment leader via TCP on `data_port`.

```
Vnode A commits CreateTopic → sends SegmentAssignment to broker D
   via TCP to D's data_port
   { shard_group_id, range_id, segment_id, replica_set: [D, E, F] }

D: creates segment file, ready to accept produce immediately
```

**2. Self-authorizing ReplicaAppend via `data_port` (follower bootstrap)**

Followers (E, F) learn about new segments from the leader's first `ReplicaAppend`, which carries `replica_set` for validation. No coordinator notification required for followers (see "Replica Authorization on ReplicaAppend" in Phase D3).

**3. Explicit `SegmentSealed` message via `data_port` (old follower cleanup)**

When a segment is sealed, the leader sends `SegmentSealed { segment_id }` to old followers so they close the segment file for writes and know to stop expecting replication.

**4. Metadata query (fallback)**

Any node that needs segment state but isn't a direct participant (e.g., routing a produce to the right segment leader, or a recovered node catching up) queries the vnode leader via `GetShardInfo` or `GetSegmentState`. One RPC, cacheable locally.

`DataTransportActor` handles all data plane message types:
- `SegmentAssignment` — coordinator → segment leader (segment creation/roll)
- `SegmentSealed` — segment leader → old followers (cleanup)
- `SealRequest/Response` — segment leader or follower → coordinator (failure-triggered seal)
- `ReplicaAppend` — segment leader → followers (replication)
- `CatchUpRequest` — replacement replica → healthy replica (sealed segment repair). Copies a sealed segment's data from a healthy replica to a replacement node assigned by the coordinator. In the seal-on-failure model, there is no "catch up" for active segments: any replica failure triggers seal-and-replace, and the new segment starts fresh with all replicas in sync from `start_offset`. Nodes get new NodeIds on restart, so a recovered node is a new node — it doesn't rejoin old replica sets.

Port layout:
- `client_port` (2921, TCP) — external clients only (produce, consume, query)
- `cluster_port` (2922, UDP+TCP) — SWIM gossip (membership + shard leaders) + Raft RPCs
- `data_port` (2923, TCP) — all data plane internal communication (control + replication)

### Under-Replication Detection

When a node dies, the coordinator must find all **active** segments that had that node in their `replica_set` (seal them) and all **sealed** segments (repair them).

**Reverse index in MetadataStateMachine (active segments only):**
```
node_active_segment_index: HashMap<NodeId, Vec<(TopicId, RangeId, SegmentId)>>
```

Tracks only active segments. Updated on segment creation, seal, and `replica_set` changes. On `HandleNodeDeath(F)`:
1. Look up F in the index → get all affected **active** segments in O(1)
2. Active segments → `RollSegment` (seal, replace F in replica_set)
3. Sealed segments → found by scanning `MetadataStateMachine` segment metadata, filtering by `replica_set.contains(F)`. Mark under-replicated, assign new broker, trigger repair via `CatchUpRequest`.

The scan for sealed segments iterates all topics → ranges → sealed segments. This is O(all sealed segments), but node death is infrequent (~6-7s SWIM detection) and the scan takes milliseconds — negligible relative to detection time. Keeping sealed segments out of the reverse index avoids unbounded index growth proportional to total segment count.

### Example Scenario: Seal-on-Failure with Separate Metadata and Data Nodes

See [Phase D3: Segment Replication](d3_segment_replication.md) for the full step-by-step walkthrough.

---

## Phase Summary

| Phase | Goal | Depends on | Details |
|---|---|---|---|
| [D1: Storage Engine](d1_storage_engine.md) | WAL, segment files, sparse index, app cache | Nothing | Local single-node storage |
| [D2: Produce/Consume API](d2_produce_consume_api.md) | Client produce/consume, DataActor, routing | D1 | End-to-end client path |
| [D3: Segment Replication](d3_segment_replication.md) | Primary-backup, seal-on-failure, DataTransportActor | D2 | Multi-node durability |
| [D4: Segment Roll Integration](d4_segment_roll_integration.md) | Size/time/failure seal triggers, lifecycle events | D3, metadata Phase 6 | Connect storage to metadata |
| [D5: Consumer Range Tracking](d5_consumer_range_tracking.md) | Follow split/merge/seal transitions | D4 | Consumer discovers range changes |
| [D6: Crash Recovery](d6_crash_recovery.md) | WAL replay, index rebuild, sealed segment repair | D1 (D3 for repair) | Data plane recovery |

### Phase Dependency Graph

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
| `MultiRaftActor` | Vnode-side only. Commits segment lifecycle changes. Not directly connected to DataActor — changes propagated via `data_port`. |
| `Topology` (hash ring) | Determines vnode membership (metadata placement). Segment `replica_set` assigned independently by coordinator based on storage policy. |
| `SwimActor` | Address resolution (`ResolveAddress`). Node death triggers segment seal. SWIM gossip stays coarse-grained (membership + shard leaders) — segment state propagated via `data_port`, not gossip. |
| `StoragePolicy` | `replication_factor` determines replica set size. Broker attributes + constraint expressions determine replica placement. `retention_ms` drives future GC. |
| `RocksDB` (metadata) | Untouched. Data plane uses separate RocksDB instance for sparse offset index. |

---

## Backlog (Out of Scope)

### Exactly-Once Semantics
Producer idempotency keys, deduplication at segment leader. Requires producer session tracking.

### Segment GC / Retention
Deleting sealed segments past `retention_ms`. Triggered by periodic ticker, cascades `Deleting` state through metadata.

### Consumer Groups
Multiple consumers sharing work across ranges. Offset commit tracking. Rebalancing on consumer join/leave. Consumer offset storage is a separate system concern (like Northguard/Xinfra separation) — not part of the log storage layer.

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
| Frequent segment seals under flaky network | Ack timeout tuned conservatively (e.g. 500ms–1s). Transient network blips don't trigger seal. SWIM convergence (~6-7s) handles true node death. Sealed segment count is observable — alert if excessive. |
| Application cache complexity | O_DIRECT bypasses page cache — application must manage its own read cache. Consume-stream-aware caching provides better eviction policy than OS page cache, but adds implementation complexity. |
| Sparse index RocksDB grows large | Index is sparse (one entry per batch). Segment deletion triggers range-delete in index. Rebuilt from segment files on crash — not authoritative. |
| Segment leader failover latency | For active segments: followers detect leader crash sub-second via TCP connection drop and trigger seal. For idle segments: SWIM detects node death in ~6-7s. Leader hang without TCP drop: SWIM catches it (6-7s). |
| Data transport competing with Raft transport | Separate ports, separate TCP connections, separate actors. Raft heartbeats (1s interval, small packets) unaffected by data throughput. |
