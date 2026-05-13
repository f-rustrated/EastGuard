# EastGuard Data Plane Roadmap

The metadata control plane is complete — SWIM membership, MultiRaft consensus, MetadataStateMachine, RocksDB persistence, client propose/query/forwarding, hot range detection. This roadmap covers the **data plane**: producing and consuming actual messages through the metadata-managed topic/range/segment hierarchy.

---

## Core Design Principles

### 1. Storage Engine: WAL + Segment Files + Sparse Index + Per-Segment Cache

Following Northguard's storage engine design:

```
Write path:
  Records batched (10ms / 20k records / 10MB)
    → WAL (sequential append, fsync)         ← durability point
    → Per-segment cache (in-memory)          ← serves reads immediately
    → ACK producer                           ← after cache insert (+ all replica acks in D2+)
    → Segment file (checkpoint flush)        ← background, driven by checkpoint
    → Sparse index in RocksDB (update)       ← after segment file flush

Read path:
  Check per-segment cache (hot tail reads)
    → hit: serve from memory, zero disk I/O
    → miss: sparse index → seek segment file → scan forward (cold reads)
```

Producer latency is bounded by WAL fsync only — one sequential write to the active WAL file. Segment file writes happen in the background, driven by checkpoint logic. WAL is the sole disk I/O on the critical path. See [D1: Storage Engine](d1_storage_engine.md) for full specification (record format, WAL lifecycle, checkpoint triggers, cache policy, invariants).

**Key design choices:**

- **Shared WAL per node.** All segments append to one active WAL file (~64MB rotation). Batching amortizes fsync — one fsync per batch covers all segments in that batch.
- **O_DIRECT for segment file writes.** Predictable fsync latency, no double buffering with page cache. Reads use standard buffered I/O + `POSIX_FADV_DONTNEED` (kernel readahead benefits sequential reads; `FADV_DONTNEED` prevents page cache pollution). WAL uses buffered I/O (write-once-read-never).
- **Sparse offset index.** One RocksDB entry per batch, not per record. Consumer seeks to nearest indexed offset, scans forward. Rebuilt from segment files on crash — not authoritative.
- **Per-segment actors.** Each segment owns its cache, checkpoint, and read path. Avoids single-actor bottleneck across all segments on the node.
- **WAL is source of truth.** Segment files, sparse index, and cache are all derived. Crash recovery replays WAL; runtime failures retry from cache/WAL.

### 2. Primary-Backup Replication with Seal-on-Failure

Follows Northguard's replication model. NOT Kafka ISR. NOT BookKeeper quorum.

**Ack model:** Producer connects only to the segment leader. The leader replicates to ALL followers internally, waits for fsync ack from ALL replicas, then ACKs the producer. The producer has no knowledge of replicas or replication — it sees one connection to one node.

**Failure handling:** When any replica fails, **seal the segment and open a new one** with a healthy replica set. No ISR shrink/expand. No ensemble changes. The sealed segment is immutable — its data never changes. The missing replica is restored asynchronously by copying data from a healthy replica to a replacement node.

**Why seal-on-failure instead of ISR:**

- **Simpler.** No ISR set tracking, no shrink/expand protocol, no high watermark management. The invariant is trivial: active segment has ALL replicas healthy, or it gets sealed.
- **Faster recovery.** Sealed segments are immutable — replication is just "read and copy bytes". No divergent state to reconcile.
- **Matches metadata machinery.** `RollSegment` already seals current segment and creates a new one with a new `replica_set`.

### Performance Implications of ALL-Replica Ack

**In steady state (all replicas healthy), ALL-replica and ISR/majority perform identically.** All models send to all followers. The difference is only in what you wait for. The costs show during degradation and failures:

**1. Tail latency — slowest replica determines produce latency.** With 3 replicas, produce latency = `max(fsync_A, fsync_B, fsync_C)`. With majority (2 of 3), it would be `median(fsync_A, fsync_B, fsync_C)` — insulated from the slowest. Seal-on-failure handles **chronic** slowness (slow replica gets replaced). But **transient** spikes — a single 50ms fsync on one replica — don't trigger a seal. Majority quorum absorbs these spikes silently.

**2. Producer stall during seal transition.** Each seal causes ~100ms of producer blocking (Raft round-trip for `RollSegment`). Additionally: new replication TCP connections to replacement node if not already connected, replacement node starts with cold cache, un-acked records must be retried against the new segment. Mitigation: ack timeout tuned conservatively (500ms–1s) so transient blips don't trigger seals.

**3. Segment count growth under sustained failures.** Each failure creates a new segment — more file handles, more offset index entries, more metadata entries. Under sustained instability (e.g., rolling restarts), segment count grows faster than with ISR/majority. Mitigation: retention-based deletion continuously removes old segments. Extra segments from failures are typically short-lived and get GC'd first.

**4. Where ALL-replica wins on performance.** Consumer reads are simpler and faster: ANY replica has ALL committed data. Followers track a `commit_offset` piggybacked on each `ReplicaAppend` — one extra `u64` per replication message. Followers serve reads only up to `commit_offset`, preventing the brief window where a follower has fsynced records that the leader hasn't confirmed committed. No ISR tracking, no watermark advancement protocol. Consumer can read from the nearest/fastest replica.

---

## Replication Failure Detection

**Coordinator** = the vnode leader (Raft group leader) for the shard group that owns the topic. Not a separate component — it's a role. Any vnode member can become coordinator by winning Raft election. The coordinator has authority to propose metadata changes (`CreateTopic`, `RollSegment`, `SplitRange`, etc.) via Raft.

Two detection paths catch different failure modes:

### Path 1: Write-Path Timeout (fast, per-segment)

The segment leader sends `ReplicaAppend` to each follower. Every follower must ack (with fsync) within a configurable timeout. If any follower fails to ack:

```
Segment leader (broker D)                    Coordinator (vnode leader A)
   |                                              |
   |── ReplicaAppend ──> E  ✓ ack                 |
   |── ReplicaAppend ──> F  ✗ timeout             |
   |                                              |
   D ──(SealRequest RPC)─────────────────────────> A
   |    { shard_group_id, range_id, segment_id,   |
   |      failed_node: F, end_offset }            |
   |                                              A proposes RollSegment via Raft
   |                                              Raft commits on [A, B, C]
   |                                              |
   D <──(SealResponse)────────────────────────────|
   |    { new_segment_id: 8,                      |
   |      new_replica_set: [D, E, G] }            |
   |                                              |
   D opens segment 8, resumes produce  (~100ms)   |
```

The segment leader (broker) cannot propose Raft commands — it's not in the vnode Raft group. It sends a `SealRequest` RPC to the coordinator, which proposes `RollSegment` on its behalf and replies with the new segment info after Raft commit.

**Segment leader preference on roll:** The coordinator preserves the previous segment leader at `replica_set[0]` when possible — preserving cache locality and active producer connections. Only when the leader itself failed does a new node take position 0.

**What this catches:** Follower crash, follower disk failure, follower slow disk, network partition between leader and follower. Detection latency: sub-second.

**Key property:** A slow replica is as bad as a dead replica. If one node's fsync takes 500ms instead of 10ms, every produce to that segment is blocked waiting for it. Seal and move on — the new segment gets a healthy replica set.

### Path 2: SWIM Node Death (slower, node-level)

SWIM detects node death after multiple probe rounds (~6-7 seconds worst case: probe_interval 1s + direct_ack_timeout 300ms + indirect_ack_timeout 600ms + suspect_timeout 5s). This is the sole detection mechanism for leader failures and idle segment failures.

```
SWIM: Node X is Dead
SwimActor ──> MultiRaftActor: HandleNodeDeath(X)

For each shard group where this node is coordinator:
  For each active segment where X is in replica_set:
    Coordinator proposes RollSegment { new_replica_set: exclude X }
```

**What this catches:**
- Leader crash (active or idle segments — coordinator seals all affected segments)
- Failed replica on an idle segment (no produce traffic → write-path detection never triggers)
- Node-level failure affecting multiple segments simultaneously (single SWIM event seals all)

**Detection latency:** ~6-7 seconds worst case (SWIM protocol convergence).

For active segments with produce traffic, write-path timeout (Path 1) catches follower failures faster (~sub-second). SWIM serves as the catch-all for anything the write-path misses.

**RollSegment idempotency:** Both paths may fire for the same failure — write-path timeout and SWIM detection can race. This is safe: `apply_roll_segment()` checks preconditions, and if the segment is already sealed, subsequent proposals are no-ops (DS-RSM invariant 9).

### Coverage Matrix

| Failure Mode | Write-Path (leader detects follower) | SWIM | Notes |
|---|---|---|---|
| Follower crash | ✓ (sub-second) | ✓ (6-7s) | Write-path catches first for active segments |
| Follower disk failure | ✓ (sub-second) | ✗ | SWIM pings are UDP (network), not disk |
| Follower slow disk | ✓ (sub-second) | ✗ | Treated as failure — seal and move on |
| Leader crash | ✗ | ✓ (6-7s) | Coordinator seals all affected segments on node death |
| Leader disk failure | ✗ | ✗ | Caught on next produce (WAL fsync fails → timeout) |
| Network partition | ✓ (sub-second) | ✓ (6-7s) | Write-path catches first for active segments |
| Idle segment, node dead | ✗ | ✓ (6-7s) | No produce traffic → write-path never triggers |
| Idle segment, disk dead | ✗ | ✗ | Gap — caught on next produce to that segment |

**The gap: idle segment with dead disk.** Node alive (passes SWIM health checks), disk dead (can't fsync). No produce traffic. Caught when the next produce arrives. Acceptable tradeoff — a storage health heartbeat is a future optimization (backlog).

### Sealed Segment Repair

After sealing, the old segment may be under-replicated — e.g., segment 7 had `replica_set = [D, E, F]`, F died, now only D and E have copies. The coordinator knows immediately: when it commits `RollSegment`, the sealed segment's `replica_set` still lists F.

**Repair flow:**
1. Coordinator selects a replacement node H (least-loaded, not already in `replica_set`)
2. Coordinator proposes `ReassignSegment` via Raft, updating sealed segment's `replica_set` from `[D, E, F]` to `[D, E, H]`
3. After Raft commit, coordinator sends `CatchUpRequest` assignment to H via `data_port`
4. H checks its local segment inventory (see "Local Data Reuse on Recovery" below). If H already has data for this segment from a previous node lifecycle, it advertises its `local_end_offset` in the `CatchUpRequest`
5. Healthy replica streams only the delta (offsets beyond `local_end_offset`) to H — or nothing if H already has the complete segment
6. Once H has all data (verified by offset), repair is complete

**Under-replication detection on node death (SWIM):**

Reverse index in MetadataStateMachine (active segments only):
```
node_active_segment_index: HashMap<NodeId, Vec<(TopicId, RangeId, SegmentId)>>
```

On `HandleNodeDeath(F)`: look up F in the index → active segments get `RollSegment` (seal, replace F). Sealed segments found by scanning MetadataStateMachine segment metadata, filtering by `replica_set.contains(F)` — O(all sealed segments), but node death is infrequent and the scan takes milliseconds. Mark under-replicated, assign new broker, trigger repair.

### Local Data Reuse on Recovery

Nodes get new NodeIds on restart (UUID regenerated). From the cluster's perspective, a recovered node is a new member — no zombie confusion, no stale identity problems. However, its local disk still contains valid WAL files, segment files, and sparse index from its previous lifecycle.

On startup, the node scans its data directory and builds a **local segment inventory**: `(shard_group_id, range_id, segment_id) → local_end_offset`. This inventory is not advertised to the cluster proactively — the node is just a new member.

When the coordinator assigns this node to a sealed segment's `replica_set` (as a replacement or via least-loaded placement), and the node receives a `CatchUpRequest`, it checks its local inventory:

- **Full match:** Node already has complete data for the segment (`local_end_offset ≥ segment.end_offset`). Verifies integrity (CRC check) and reports completion. Zero network transfer.
- **Partial match:** Node has data up to some offset. Requests only the delta from the healthy replica.
- **No match:** Full copy from healthy replica (the default path).

This gives the performance benefit of local data reuse without the complexity of stable NodeIds (no zombie detection, no stale identity resolution). The node doesn't need to know it was previously "node F" — it just has files on disk that happen to match.

**Orphaned data cleanup:** After local WAL replay completes, the node queries the coordinator for segment assignments relevant to its local data. Any local segment data not in an active or sealed `replica_set` that includes this node is orphaned — eligible for background GC.

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
             Coordinator  <======> DataPlaneActor
             (vnode leader          (node-level, single entry point
              for shard group)       for all data plane traffic)
                    |                     |
                    |            ┌────────┼────────┐
                    |            v        |        v
                    |      WAL (shared)   |   SegmentActor(s)
                    |      batch + fsync  |   (per-segment tasks)
                    |                     |   ┌────┴────┐
                    |                routing   v         v
                    |              + lifecycle cache  segment files
                    |                       + replication + checkpoint
                    v                       + read serving + sparse index
             MetadataStateMachine
             (Raft consensus,
              RocksDB log store)

  Coordinator ↔ DataPlaneActor (via data_port):
    SealRequest        — DataPlaneActor → Coordinator (on replica failure)
    SealResponse       — Coordinator → DataPlaneActor (new segment info)
    SegmentAssignment  — Coordinator → DataPlaneActor (spawn SegmentActor)

  Produce write path:  Client → DataPlaneActor (batch → WAL fsync)
                                    → SegmentActor (cache + replication + ACK)
  Consume read path:   Client → DataPlaneActor (route)
                                    → SegmentActor (cache hit or segment file)
```

**Metadata path** — strong consensus via Raft. Total ordering. Low throughput (topic/range lifecycle events). Already built.

**Data path** — primary-backup replication. Per-segment ordering. High throughput (producer payloads). This roadmap.

**Data↔Metadata path** — segment lifecycle coordination via `data_port` (TCP). Segment leader sends `SealRequest` to coordinator on failure detection. Coordinator sends `SegmentAssignment` to segment leader on segment creation/roll. See Phase D3 for full event propagation details.

Port layout:
- `client_port` (2921, TCP) — external clients only (produce, consume, query)
- `cluster_port` (2922, UDP+TCP) — SWIM gossip (membership + shard leaders) + Raft RPCs
- `data_port` (2923, TCP) — all data plane internal communication (control + replication)

---

## Phase Summary

| Phase | Goal | Depends on | Details |
|---|---|---|---|
| [D1: Storage Engine](d1_storage_engine.md) | WAL, segment files, sparse index, DataPlaneActor, SegmentActor | Nothing | Single-node storage + actor architecture |
| [D2: Segment Replication](d2_segment_replication.md) | Primary-backup, seal-on-failure, DataTransportActor | D1 | Multi-node durability |
| [D3: Segment Roll Integration](d3_segment_roll_integration.md) | Size/time/failure seal triggers, lifecycle events | D2, metadata Phase 6 | Connect storage to metadata |
| [D4: Consumer Range Tracking](d4_consumer_range_tracking.md) | Follow split/merge/seal transitions | D3 | Consumer discovers range changes |
| [D5: Crash Recovery](d5_crash_recovery.md) | WAL replay, local inventory, sealed segment repair | D1 (D2 for repair) | Data plane recovery |
| [D6: Produce/Consume API](d6_produce_consume_api.md) | Client produce/consume protocol, routing, connection management | D4, D5 | Client-facing protocol layer |

D1 defines both the storage primitives (WAL, segment files, sparse index, cache) and the actor architecture that drives them (DataPlaneActor + SegmentActor). The storage state machines follow the same sync-first pattern as `Swim`, `Raft`, and `MetadataStateMachine` — managing cache, offset tracking, and checkpoint decisions synchronously, producing I/O commands as buffered side effects. The actors own async boundaries (channels, timers, I/O) and drain state machine outputs. D2–D5 extend the D1 foundation with replication, metadata integration, consumer tracking, and crash recovery. D6 adds the client-facing protocol layer (produce/consume wire format, connection management, routing) on top of the actors defined in D1.

### Phase Dependency Graph

```
D1 (Storage Engine)
 |
 ├──────────────────┐
 v                  v
D2 (Replication)  D5 (Crash Recovery)
 |                  |
 v                  |
D3 (Segment Roll)   |
 |                  |
 v                  |
D4 (Range Tracking) |
 |                  |
 ├──────────────────┘
 v
D6 (Produce/Consume API)
```

---

## Integration with Existing Components

| Existing Component | Data Plane Interaction |
|---|---|
| `MetadataStateMachine` | Coordinator (vnode leader) manages segment lifecycle. SegmentActor on broker requests `RollSegment` (via DataPlaneActor) on size threshold or replica failure. |
| `MultiRaftActor` | Vnode-side only. Commits segment lifecycle changes. Not directly connected to DataPlaneActor — changes propagated via `data_port`. |
| `Topology` (hash ring) | Determines vnode membership (metadata placement). Segment `replica_set` assigned independently by coordinator based on storage policy. |
| `SwimActor` | Address resolution (`ResolveAddress`). Node death triggers segment seal (leader and idle segment failures detected here). SWIM gossip stays coarse-grained (membership + shard leaders) — segment state propagated via `data_port`, not gossip. |
| `StoragePolicy` | `replication_factor` determines replica set size. Broker attributes + constraint expressions determine replica placement. `retention_ms` drives future GC. |
| `RocksDB` (metadata) | Untouched. Data plane uses separate RocksDB instance for sparse offset index. |

---

## Backlog (Out of Scope)

### Exactly-Once Semantics
Producer idempotency keys, deduplication at segment leader. Requires producer session tracking.

### Segment GC / Retention
Deleting sealed segments past `retention_ms`. Triggered by periodic ticker, cascades `Deleting` state through metadata.

### Consumer Groups
Multiple consumers sharing work across ranges. Offset commit tracking. Rebalancing on consumer join/leave. Consumer offset storage is a separate system concern — not part of the log storage layer.

### Batching / Compression
Batch multiple records into a single write. Compress batches (LZ4/Snappy). Reduces I/O and network.

### Zero-Copy Reads
`sendfile()` / `splice()` from segment file directly to TCP socket. Eliminates kernel-to-userspace copy on consume path.

### Dynamic Segment Leader Rebalancing
Actively rebalance `replica_set[0]` across nodes based on real-time load metrics rather than static assignment at segment creation.

### Tiered Storage
Cold segments offloaded to object storage (S3). Hot segments on local disk. Consumer transparently reads from either tier.

### Storage Health Heartbeat
Periodic fsync-and-ack between segment replicas. Catches disk failure on idle segments (the one gap in the current failure detection model — see coverage matrix above).

---

## Risk Areas

| Risk | Mitigation |
|---|---|
| WAL replay on crash | WAL is sequential and CRC-framed. Replay is fast (forward scan). Segment files rebuilt from WAL if needed. |
| ALL-replica ack latency | Seal-on-failure handles chronic slowness. Batch fsync (10ms window) amortizes per-record overhead. Tail latency spikes bounded by ack timeout. |
| Frequent segment seals under flaky network | Ack timeout tuned conservatively (500ms–1s). SWIM convergence (~6-7s) handles true node death. Sealed segment count is observable — alert if excessive. |
| Per-segment actor complexity | O_DIRECT bypasses page cache — each segment actor manages its own cache and read buffers. Write staging + read serving per-segment keeps contention local but adds per-segment memory overhead. |
| Sparse index RocksDB grows large | Index is sparse (one entry per batch). Segment deletion triggers range-delete in index. Rebuilt from segment files on crash — not authoritative. |
| Segment leader failover latency | Follower failure on active segments: sub-second (write-path timeout). Leader or idle segment failure: ~6-7s (SWIM detection). |
| Data transport competing with Raft transport | Separate ports, separate TCP connections, separate actors. Raft heartbeats (1s interval, small packets) unaffected by data throughput. |
| Local data reuse integrity | CRC verification on locally-discovered segment data before advertising. Corrupt local data falls back to full copy from healthy replica. |
