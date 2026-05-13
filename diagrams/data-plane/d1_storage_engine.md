# Phase D1: Storage Engine

**Goal:** Single-node storage engine — WAL, segment files, sparse index, application-level cache, and the actor architecture that drives them (DataPlaneActor + SegmentActor). No replication, no client API. Replication (D2) and client protocol (D6) plug into the actor interfaces defined here.

**Depends on:** Nothing.

---

## Filesystem Layout

```
{data_dir}/
  wal/
    wal-000001.log                       # sealed, eligible for deletion
    wal-000002.log                       # sealed, eligible for deletion
    wal-000003.log                       # active, being appended to
  {shard_group_id}/
    {range_id}/
      {segment_id}.seg                   # append-only, O_DIRECT, ≤1GB
```

WAL is per-node (single sequential write point), split into fixed-size log files (e.g., 64MB each). When the active file reaches the size limit, it is sealed and a new file is opened. Old WAL files are deleted once all their records have been durably written to their respective segment files (see WAL Lifecycle below). Segment files organized by ownership hierarchy. Removing a shard group = `rm -rf {data_dir}/{shard_group_id}/`.

## Record Format

```
Base record (both WAL and segment files):
[crc32: 4 bytes][type: 1 byte][length: 4 bytes][payload: N bytes][length: 4 bytes]
                                                                   ^ trailing length
```

Trailing length enables O(1) backward scan from EOF — read last 4 bytes, jump back `length + 9` (header size), verify CRC. Used in both WAL and segment files.

Record types: `Data` (user payload), `BatchEnd` (marks flush boundary).

**WAL `Data` records** prepend a 32-byte routing header inside the payload — not in the base record header, not in user data. CRC covers the routing header.

```
WAL Data record payload layout:
[shard_group_id: 8B][range_id: 8B][segment_id: 8B][logical_offset: 8B][user_data: M bytes]
 └──────────────────── routing header (32B) ──────────────────────────┘

Segment file Data record payload layout:
[user_data: N bytes]
```

Routing header is WAL-only. During checkpoint, records are written to segment files with the routing header stripped — segment file path (`{shard_group_id}/{range_id}/{segment_id}.seg`) already encodes routing, and `logical_offset` is tracked by the sparse index. `logical_offset` enables idempotent WAL replay: records already in the segment file are skipped by comparing the record's `logical_offset` against the segment's current `end_offset` (see D5).

**Batch boundaries:** A `BatchEnd` record is written at the end of each batch. Batches are closed by whichever trigger fires first: 10ms elapsed, 20k records accumulated, or 10MB buffered. In the WAL, `BatchEnd` marks the fsync boundary (one fsync per batch). In segment files, `BatchEnd` additionally absorbs O_DIRECT alignment padding (see below). The sparse index writes one entry per batch at the `BatchEnd` boundary.

**O_DIRECT alignment (segment file writes only):** O_DIRECT requires sector-aligned buffers and file offsets (typically 4KB). Individual records are variable-length, so alignment is at the **batch level** — each batch is padded to the next 4KB boundary before writing. The `BatchEnd` record absorbs the padding. Reads use standard buffered I/O — kernel readahead benefits sequential cold reads. `posix_fadvise(POSIX_FADV_DONTNEED)` after each read evicts pages so cold reads don't pollute the OS page cache. Reads account for padding by scanning records within a batch and stopping at `BatchEnd`. WAL also uses standard buffered I/O + fsync (write-once-read-never in normal path).

## Sparse Offset Index

Separate RocksDB instance (not the metadata RocksDB). Sparse — not every offset indexed:

```
Key:   [shard_group_id: 8B][range_id: 8B][segment_id: 8B][offset: 8B]
Value: [byte_position: 8B]
```

One entry per batch (e.g., every ~1000 records or every batch boundary). Consumer seek: RocksDB `seek_for_prev(target_offset)` → nearest indexed offset ≤ target → `pread()` segment file at that position → scan forward to exact offset.

Sparse index trades one short sequential scan on read for dramatically fewer index writes. Rebuilt from segment files on crash — not a source of truth.

## Write Path (single node)

1. Accumulate records in application buffer until batch trigger (10ms / 20k records / 10MB)
2. Write batch to WAL (buffered I/O), fsync WAL — durability point
3. Insert records into application cache — readable by consumers immediately
4. ACK producer (D1; in D2+, ACK delayed until all replicas confirm fsync)
5. Background checkpoint (see Checkpoint triggers below)
6. Track `size_bytes` — when approaching 1GB, signal metadata layer to propose `RollSegment`

WAL fsync is the sole disk write on the critical path (steps 1-2). Cache insert (step 3) is in-memory — negligible compared to the batch window. At ACK (step 4), data is both durable and readable. Steps 5-6 are background work.

## Read Path

1. Check application cache for requested offset range
2. Cache hit (hot tail reads): serve from memory — zero disk I/O
3. Cache miss (cold reads): resolve `(range_id, segment_id, offset)` → nearest `byte_position` via sparse index (`seek_for_prev`)
4. `pread()` on segment file at `byte_position`, scan forward to exact offset
5. Stream records forward until requested `max_bytes` or EOF
6. `posix_fadvise(POSIX_FADV_DONTNEED)` on read range — evicts pages so cold reads don't pollute OS page cache

**Consumer offset tracking:** Consumers track their read position client-side. Each `Consume(topic, range, offset)` request carries the offset explicitly — the server is stateless with respect to consumer position. Consumer group offset commit and resume semantics are deferred (see roadmap backlog "Consumer Groups").

## Storage Actor Architecture

A node hosts many segments simultaneously — some as leader, some as follower. Two levels manage this:

```
DataPlaneActor (one per node)
├── WAL writer (shared, single sequential write point for all segments)
├── segment_registry: HashMap<(ShardGroupId, RangeId, SegmentId), SegmentSender>
├── routing: incoming produce/consume/replication → correct SegmentActor
└── lifecycle: spawn/stop SegmentActors based on segment events

SegmentActor (spawned tokio task, one per segment on this node)
├── cache (write staging + hot reads)
├── checkpoint (cache → segment file, O_DIRECT)
├── read serving (cache hit or segment file)
└── replication (leader: fan-out to followers; follower: accept + ack)
```

### DataPlaneActor (node-level coordinator)

Owns the shared WAL and a unified segment registry:

```
segment_registry: HashMap<(ShardGroupId, RangeId, SegmentId), SegmentEntry>

SegmentEntry {
    meta: LocalSegmentMeta,          // always present — from disk scan at startup
    actor: Option<SegmentSender>,    // Some = spawned, None = on-disk only
    last_active_at: Instant,         // updated on every routed request
}
```

Built from WAL replay + segment directory scan at startup (D5). All entries start with `actor: None`. DataPlaneActor looks up the registry by `(shard_group_id, range_id, segment_id)` and decides the path based on segment state and request type:

**Active segment (writes + hot reads) → SegmentActor required:**
- Produce, replication, or consume on active segment: if `actor` is `None`, spawn SegmentActor and set to `Some`. SegmentActor manages cache, replication, and consumer sessions.

**Sealed segment cold read → no actor, serve directly:**
- Consume on sealed segment with `actor: None`: DataPlaneActor serves directly — sparse index lookup → `pread()` from segment file + `POSIX_FADV_DONTNEED`. No mutable state, no cache, no actor spawn. Just file I/O.

**Spawn triggers:**
- `SegmentAssignment` from coordinator via `data_port` — this node assigned as leader or follower for a new/rolled segment (D3)
- First `ReplicaAppend` for an unknown segment — self-authorizing (D2). DataPlaneActor validates `replica_set` membership, spawns SegmentActor if this node is listed.
- First produce or hot consume (active segment) targeting a segment with `actor: None`

**Stop triggers:**
- Idle eviction — `DataPlaneTimer` implements `TTimer`, driven by the shared scheduling actor (`run_scheduling_actor`). The `Default` callback serves as a periodic sweep trigger (same pattern as Swim's protocol-period-elapsed). Each sweep checks `last_active_at` on every entry where `actor.is_some()`. If `now - last_active_at > idle_threshold` (e.g., 60s), stops the SegmentActor and sets `actor` back to `None`. Segment data stays on disk.
- Segment sealed — DataPlaneActor stops routing writes, SegmentActor drains active consumers, completes final checkpoint, `actor` set to `None`. Future reads on this segment served directly without actor.
- Node shutdown — all actors stopped gracefully (flush pending checkpoints before exit)

**Write path (active segments):** Follows the Write Path above. DataPlaneActor owns steps 1–2 (batching + WAL). After WAL fsync, DataPlaneActor dispatches records to respective SegmentActors — each record carries its explicit WAL LSN. SegmentActor owns steps 3–7 (cache, ACK, checkpoint). ACK always originates from SegmentActor — in D1 after cache insert, in D2+ after cache insert + all replica acks. One WAL fsync per batch covers all segments in that batch — amortization is the reason for the shared WAL.

**Hot read path (active segments, through SegmentActor):** DataPlaneActor routes to SegmentActor. Consumer reads from cache (zero disk I/O) or falls through to segment file if data already checkpointed.

**Cold read path (sealed segments, no actor):** DataPlaneActor handles directly — sparse index lookup for byte position, `pread()` from segment file, write to TCP socket, `posix_fadvise(POSIX_FADV_DONTNEED)` on read range. No actor, no cache, no memory overhead. Future optimization: `sendfile()` for sealed segments (see roadmap backlog "Zero-Copy Reads") — kernel-level file-to-socket transfer, pages still evicted via `FADV_DONTNEED` after send.

### SegmentActor (per-segment)

Each SegmentActor is a spawned task owning one segment's state. Per-segment actors keep contention local — a single shared actor would bottleneck on concurrent write staging and read serving across all segments. Since O_DIRECT bypasses the OS page cache, each actor manages its own cache:

- **Write staging.** After WAL commit notification from DataPlaneActor, records enter cache and are immediately readable. Records remain in cache until background checkpoint flushes them to segment files.
- **Consume-stream-aware.** Tracks active consumer sessions and their read positions. Pre-fetches ahead of active consumers.
- **Hot tail reads served from cache.** Consumer chasing the write head reads data that was just written — served from cache, zero disk I/O. This is the most common consumer pattern.
- **Cold reads bypass cache.** Consumers reading old sealed segments (already flushed and evicted) go directly to segment files via buffered I/O + `POSIX_FADV_DONTNEED`. No pollution of OS page cache.
- **Checkpoint drives eviction.** After checkpoint flush + fsync, cached records become evictable (they're now durable in segment files). SegmentActor reports its last-checkpointed LSN to DataPlaneActor for WAL file deletion (see WAL Lifecycle).

**Checkpoint triggers:** Two independent triggers, whichever fires first:

1. **Timer-based.** `CheckpointTimer` implements `TTimer`, driven by the shared scheduling actor (`run_scheduling_actor`). Fires periodically (e.g., every 5s). The timer callback checks SegmentActor's last-checkpoint state — if no new records since last checkpoint, the callback is a no-op. This makes timer-triggered checkpoints idempotent: a size-triggered checkpoint that already flushed the data prevents the next timer callback from doing redundant work.
2. **Size-based.** Checked on each cache insert (real-time, not timer-driven). When unflushed cache exceeds a threshold (e.g., 16MB), checkpoint triggers immediately. This bounds memory usage and WAL retention without waiting for the next timer tick.

Both triggers feed into the same checkpoint path:

1. Flush cached records to segment file (O_DIRECT, batch-aligned)
2. `fsync` segment file
3. Update sparse index in RocksDB
4. Report last-checkpointed LSN to DataPlaneActor (for WAL file deletion — see WAL Lifecycle)
5. Mark flushed cache entries as evictable

The last-checkpointed offset is the single source of truth for both triggers.

**Cache policy:** Event streaming consumers typically tail the log — reading seconds behind the write head. Cache is optimized for this pattern:

- **FIFO eviction.** Records evicted oldest-first within each segment. Sequential consumption means the oldest cached records are the least likely to be read. Only checkpointed records are evictable (uncheckpointed records must stay in cache — they're the only copy besides the WAL).
- **Per-segment cache bound.** Each segment cache has a configurable size limit (e.g., 32MB). Bounds memory per segment regardless of write rate.
- **Node-level cache budget.** Hard cap on total cache memory across all active segments (e.g., 4GB). Prevents memory explosion when many segments are active simultaneously. Under budget pressure: force checkpoint on segments with the most evictable data, then evict. If all cached records are uncheckpointed (waiting for checkpoint I/O), backpressure propagates — DataPlaneActor slows WAL batch dispatch to that segment until cache drains.

## WAL Lifecycle

The WAL is a sequence of fixed-size log files, not a single ever-growing file. This makes deletion file-level (`unlink`) rather than truncation (which would require rewriting).

**Rotation:** When the active WAL file reaches the size limit (~64MB), it is sealed (no more writes) and a new file is opened with the next sequence number. Writes always go to exactly one file — the active one.

**LSN (Log Sequence Number):** Each WAL record carries an explicit node-local LSN — a monotonically increasing `u64` assigned by DataPlaneActor during WAL write. LSN is part of the dispatched record from DataPlaneActor to SegmentActor, enabling SegmentActors to report their last-checkpointed LSN back to DataPlaneActor without recomputing from file positions.

**Deletion:** A sealed WAL file is eligible for deletion once ALL records in it have been checkpointed — flushed to their respective segment files and fsynced. Deletion gated by a **checkpoint LSN watermark**: each WAL file covers a contiguous LSN range (`min_lsn` to `max_lsn`). Per-segment actors report the LSN of their last checkpointed record; the global checkpoint watermark is the minimum across all active segments. A WAL file is unlinked when the global checkpoint watermark exceeds its maximum LSN.

```
WAL file lifecycle:

  wal-000001.log  [active]       → batch writes + fsync
                  [sealed]       → size limit reached, new file opened
                  [checkpointing] → cache flushing records to segment files
                  [deleted]      → all records checkpointed, unlink

Deletion condition for wal-NNNNNN.log:
  checkpoint_watermark = min(each segment's last-checkpointed LSN)
  wal_file.max_lsn ≤ checkpoint_watermark
```

**Crash recovery** scans from the oldest un-deleted WAL file forward (see Phase D5). The oldest un-deleted file marks the checkpoint boundary — no separate checkpoint file needed.

**Bounded WAL size:** Under normal operation, the lag between WAL write and checkpoint is bounded by checkpoint frequency and cache size. WAL accumulation is bounded by the slowest checkpoint across all active segments on the node. Checkpoint frequency balances WAL retention (unflushed records keep WAL files alive) against disk I/O scheduling (larger, less frequent flushes are more efficient).

## Invariants

1. **Single active WAL file.** Writes go to exactly one file at any time. Rotation atomically seals the current file and opens the next.

2. **LSN monotonicity.** WAL LSNs are monotonically increasing. Each record's LSN is strictly greater than the previous record's LSN, across all WAL files.

3. **Segment file records ordered by logical offset.** Records within a segment file are strictly ordered by `logical_offset`. No gaps, no reordering.

4. **WAL deletion gated by checkpoint watermark.** A WAL file is deleted only when `global_checkpoint_watermark ≥ wal_file.max_lsn`. The watermark is the minimum last-checkpointed LSN across all active segments.

5. **Cache spans checkpoint frontier to write head.** Records between the last-checkpointed offset and the write head are always in cache. Records at or below the checkpoint frontier are evictable but may still be cached. Records above the write head do not exist.

6. **Checkpoint is idempotent.** Both timer-triggered and size-triggered checkpoints use the same last-checkpoint tracking. A checkpoint with no new records since the last checkpoint is a no-op.

7. **O_DIRECT alignment at batch boundaries (writes only).** Each batch written to a segment file is padded to sector alignment (4KB). The `BatchEnd` record absorbs padding bytes. Reads use standard buffered I/O + `POSIX_FADV_DONTNEED`.

8. **One SegmentActor per segment per node.** `segment_registry` enforces at most one spawned actor per `(shard_group_id, range_id, segment_id)` key.

9. **Segment file ≤ 1GB.** DataPlaneActor signals `RollSegment` when approaching the limit. Enforced at the write path — never retroactively truncated.

10. **WAL fsync is the durability boundary.** Records are durable only after the WAL batch containing them is fsynced. Producer ACK sent after WAL fsync + cache insert (step 4). In D2+, ACK further delayed until all replicas confirm fsync. ACK always originates from SegmentActor — consistent across phases. Segment files and sparse index are derived — their absence never means data loss.

11. **Node-level cache budget is a hard cap.** Sum of all per-segment caches never exceeds the configured node-level budget. Under pressure, checkpointed records are evicted FIFO; if no evictable records exist, backpressure propagates to WAL dispatch.
