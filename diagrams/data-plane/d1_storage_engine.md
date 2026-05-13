# Phase D1: Storage Engine

**Goal:** Single-node storage engine — WAL, segment files, sparse index, lock-free per-segment cache, and the threading model that drives them (DataPlaneActor on dedicated OS thread + I/O thread pools). No replication, no client API. Replication (D2) and client protocol (D6) plug into the interfaces defined here.

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
      {segment_id}.seg                   # append-only, ≤1GB
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

**Batch boundaries:** A `BatchEnd` record is written at the end of each batch. Batches are closed by whichever trigger fires first: 10ms elapsed, 20k records accumulated, or 10MB buffered. In the WAL, `BatchEnd` marks the fsync boundary (one fsync per batch). In segment files, `BatchEnd` marks batch boundaries for readers (consumer scan stops at `BatchEnd`). The sparse index writes one entry per batch at the `BatchEnd` boundary.

**Segment file I/O:** Both reads and writes use standard buffered I/O + `posix_fadvise(POSIX_FADV_DONTNEED)`. `FADV_DONTNEED` after each write and read evicts pages so segment file I/O doesn't pollute the OS page cache. No alignment constraints, no padding, same I/O model for reads and writes. WAL also uses standard buffered I/O + fsync (write-once-read-never in normal path).

## Sparse Offset Index

Separate RocksDB instance (not the metadata RocksDB). Sparse — not every offset indexed:

```
Key:   [shard_group_id: 8B][range_id: 8B][segment_id: 8B][offset: 8B]
Value: [byte_position: 8B]
```

One entry per batch (e.g., every ~1000 records or every batch boundary). Consumer seek: RocksDB `seek_for_prev(target_offset)` → nearest indexed offset ≤ target → `pread()` segment file at that position → scan forward to exact offset.

Sparse index trades one short sequential scan on read for dramatically fewer index writes. Rebuilt from segment files on crash — not a source of truth.

## Write Path (RF=3)

1. Accumulate records into **per-segment buffers** until batch trigger (10ms / 20k records / 10MB). Records grouped by `(shard_group_id, range_id, segment_id)` during accumulation — arrival order preserved within each segment's buffer
2. Write **flat interleaved** batch to WAL (all segments' records serialized with routing headers), fsync WAL — local durability point
3. Publish per-segment `Arc<SegmentSegmentRecordBatch>` to each segment's lock-free cache (atomic store + advance tail), notify tailing consumers
4. Replicate to followers: send per-segment batches to each follower, wait for fsync ACK from ALL replicas (D2)
5. ACK producer — data is durable on all replicas and readable from cache
6. Background checkpoint (see Checkpoint below)
7. Track `size_bytes` — when approaching 1GB, signal metadata layer to propose `RollSegment`

WAL fsync (step 2) is the sole local disk write on the critical path. Cache publish (step 3) is two atomic stores + a notification — negligible. Replication (step 4) adds one network RTT. At ACK (step 5), data is durable on all replicas and readable from leader's cache. Steps 6-7 are background work.

In D1 (no replication), step 4 is skipped — ACK follows immediately after cache publish.

## Read Path

1. Check application cache for requested offset range
2. Cache hit (hot tail reads): serve from memory — zero disk I/O
3. Cache miss (cold reads): resolve `(range_id, segment_id, offset)` → nearest `byte_position` via sparse index (`seek_for_prev`)
4. `pread()` on segment file at `byte_position`, scan forward to exact offset
5. Stream records forward until requested `max_bytes` or EOF
6. `posix_fadvise(POSIX_FADV_DONTNEED)` on read range — evicts pages so cold reads don't pollute OS page cache

**Consumer offset tracking:** Consumers track their read position client-side. Each `Consume(topic, range, offset)` request carries the offset explicitly — the server is stateless with respect to consumer position. Consumer group offset commit and resume semantics are deferred (see roadmap backlog "Consumer Groups").

## Architecture

Three concerns, three isolation boundaries. No locks on the hot path.

```
DataPlaneActor (dedicated OS thread)
├── WAL writer (std::fs::File, blocking fsync)
├── per-segment accumulation buffers (grouped during batch window)
├── segment_caches: HashMap<SegmentKey, Arc<SegmentCache>>
├── mailbox: crossbeam::channel (blocking recv)
└── timer: crossbeam::channel::select! with timeout

SegmentCache (lock-free, per segment)                 Consumer tasks (tokio)
├── batches: [Arc<SegmentRecordBatch>; CAPACITY]         ├── load tail (Acquire)
│   (lock-free via AtomicPtr + manual refcount)          ├── read batches behind tail
├── tail: AtomicU64  ← writer                            ├── clone Arc (refcount bump)
├── eviction_frontier: AtomicU64  ← checkpoint_pool      └── await notify (tailing)
└── notify: tokio::sync::Notify

I/O Thread Pools
├── checkpoint_pool: cache → segment file → fsync → FADV_DONTNEED → sparse index → report LSN
└── cold_read_pool: sparse index → pread segment file → POSIX_FADV_DONTNEED
```

### DataPlaneActor (dedicated OS thread)

Runs on its own OS thread — WAL fsync blocks this thread without affecting the tokio runtime (SWIM, Raft, consumer TCP). Uses `crossbeam::channel` for its mailbox (blocking recv, higher throughput than tokio::mpsc). No async, no tokio dependency.

Owns the shared WAL and all segment caches:

```
segment_caches: HashMap<(ShardGroupId, RangeId, SegmentId), Arc<SegmentCache>>
```

Built from WAL replay + segment directory scan at startup (D5).

**Cache lifecycle:**
- Created on `SegmentAssignment` from coordinator via `data_port` (D3), or on first produce targeting an unknown segment
- Stays in HashMap even when idle (lightweight — just ring buffer + atomics, no thread/task)
- Removed on segment seal: final checkpoint via `checkpoint_pool`, then entry dropped
- No spawn/stop lifecycle — `SegmentCache` is a data structure, not an actor

**Write path:** Follows the Write Path above. DataPlaneActor owns all steps on the critical path (1–4). Accumulates records into per-segment buffers during the batch window, serializes all buffers into one flat WAL write (with routing headers), fsyncs, then publishes each per-segment buffer as an `Arc<SegmentRecordBatch>` to that segment's cache. One WAL fsync per batch covers all segments — amortization is the reason for the shared WAL.

**Cold read path (sealed segments, no cache):** Dispatched to `cold_read_pool` — sparse index lookup for byte position, `pread()` from segment file, `posix_fadvise(POSIX_FADV_DONTNEED)` on read range. Future optimization: `sendfile()` (see roadmap backlog "Zero-Copy Reads") — trades buffered I/O for kernel-level file-to-socket transfer.

**Backpressure:** Natural. If WAL fsync is slow, the dedicated thread stalls, the inbound `crossbeam` channel fills, producers block on `send()`. No async flow control needed.

**Timer:** `DataPlaneTimer` implements `TTimer`, driven by `crossbeam::channel::select!` with timeout (same tick model as SWIM/Raft scheduling actor, but on the dedicated thread instead of a tokio task). Drives periodic sweep for removing idle `SegmentCache` entries.

### SegmentCache (lock-free, per segment)

Ring buffer of immutable `Arc<SegmentRecordBatch>` batches. Single writer (DataPlaneActor), multiple readers (consumer tasks on tokio). No locks.

```
SegmentCache {
    batches: [Arc<SegmentRecordBatch>; CAPACITY],  // ring buffer slots (lock-free via AtomicPtr)
    tail: AtomicU64,                               // batch count (slot = tail % CAPACITY)
    eviction_frontier: AtomicU64,                  // advanced by checkpoint_pool
    notify: tokio::sync::Notify,                   // wake tailing consumers
}

SegmentRecordBatch {
    records: Vec<Record>,          // immutable after publish
    start_offset: u64,             // first record's logical offset
    end_offset: u64,               // last record's logical offset
    lsn: u64,                      // WAL LSN for checkpoint tracking
}
```

Slots conceptually hold `Arc<SegmentRecordBatch>`. The lock-free implementation uses `AtomicPtr` + manual refcount under the hood: writer stores via `Arc::into_raw()` (cache slot holds the initial strong ref), readers clone via `Arc::increment_strong_count()` + `Arc::from_raw()`. On eviction, cache drops its ref — batch freed when last consumer releases theirs.

**Writer (DataPlaneActor, sole writer):**
1. Build `SegmentRecordBatch` from per-segment accumulation buffer (owned, mutable)
2. Freeze: wrap in `Arc<SegmentRecordBatch>` (immutable from here)
3. Store ptr at `batches[tail % CAPACITY]` (Release ordering) — cache slot takes initial strong ref
4. Advance `tail` (Release ordering)
5. `notify.notify_waiters()` — wakes all tailing consumers on this segment

**Reader (consumer task on tokio, concurrent):**
1. Load `tail` (Acquire ordering)
2. If `tail > my_position`: clone `Arc` from `batches[my_position..tail]` (refcount bump, no lock)
3. If `my_position == tail`: `notify.notified().await` (tailing — wait for new data)
4. If `my_position < eviction_frontier`: consumer too slow — redirect to cold read path (segment files)

**Consumer seek:** Each `SegmentRecordBatch` carries `(start_offset, end_offset)`. Binary search over published batches by offset range to find the batch containing the target offset, then linear scan within the batch.

**Ordering:** Within a segment, record ordering is preserved. Per-segment accumulation buffers are appended in arrival order during the batch window. Cross-segment ordering is not guaranteed (and not needed — event streaming guarantees ordering per-range, not globally).

### I/O Thread Pools

Heavy I/O offloaded from both the DataPlaneActor thread and tokio worker threads.

**`checkpoint_pool`** — flushes cached batches to segment files:

1. Read published batches from cache (read-only, behind `tail` — no contention with writer appending at tail)
2. Coalesce batches into one write buffer, write to segment file (buffered I/O)
3. `fsync` segment file
4. `posix_fadvise(POSIX_FADV_DONTNEED)` on written range — evict from page cache
5. Update sparse index in RocksDB
6. Advance `eviction_frontier` on the `SegmentCache` (Release ordering)
7. Report last-checkpointed LSN to DataPlaneActor via `crossbeam` channel (for WAL file deletion)

Batches behind `eviction_frontier` with `Arc::strong_count() == 1` (only the cache slot holds a ref — no active consumer reading them) are freed. Batches still held by consumers stay alive via `Arc` refcount until those consumers advance past them.

**`cold_read_pool`** — serves cache misses:
- Sparse index lookup → `pread()` segment file → `posix_fadvise(POSIX_FADV_DONTNEED)`. For consumers reading sealed segments or consumers that fell behind the eviction frontier.

### Checkpoint

Data is durable in WAL (and replicated in D2+). Segment files are derived — they exist for cold read performance, not durability. Checkpoint is driven by data volume, not time. No checkpoint timer needed.

Three triggers — checkpoint happens when there's a reason, not on a clock:

1. **Segment size approaching limit.** DataPlaneActor tracks `size_bytes` per segment (cumulative cached + checkpointed). When approaching 1GB, DataPlaneActor submits a checkpoint job for that segment — one large, efficient bulk write. After checkpoint, the segment rolls (`RollSegment`).
2. **Node cache budget pressure.** When total cache memory across all segments approaches the node-level budget (e.g., 4GB), DataPlaneActor force-checkpoints the segments with the most uncheckpointed batches to free cache slots.
3. **WAL retention pressure.** When total WAL size exceeds a threshold (e.g., 2GB), DataPlaneActor checkpoints the segments that are keeping the oldest WAL files alive. Bounds crash recovery time and WAL disk usage — prevents idle segments from holding WAL files indefinitely.

All triggers submit work to `checkpoint_pool`. The `eviction_frontier` is the single source of truth — triggers check it against `tail` to determine what needs flushing. Data stays in cache as long as possible — longer cache residency means more hot reads served from memory.

### Cache Policy

Event streaming consumers typically tail the log — reading seconds behind the write head. The ring buffer cache is optimized for this pattern:

- **Ring buffer capacity is the per-segment cache bound.** Configurable slot count (e.g., 1024 batches ≈ 32MB at typical batch sizes). When the ring wraps, the writer overwrites the oldest slot — but only if it's behind the eviction frontier (already checkpointed) and has `Arc::strong_count() == 1` (no active reader). If the oldest slot is still held, checkpoint is forced before advancing.
- **Node-level cache budget.** Hard cap on total ring buffer memory across all segment caches (e.g., 4GB). Under budget pressure: force checkpoint on segments with the most uncheckpointed batches (advance their eviction frontiers), then shrink ring buffer capacity for low-traffic segments.
- **Slow consumer redirect.** Consumer whose position falls behind the eviction frontier is transparently redirected to the cold read path (segment files + `POSIX_FADV_DONTNEED`). No special handling — just a position comparison on each read.

## WAL Lifecycle

The WAL is a sequence of fixed-size log files, not a single ever-growing file. This makes deletion file-level (`unlink`) rather than truncation (which would require rewriting).

**Rotation:** When the active WAL file reaches the size limit (~64MB), it is sealed (no more writes) and a new file is opened with the next sequence number. Writes always go to exactly one file — the active one.

**LSN (Log Sequence Number):** Each WAL record carries an explicit node-local LSN — a monotonically increasing `u64` assigned by DataPlaneActor during WAL write. LSN is carried by each `SegmentRecordBatch` in the cache, enabling `checkpoint_pool` to report last-checkpointed LSN back to DataPlaneActor without recomputing from file positions.

**Deletion:** A sealed WAL file is eligible for deletion once ALL records in it have been checkpointed — flushed to their respective segment files and fsynced. Deletion gated by a **checkpoint LSN watermark**: each WAL file covers a contiguous LSN range (`min_lsn` to `max_lsn`). `checkpoint_pool` reports LSN via `crossbeam` channel to DataPlaneActor; the global checkpoint watermark is the minimum across all active segments. A WAL file is unlinked when the global checkpoint watermark exceeds its maximum LSN.

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

5. **Cache spans eviction frontier to tail.** Batches between `eviction_frontier` and `tail` are always in the ring buffer. Batches behind `eviction_frontier` may still exist (held by consumer `Arc` refs) but are eligible for slot reuse. Batches at or beyond `tail` do not exist.

6. **Checkpoint is idempotent.** All three triggers (segment size, cache budget, WAL retention) check `eviction_frontier` against `tail`. No new batches since last checkpoint → no-op.

7. **Segment file I/O uses buffered I/O + `POSIX_FADV_DONTNEED`.** Both checkpoint writes and cold reads use standard buffered I/O. `FADV_DONTNEED` after each operation evicts pages from OS page cache.

8. **One SegmentCache per segment per node.** `segment_caches` enforces at most one cache per `(shard_group_id, range_id, segment_id)` key.

9. **Segment file ≤ 1GB.** DataPlaneActor signals `RollSegment` when approaching the limit. Enforced at the write path — never retroactively truncated.

10. **WAL fsync is the durability boundary.** Records are durable only after the WAL batch containing them is fsynced. Producer ACK sent after WAL fsync + cache publish + replication (step 5). Segment files and sparse index are derived — their absence never means data loss.

11. **Node-level cache budget is a hard cap.** Sum of all ring buffer allocations never exceeds the configured budget. Under pressure, force checkpoint to advance eviction frontiers; if all batches are uncheckpointed, backpressure propagates to WAL dispatch.

12. **Published batches are immutable.** No mutation after `tail` advancement. Writer never modifies data behind `tail`. Readers only access data behind `tail`. This is the SWMR (single-writer, multiple-reader) invariant that eliminates locking.

13. **Within-segment record ordering preserved.** Per-segment accumulation buffers maintain arrival order. Records in a `SegmentRecordBatch` are ordered by `logical_offset`. Cross-segment ordering is not guaranteed.
