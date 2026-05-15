# Phase D1: Storage Engine

**Goal:** Single-node storage engine — WAL, segment files, sparse index, lock-free per-segment cache, and the threading model that drives them (DataPlaneActor on dedicated OS thread + I/O thread pools). No replication, no client API. Replication (D2) and client protocol (D6) plug into the interfaces defined here.

**Depends on:** Nothing.

---

## Data Formats

### Filesystem Layout

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

### Record Format

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

```rust
type SegmentKey = (ShardGroupId, RangeId, SegmentId);

enum RecordType {
    Data,
    BatchEnd,
}

struct RoutingHeader {
    shard_group_id: ShardGroupId,
    range_id: RangeId,
    segment_id: SegmentId,
    logical_offset: u64,
}

struct Record {
    crc32: u32,
    record_type: RecordType,
    payload: Bytes,         // WAL: routing header + user data. Segment file: user data only.
}
```

### Sparse Offset Index

Separate RocksDB instance (not the metadata RocksDB). Sparse — not every offset indexed:

```
Key:   [shard_group_id: 8B][range_id: 8B][segment_id: 8B][offset: 8B]
Value: [byte_position: 8B]
```

One entry per batch (e.g., every ~1000 records or every batch boundary). Consumer seek: RocksDB `seek_for_prev(target_offset)` → nearest indexed offset ≤ target → `pread()` segment file at that position → scan forward to exact offset. Batch fetching is native: `pread()` from the indexed position reads all records up to the next `BatchEnd` marker (or the next index entry's byte position if bounded). No per-record index lookup needed — one seek locates the batch, one sequential read returns it.

Sparse index trades one short sequential scan on read for dramatically fewer index writes. Rebuilt from segment files on crash — not a source of truth.

---

## Write Path

```
records arrive
    │
    ▼
accumulation_buffers[segment_key].push(record)
    │
    │ batch trigger fires (10ms / 20k records / 10MB)
    ▼
╔════════════════════════════════════════════════════╗
║ WAL: serialize all buffers with routing headers    ║
║      → flat interleaved write → fsync              ║
║      assign LSN to each record                     ║
╚════════════════════════════╤═══════════════════════╝
                         │ ◄── durability point
                         ▼
   ┌─────────── for each segment_key ────────────┐
   │ Vec<Record> ──move──▶ SegmentRecordBatch    │
   │ Arc::new() → store at batches[tail % CAP]   │
   │ tail.store(tail + 1, Release)               │
   └─────────────────────┬──────────────────────-┘
                         │ ◄── in cache, not yet visible
                         ▼
   ┌─────────── D2+: replicate ─────────────────┐
   │ send per-segment batches to all followers  │
   │ wait ALL replica fsync ACK                 │
   └─────────────────────┬─────────────────────-┘
                         │
                         ▼
   commit_offset.store(tail, Release)
   notify.notify_waiters()  ◄── consumers can read
                         │
                         ▼
   ACK producer
                         │
                         ▼ (background)
   checkpoint worker       size_bytes → RollSegment at ~1GB
```

WAL fsync is the sole disk write on the critical path. Cache publish is two atomic stores — negligible. Replication adds one network RTT. In D1 (no replication), replication is skipped — `commit_offset` advances immediately after `tail`, consumers are notified, and ACK follows.

---

## Read Path

```
Consume(topic, range, offset)
    │
    ▼
position < eviction_frontier?
    ├── yes → COLD: dispatch to cold read pool (see below)
    │
    ├── no, position < commit_offset?
    │       └── yes → HOT: cache.read_batch(position) — Arc clone, no lock
    │
    └── position == commit_offset?
            └── yes → TAILING: cache.notified().await — wake on commit_offset advance
```

Consumer offset tracking: consumers track their read position client-side. Each `Consume(topic, range, offset)` request carries the offset explicitly — the server is stateless with respect to consumer position. Consumer group offset commit and resume semantics are deferred (see roadmap backlog "Consumer Groups").

**Hot/cold transition:** Consumer checks `eviction_frontier` on each iteration. When reading behind the frontier, it dispatches to cold read pool. Once its position catches up to `eviction_frontier`, the next read switches to the hot cache path (direct `Arc` clone from the ring buffer). The transition is seamless — same consumer task, same stream, just a different data source.

---

## Components

### WAL (WalStorage + WalWriter + Lifecycle)

WAL is the durability foundation — all other components depend on it. `SegmentCache` batches carry WAL LSNs, `DataPlane` writes through the WAL before publishing to cache.

```rust
// WAL I/O trait — injected into DataPlane. Mock in tests, WalWriter in production.
trait WalStorage {
    fn write_batch(&mut self, records: &[u8]) -> io::Result<u64>;  // returns LSN
    fn fsync(&mut self) -> io::Result<()>;
    fn rotate(&mut self) -> io::Result<()>;                        // seal active, open next
    fn delete_below(&mut self, watermark_lsn: u64);                // unlink eligible files
}

// Production implementation of WalStorage trait.
struct WalWriter {
    files: VecDeque<WalFileEntry>,  // last entry is active; all preceding are sealed
    max_file_size: u64,             // ~64MB
    next_lsn: u64,
    data_dir: PathBuf,
}

impl WalStorage for WalWriter { /* blocking file I/O */ }

struct WalFileEntry {
    seq: u64,
    min_lsn: u64,
    max_lsn: u64,
    size: u64,
    path: PathBuf,
    file: Option<File>,  // Some only for last entry (active)
}
```

The WAL is a sequence of fixed-size log files, not a single ever-growing file. This makes deletion file-level (`unlink`) rather than truncation (which would require rewriting). `files` is an ordered list — the last entry is always the active file (invariant 1), all preceding entries are sealed. On rotation, the active entry's `file` handle is taken (`Option::take`), and a new entry with `file: Some(...)` is pushed to the back.

**Rotation:** When the active WAL file reaches the size limit (~64MB), it is sealed (no more writes) and a new file is opened with the next sequence number. Writes always go to exactly one file — the active one.

**LSN (Log Sequence Number):** Each WAL record carries an explicit node-local LSN — a monotonically increasing `u64` assigned by DataPlane during WAL write. LSN is carried by each `SegmentRecordBatch` in the cache, enabling the checkpoint worker to report last-checkpointed LSN back to DataPlaneActor without recomputing from file positions.

**Deletion:** A sealed WAL file is eligible for deletion once ALL records in it have been checkpointed — flushed to their respective segment files and fsynced. Deletion gated by a **checkpoint LSN watermark**: each `WalFileEntry` covers a contiguous LSN range (`min_lsn` to `max_lsn`). The checkpoint worker reports LSN via `CheckpointComplete` to DataPlaneActor; the global checkpoint watermark is the minimum of `segment_checkpoint_lsns` across all active segments. A WAL file is unlinked when the global checkpoint watermark exceeds its `max_lsn`.

**Checkpoint LSN relationships:**

```
checkpointed_lsn (per-segment)                    — reported by checkpoint worker in CheckpointComplete
        │                                            "I flushed batches up to this WAL LSN for this segment"
        ▼
segment_checkpoint_lsns (DataPlane)                — HashMap<SegmentKey, u64>, accumulates latest
        │                                            checkpointed_lsn from each segment
        ▼
global_checkpoint_watermark                        — min(segment_checkpoint_lsns.values())
        │                                            "all segments have been checkpointed up to here"
        ▼
WAL deletion: unlink files where max_lsn ≤ watermark

eviction_frontier (per-segment, in SegmentCache)   — ring buffer batch position (not an LSN)
                                                     tracks which cache slots can be reused
                                                     NOT comparable across segments or to WAL LSNs
```

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

**Bounded WAL size:** Under normal operation, the lag between WAL write and checkpoint is bounded by checkpoint frequency and cache size. WAL accumulation is bounded by the slowest checkpoint across all active segments on the node. Checkpoint frequency balances WAL retention (unflushed records keep WAL files alive) against disk I/O scheduling (larger, less frequent flushes are more efficient).

### SegmentCache

Lock-free ring buffer of immutable `Arc<SegmentRecordBatch>` batches. Single writer (`DataPlane` calls `publish` + `commit`), multiple readers (consumer tasks). No locks. Two cursors: `tail` is the write cursor — where the next batch will be stored; `commit_offset` is the read cursor — consumers only see batches behind `commit_offset`. In D1 they advance together; in D2+ `commit_offset` lags by one replication RTT.

```rust
struct SegmentCache {
    // All fields private — access only through methods below.
    batches: [AtomicPtr<SegmentRecordBatch>; CAPACITY],
    tail: AtomicU64,
    commit_offset: AtomicU64,
    eviction_frontier: AtomicU64,
    notify: tokio::sync::Notify,
}

impl SegmentCache {
    // Write path — called by DataPlane
    fn publish(&self, batch: Arc<SegmentRecordBatch>);       // store ptr, advance tail
    fn commit(&self, new_offset: u64);                       // advance commit_offset, notify_waiters()

    // Read path — called by consumer tasks
    fn load_commit_offset(&self) -> u64;                     // Acquire load
    async fn notified(&self);                                // notify.notified().await
    fn read_batch(&self, position: u64) -> Option<Arc<SegmentRecordBatch>>;
    fn load_eviction_frontier(&self) -> u64;                 // Acquire load

    // Checkpoint path — called by checkpoint worker
    fn read_batches_for_checkpoint(&self, from: u64, to: u64) -> Vec<Arc<SegmentRecordBatch>>;
    fn advance_eviction_frontier(&self, new_frontier: u64);  // Release store
}

struct SegmentRecordBatch {
    records: Vec<Record>,          // immutable after publish
    start_offset: u64,             // first record's logical offset
    end_offset: u64,               // last record's logical offset
    lsn: u64,                      // WAL LSN for checkpoint tracking
}
```

Internally, `publish` stores via `Arc::into_raw()` (cache slot holds the initial strong ref), readers clone via `Arc::increment_strong_count()` + `Arc::from_raw()`. On eviction, cache drops its ref — batch freed when last consumer releases theirs.

**Consumer seek:** Each `SegmentRecordBatch` carries `(start_offset, end_offset)`. Binary search over published batches by offset range to find the batch containing the target offset, then linear scan within the batch.

**Ordering:** Within a segment, record ordering is preserved. Per-segment accumulation buffers are appended in arrival order during the batch window. Cross-segment ordering is not guaranteed (and not needed — event streaming guarantees ordering per-range, not globally).

### DataPlane + DataPlaneActor

Dedicated OS thread — WAL fsync blocks without affecting the tokio runtime (SWIM, Raft, consumer TCP). Uses `crossbeam::channel` for its mailbox (blocking recv, higher throughput than tokio `mpsc`). No async, no tokio dependency.

Follows the sync-first pattern (see `build-actor` skill): pure sync state machine (`DataPlane`) holds all logic and buffers side effects, actor wrapper (`DataPlaneActor`) owns channels and drains events.

WAL does blocking file I/O (write, fsync, rotate, unlink) — injected via `WalStorage` trait so `DataPlane` can be tested with a mock WAL that records calls without touching disk. Same pattern as `Raft` + `RaftStorage` (see `src/clusters/raft/storage.rs`).

```rust
// State machine — pure sync, no channels. I/O only through injected WalStorage.
struct DataPlane<W: WalStorage> {
    wal: W,
    segment_caches: HashMap<SegmentKey, Arc<SegmentCache>>,
    accumulation_buffers: HashMap<SegmentKey, Vec<Record>>,
    segment_sizes: HashMap<SegmentKey, u64>,            // accumulated data bytes per segment, triggers RollSegment at ~1GB
    segment_checkpoint_lsns: HashMap<SegmentKey, u64>,  // last-checkpointed WAL LSN per segment
    pending_events: Vec<DataPlaneEvent>,
}

enum DataPlaneEvent {
    SubmitCheckpoint(CheckpointJob),
    ProduceAck { reply: oneshot::Sender<ProduceResult> },
    // ProduceAck is live-path only. WAL replay (D5) calls internal state machine
    // methods directly (populate buffers, publish to cache) — no DataPlaneCommand,
    // no oneshot, no ProduceAck event emitted.
}

// Actor wrapper — owns channels, drains state machine events.
struct DataPlaneActor;

impl DataPlaneActor {
    fn run(
        mut state: DataPlane<WalWriter>,
        mailbox: crossbeam::channel::Receiver<DataPlaneCommand>,
        checkpoint_tx: crossbeam::channel::Sender<CheckpointJob>,
    ) {
        // crossbeam::select! on mailbox + timer, process → flush
    }
}

enum DataPlaneCommand {
    Produce {
        segment_key: SegmentKey,
        records: Vec<Record>,
        reply: oneshot::Sender<ProduceResult>,
    },
    SegmentAssignment {
        segment_key: SegmentKey,
        replica_set: Vec<NodeId>,
    },
    SealSegment {
        segment_key: SegmentKey,
    },
    CheckpointComplete(CheckpointComplete),
}
```

**Channel traffic profiles:** `mailbox` is hot — every `Produce` request flows through it. `checkpoint_tx` and `data_plane_tx` (the return path for `CheckpointComplete`) are occasional — triggered only by checkpoint conditions (segment size, cache budget, WAL retention), not by time or per-batch. The checkpoint worker is a single sequential thread; each cycle includes at least one segment file fsync, so `CheckpointComplete` messages arrive at most low tens per second under sustained throughput.

Built from WAL replay + segment directory scan at startup (D5).

**Cache lifecycle:**
- Created on `SegmentAssignment` from coordinator via `data_port` (D3). Produce targeting an unknown segment is rejected — producer retries after routing refresh
- Stays in HashMap even when idle (lightweight — just ring buffer + atomics, no thread/task)
- Removed on segment seal: final checkpoint via checkpoint worker, then entry dropped
- No spawn/stop lifecycle — `SegmentCache` is a data structure, not an actor

**Backpressure:** Natural. If WAL fsync is slow, the dedicated thread stalls, the inbound `crossbeam` channel fills, producers block on `send()`. No async flow control needed.

**Timer:** `DataPlaneTimer` implements `TTimer`, driven by `crossbeam::channel::select!` with timeout (same tick model as SWIM/Raft scheduling actor, but on the dedicated thread instead of a tokio task). Drives periodic sweep for removing idle `SegmentCache` entries.

### Checkpoint Worker

Single dedicated OS thread. Sequential pipeline: read batches → write segment file → fsync → update sparse index → advance eviction frontier → report. No protocol logic, no state transitions — just a worker loop. Parallelism wouldn't help — checkpoint is write-bound (sequential writes to one stream, fsync dominates), and multiple concurrent fsyncs to the same disk add contention, not throughput. Cold read pool uses multiple threads because reads are latency-bound with independent `pread()` calls across different files. No caller waits on the checkpoint result (fire-and-forget from DataPlaneActor).

```rust
fn run_checkpoint_worker(
    sparse_index: rocksdb::DB,
    mailbox: crossbeam::channel::Receiver<CheckpointJob>,
    data_plane_tx: crossbeam::channel::Sender<DataPlaneCommand>,
) {
    // loop { recv job → read batches → write .seg → fsync → FADV_DONTNEED
    //        → update sparse_index → advance eviction_frontier → send CheckpointComplete }
}

struct CheckpointJob {
    segment_key: SegmentKey,
    cache: Arc<SegmentCache>,
    segment_file_path: PathBuf,
}

struct CheckpointComplete {
    segment_key: SegmentKey,
    checkpointed_lsn: u64,
}
```

```
DataPlaneActor                     checkpoint worker
──────────────                     ───────────────
submit CheckpointJob {
  segment_key, cache,
  segment_file_path
} ─── crossbeam channel ─────────► recv CheckpointJob
                                       │
                                       ▼
                                   read batches from cache
                                   (read-only, behind tail)
                                       │
                                       ▼
                                   coalesce → write to .seg file
                                       │
                                       ▼
                                   fsync segment file
                                       │
                                       ▼
                                   POSIX_FADV_DONTNEED
                                       │
                                       ▼
                                   update sparse index (RocksDB)
                                       │
                                       ▼
                                   eviction_frontier.store(Release)
                                       │
                                       ▼
                                   send DataPlaneCommand::
mailbox.recv() ◄─────────────────    CheckpointComplete {
  segment_checkpoint_lsns[key] = lsn     segment_key,
  watermark = min(all lsns)              checkpointed_lsn
  unlink WAL files ≤ watermark         }
```

Batches behind `eviction_frontier` with `Arc::strong_count() == 1` (only the cache slot holds a ref — no active consumer reading them) are freed. Batches still held by consumers stay alive via `Arc` refcount until those consumers advance past them.

### Checkpoint Triggers and Cache Policy

Segment files are derived from WAL — they exist for cold read performance, not durability. Checkpoint is driven by data volume, not time. No checkpoint timer needed.

**Ring buffer capacity:** configurable slot count per segment (e.g., 1024 batches ≈ 32MB). Node-level hard cap on total ring buffer memory across all segment caches (e.g., 4GB). Data stays in cache as long as possible — longer residency means more hot reads served from memory.

**Three checkpoint triggers** — checkpoint happens when there's a reason, not on a clock:

1. **Segment size approaching limit.** `DataPlane` tracks `segment_sizes: HashMap<SegmentKey, u64>` — accumulated data bytes per segment, incremented as records are accumulated. When approaching 1GB, submits a checkpoint job — one large, efficient bulk write. After checkpoint, the segment rolls (`RollSegment`).
2. **Node cache budget pressure.** When total cache memory approaches the node-level budget, DataPlaneActor force-checkpoints the segments with the most uncheckpointed batches to free cache slots.
3. **WAL retention pressure.** When total WAL size exceeds a threshold (e.g., 2GB), DataPlaneActor checkpoints the segments keeping the oldest WAL files alive. Bounds crash recovery time and WAL disk usage.

All triggers submit work to checkpoint worker. The `eviction_frontier` is the single source of truth — triggers check it against `tail` to determine what needs flushing. When the ring wraps, the writer overwrites the oldest slot — but only if it's behind the eviction frontier (already checkpointed) and has `Arc::strong_count() == 1` (no active reader). If the oldest slot is still held, checkpoint is forced before advancing.

### Cold Read Pool

Fixed-size thread pool (e.g., 4 threads). Cold reads are initiated by many independent consumer tasks, each waiting on the result. A single thread would serialize all consumers behind one read. Multiple threads serve concurrent consumers in parallel — each `pread()` is independent (different file positions, different segment files). Consumer-visible latency directly depends on pool capacity.

```rust
struct ColdReadRequest {
    segment_key: SegmentKey,
    start_offset: u64,
    max_bytes: u64,
    respond_tx: oneshot::Sender<ColdReadResult>,
}

struct ColdReadResult {
    records: Vec<Record>,
    next_offset: u64,
}
```

```
Consumer task                        cold_read_pool (dedicated threads)
─────────────────────                ──────────────────────────────────
position < eviction_frontier
or segment sealed (no cache)

send ColdReadRequest {
  segment_key, start_offset,
  max_bytes, respond_tx
} ─── crossbeam channel ────────► recv ColdReadRequest
                                      │
                                      ▼
                                  seek_for_prev(start_offset)
                                  → byte_position
                                      │
                                      ▼
                                  pread(seg_file, byte_position)
                                  scan forward to exact offset
                                      │
                                      ▼
                                  POSIX_FADV_DONTNEED on range
                                      │
                                      ▼
.await oneshot ◄───────────────── respond_tx.send(ColdReadResult)
(non-blocking)

yield records to consumer stream
check eviction_frontier:
  if position >= eviction_frontier
    → switch to hot cache path
```

**Why a dedicated pool instead of `tokio::spawn_blocking`:** Bounded concurrency — the pool has a fixed thread count (e.g., 4), preventing a burst of cold reads from saturating the tokio blocking thread pool. Disk I/O isolation — cold reads (seek + pread) do not compete with checkpoint writes or WAL fsyncs for thread resources.

---

## Failure Cases

### Crash Recovery

Crash recovery scans from the oldest un-deleted WAL file forward (see Phase D5). The oldest un-deleted file marks the checkpoint boundary — no separate checkpoint file needed.

### Uncommitted Tail on Replication Failure

When replication (D2+) partially fails, records may be in the leader's cache (`tail` advanced) but not yet committed (`commit_offset` not advanced). Two sub-cases:

**Follower fails, leader healthy:** The leader seals the segment with `end_offset = commit_offset` — the last offset ACKed by all replicas. Batches at positions `commit_offset` through `tail - 1` are uncommitted: cached on the leader and fsynced in the leader's WAL, but never ACKed to the producer. The leader drains these uncommitted batches from cache and replays them into the new segment (opened after `RollSegment`), where they will be re-committed with the new replica set. Producers whose records were in the uncommitted window also retry (no ACK received).

**Leader crashes after WAL write:** WAL records beyond the sealed segment's `end_offset` are orphaned. On recovery (D5), WAL replay compares each record's `logical_offset` against the segment's `end_offset` — records past it belong to the old, sealed segment and are skipped. The producer never received an ACK for these records and retries against the new segment leader.

In D1 (no replication, `commit_offset == tail`), this subsection does not apply — there is no replication failure mode. See invariant 15.

---

## Invariants

1. **Single active WAL file.** Writes go to exactly one file at any time. Rotation atomically seals the current file and opens the next.

2. **Active file is last in `files`.** `WalWriter.files.back()` is always the active file (`file: Some(...)`); all preceding entries are sealed (`file: None`). Rotation takes the active entry's `File` handle (`Option::take`) and pushes a new active entry to the back.

3. **LSN monotonicity.** WAL LSNs are monotonically increasing. Each record's LSN is strictly greater than the previous record's LSN, across all WAL files.

4. **Segment file records ordered by logical offset.** Records within a segment file are strictly ordered by `logical_offset`. No gaps, no reordering.

5. **WAL deletion gated by checkpoint watermark.** A WAL file is deleted only when `global_checkpoint_watermark ≥ wal_file.max_lsn`. The watermark is the minimum last-checkpointed LSN across all active segments.

6. **Cache spans eviction frontier to tail; readers bounded by commit_offset.** Batches between `eviction_frontier` and `tail` are always in the ring buffer. Consumer-visible range is positions `eviction_frontier` through `commit_offset - 1`. Batches at positions `commit_offset` through `tail - 1` exist in cache but are not yet visible to consumers. Batches behind `eviction_frontier` may still exist (held by consumer `Arc` refs) but are eligible for slot reuse. Batches at or beyond `tail` do not exist.

7. **Checkpoint is idempotent.** All three triggers (segment size, cache budget, WAL retention) check `eviction_frontier` against `tail`. No new batches since last checkpoint → no-op.

8. **Segment file I/O uses buffered I/O + `POSIX_FADV_DONTNEED`.** Both checkpoint writes and cold reads use standard buffered I/O. `FADV_DONTNEED` after each operation evicts pages from OS page cache.

9. **One SegmentCache per segment per node.** `segment_caches` enforces at most one cache per `(shard_group_id, range_id, segment_id)` key.

10. **Segment file ≤ 1GB.** DataPlaneActor signals `RollSegment` when approaching the limit. Enforced at the write path — never retroactively truncated.

11. **WAL fsync is the durability boundary.** Records are durable only after the WAL batch containing them is fsynced. Producer ACK sent after WAL fsync + cache publish + replication. Segment files and sparse index are derived — their absence never means data loss.

12. **Node-level cache budget is a hard cap.** Sum of all ring buffer allocations never exceeds the configured budget. Under pressure, force checkpoint to advance eviction frontiers; if all batches are uncheckpointed, backpressure propagates to WAL dispatch.

13. **Published batches are immutable.** No mutation after `tail` advancement. Writer never modifies data behind `tail`. Readers only access data behind `tail`. This is the SWMR (single-writer, multiple-reader) invariant that eliminates locking.

14. **Within-segment record ordering preserved.** Per-segment accumulation buffers maintain arrival order. Records in a `SegmentRecordBatch` are ordered by `logical_offset`. Cross-segment ordering is not guaranteed.

15. **`eviction_frontier` ≤ `commit_offset` ≤ `tail` always.** Three monotonically advancing markers partition the ring buffer into zones. In D1 (no replication), `commit_offset == tail` — both advance together on cache publish. In D2+, `commit_offset` lags `tail` by one replication RTT: `tail` advances on cache publish, `commit_offset` advances after all replicas ACK.

16. **Sealed segment `end_offset` = last committed offset.** On seal, `end_offset` is set to `commit_offset` at seal time, not `tail`. Batches at positions `commit_offset` through `tail - 1` are uncommitted — replayed into the next segment by the leader (see "Uncommitted Tail on Replication Failure").

17. **Consumer read position determines data source.** A consumer at `position` follows exactly one path: `position < eviction_frontier` → cold read path (cold read pool); `eviction_frontier ≤ position < commit_offset` → hot cache read (clone `Arc` from ring buffer); `position == commit_offset` → tailing (await `notify`). Consumers never read at or beyond `commit_offset` — batches at positions `commit_offset` through `tail - 1` are uncommitted and invisible. This prevents consumers from observing records that may be rolled back on replication failure.
