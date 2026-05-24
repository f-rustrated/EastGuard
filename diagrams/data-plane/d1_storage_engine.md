# Phase D1: Storage Engine

**Goal:** Single-node storage engine — WAL, segment files, sparse index, lock-free per-segment cache, and the threading model that drives them (DataPlaneActor on dedicated OS thread + I/O thread pools). No replication, no client API. Replication (D2) and client protocol (D6) plug into the interfaces defined here.

**Depends on:** Nothing.

---

## Offset Model

Follows Northguard/Pulsar: offsets are **per-entry (batch), not per-record**. Producers are offset-agnostic — the broker assigns `entry_id` during WAL flush.

| Concept | EastGuard | Pulsar equivalent |
|---|---|---|
| Segment | `SegmentKey` | `ledgerId` |
| Entry offset | `entry_id: u64` (per-segment, broker-assigned) | `entryId` |
| Record within entry | `record_index: u32` (consumer-side) | `batchIndex` |
| Record ID | `(SegmentKey, entry_id, record_index)` | `(ledgerId, entryId, batchIndex)` |

Each `Produce` command carries an opaque `EntryPayload` — one pre-serialized blob from the producer client. The broker never parses or re-serializes record contents. Multiple `Produce` commands accumulate as separate `StagedEntry`s in the tracker; on flush, each gets its own `entry_id`. Batching win comes from amortizing WAL fsync across multiple entries, not from merging their contents.

---

## Data Formats

### Filesystem Layout

```
{data_dir}/
  wal/
    wal-000001.log                       # sealed, eligible for deletion
    wal-000002.log                       # sealed, eligible for deletion
    wal-000003.log                       # active, being appended to
  {topic_id}/
    {range_id}/
      {segment_id}.seg                   # append-only, ≤1GB
```

WAL is per-node (single sequential write point), split into fixed-size log files (e.g., 64MB each). When the active file reaches the size limit, it is sealed and a new file is opened. Old WAL files are deleted once all their entries have been durably written to their respective segment files (see WAL Lifecycle below). Segment files organized by ownership hierarchy. Removing a topic's data = `rm -rf {data_dir}/{topic_id}/`.

### Record Format

```
Base record (both WAL and segment files):
[crc32: 4 bytes][type: 1 byte][length: 4 bytes][payload: N bytes][length: 4 bytes]
                                                                   ^ trailing length
```

Trailing length enables O(1) backward scan from EOF — read last 4 bytes, jump back `length + 9` (header size), verify CRC. Used in both WAL and segment files.

Record types: `Data` (entry payload), `BatchEnd` (marks flush boundary).

### Entry Format: Header + Opaque Payload

An entry lives inside the base record wrapper — CRC, trailing length, and backward scan are inherited unchanged. Within the payload, an entry has two layers — a decompressed header the broker reads, and an opaque blob the broker never touches:

```
WAL Data record payload layout:

[topic_id: 8B][range_id: 8B][segment_id: 8B][entry_id: 8B][record_count: 4B]
 └──────────────────── routing header (36B, WAL-only) ─────────────────────────┘
[opaque payload: remaining bytes]   ← compressed record data (broker-opaque)
```

The broker reads the routing header (segment routing + `entry_id` + `record_count`). The opaque payload is stored, replicated, and served as-is — zero serde at the broker.

**Segment file Data record payload layout** (checkpoint strips routing header, keeps entry payload):

```
[opaque payload: N bytes]   ← entry data blob (no routing header)
```

Same as WAL minus the routing header — segment file path already encodes routing, and `entry_id` is tracked by the sparse index.

**Opaque payload internal format** (producer/consumer contract, invisible to broker):

```
decompress →
  [key_len: u32][key][value_len: u32][value]   ← record 0
  [key_len: u32][key][value_len: u32][value]   ← record 1
  ...
  (repeat record_count times)
```

Each record is a `(key, value)` pair — matching Northguard's record definition ("composed of a key, a value, as well as user-defined headers, all of which are just a sequence of bytes"). Compression (LZ4/ZSTD) applies to the entire payload block — one compress/decompress per entry, amortized across all records.

**Data flow — broker never touches payload internals:**

```
Producer client: encode records → compress → EntryPayload
        ↓
Produce { segment_key, data: EntryPayload, record_count }
        ↓
Broker: stamp entry_id → WAL (routing header + blob) → cache (blob as-is) → replicate (blob as-is)
        ↓
Consumer: receive blob → decompress → parse records by record_index
```

**Batch boundaries:** A `BatchEnd` record is written at the end of each WAL flush. Flushes are closed by whichever trigger fires first: 10ms elapsed or 10MB buffered. In the WAL, `BatchEnd` marks the fsync boundary (one fsync per flush). In segment files, `BatchEnd` marks entry boundaries for readers. The sparse index writes one index entry per `entry_id` at the `BatchEnd` boundary.

**Segment file I/O:** Both reads and writes use standard buffered I/O + `posix_fadvise(POSIX_FADV_DONTNEED)`. `FADV_DONTNEED` after each write and read evicts pages so segment file I/O doesn't pollute the OS page cache. No alignment constraints, no padding, same I/O model for reads and writes. WAL also uses standard buffered I/O + fsync (write-once-read-never in normal path).

```rust
type SegmentKey = (TopicId, RangeId, SegmentId);

enum RecordType {
    Data,
    BatchEnd,
}

struct RoutingHeader {
    topic_id: TopicId,
    range_id: RangeId,
    segment_id: SegmentId,
    entry_id: u64,
    record_count: u32,
}

// Opaque blob from producer — broker never parses.
// Implements bincode Encode/Decode for wire transport (D2 ReplicaAppend).
// Deref<Target = Bytes> for zero-copy access.
struct EntryPayload(Bytes);

struct Record {
    crc32: u32,
    record_type: RecordType,
    payload: Bytes,         // WAL: routing header + entry payload. Segment file: entry payload only.
}
```

### Sparse Offset Index

Separate RocksDB instance (not the metadata RocksDB). Sparse — one entry per `entry_id`:

```
Key:   [topic_id: 8B][range_id: 8B][segment_id: 8B][entry_id: 8B]
Value: [byte_position: 8B]
```

One index entry per entry in the segment file. Consumer seek: RocksDB `seek_for_prev(target_entry_id)` → nearest indexed entry ≤ target → `pread()` segment file at that position → scan forward to exact entry. No per-record index lookup needed — one seek locates the entry, consumer uses `record_index` within it.

Sparse index trades one short sequential scan on read for dramatically fewer index writes. Rebuilt from segment files on crash — not a source of truth.

---

## Write Path

```
Produce arrives (one per producer, pre-serialized EntryPayload)
    │
    ▼
tracker.staged_entries.push(StagedEntry)
    │
    │ flush trigger fires (10ms / 10MB)
    ▼
╔═══════════════════════════════════════════════════════════╗
║ WAL: for each staged entry, write routing header + blob   ║
║      → flat interleaved write → BatchEnd → fsync          ║
║      assign entry_id to each entry                        ║
╚════════════════════════════╤══════════════════════════════╝
                         │ ◄── durability point
                         ▼
   ┌─────────── for each dirty segment ────────────────┐
   │ tracker.publish_staged(lsn)                       │
   │   for each staged entry:                          │
   │     → entry_id = next_entry_id++                  │
   │     → Arc::new(CachedEntry) → ring buffer publish │
   └─────────────────────┬────────────────────────────-┘
                         │ ◄── in cache, not yet visible
                         ▼
   ┌─────────── D2+: replicate ─────────────────┐
   │ send per-entry ReplicaAppend to followers  │
   │ wait ALL replica fsync ACK                 │
   └─────────────────────┬─────────────────────-┘
                         │
                         ▼
   read_cursor advance (per entry, after all replicas ACK)
   notify.notify_waiters()  ◄── consumers can read
                         │
                         ▼
   ACK producer
                         │
                         ▼ (background)
   checkpoint worker       size_bytes → RollSegment at ~1GB
```

WAL fsync is the sole disk write on the critical path. Cache publish is two atomic stores — negligible. Replication adds one network RTT. In D1 (no replication), replication is skipped — `read_cursor` advances immediately after `write_cursor`, consumers are notified, and ACK follows.

---

## Read Path

```
Consume(topic, range, entry_id, record_index)
    │
    ▼
position < eviction_frontier?
    ├── yes → COLD: dispatch to cold read pool (see below)
    │
    ├── no, position < read_cursor?
    │       └── yes → HOT: cache.read_committed(position) — Arc clone, no lock
    │
    └── position == read_cursor?
            └── yes → TAILING: cache.notified().await — wake on read_cursor advance
```

Consumer offset tracking: consumers track their read position client-side via `(entry_id, record_index)`. The server is stateless with respect to consumer position. Consumer group offset commit and resume semantics are deferred (see roadmap backlog "Consumer Groups").

**Hot/cold transition:** Consumer checks `eviction_frontier` on each iteration. When reading behind the frontier, it dispatches to cold read pool. Once its position catches up to `eviction_frontier`, the next read switches to the hot cache path (direct `Arc` clone from the ring buffer). The transition is seamless — same consumer task, same stream, just a different data source.

---

## Components

### WAL (WalStorage + WalWriter + Lifecycle)

WAL is the durability foundation — all other components depend on it. `SegmentRingBuffer` entries carry WAL LSNs, `DataPlane` writes through the WAL before publishing to cache.

```rust
// WAL I/O trait — injected into DataPlane. Mock in tests, WalWriter in production.
trait WalStorage {
    fn write_batch(&mut self, records: &[u8]) -> io::Result<u64>;  // returns LSN
    fn fsync(&mut self) -> io::Result<()>;
    fn rotate(&mut self) -> io::Result<()>;                        // seal active, open next
    fn delete_below(&mut self, watermark_lsn: u64);                // unlink eligible files
    fn buf(&mut self) -> &mut Vec<u8>;                             // shared serialization buffer
    fn next_lsn(&self) -> u64;                                    // for timer seq dedup
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

**LSN (Log Sequence Number):** Each WAL flush gets a node-local LSN — a monotonically increasing `u64` assigned by DataPlane. LSN is carried by each `CachedEntry` in the ring buffer, enabling the checkpoint worker to report last-checkpointed LSN back to DataPlaneActor without recomputing from file positions.

**Deletion:** A sealed WAL file is eligible for deletion once ALL entries in it have been checkpointed — flushed to their respective segment files and fsynced. Deletion gated by a **checkpoint LSN watermark**: each `WalFileEntry` covers a contiguous LSN range (`min_lsn` to `max_lsn`). The checkpoint worker reports LSN via `CheckpointComplete` to DataPlaneActor; the global checkpoint watermark is the minimum `checkpoint_lsn` across all active `SegmentTracker`s. A WAL file is unlinked when the global checkpoint watermark exceeds its `max_lsn`.

**Checkpoint LSN relationships:**

```
checkpointed_lsn (per-segment)                    — reported by checkpoint worker in CheckpointComplete
        │                                            "I flushed entries up to this WAL LSN for this segment"
        ▼
segments[key].checkpoint_lsn (DataPlane)           — SegmentTracker field, accumulates latest
        │                                            checkpointed_lsn from each segment
        ▼
global_checkpoint_watermark                        — min(segments.values().map(|t| t.checkpoint_lsn))
        │                                            "all segments have been checkpointed up to here"
        ▼
WAL deletion: unlink files where max_lsn ≤ watermark

eviction_frontier (per-segment, in SegmentRingBuffer)   — ring buffer slot position (not an LSN)
                                                     tracks which cache slots can be reused
                                                     NOT comparable across segments or to WAL LSNs
```

```
WAL file lifecycle:

  wal-000001.log  [active]       → entry writes + fsync
                  [sealed]       → size limit reached, new file opened
                  [checkpointing] → cache flushing entries to segment files
                  [deleted]      → all entries checkpointed, unlink

Deletion condition for wal-NNNNNN.log:
  checkpoint_watermark = min(each segment's last-checkpointed LSN)
  wal_file.max_lsn ≤ checkpoint_watermark
```

**Bounded WAL size:** Under normal operation, the lag between WAL write and checkpoint is bounded by checkpoint frequency and cache size. WAL accumulation is bounded by the slowest checkpoint across all active segments on the node. Checkpoint frequency balances WAL retention (unflushed entries keep WAL files alive) against disk I/O scheduling (larger, less frequent flushes are more efficient).

### SegmentRingBuffer

Lock-free ring buffer of immutable `Arc<CachedEntry>` entries. Single writer (`DataPlane` calls `publish` + `advance_read_cursor`), multiple readers (consumer tasks). No locks. Two cursors: `write_cursor` is the write cursor — where the next entry will be stored; `read_cursor` is the read cursor — consumers only see entries behind `read_cursor`. In D1 they advance together; in D2+ `read_cursor` lags by one replication RTT.

```rust
struct SegmentRingBuffer {
    // All fields private — access only through methods below.
    batches: [ArcSwapOption<CachedEntry>; CAPACITY],
    write_cursor: AtomicU64,
    read_cursor: AtomicU64,
    eviction_frontier: AtomicU64,
    notify: tokio::sync::Notify,
}

impl SegmentRingBuffer {
    // Write path — called by DataPlane via SegmentTracker
    fn publish(&self, entry: Arc<CachedEntry>);              // store into slot, advance write_cursor
    fn advance_read_cursor(&self, new_offset: u64);          // advance read_cursor, notify_waiters()

    // Read path — called by consumer tasks
    fn load_read_cursor(&self) -> u64;                       // Acquire load
    async fn notified(&self);                                // notify.notified().await
    fn read_committed(&self, position: u64) -> Option<Arc<CachedEntry>>;  // committed only
    fn load_published(&self, position: u64) -> Option<Arc<CachedEntry>>;  // committed + uncommitted
    fn load_eviction_frontier(&self) -> u64;                 // Acquire load

    // Checkpoint path — called by checkpoint worker
    fn drain_for_checkpoint(&self) -> CheckpointBatch;       // reads frontier..read_cursor, returns entries + new_frontier
    fn advance_eviction_frontier(&self, new_frontier: u64);  // Release store
}

struct CachedEntry {
    data: EntryPayload,        // opaque blob from producer — broker never parses
    record_count: u32,         // number of records in this entry
    entry_id: u64,             // broker-assigned, per-segment monotonic
    lsn: u64,                  // WAL LSN for checkpoint tracking
}
```

Slots use `arc_swap::ArcSwapOption` — load + refcount increment is atomic, eliminating the dangling-pointer risk that raw `AtomicPtr` + manual `Arc::increment_strong_count` has when a consumer is preempted at the hot/cold boundary. `publish` calls `ArcSwapOption::store()`, readers call `load_full()` which returns `Option<Arc<…>>` with the refcount already incremented. On eviction, the old Arc is dropped when the slot is overwritten — entry freed when last consumer releases theirs.

**Ordering:** Within a segment, entries are ordered by `entry_id`. Multiple `Produce` commands from different producers are staged as separate `StagedEntry`s and published in FIFO order — each with its own `entry_id`. Cross-segment ordering is not guaranteed (and not needed — event streaming guarantees ordering per-range, not globally).

### DataPlane + DataPlaneActor

Dedicated OS thread — WAL fsync blocks without affecting the tokio runtime (SWIM, Raft, consumer TCP). Uses `crossbeam::channel` for its mailbox (blocking recv, higher throughput than tokio `mpsc`). No async, no tokio dependency.

Follows the sync-first pattern (see `build-actor` skill): pure sync state machine (`DataPlane`) holds all logic and buffers side effects, actor wrapper (`DataPlaneActor`) owns channels and drains events.

WAL does blocking file I/O (write, fsync, rotate, unlink) — injected via `WalStorage` trait so `DataPlane` can be tested with a mock WAL that records calls without touching disk. Same pattern as `Raft` + `RaftStorage` (see `src/clusters/raft/storage.rs`).

```rust
struct StagedEntry {
    data: EntryPayload,             // opaque blob from producer
    record_count: u32,
    segment_key: SegmentKey,
}

struct SegmentTracker {
    cache: Arc<SegmentRingBuffer>,
    size_bytes: u64,                     // accumulated data bytes, triggers RollSegment at ~1GB
    checkpoint_lsn: u64,                 // last-checkpointed WAL LSN
    segment_file_path: PathBuf,
    role: SegmentRole,                   // Leader | Follower (D2)
    replica_set: Vec<NodeId>,            // [leader, follower, ...] (D2)
    committed_entry_id: u64,             // last committed entry_id (D2)
    next_entry_id: u64,                  // next entry_id to assign on publish
    staged_entries: Vec<StagedEntry>,    // accumulated entries awaiting flush
}

// State machine — pure sync, no channels. I/O only through injected WalStorage.
// Reply channels tracked by ReplicationState, not held in state machine events.
struct DataPlane<W: WalStorage> {
    node_id: NodeId,
    wal: W,
    segments: HashMap<SegmentKey, SegmentTracker>,
    dirty_segments: Vec<SegmentKey>,
    pending_events: Vec<DataPlaneEvent>,
    buffer_byte_count: usize,
    needs_flush: bool,
    data_dir: PathBuf,
    replication: ReplicationState,
}

enum DataPlaneEvent {
    CheckpointRequired(CheckpointJob),
    BatchFlushTimerScheduled(TimerCommand<BatchFlushTimer>),
    BatchPublished(BatchPublished),                // D2: replication fan-out
    ReplicaAckReceived(ReplicaAckReceived),         // D2: follower ack arrived
    InterNodeCommandQueued(InterNodeCommandQueued), // D2: send inter-node command
    ReplicationTimedOut(ReplicationTimedOut),        // D2: replication timeout
}

// Producer-facing response — Ok or Err. Error reason is opaque string
// (segment not found, WAL failure, replication failure in D2+).
enum ProduceAck {
    Ok,
    Err(String),
}

// Actor wrapper — owns channels, drains state machine events.
struct DataPlaneActor;

impl DataPlaneActor {
    fn spawn(
        data_dir: PathBuf,
        checkpoint_tx: crossbeam::channel::Sender<CheckpointJob>,
        batch_scheduler: SchedulerSender<BatchFlushTimer>,
        repl_scheduler: SchedulerSender<ReplicationTimer>,
        transport_tx: tokio::sync::mpsc::Sender<DataTransportCommand>,
    ) -> crossbeam::channel::Sender<DataPlaneCommand> {
        // spawns dedicated OS thread, returns sender
    }
}

enum DataPlaneCommand {
    Produce(Produce),
    CheckpointComplete(CheckpointComplete),
    Timeout(DataPlaneTimeoutCallback),
    InterNode(DataPlaneInterNodeCommand),  // all inter-node commands via transport
}

struct Produce {
    segment_key: SegmentKey,    // resolved by broker's routing layer, not from producer
    data: EntryPayload,         // opaque blob from producer client
    record_count: u32,          // producer declares record count
    reply: oneshot::Sender<ProduceAck>,
}
```

**Event flow for produce:**

```
handle_command(Produce { reply, .. })
    │
    ├── segment not found or follower → reply.send(Err) immediately
    │
    └── segment exists, role == Leader
            │
            ▼
        replication.enqueue_reply(key, reply)  ◄── reply tracked in ReplicationState
        tracker.stage_entry(key, data, record_count) ◄── push to staged_entries
            │
            │ flush trigger fires (10ms / 10MB / needs_flush)
            ▼
        flush_batch()
            │
            ├── WAL write/fsync fails → replication.fail_all(err)
            │       drains all pending + in-flight replies with Err
            │
            └── WAL fsync succeeds
                    publish_staged(lsn) for each dirty segment
                    │ (each staged entry gets its own entry_id)
                    │
                    ├── Leader, no followers → commit_entry, drain pending_replies with Ok
                    ├── Leader, followers → emit BatchPublished (replication fan-out)
                    │       dispatch: begin_replication → send ReplicaAppend per entry
                    │       on all acks → commit_segment → drain replies with Ok
                    └── Follower → send ReplicaAck to leader via InterNodeCommandQueued
```

**Channel traffic profiles:** `mailbox` is hot — every `Produce` request flows through it. `checkpoint_tx` and `data_plane_tx` (the return path for `CheckpointComplete`) are occasional — triggered only by checkpoint conditions (segment size, cache budget, WAL retention), not by time or per-batch. The checkpoint worker is a single sequential thread; each cycle includes at least one segment file fsync, so `CheckpointComplete` messages arrive at most low tens per second under sustained throughput.

Built from WAL replay + segment directory scan at startup (D5).

**Segment tracker lifecycle:**
- Created on `SegmentAssignment` from coordinator via `data_port` (D3). Produce targeting an unknown segment is rejected — producer retries after routing refresh
- Stays in HashMap even when idle (lightweight — just ring buffer + atomics, no thread/task)
- Removed on segment seal: final checkpoint via checkpoint worker, then `SegmentTracker` entry dropped
- No spawn/stop lifecycle — `SegmentTracker` and its `SegmentRingBuffer` are data structures, not actors

**Backpressure:** Natural. If WAL fsync is slow, the dedicated thread stalls, the inbound `crossbeam` channel fills, producers block on `send()`. No async flow control needed.

**Timer:** Two timer types: `BatchFlushTimer` (10ms deadline) and `ReplicationTimer` (1s replication timeout, D2), both implementing `TTimer`. Each has its own `SchedulerSender`. Callbacks arrive as `DataPlaneTimeoutCallback` variants via `DataPlaneCommand::Timeout`.

### Checkpoint Worker

Single dedicated OS thread. Sequential pipeline: read entries → write segment file → fsync → update sparse index → advance eviction frontier → report. No protocol logic, no state transitions — just a worker loop. Parallelism wouldn't help — checkpoint is write-bound (sequential writes to one stream, fsync dominates), and multiple concurrent fsyncs to the same disk add contention, not throughput. Cold read pool uses multiple threads because reads are latency-bound with independent `pread()` calls across different files. No caller waits on the checkpoint result (fire-and-forget from DataPlaneActor).

```rust
fn run_checkpoint_worker(
    sparse_index: rocksdb::DB,
    mailbox: crossbeam::channel::Receiver<CheckpointJob>,
    data_plane_tx: crossbeam::channel::Sender<DataPlaneCommand>,
) {
    // loop { recv job → read entries → write .seg → fsync → FADV_DONTNEED
    //        → update sparse_index → advance eviction_frontier → send CheckpointComplete }
}

struct CheckpointJob {
    segment_key: SegmentKey,
    cache: Arc<SegmentRingBuffer>,
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
                                   read entries from cache
                                   (read-only, behind write_cursor)
                                       │
                                       ▼
                                   write entry payload to .seg file
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
  segments[key].checkpoint_lsn = lsn      segment_key,
  watermark = min(all lsns)              checkpointed_lsn
  unlink WAL files ≤ watermark         }
```

Entries behind `eviction_frontier` with `Arc::strong_count() == 1` (only the cache slot holds a ref — no active consumer reading them) are freed. Entries still held by consumers stay alive via `Arc` refcount until those consumers advance past them.

### Checkpoint Triggers and Cache Policy

Segment files are derived from WAL — they exist for cold read performance, not durability. Checkpoint is driven by data volume, not time. No checkpoint timer needed.

**Ring buffer capacity:** configurable slot count per segment (e.g., 1024 entries ≈ 32MB). Node-level hard cap on total ring buffer memory across all segment caches (e.g., 4GB). Data stays in cache as long as possible — longer residency means more hot reads served from memory.

**Two checkpoint triggers** — checkpoint happens when there's a reason, not on a clock:

1. **Segment size approaching limit.** `DataPlane` tracks `segments[key].size_bytes` — accumulated data bytes per segment, incremented on each `stage_entry`. When approaching 1GB, submits a checkpoint job — one large, efficient bulk write. After checkpoint, the segment rolls (`RollSegment`).
2. **Node cache budget pressure.** When total cache memory approaches the node-level budget, DataPlaneActor force-checkpoints the segments with the most uncheckpointed entries to free cache slots.

All triggers submit work to checkpoint worker. The `eviction_frontier` is the single source of truth — triggers check it against `write_cursor` to determine what needs flushing. When the ring wraps, the writer overwrites the oldest slot — but only if it's behind the eviction frontier (already checkpointed) and has `Arc::strong_count() == 1` (no active reader). If the oldest slot is still held, checkpoint is forced before advancing.

### Cold Read Pool

Fixed-size thread pool (e.g., 4 threads). Cold reads are initiated by many independent consumer tasks, each waiting on the result. A single thread would serialize all consumers behind one read. Multiple threads serve concurrent consumers in parallel — each `pread()` is independent (different file positions, different segment files). Consumer-visible latency directly depends on pool capacity.

```rust
struct ColdReadRequest {
    segment_key: SegmentKey,
    start_entry_id: u64,
    max_bytes: u64,
    respond_tx: oneshot::Sender<ColdReadResult>,
}

struct ColdReadResult {
    entries: Vec<EntryPayload>,
    next_entry_id: u64,
}
```

```
Consumer task                        cold_read_pool (dedicated threads)
─────────────────────                ──────────────────────────────────
position < eviction_frontier
or segment sealed (no cache)

send ColdReadRequest {
  segment_key, start_entry_id,
  max_bytes, respond_tx
} ─── crossbeam channel ────────► recv ColdReadRequest
                                      │
                                      ▼
                                  seek_for_prev(start_entry_id)
                                  → byte_position
                                      │
                                      ▼
                                  pread(seg_file, byte_position)
                                  scan forward to exact entry
                                      │
                                      ▼
                                  POSIX_FADV_DONTNEED on range
                                      │
                                      ▼
.await oneshot ◄───────────────── respond_tx.send(ColdReadResult)
(non-blocking)

yield entry to consumer stream
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

When replication (D2+) partially fails, entries may be in the leader's cache (`write_cursor` advanced) but not yet committed (`read_cursor` not advanced). Two sub-cases:

**Follower fails, leader healthy:** The leader seals the segment with `end_entry_id = committed_entry_id` — the last entry_id ACKed by all replicas. Entries at positions `read_cursor` through `write_cursor - 1` are uncommitted: cached on the leader and fsynced in the leader's WAL, but never ACKed to the producer. The leader drains these uncommitted entries from cache via `uncommitted_entries()` and replays them into the new segment (opened after `RollSegment`), where they will be re-committed with the new replica set. Producers whose entries were in the uncommitted window also retry (no ACK received).

**Leader crashes after WAL write:** WAL entries beyond the sealed segment's `end_entry_id` are orphaned. On recovery (D5), WAL replay compares each entry's `entry_id` against the segment's `end_entry_id` — entries past it belong to the old, sealed segment and are skipped. The producer never received an ACK for these entries and retries against the new segment leader.

In D1 (no replication, `read_cursor == write_cursor`), this subsection does not apply — there is no replication failure mode. See invariant 14.

---

## Invariants

1. **Single active WAL file.** Writes go to exactly one file at any time. Rotation atomically seals the current file and opens the next.

2. **Active file is last in `files`.** `WalWriter.files.back()` is always the active file (`file: Some(...)`); all preceding entries are sealed (`file: None`). Rotation takes the active entry's `File` handle (`Option::take`) and pushes a new active entry to the back.

3. **LSN monotonicity.** WAL LSNs are monotonically increasing. Each flush's LSN is strictly greater than the previous flush's LSN, across all WAL files.

4. **Segment file entries ordered by entry_id.** Entries within a segment file are strictly ordered by `entry_id`. No gaps, no reordering.

5. **WAL deletion gated by checkpoint watermark.** A WAL file is deleted only when `global_checkpoint_watermark ≥ wal_file.max_lsn`. The watermark is the minimum last-checkpointed LSN across all active segments.

6. **Cache spans eviction frontier to tail; readers bounded by read_cursor.** Entries between `eviction_frontier` and `write_cursor` are always in the ring buffer. Consumer-visible range is positions `eviction_frontier` through `read_cursor - 1`. Entries at positions `read_cursor` through `write_cursor - 1` exist in cache but are not yet visible to consumers. Entries behind `eviction_frontier` may still exist (held by consumer `Arc` refs) but are eligible for slot reuse. Entries at or beyond `write_cursor` do not exist.

7. **Checkpoint is idempotent.** Both triggers (segment size, cache budget) check `eviction_frontier` against `write_cursor`. No new entries since last checkpoint → no-op.

8. **Segment file I/O uses buffered I/O + `POSIX_FADV_DONTNEED`.** Both checkpoint writes and cold reads use standard buffered I/O. `FADV_DONTNEED` after each operation evicts pages from OS page cache.

9. **One SegmentTracker per segment per node.** `segments` enforces at most one tracker (and its cache) per `(topic_id, range_id, segment_id)` key.

10. **Segment file ≤ 1GB.** DataPlaneActor signals `RollSegment` when approaching the limit. Enforced at the write path — never retroactively truncated.

11. **WAL fsync is the durability boundary.** Entries are durable only after the WAL flush containing them is fsynced. Producer ACK sent after WAL fsync + cache publish + replication. Segment files and sparse index are derived — their absence never means data loss.

12. **Node-level cache budget is a hard cap.** Sum of all ring buffer allocations never exceeds the configured budget. Under pressure, force checkpoint to advance eviction frontiers; if all entries are uncheckpointed, backpressure propagates to WAL dispatch.

13. **Published entries are immutable.** No mutation after `write_cursor` advancement. Writer never modifies data behind `write_cursor`. Readers only access data behind `write_cursor`. This is the SWMR (single-writer, multiple-reader) invariant that eliminates locking.

14. **`eviction_frontier` ≤ `read_cursor` ≤ `write_cursor` always.** Three monotonically advancing markers partition the ring buffer into zones. In D1 (no replication), `read_cursor == write_cursor` — both advance together on cache publish. In D2+, `read_cursor` lags `write_cursor` by one replication RTT: `write_cursor` advances on cache publish, `read_cursor` advances after all replicas ACK.

15. **Sealed segment `end_entry_id` = last committed entry_id.** On seal, `end_entry_id` is set to `committed_entry_id` at seal time, not `write_cursor`. Entries at positions `read_cursor` through `write_cursor - 1` are uncommitted — replayed into the next segment by the leader (see "Uncommitted Tail on Replication Failure").

16. **Consumer read position determines data source.** A consumer at `position` follows exactly one path: `position < eviction_frontier` → cold read path (cold read pool); `eviction_frontier ≤ position < read_cursor` → hot cache read (clone `Arc` from ring buffer); `position == read_cursor` → tailing (await `notify`). Consumers never read at or beyond `read_cursor` — entries at positions `read_cursor` through `write_cursor - 1` are uncommitted and invisible. This prevents consumers from observing entries that may be rolled back on replication failure.

17. **Broker is opaque to entry payloads.** The broker never parses, re-serializes, or merges `EntryPayload` contents. Producer client encodes records, consumer client decodes them. The broker stamps `entry_id`, writes the blob to WAL, caches it, and replicates it as-is. Compression is end-to-end between producer and consumer.

18. **One Produce = one entry = one entry_id.** Each `Produce` command stages one `StagedEntry`. Multiple produces accumulate as separate entries in `staged_entries`. On flush, each gets its own monotonically increasing `entry_id`. Entries from different producers are never merged.
