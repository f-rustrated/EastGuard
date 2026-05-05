# Phase D1: Storage Engine

**Goal:** On-disk format for a single node — WAL, segment files, sparse index, application-level cache. No replication, no client API.

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
[crc32: 4 bytes][type: 1 byte][length: 4 bytes][payload: N bytes][length: 4 bytes]
                                                                   ^ trailing length
```

Trailing length enables O(1) backward scan from EOF — read last 4 bytes, jump back `length + 9` (header size), verify CRC. Used in both WAL and segment files.

WAL records additionally carry `(shard_group_id, range_id, segment_id)` so replay knows which segment file to write to.

Record types: `Data` (user payload), `BatchEnd` (marks flush boundary).

**O_DIRECT alignment:** O_DIRECT requires sector-aligned buffers and file offsets (typically 4KB). Individual records are variable-length, so alignment is at the **batch level** — each batch is padded to the next 4KB boundary before writing. The `BatchEnd` record absorbs the padding. Reads account for padding by scanning records within a batch and stopping at `BatchEnd`.

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
2. Write batch to WAL (O_DIRECT, aligned), fsync WAL — durability point, ACK producer here
3. Append records to segment file(s) (O_DIRECT, aligned) — async, for serving reads
4. Update sparse index in RocksDB (batch boundary entries only) — async
5. Track `size_bytes` — when approaching 1GB, signal metadata layer to propose `RollSegment`

## Read Path

1. Check application-level cache for requested offset range
2. Cache miss: resolve `(range_id, segment_id, offset)` → nearest `byte_position` via sparse index (`seek_for_prev`)
3. `pread()` on segment file (O_DIRECT) at `byte_position`, scan forward to exact offset
4. Populate application cache with fetched records
5. Stream records forward until requested `max_bytes` or EOF

## Application-Level Read Cache

Since O_DIRECT bypasses the OS page cache, the application manages its own read cache:

- **Consume-stream-aware.** Tracks active consumer sessions and their read positions. Pre-fetches ahead of active consumers.
- **Hot tail reads served from cache.** Consumer chasing the write head reads data that was just written — served from the write buffer or cache, zero disk I/O.
- **Cold reads bypass cache.** Consumers reading old sealed segments go directly to disk. No eviction of hot data.

## WAL Lifecycle

The WAL is a sequence of fixed-size log files, not a single ever-growing file. This makes deletion file-level (`unlink`) rather than truncation (which would require rewriting).

**Rotation:** When the active WAL file reaches the size limit (~64MB), it is sealed (no more writes) and a new file is opened with the next sequence number. Writes always go to exactly one file — the active one.

**Deletion:** A sealed WAL file is eligible for deletion once ALL records in it have been durably fsynced to their respective segment files. DataActor tracks the per-segment-file fsync progress and determines when a WAL file is fully drained.

```
WAL file lifecycle:

  wal-000001.log  [active]   → batch writes + fsync
                  [sealed]   → size limit reached, new file opened
                  [draining] → segment files catching up
                  [deleted]  → all records in segment files, unlink

Deletion condition for wal-NNNNNN.log:
  For every record in this file:
    the target segment file has fsynced past this record's position
```

**Crash recovery** scans from the oldest un-deleted WAL file forward (see Phase D6). The oldest un-deleted file is effectively the checkpoint — no separate checkpoint file needed.

**Bounded WAL size:** Under normal operation, the lag between WAL write and segment file fsync is small (segment file writes are async but fast — same batch, same disk). WAL accumulation is bounded by the slowest segment file writer across all active segments on the node. If a segment file writer falls behind (e.g., slow disk for one segment), the WAL files for that period are retained until it catches up.
