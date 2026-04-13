# Storage Engine

## Purpose

`StorageEngine` is an append-only log abstraction for durable, indexed log entry persistence.
Its only responsibility is to store and retrieve log entries safely — crash recovery, write ordering,
and truncation correctness. It should have no knowledge of other components. 

## Directory Layout

```
<base_dir>/
    data.log     ← [entry_len: u32 LE (4 B) | entry_data (entry_len B)]*
    index.log    ← [byte_offset: u64 LE (8 B)]*  (one entry per log entry)
    snapshot/    ← Phase 2 (not yet implemented)
```

`index.log[i]` stores the byte offset in `data.log` of entry `(first_index + i)`.

## Entry Format

`Entry.data` is raw bytes — no envelope, no timestamp, no key.
`Entry.index` (a `u64`) is the log index (1-based; 0 is reserved).

## Single-File Design

`DiskEngine` uses one `data.log` and one `index.log` — no chunking.

Appropriate for small, infrequently written metadata logs: entries are small, `fsync()` and
`set_len()` costs are negligible, and a single snapshot suffices for compaction.

**Write order**: `data.log` before `index.log`. An orphaned data tail is detectable on recovery;
a dangling index pointer is not.

**Truncation**: `set_len()` on both files. Disk updated before in-memory state to prevent
divergence if the process crashes mid-truncation.

## In-Memory State

```rust
pub struct DiskEngine {
    base_dir: PathBuf,
    first_index: Index,  // always 1 until snapshot support (Phase 2)
    num_entries: u64,
    data_bytes: u64,     // mirrors data.log size; verified by debug_assert in append_log
}
```

No file handles are kept open. The OS page cache provides read-after-write performance.

## Key Types

| Type | File | Description |
|---|---|---|
| `StorageEngine` | `src/storage/mod.rs` | Trait: `append_log`, `get_range`, `get`, `get_last`, `truncate_log`, `sync` |
| `DiskEngine` | `src/storage/disk.rs` | Persistent single-file implementation. |
| `MemEngine` | `src/storage/memory.rs` | In-memory implementation. |
| `Entry` | `src/storage/mod.rs` | `{ index: u64, data: Vec<u8> }` — raw bytes, no envelope. |
| `Index` | `src/storage/mod.rs` | `u64`, 1-based. `0` is reserved ("before the log begins"). |

## Invariants

1. **One DiskEngine per directory.** Never share an engine across callers.
2. **`Entry.index` must be monotonically increasing.** `append_log` does not validate this — the caller is responsible.
3. **`sync()` must be called to make writes durable.** Unflushed entries may be lost on crash.
4. **`truncate_log(from)` removes all entries at and after `from`.** The caller ensures only safe entries are truncated.

## Open TODOs

- **Snapshot support** (`snapshot/` directory): allows `first_index` to advance past 1 (Phase 2).
- **Log compaction**: delete the log once a snapshot covers all entries.
- **CRC checksums**: detect silent data corruption in `data.log`.
- **LRU read cache**: cache hot entries to avoid repeated disk seeks.
