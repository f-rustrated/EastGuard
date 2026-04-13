# Storage Engine

## Purpose

`LogStore` is an append-only log abstraction for durable, indexed log entry persistence.
Its only responsibility is to store and retrieve log entries safely — crash recovery, write ordering,
and truncation correctness. It should have no knowledge of other components.

## Concepts

- **Append-only**: entries are only ever added to the tail or truncated from the tail — no random writes.
- **Indexed**: each entry has a 1-based `Index` (`u64`); `0` is reserved to mean "before the log begins".
- **Write ordering**: data must be written before the index pointer that references it. An orphaned data tail is detectable on recovery; a dangling index pointer is not.
- **Truncation safety**: disk must be updated before in-memory state. A crash mid-truncation must never leave the engine believing fewer entries than are on disk.
- **Durability is explicit**: `sync()` must be called to make writes durable. Unflushed entries may be lost on crash.
- **One store per directory**: never share a `FileLogStore` across callers.

## Invariants

1. `Entry.index` must be monotonically increasing. `append_log` does not validate this — the caller is responsible.
2. `truncate_log(from)` removes all entries at and after `from`. The caller ensures only safe entries are truncated.

## Key Types

| Type | File |
|---|---|
| `LogStore` | `src/storage/mod.rs` |
| `FileLogStore` | `src/storage/disk.rs` |
| `MemoryLogStore` | `src/storage/memory.rs` |

## Open TODOs

- **Snapshot support**: allows `first_index` to advance past 1 (Phase 2).
- **Log compaction**: delete the log once a snapshot covers all entries.
- **CRC checksums**: detect silent data corruption.
- **LRU read cache**: cache hot entries to avoid repeated disk seeks.
