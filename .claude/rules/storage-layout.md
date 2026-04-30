# Storage Key Layout

## Purpose

All shard groups share a single RocksDB default column family. Each key is prefixed with the shard group ID to isolate groups within the same keyspace.

## Key Structure

```
[group_id: 8 bytes BE][key_type: 1 byte][optional index: 8 bytes BE]
```

| Key Type | Prefix | Suffix | Total Size |
|---|---|---|---|
| LogEntry | 0x01 | u64 BE index | 17 bytes |
| HardState | 0x02 | none | 9 bytes |
| SnapMeta | 0x03 | none | 9 bytes (reserved) |
| SnapData | 0x04 | none | 9 bytes (reserved) |
| AppliedIndex | 0x05 | none | 9 bytes (reserved) |
| Epoch | 0x06 | none | 9 bytes (reserved) |

## Invariants

1. **Single CF for all groups.** No per-group column families. Previous design used one CF per shard group — with 768 groups, each `create_cf` fsynced MANIFEST (~230ms each, ~177s total at startup). Single CF with composite keys eliminates this.

2. **Big-endian encoding preserves sort order.** RocksDB sorts keys lexicographically. Big-endian makes lexicographic order match numeric order: group 1's keys cluster together, group 7 follows, within each group log entries sort by index.

3. **Log entries sort before metadata within each group.** Key type 0x01 (LogEntry) < 0x02 (HardState) < 0x03+ (other metadata). Enables prefix-bounded range scans that cover only log entries.

4. **Group isolation via prefix.** All keys for a group share the same 8-byte prefix. Deleting a group = `delete_range([group_id], [group_id + 1])`. Scanning a group's log = `range([group_id][0x01][0x00...], [group_id][0x02])`.

5. **Flush is atomic via WriteBatch.** `persist_mutations()` writes all log mutations and hard state updates for all dirty groups in a single `WriteBatch`. Either all writes succeed or none do.

6. **Log index is 1-based.** Raft log entries use 1-based indexing. Index 0 is never stored. `stabled_index` tracks the last persisted log entry.

7. **Truncation preserves metadata.** `TruncateFrom(index)` deletes log entries from `index` onward but preserves HardState and other metadata keys (upper bound is `[group_id][0x02]`, exclusive).

8. **stabled_index advances only after successful flush.** `advance_stabled_index()` called after `persist_mutations()` returns. Raft's `commit_index` is bounded by `min(commit_index, stabled_index)` — ensures entries are durable before being applied.
