# Storage Key Layout

## Overview

All shard groups share a single RocksDB default column family. Each key is prefixed with the shard group ID to isolate groups within the same keyspace.

## Key Structure

```
[group_id: 8 bytes BE][key_type: 1 byte][optional index: 8 bytes BE]
```

| Key Type | Prefix | Suffix | Total Size | Description |
|----------|--------|--------|------------|-------------|
| LogEntry | 0x01 | u64 BE index | 17 bytes | Raft log entry at 1-based index |
| HardState | 0x02 | none | 9 bytes | Durable Raft state (term, voted_for) |
| SnapMeta | 0x03 | none | 9 bytes | Snapshot descriptor (reserved) |
| SnapData | 0x04 | none | 9 bytes | State machine payload (reserved) |
| AppliedIndex | 0x05 | none | 9 bytes | Last applied log index (reserved) |
| Epoch | 0x06 | none | 9 bytes | Shard membership version (reserved) |

Within each group, log entries (0x01) sort before metadata keys (0x02-0x06).

## Concrete Example: 3 Shard Groups

```
Group 1 (0x0000000000000001):
  00 00 00 00 00 00 00 01 | 01 | 00 00 00 00 00 00 00 01  -> LogEntry(1)
  00 00 00 00 00 00 00 01 | 01 | 00 00 00 00 00 00 00 02  -> LogEntry(2)
  00 00 00 00 00 00 00 01 | 01 | 00 00 00 00 00 00 00 03  -> LogEntry(3)
  00 00 00 00 00 00 00 01 | 02 |                           -> HardState

Group 7 (0x0000000000000007):
  00 00 00 00 00 00 00 07 | 01 | 00 00 00 00 00 00 00 01  -> LogEntry(1)
  00 00 00 00 00 00 00 07 | 02 |                           -> HardState

Group 42 (0x000000000000002A):
  00 00 00 00 00 00 00 2A | 01 | 00 00 00 00 00 00 00 01  -> LogEntry(1)
  00 00 00 00 00 00 00 2A | 02 |                           -> HardState
```

## Why Big-Endian

RocksDB sorts keys lexicographically (byte-by-byte). Big-endian encoding makes lexicographic order match numeric order:

- All keys for group 1 cluster together
- All keys for group 7 come after group 1
- Within each group, log entries sort before metadata
- Within log entries, index 1 sorts before index 2

## Access Patterns

### Fetch All Log Entries for a Group

Prefix-bounded range scan:

```
start = [group_id][0x01][0x0000000000000000]
end   = [group_id][0x02]                      (exclusive upper bound)
```

RocksDB iterator reads only that group's log entries sequentially. `set_iterate_upper_bound` tells RocksDB to skip SST blocks past the bound. No wasted I/O on other groups.

### Point Lookup (e.g., HardState)

```
key = [group_id][0x02]
```

Direct `db.get(key)`. O(log N) in the SST block index.

### Log Truncation (TruncateFrom)

Delete entries from a given index onward:

```
start = [group_id][0x01][from_index]
end   = [group_id][0x02]              (exclusive — preserves HardState)
```

### Delete Entire Group

```
start = [group_id]
end   = [group_id + 1]               (exclusive — preserves next group)
```

Removes all keys (log entries + metadata) for that group.

## Design Rationale

Previous design used one RocksDB column family per shard group. With 768 groups (256 vnodes x 3 nodes), each `create_cf` call fsyncs the MANIFEST file (~230ms on macOS APFS), totaling ~177 seconds at startup.

Single CF with composite keys eliminates CF creation entirely. Trade-off: iterator index is shared across groups, but block index lookup is O(log N) — negligible overhead.
