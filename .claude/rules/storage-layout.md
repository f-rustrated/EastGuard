# Storage Key Layout (Invariants)

All shard groups share a single RocksDB default column family. Each key is prefixed with the shard group ID to isolate groups within the same keyspace. For the design rationale, see `docs/metadata-management/d2_replicated_apply_and_durability.md` § "Shared storage, isolated key ranges".

## Key Structure

```
[group_id: 8 bytes BE][key_type: 1 byte][optional index: 8 bytes BE]
```

| Key Type | Prefix | Suffix | Total Size |
|---|---|---|---|
| LogEntry | 0x01 | u64 BE index | 17 bytes |
| HardState | 0x02 | none | 9 bytes |
| SnapMeta | 0x03 | none | 9 bytes |
| SnapData | 0x04 | none | 9 bytes |
| AppliedIndex | 0x05 | none | 9 bytes |
| Epoch | 0x06 | none | 9 bytes (reserved) |

## Invariants

1. **Lexicographic order matches numeric order.** Big-endian encoding for both group ID and log index. RocksDB sorts by raw bytes, so big-endian gives us group-contiguous storage and in-group log entries sorted by index. Little-endian would scatter entries and break range scans.

2. **Within a group, log entries sort before metadata.** Key types are ordered `0x01 (LogEntry) < 0x02 (HardState) < 0x03+ (reserved)`. Enables prefix-bounded range scans that cover only log entries (`[group_id][0x01]..[group_id][0x02]`).

3. **Group isolation via prefix.** Every key for a group starts with the same 8-byte `group_id`. Deleting a group is a single range delete `[group_id]..[group_id + 1]`. Scanning a group's log is a single range scan. No iteration across groups; no key collisions between groups.

4. **Flush is atomic.** `persist_mutations()` writes all log mutations and hard-state updates for all dirty groups in one `WriteBatch`. Either the whole batch lands or none of it does — partial application would let some groups advance their stable index while others lost entries.

5. **Log index is 1-based.** Index 0 is never stored. A `stabled_index` of 0 means "no entries persisted yet"; index 0 is not a valid log slot. Removes the ambiguity between "no entries" and "entry at index 0".

6. **Truncation preserves non-log keys.** `TruncateFrom(index)` deletes log entries from `index` onward but never touches `HardState` or other metadata keys. Upper bound of the delete is `[group_id][0x02]`, exclusive. Losing hard state would break Raft's term/voted-for guarantees.

7. **`stabled_index` advances only after a successful flush.** Set only after `persist_mutations()` returns successfully. Apply is bounded by `min(commit_index, stabled_index)` — entries cannot be applied before they are durable. Apply-before-durable would let a crash erase already-applied state.

8. **Snapshot publication and prefix compaction are atomic.** Snapshot metadata, contents, applied index, and deletion of covered log entries share one synced `WriteBatch`. A crash exposes either the previous valid snapshot and log or the complete replacement; it cannot expose a compacted prefix without the snapshot that replaces it.

9. **Snapshot contents are validated before restore.** Stored size, CRC32 checksum, and applied index must match the snapshot metadata. Corruption fails recovery explicitly rather than exposing partial metadata or silently replaying from an unavailable prefix.
