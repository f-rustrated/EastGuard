# Phase D5: Crash Recovery

**Goal:** Recover data plane state after node crash. Distinct from Raft log recovery (which already works).

**Depends on:** Phase D1 (storage engine format). Phase D2 (replication) for sealed segment repair, but local recovery (Phase 1) is independent.

---

## Recovery Procedure

### Phase 1: Local WAL Replay (no network)

1. **Scan WAL files forward from oldest un-deleted file.** WAL is segmented into fixed-size files (see D1 "WAL Lifecycle"). The oldest un-deleted file is the implicit checkpoint — everything before it is already durable in segment files. Find all committed batches (CRC-verified). For each batch, identify target `(shard_group_id, range_id, segment_id)`.
2. **Replay to segment files.** For each WAL entry, check if the corresponding segment file has the record. If not (crash happened between WAL fsync and segment file fsync), append it.
3. **Rebuild sparse index.** For any segment files with records not in the index, scan forward and rebuild index entries.
4. **Truncate partial writes.** WAL and segment files may have partial records at tail (crash mid-write). Backward scan using trailing length + CRC to find last valid record, truncate.
5. **Delete fully-drained WAL files.** After replay, WAL files whose records are all in segment files can be deleted.

### Phase 1.5: Local Segment Inventory

After WAL replay, the node scans its data directory and builds a **local segment inventory**: `(shard_group_id, range_id, segment_id) → local_end_offset`. This is data that persists across restarts even though the node gets a new NodeId. The inventory is used during sealed segment repair — when the node receives a `CatchUpRequest`, it checks the inventory and advertises what it already has, skipping redundant network transfer.

**CRC verification:** Before advertising `local_end_offset` for any segment, the node verifies data integrity by CRC-checking the local segment file. Corrupt local data is discarded — the node falls back to a full copy from a healthy replica rather than advertising a stale `local_end_offset`.

### Phase 2: Metadata Discovery (requires network)

6. **Query metadata for current segment assignments.** The recovering node (with its new NodeId) queries the vnode leaders to learn its current segment assignments. During downtime, segments this node was in may have been sealed and replaced (seal-on-failure removes the crashed node from the new `replica_set`).

### Phase 2.5: Orphaned Data Cleanup

After metadata discovery, the node compares its local segment inventory against its current segment assignments. Any local segment data not in an active or sealed `replica_set` that includes this node is **orphaned** — the node was replaced while down, or the segment was deleted. Orphaned data is eligible for background GC (file-level `unlink`). Not urgent — orphaned files waste disk but don't affect correctness.

### Phase 3: Sealed Segment Repair (async, coordinator-driven)

7. **No active segment catch-up needed.** In the seal-on-failure model, when a follower crashes, the segment is sealed and a new segment is created with a replacement node. The crashed node is NOT in the new segment's `replica_set`. There is nothing to "catch up" on — the node was replaced, not lagged.

8. **Sealed segment repair with local data reuse.** The recovering node may have incomplete copies of sealed segments from its previous lifecycle. When the coordinator assigns this node to a sealed segment's `replica_set` and triggers repair via `CatchUpRequest`, the node checks its local segment inventory. If it already has partial or complete data, it advertises `local_end_offset` — the healthy replica streams only the delta. Complete matches (common when the node crashed after fsync but before the seal was processed) require zero network transfer. This is the same sealed segment repair from D2 — no special recovery protocol.

9. **Available for future assignments.** After local recovery completes, the node rejoins the cluster (SWIM alive) and becomes eligible for future segment `replica_set` assignments. New segments may include this node via the least-loaded placement strategy (D3).

## Properties

- WAL is the source of truth for crash recovery — segment files and index are derived
- Local recovery (Phase 1) is parallelizable per shard group (each group's segments are independent)
- Local recovery requires no network — purely disk I/O
- Sparse index is treated as a cache — rebuilt from segment files, not authoritative
- Node data plane startup runs local recovery (Phase 1) before spawning any SegmentActors or accepting produce/consume commands
- No active segment catch-up exists — seal-on-failure replaces crashed nodes, it does not wait for them
- Sealed segment repair is coordinator-driven and async — does not block the recovering node from accepting new work
- Local segment inventory enables data reuse across NodeId changes — reduces repair bandwidth proportionally to locally-available data

## Data Loss Semantics

With fsync-on-all-replicas before ACK:
- **ACKed records** are fsynced on ALL replicas. Survive any single-node crash. Survive any single-disk failure. Only total cluster failure loses data.
- **Un-ACKed records** were never confirmed to the producer. Producer retries. No data loss from producer's perspective.

No "un-fsynced but acked" window — this is the key difference from Kafka's deferred-fsync model.
