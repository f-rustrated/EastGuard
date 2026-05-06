# Phase D4: Segment Roll Integration

**Goal:** Connect storage engine lifecycle to metadata lifecycle. Four triggers for segment seal.

**Depends on:** Phase D3 (replication), existing metadata Phase 6 (auto-split/merge).

---

## Seal Triggers

| Trigger | Source | Mechanism |
|---|---|---|
| Size limit (~1GB) | DataActor monitoring `size_bytes` | Segment leader sends `SealRequest` to vnode leader → vnode proposes `RollSegment` |
| Replica failure | Write-path timeout on `ReplicaAppend` | Segment leader sends `SealRequest` to vnode leader → vnode proposes `RollSegment` with updated `replica_set` |
| Time limit (1 hour) | DataActor monitoring segment age | Segment leader sends `SealRequest` to vnode leader → vnode proposes `RollSegment` |
| Node death | SWIM `NodeDead` event | Vnode leader proposes `RollSegment` for all affected active segments (vnode-initiated, no broker request needed) |

All triggers result in the same Raft command (`RollSegment`). MetadataStateMachine applies it identically regardless of trigger. First three are broker-initiated (via `SealRequest`, which carries `end_offset` — see D3 "Offset Handoff"). Last one is vnode-initiated (SWIM event processed directly by vnode leader — vnode queries the segment leader for `end_offset` before proposing, or the `HandleNodeDeath` path resolves it from the last known committed offset).

**`size_bytes` tracking:** During normal operation, DataActor tracks `size_bytes` locally — updating `SegmentMeta.size_bytes` via Raft per produce would be prohibitively expensive. `SegmentMeta.size_bytes` in MetadataStateMachine is set once at seal time (final size carried in `SealRequest`/`RollSegment`). DataActor is the authority during the segment's active lifetime; MetadataStateMachine records the final value for sealed segment metadata.

## Data Port Event Propagation

Metadata changes propagated to data nodes via `data_port` TCP — not SWIM gossip. SWIM stays coarse-grained (membership + shard leaders only).

After Raft commits a segment lifecycle change, the coordinator directly notifies affected brokers:

- `SegmentAssignment { shard_group_id, range_id, segment_id, replica_set }` — coordinator → new segment leader (and leader bootstraps followers via self-authorizing ReplicaAppend)
- `SegmentSealed { shard_group_id, range_id, segment_id }` — segment leader → old followers
- Range split/merge: coordinator sends `SegmentAssignment` for new child/merged segments to their respective segment leaders

For nodes not directly involved (e.g., routing queries), metadata query to the vnode leader is the fallback.

## Segment Leader Determination

Segment leader = `replica_set[0]`. Deterministic from metadata — any node reading `SegmentMeta` knows the leader without election or extra state.

`replica_set` ordering is set at segment creation time by the Raft leader proposing `CreateTopic` or `RollSegment`. The Raft leader chooses `replica_set[0]` based on load distribution — it can spread data write leadership across nodes by varying which node is placed first. This is independent of Raft leadership: the Raft leader handles metadata consensus (small writes), while `replica_set[0]` handles data writes (high throughput, fan-out replication). Different concerns, potentially different nodes.

On seal-and-replace, the new segment gets a new `replica_set` with a potentially different `[0]` — the Raft leader can rebalance data write leadership at each segment transition.

Producer routing: client discovers the segment leader via `GetShardInfo` or produce-forwarding (same 1-hop pattern as metadata propose).

## Replica Placement

When the coordinator (vnode leader) creates a new segment or seals-and-replaces, it selects `replica_set` based on:

1. **Replication factor** from `StoragePolicy` — determines set size (e.g., 3 nodes)
2. **Least-loaded nodes** — coordinator picks healthy nodes with fewest active segment leadership assignments, spreading data write load across the cluster
3. **Exclude failed node** — on seal-and-replace, the failed node is excluded from the new set
4. **Distinct physical nodes** — no node appears twice in a `replica_set`

The coordinator tracks active segment counts per node as part of MetadataStateMachine state (derivable from scanning segments, but a running counter avoids the scan). On segment creation, it sorts candidate nodes by active segment count and picks the least loaded.

This is a simple initial strategy. Future enhancements (rack-awareness, disk-type affinity, constraint expressions) can refine placement without changing the metadata or replication protocol.
