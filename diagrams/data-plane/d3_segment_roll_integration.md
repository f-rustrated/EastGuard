# Phase D3: Segment Roll Integration

**Goal:** Connect storage engine lifecycle to metadata lifecycle. Four triggers for segment seal.

**Depends on:** Phase D2 (replication), existing metadata Phase 6 (auto-split/merge).

---

## Seal Triggers

| Trigger | Source | Mechanism |
|---|---|---|
| Size limit (~1GB) | SegmentActor monitoring `size_bytes` | Segment leader sends `SealRequest` to coordinator → coordinator proposes `RollSegment` |
| Follower failure | Write-path timeout on `ReplicaAppend` (leader detects) | Segment leader sends `SealRequest` to coordinator → coordinator proposes `RollSegment` with updated `replica_set` |
| Time limit (1 hour) | SegmentActor monitoring segment age | Segment leader sends `SealRequest` to coordinator → coordinator proposes `RollSegment` |
| Node death (leader or follower) | SWIM `NodeDead` event | Coordinator proposes `RollSegment` for all affected active segments (coordinator-initiated, no broker request needed). For leader failure, a surviving follower is promoted to `replica_set[0]`. Write-path timeout and SWIM may race for the same follower failure — `apply_roll_segment()` precondition check makes duplicate proposals no-ops. |

All triggers result in the same Raft command (`RollSegment`). MetadataStateMachine applies it identically regardless of trigger. First three are broker-initiated (via `SealRequest`, which carries `end_offset` — see D2 "Offset Handoff"). Last one is coordinator-initiated (SWIM event processed directly by coordinator — coordinator queries the segment leader for `end_offset` before proposing, or the `HandleNodeDeath` path resolves it from the last known committed offset).

**Under-replication detection on node death:** MetadataStateMachine maintains a reverse index for fast lookup:

```
node_active_segment_index: HashMap<NodeId, Vec<(TopicId, RangeId, SegmentId)>>
```

On `HandleNodeDeath(F)`: look up F in the index → active segments get `RollSegment` (seal, replace F). Sealed segments found by scanning MetadataStateMachine segment metadata, filtering by `replica_set.contains(F)` — O(all sealed segments), but node death is infrequent and the scan takes milliseconds. Sealed segments with F in their `replica_set` are marked under-replicated and queued for repair (see D2 "Sealed Segment Replication").

**`size_bytes` tracking:** During normal operation, SegmentActor tracks `size_bytes` locally — updating `SegmentMeta.size_bytes` via Raft per produce would be prohibitively expensive. `SegmentMeta.size_bytes` in MetadataStateMachine is set once at seal time (final size carried in `SealRequest`/`RollSegment`). SegmentActor is the authority during the segment's active lifetime; MetadataStateMachine records the final value for sealed segment metadata.

## Data Port Event Propagation

Metadata changes propagated to data nodes via `data_port` TCP — not SWIM gossip. SWIM stays coarse-grained (membership + shard leaders only).

After Raft commits a segment lifecycle change, the coordinator directly notifies affected brokers:

- `SegmentAssignment { shard_group_id, range_id, segment_id, replica_set }` — coordinator → new segment leader (and leader bootstraps followers via self-authorizing ReplicaAppend)
- `SegmentSealed { shard_group_id, range_id, segment_id }` — segment leader → old followers
- Range split/merge: coordinator sends `SegmentAssignment` for new child/merged segments to their respective segment leaders

For nodes not directly involved (e.g., routing queries), metadata query to the vnode leader is the fallback.

## Segment Leader Determination

Segment leader = `replica_set[0]`. Deterministic from metadata — any node reading `SegmentMeta` knows the leader without election or extra state.

`replica_set` ordering is set at segment creation time by the coordinator proposing `CreateTopic` or `RollSegment`. The coordinator chooses `replica_set[0]` based on load distribution — it can spread data write leadership across nodes by varying which node is placed first. This is independent of Raft leadership: the coordinator handles metadata consensus (small writes), while `replica_set[0]` handles data writes (high throughput, fan-out replication). Different concerns, potentially different nodes.

On seal-and-replace, the coordinator preserves the previous segment leader at `replica_set[0]` when possible — preserving cache locality and active producer connections. Only when the leader itself failed does a new node take position 0 (a surviving follower is promoted). Initial segment creation uses least-loaded placement (see below).

Producer routing: client discovers the segment leader via `GetShardInfo` or produce-forwarding (same 1-hop pattern as metadata propose).

## Replica Placement

When the coordinator (vnode leader) creates a new segment or seals-and-replaces, it selects `replica_set` based on:

1. **Replication factor** from `StoragePolicy` — determines set size (e.g., 3 nodes)
2. **Least-loaded nodes** — coordinator picks healthy nodes with fewest active segment leadership assignments, spreading data write load across the cluster
3. **Exclude failed node** — on seal-and-replace, the failed node is excluded from the new set
4. **Distinct physical nodes** — no node appears twice in a `replica_set`

The coordinator tracks active segment counts per node as part of MetadataStateMachine state (derivable from scanning segments, but a running counter avoids the scan). On segment creation, it sorts candidate nodes by active segment count and picks the least loaded.

This is a simple initial strategy. Future enhancements (rack-awareness, disk-type affinity, constraint expressions) can refine placement without changing the metadata or replication protocol.
