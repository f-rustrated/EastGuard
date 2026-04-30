# MetadataStateMachine

## Purpose

`MetadataStateMachine` — application state machine for DS-RSM. One instance per shard group, owned by `Raft`. Manages topic, range, and segment metadata. Pure synchronous, no I/O.

## Entity Hierarchy

```
MetadataStateMachine (one per shard group)
│
├── topics: HashMap<TopicId, TopicMeta>
│   └── TopicMeta
│       ├── active_ranges: Vec<RangeId>     (write routing, sorted by keyspace_start)
│       ├── ranges: HashMap<RangeId, RangeMeta>  (all ranges including sealed)
│       └── next_range_id: u64
│
│   RangeMeta (nested inside TopicMeta)
│       ├── active_segment: Option<SegmentId>
│       ├── segments: HashMap<SegmentId, SegmentMeta>
│       ├── next_segment_id: u64
│       └── next_offset: u64
│
│   SegmentMeta (nested inside RangeMeta)
│
└── topic_name_index: HashMap<String, TopicId>
```

Ownership expressed by nesting — no back-references. Parent always known from traversal context.

## Commands

| RaftCommand variant | Method | Effect |
|---|---|---|
| `CreateTopic` | `apply_create_topic()` | Creates topic + initial full-keyspace range + initial segment |
| `SealSegment` | `apply_seal_segment()` | Seals active segment, creates next segment (segment roll) |
| `SplitRange` | `apply_split_range()` | Seals parent range, creates two child ranges with new segments |
| `MergeRange` | `apply_merge_range()` | Seals both source ranges, creates merged range with new segment |
| `DeleteTopic` | `apply_delete_topic()` | Cascades Deleting state to all ranges and segments |

## Invariants

1. **Keyspace coverage.** For any Active topic, the union of its active ranges' keyspaces equals the full keyspace `[KEYSPACE_MIN, KEYSPACE_MAX]`. No gaps, no overlaps. Enforced by split/merge being the only mutations to range boundaries.

2. **Single active segment per range.** Each Active range has exactly one Active segment (the write head). Sealed/Deleting ranges have zero (`active_segment = None`).

3. **Segment immutability after seal.** Once `state = Sealed`, a segment's data never changes. Only `replica_set` can change (via Reassigning). `size_bytes`, `end_offset`, and `sealed_at` are final.

4. **Lineage consistency.** `split_into` and `merged_from`/`merged_into` form a tree. A range cannot be both split and merged. Lineage is write-once — set on seal, never modified.

5. **Delete cascades downward.** Deleting a topic marks all its ranges and segments for deletion. No orphaned ranges or segments.

6. **ID monotonicity.** `next_topic_id`, `next_range_id`, `next_segment_id` only increase within their scope. Never reused. Gaps allowed (counter advances on committed apply, not on propose).

7. **Single shard group owns entire subtree.** A topic and all its ranges and segments live in the same shard group. No cross-shard references within the entity hierarchy. `hash(topic_name)` deterministically routes to one shard group.

8. **Offset continuity within a range.** `segment[N].end_offset + 1 == segment[N+1].start_offset`. No gaps, no overlaps within a range's segment chain. On split/merge, child ranges start at offset 0.

9. **Offset monotonicity.** `range.next_offset` only increases. `segment.start_offset` set at creation, never changes. `segment.end_offset` set once on seal, never changes.

10. **Purely passive.** MetadataStateMachine never initiates proposals. All mutations come from applied Raft log entries. Only the leader proposes system-initiated operations (seal, split, merge) — followers apply committed decisions but never propose.

11. **Deterministic apply.** Given the same log, every replica produces the same state. Precondition failures in `apply_*` are logged but not fatal — they indicate a bug in proposal logic, not in the state machine.

12. **`topic_name_index` and `topics` always in sync.** Both updated atomically in `create_topic()` and `delete_topic()`. `topic_name_index` not snapshotted — rebuilt from `topics` on restore.

13. **Fixed partition strategy rejects splits.** Topics with `PartitionStrategy::Fixed` guarantee total ordering — `SplitRange` rejected at apply. Immutable after creation.

14. **Split point strictly interior.** `split_point` must satisfy `keyspace_start < split_point < keyspace_end`. Boundary values rejected.

15. **Merge requires adjacency.** Only ranges with contiguous keyspaces (`r1.keyspace_end == r2.keyspace_start` or vice versa) can merge.
