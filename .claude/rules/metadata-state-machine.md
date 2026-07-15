# MetadataStateMachine (Invariants)

Application state machine for DS-RSM. One instance per shard group, owned by `Raft`. Manages topic, range, and segment metadata. Pure synchronous, no I/O. For the entity model and lifecycle, see `docs/metadata-management/mental-model.md` § "The entity model".

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

## Commands

| `MetadataCommand` variant | Method | Effect |
|---|---|---|
| `CreateTopic` | `apply_create_topic()` | Creates topic + initial full-keyspace range + initial segment |
| `RollSegment` | `apply_roll_segment()` | Seals active segment, creates next segment (segment roll) |
| `SplitRange` | `apply_split_range()` | Seals parent range, creates two child ranges with new segments |
| `MergeRange` | `apply_merge_range()` | Seals both source ranges, creates merged range with new segment |
| `DeleteTopic` | `apply_delete_topic()` | Cascades Deleting state to all ranges and segments |
| `ReassignSegment` | `apply_reassign_segment()` | Swaps a **sealed** segment's `replica_set` (D5 repair); state stays Sealed |

## Invariants

1. **Keyspace coverage.** For any Active topic, the union of its active ranges' keyspaces equals the full keyspace `[KEYSPACE_MIN, KEYSPACE_MAX]`. No gaps, no overlaps. Split and merge are the only operations that mutate range boundaries, and both preserve this property.

2. **Single active segment per range.** Each Active range has exactly one Active segment (the write head). Sealed/Deleting ranges have zero (`active_segment = None`). Two active segments per range would admit concurrent writes with conflicting offsets.

3. **Segment immutability after seal.** Once `state = Sealed`, the segment's data, `size_bytes`, `end_offset`, and `sealed_at` never change. Only `replica_set` can change — via `ReassignSegment` (D5 sealed-segment repair), which leaves the segment `Sealed`. Reads against sealed segments stay safe to cache: `replica_set` is *where* a copy lives, not *what* the segment holds.

4. **Lineage is write-once.** `split_into`, `merged_from`, and `merged_into` are set on seal and never modified. A range cannot be both split and merged. The lineage tree is append-only.

5. **Deletes cascade downward.** Deleting a topic marks all its ranges and segments for deletion in the same applied operation. No orphan ranges or segments — the GC scan can rely on the cascade.

6. **ID monotonicity.** `next_topic_id`, `next_range_id`, `next_segment_id` only increase within their scope. Never reused. Gaps allowed (counter advances on committed apply, not on propose).

7. **Single shard group owns the entire subtree of one topic.** A topic and all its ranges and segments live in the same shard group. No cross-shard references within the entity hierarchy. Allows single-topic operations to be a single Raft log entry — atomic by construction.

8. **Offset continuity within a range.** `segment[N].end_offset + 1 == segment[N+1].start_offset`. No gaps, no overlaps within a range's segment chain. On split/merge, child ranges restart at offset 0 — a clean break, not a continuation.

9. **Offset monotonicity within a range.** `range.next_offset` only increases. `segment.start_offset` set at creation, never changes. `segment.end_offset` set once on seal, never changes.

10. **State machine never originates proposals.** All mutations come from applied Raft log entries. Even leader-initiated work (seal, split, merge) goes through the log so every replica applies the same committed entry the same way. Originating mutations outside the log would break deterministic apply.

11. **Deterministic apply.** Given the same log, every replica produces the same state. Precondition failures in `apply_*` are logged but not fatal — they indicate a bug in proposal logic, not in the state machine.

12. **`topic_name_index` and `topics` always in sync.** Both updated atomically in `create_topic()` and `delete_topic()`. Out-of-sync would let `CreateTopic` for an existing name succeed while still rejecting reads.

13. **Fixed partition strategy rejects splits.** Topics with `PartitionStrategy::Fixed` reject `SplitRange` at apply. Strategy is immutable after creation. Required for topics that promise total ordering across keys.

14. **Split points must be strictly interior** to the parent's keyspace. `keyspace_start < split_point < keyspace_end`. Boundary values produce a child with empty keyspace and would break invariant 1.

15. **Merges require adjacency.** Only ranges with contiguous keyspaces (`r1.keyspace_end == r2.keyspace_start` or vice versa) can merge. Otherwise the merged range's keyspace would not be a contiguous slice.

16. **Seal history is deterministic.** `seal_history` on `RangeMeta` is updated only inside `apply_roll_segment()`, which runs on ALL replicas with the same input. No leader-only state leaks into `RangeSealHistory`. The split/merge *decision* is leader-only, but the data the decision reads is identical on every replica.

17. **Children of a split carry a cooldown anchor.** Child ranges created by `apply_split_range()` always have `seal_history.created_by_split_at` set to the split's `created_at` timestamp. Enforces `SPLIT_COOLDOWN_MS` before children can be re-split — prevents split storms from transient load.

18. **`correct_end_offset` is write-once.** Updates the sealed segment's `end_offset` only when the current value is `None` AND the incoming `end_entry_id` is `Some`. Simultaneously sets the active segment's `start_offset` to `end_entry_id + 1`. Re-application is a no-op. Restores invariant 8 after a death-triggered roll. This is now the **fallback** correction — for the follower-death race (the live leader's write-path seal arrives after a reconcile `None`-roll) and the unknown-end fallback. The leader-crash path no longer relies on it: it recovers the committed end *before* rolling (`raft-actor.md` #6), so the successor opens at `min+1` directly rather than at 0-then-corrected.

19. **`RollSegment` is idempotent.** Re-applying a `RollSegment` for a segment that is no longer active returns `ApplyResult::Noop` rather than an error. Tolerates the race where both the write-path timeout and SWIM node death fire `RollSegment` for the same failure.

20. **An entity's state never reverts.** Topic: Active → Sealed → Deleted. Range: Active → Sealed → Deleting. Segment: Active → Sealed → Deleting. No backward transitions. Once a segment is Sealed it never becomes Active again — `ReassignSegment` swaps its `replica_set` but keeps it `Sealed`.

21. **`ReassignSegment` only re-points a sealed segment.** `apply_reassign_segment()` accepts only a `Sealed` segment, swaps `replica_set`, and changes nothing else — state stays `Sealed`; data, offsets, lineage, and timestamps stay frozen (invariant 3). An active, deleting, or unknown segment is rejected (`SegmentNotSealed` / `SegmentNotFound`), logged but not fatal (invariant 11). Re-applying with the same `replica_set` returns `ApplyResult::Noop`, tolerating duplicate death detection and no-leader re-proposals (cf. invariant 19). The swap runs through `apply`, so the umbrella `assert_invariants` re-checks every other invariant afterward — a reassignment cannot leave the machine inconsistent.

22. **A committed consumer-group generation assigns each active range exactly once.** When a group has members, its assignment keys exactly equal the topic's active ranges and every assignment names a current member. When it has no members, it has no assignments. Membership or range-topology changes advance the generation and recompute the full desired assignment through the Raft log; heartbeat refreshes that do not change membership leave the generation unchanged.
