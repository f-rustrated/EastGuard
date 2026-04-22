# Coordinator Data Model — Design Doc

How Topic, Range, and Segment relate, transition, and cascade through the `CoordinatorStateMachine`.

---

## Entity Hierarchy

```
CoordinatorStateMachine (one per shard group)
│
├── topics: HashMap<TopicId, TopicMeta>
│   │
│   └── TopicMeta
│       ├── topic_id:       TopicId
│       ├── name:           String           (globally unique — hash(name) → one shard group)
│       ├── state:          TopicState
│       ├── storage_policy: StoragePolicy
│       └── ranges:         Vec<RangeId>     (ordered by keyspace_start)
│
├── ranges: HashMap<RangeId, RangeMeta>
│   │
│   └── RangeMeta
│       ├── range_id:       RangeId
│       ├── topic_id:       TopicId          (back-reference)
│       ├── keyspace:       [start, end)     (byte range, never gaps/overlaps)
│       ├── state:          RangeState
│       ├── segments:       Vec<SegmentId>   (ordered by creation time)
│       ├── active_segment: Option<SegmentId>(at most one — the write head)
│       ├── split_into:     Option<[RangeId; 2]>
│       ├── merged_into:    Option<RangeId>
│       └── merged_from:    Option<[RangeId; 2]>
│
└── segments: HashMap<SegmentId, SegmentMeta>
    │
    └── SegmentMeta
        ├── segment_id:  SegmentId
        ├── range_id:    RangeId             (back-reference)
        ├── topic_id:    TopicId             (denormalized for fast lookup)
        ├── state:       SegmentState
        ├── replica_set: Vec<NodeId>
        ├── size_bytes:  u64
        ├── created_at:  u64                 (monotonic timestamp)
        └── sealed_at:   Option<u64>
```

### Ownership Direction

```
Topic ──owns──▶ Range ──owns──▶ Segment
  1         *      1         *
```

Topic owns ranges. Range owns segments. Back-references (topic_id on Range, range_id on Segment) exist for O(1) parent lookup — denormalized but never out of sync because all mutations go through a single Raft log.

### How Range ID and Keyspace Are Determined

Both are system-determined. Users never specify either.

**`range_id`** — monotonic counter (`next_range_id++`) in `CoordinatorStateMachine`, auto-assigned on creation.

**`keyspace`** — the byte space of message keys from producers. Determined by which operation creates the range:

| Operation | Keyspace assigned |
|---|---|
| `CreateTopic` | Full range: `[0x00..., 0xFF...]` — single range covers everything |
| `SplitRange(parent, split_point)` | Parent `[A, C)` → child1 `[A, B)` + child2 `[B, C)` |
| `MergeRange(R1, R2)` | Two buddies `[A, B)` + `[B, C)` → merged `[A, C)` |

When a producer writes message with key `K` to topic "blue":
1. `hash("blue")` → shard group (metadata routing)
2. Key `K` falls within some range's `[start, end)` → that range's active segment receives the write

Initially one range handles all keys. Coordinator monitors load, proposes `SplitRange` at hotspot boundaries:

```
[0x00 ─────────────────── 0xFF]        ← CreateTopic (1 range)
        │ SplitRange
[0x00 ──── 0x80) [0x80 ── 0xFF]       ← 2 ranges
    │ SplitRange
[0x00─0x40) [0x40─0x80)  [0x80─0xFF]  ← 3 ranges
```

Ranges are never created standalone — they emerge from `CreateTopic` (initial) or `SplitRange`/`MergeRange` (lifecycle). No separate `AssignRange` command exists.

---

## How Sharding Maps to the Data Model

Entities **do not** shard independently. The entire subtree (Topic + its Ranges + their Segments) lives in a single shard group.

```
hash(topic_name) ──▶ consistent hash ring ──▶ ShardGroupId
                                                   │
                                          CoordinatorStateMachine
                                          on the 3 nodes in this
                                          shard group's Raft cluster
```

Why? A `CreateTopic` must atomically create the topic AND its initial range AND its initial segment. If these lived in different shard groups, we'd need distributed transactions. By keeping the entire subtree in one shard group, every mutation is a single Raft log entry — atomic by construction.

**Implication:** Range splits, segment seals, and all lifecycle operations for a topic are proposed to the same shard group's leader. No cross-shard coordination needed for single-topic operations.

**Cross-topic operations** (e.g., "list all topics") require scatter-gather across shard groups. This is fine — such queries are rare admin operations, not hot-path.

---

## State Machines

### Topic States

```
                CreateTopic
                    │
                    ▼
              ┌──────────┐
              │  Active  │
              └────┬─────┘
                   │  SealTopic (admin or retention)
                   ▼
              ┌──────────┐
              │  Sealed  │──── no new writes; existing data readable
              └────┬─────┘
                   │  DeleteTopic (after retention expires)
                   ▼
              ┌──────────┐
              │  Deleted │──── GC: remove all ranges + segments
              └──────────┘
```

- **Active:** Accepts writes. Has at least one Active range.
- **Sealed:** Read-only. All ranges sealed. Retention clock ticking.
- **Deleted:** Terminal. GC reclaims storage. Metadata removed after all segments physically deleted.

### Range States

```
          CreateTopic / SplitRange / MergeRange
                    │
                    ▼
              ┌──────────┐
              │  Active  │◀──── has exactly one active_segment (write head)
              └────┬─────┘
                   │
          ┌────────┼────────┐
          │        │        │
          ▼        ▼        ▼
      SplitRange  MergeRange  SealTopic
          │        │        │
          ▼        ▼        ▼
              ┌──────────┐
              │  Sealed  │──── no active_segment; all segments sealed
              └────┬─────┘
                   │  DeleteTopic → GC
                   ▼
              ┌──────────┐
              │ Deleting │──── segments being physically removed
              └──────────┘
```

- **Active:** Exactly one `active_segment`. Writes append to it.
- **Sealed:** All segments sealed. Happens on split, merge, or topic seal.
- **Deleting:** Segments being physically removed from data plane nodes.

**Split invariant:** When range `[A, C)` splits at midpoint `B`:
- Parent `[A, C)` → Sealed, `split_into = [child1, child2]`
- Child1 `[A, B)` → Active, new segment
- Child2 `[B, C)` → Active, new segment
- Topic.ranges: remove parent, insert both children (maintain sorted order)

**Merge invariant:** Only buddy ranges (from same parent split) can merge:
- Two ranges `[A, B)` + `[B, C)` → Sealed, `merged_into = merged_range`
- Merged range `[A, C)` → Active, `merged_from = [range1, range2]`

### Segment States

```
            CreateSegment (on range creation or segment roll)
                    │
                    ▼
              ┌──────────┐
              │  Active  │──── write head of its parent range
              └────┬─────┘
                   │  size_bytes ≥ threshold (~1GB) or range sealed
                   ▼
              ┌──────────┐
              │  Sealed  │──── immutable; can replicate / move
              └────┬─────┘
                   │  node failure or rebalance
                   ▼
              ┌──────────────┐
              │ Reassigning  │──── { from: NodeId, to: NodeId }
              │              │     data copying in progress
              └──────┬───────┘
                     │  copy complete
                     ▼
              ┌──────────┐
              │  Sealed  │──── replica_set updated
              └────┬─────┘
                   │  retention expired or topic deleted
                   ▼
              ┌──────────┐
              │ Deleting │──── physical files being removed
              └──────────┘
```

- **Active:** At most one per range. Write head. Grows until threshold.
- **Sealed:** Immutable. Replicated. Readable. Most segments spend most of their life here.
- **Reassigning:** Transitional. Moving replica from one node to another. Carries `{ from, to }` to track the transfer. Returns to Sealed once complete.
- **Deleting:** Terminal. Data plane nodes physically delete files.

---

## Cascading Effects of Each Operation

Each operation is a single `RaftCommand` variant proposed to the shard group's Raft log. On commit, `CoordinatorStateMachine.apply_*()` executes the cascade atomically (single-threaded, no partial apply).

### `CreateTopic { name, storage_policy }`

```
                     CreateTopic("blue", policy)
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
         TopicMeta        RangeMeta       SegmentMeta
         id: T1           id: R1          id: S1
         name: "blue"     topic_id: T1    range_id: R1
         state: Active    keyspace: full  topic_id: T1
         ranges: [R1]     active: S1      state: Active
                          state: Active   replica_set: [...]
```

One command creates three entities:
1. Topic with `state=Active`
2. Initial range covering full keyspace `[0x00..., 0xFF...]`
3. Initial segment as write head of that range

**Replica set decision:** Coordinator picks `storage_policy.replication_factor` nodes from the consistent hash ring. Could be the same shard group members or different nodes depending on data plane placement strategy.

**Reject if:** `topic_name_index` already contains `name` → duplicate topic error.

**Why names are globally unique:** `hash(topic_name)` deterministically routes to exactly one shard group. Within that shard group, names are unique (enforced by `topic_name_index`). Two different shard groups can never both own topic `"blue"` because `hash("blue")` always maps to the same shard group. Clients identify topics by name, never by `TopicId` — the ID is an internal optimization for Range/Segment back-references within the shard group.

### `SealSegment { segment_id }`

```
         SealSegment(S1)
              │
      ┌───────┴───────┐
      ▼               ▼
  SegmentMeta      SegmentMeta (new)
  id: S1           id: S2
  state: Sealed    range_id: R1
  sealed_at: now   state: Active
                   replica_set: [...]
      │
      ▼
  RangeMeta
  id: R1
  active_segment: S2     (was S1)
  segments: [S1, S2]     (S2 appended)
```

Segment roll: seal current, create next, update range's write head.

**Only valid if:** Segment state is Active. Range state is Active.
**If range is Sealed:** No new segment created. Segment just sealed. Range has no active_segment.

### `SplitRange { range_id, split_point }`

```
         SplitRange(R1, midpoint=0x80)
              │
      ┌───────┼───────────────────┐
      ▼       ▼                   ▼
  RangeMeta   RangeMeta (new)     RangeMeta (new)
  id: R1      id: R2              id: R3
  state:      keyspace: [0,0x80)  keyspace: [0x80,0xFF]
   Sealed     state: Active       state: Active
  split_into: active: S2          active: S3
   [R2, R3]   segments: [S2]      segments: [S3]

      ▼                 ▼                ▼
  Seal all         SegmentMeta       SegmentMeta
  active segs      id: S2            id: S3
  of R1            state: Active     state: Active

      ▼
  TopicMeta
  id: T1
  ranges: [R2, R3]    (R1 removed, children inserted sorted)
```

Full cascade:
1. Seal parent range R1 (and its active segment)
2. Create child range R2 with lower half of keyspace
3. Create child range R3 with upper half of keyspace
4. Create active segments S2, S3 for each child
5. Update topic's range list: remove R1, insert R2, R3 in sorted order
6. Record lineage: R1.split_into = [R2, R3]

**Keyspace invariant maintained:** `[0, 0x80) ∪ [0x80, 0xFF] = [0, 0xFF]`. No gaps, no overlaps.

### `MergeRange { range_id_1, range_id_2 }`

Inverse of split. Only valid for buddy ranges (adjacent keyspaces from same parent):

1. Seal both ranges (and their active segments)
2. Create merged range covering union of keyspaces
3. Create active segment for merged range
4. Update topic's range list
5. Record lineage on all three ranges

### `DeleteTopic { topic_id }`

```
         DeleteTopic(T1)
              │
              ▼
         TopicMeta
         state: Deleted
              │
              ▼
         For each range in topic.ranges:
              │
              ▼
         RangeMeta
         state: Deleting
              │
              ▼
         For each segment in range.segments:
              │
              ▼
         SegmentMeta
         state: Deleting
```

Marks everything for deletion. Data plane GC physically removes segment files. After all segments confirmed deleted, metadata can be garbage collected from the state machine.

---

## Split and Merge — Triggering Conditions

The Coordinator (shard group leader) monitors range health and proposes `SplitRange` or `MergeRange` when conditions are met. These are system-initiated — clients never request splits or merges directly.

### When Does a Range Split?

A range splits when it becomes a **hot partition** — disproportionate load relative to other ranges in the same topic. The Coordinator monitors:

| Signal | What it measures | Split indicator |
|---|---|---|
| **Segment fill rate** | How fast the active segment reaches the size threshold (~1GB) | Segment rolls (seals) significantly faster than peer ranges |
| **Write throughput** | Writes/sec arriving at the range's active segment | Sustained throughput above a configurable threshold |
| **Key distribution skew** | Whether writes cluster in a sub-region of the keyspace | High write density in a narrow key band suggests a finer split would help |

**Split point selection:** The Coordinator picks a split point that divides the observed write load roughly evenly. Strategies:

1. **Midpoint (simple):** Bisect the keyspace at `(start + end) / 2`. Works well for uniformly distributed keys.
2. **Percentile-based (future):** Track key distribution histogram, split at the median write key. Better for skewed workloads but requires sampling infrastructure.

Phase 1 uses midpoint. Percentile-based split is a backlog optimization.

**Cooldown:** After splitting, child ranges enter a monitoring cooldown (e.g., 5 minutes) before they become split candidates themselves. Prevents split storms from transient load spikes.

**Minimum range size:** Ranges below a minimum keyspace width (configurable) are not split further, preventing unbounded fragmentation.

### When Does a Range Merge?

Merge is the inverse of split — recombines **underutilized adjacent ranges** to reduce metadata overhead and improve read efficiency across the keyspace.

**Preconditions (all must hold):**

1. **Buddy ranges only.** The two ranges must be adjacent in keyspace AND share the same parent split (tracked via `split_into` / `merged_from` lineage). Arbitrary adjacent ranges cannot merge — this preserves the binary tree structure and ensures the merged keyspace is contiguous and was once a single range.

2. **Both ranges below utilization threshold.** Combined write throughput of both ranges is low enough that a single range can handle the load. The threshold is a fraction of the split threshold (e.g., split at 10K writes/sec, merge when combined < 3K writes/sec). Hysteresis between split and merge thresholds prevents oscillation.

3. **Both ranges in Active state.** Sealed or Deleting ranges cannot merge.

4. **No in-flight split on either range.** If a range was recently split or has pending split, merge is blocked.

```
Split threshold:    ████████████████████░░░░  (high — triggers split)
                                  ↕ hysteresis gap
Merge threshold:    ██████░░░░░░░░░░░░░░░░░░  (low — triggers merge when BOTH below)
```

**Why buddy-only merging?**

Without this constraint, merging arbitrary adjacent ranges breaks the binary tree invariant. Consider:

```
[0x00 ──── 0x40)  [0x40 ──── 0x80)  [0x80 ──── 0xFF]
     R2                R3                 R4
     └── from split of R1 ──┘             └── from split of R0
```

R3 and R4 are adjacent but NOT buddies (different parents). Merging them would create `[0x40, 0xFF]` — a range that never existed as a unit, complicating lineage tracking and future splits. Only R2+R3 (buddies from R1) or a merge after R2+R3 already merged back into R1 (then R1+R4 are buddies from R0) are valid.

### Monitoring Architecture

The Coordinator does not receive a continuous metrics stream. Instead, it uses **periodic sampling**:

```
Coordinator (shard leader)
    │
    │  periodic probe (every N seconds)
    │
    ▼
Data plane nodes hosting segments
    │
    │  report: { segment_id, size_bytes, write_rate, key_histogram }
    │
    ▼
Coordinator evaluates split/merge conditions
    │
    ▼
Proposes SplitRange / MergeRange via Raft
```

- **Who reports:** Data plane nodes that host active segments send periodic heartbeats with size and throughput metrics.
- **Where evaluated:** The Coordinator (shard group leader) aggregates reports and evaluates thresholds. Only the leader proposes — followers ignore metrics (they'll apply the committed decision).
- **Consistency:** Split/merge decisions go through Raft. Even if two leaders briefly coexist (network partition), only one proposal commits. The state machine's precondition checks in `apply_split_range()` / `apply_merge_range()` reject stale proposals (e.g., splitting an already-sealed range).

### Segment Seal — Relationship to Split

Segment sealing (`SealSegment`) and range splitting (`SplitRange`) are related but independent:

| Operation | Trigger | Effect |
|---|---|---|
| `SealSegment` | Active segment reaches size threshold (~1GB) | Seal current segment, create new one. Range stays Active. Normal segment roll — happens many times during a range's lifetime. |
| `SplitRange` | Range throughput exceeds split threshold | Seal parent range + all its segments. Create two child ranges with new segments. Structural change to the topic's range tree. |

A segment seal is routine housekeeping. A range split is a load-balancing decision. Segments seal frequently (every ~1GB of writes); ranges split rarely (when sustained load justifies it).

---

## ID Generation

All IDs (`TopicId`, `RangeId`, `SegmentId`) are `u64` generated by the `CoordinatorStateMachine` using monotonic counters:

```
next_topic_id:   u64    (incremented on CreateTopic)
next_range_id:   u64    (incremented on CreateTopic, SplitRange, MergeRange)
next_segment_id: u64    (incremented on any segment creation)
```

Counters are per-shard-group (each `CoordinatorStateMachine` has its own). IDs are unique within a shard group but NOT globally unique across shard groups. This is fine — `TopicId`, `RangeId`, `SegmentId` are internal identifiers used only within a shard group's state machine. Clients never see them. Externally, topics are identified by **name** (which is globally unique via deterministic hash routing).

**Cross-shard references** (rare, e.g., admin tooling): prefix with `ShardGroupId` → `(ShardGroupId, TopicId)` is globally unique.

Counters are part of the replicated state — they advance deterministically on every replica when applying log entries. No coordination needed.

---

## Reverse Indexes

For efficient lookup, the state machine maintains:

```
topic_name_index: HashMap<String, TopicId>
```

Needed for: duplicate name detection on `CreateTopic`, lookup by name from client queries.

Built from forward data (topics map). Rebuilt from snapshot on recovery. Maintained incrementally on apply.

Future indexes (when needed):
- `segments_by_node: HashMap<NodeId, HashSet<SegmentId>>` — for node failure handling
- `ranges_by_topic: HashMap<TopicId, Vec<RangeId>>` — redundant with TopicMeta.ranges but useful if ranges map grows large

---

## Invariants

1. **Keyspace coverage:** For any Active topic, the union of its Active ranges' keyspaces equals the full keyspace. No gaps, no overlaps. Enforced by split/merge being the only mutations to range boundaries.

2. **Single active segment:** Each Active range has exactly one Active segment (the write head). Sealed/Deleting ranges have zero.

3. **Segment immutability:** Once `state = Sealed`, a segment's data never changes. Only `replica_set` can change (via Reassigning). `size_bytes` and `sealed_at` are final.

4. **Lineage consistency:** `split_into` and `merged_from`/`merged_into` form a tree. A range cannot be both split and merged. Lineage is write-once — set on seal, never modified.

5. **Delete cascades downward:** Deleting a topic marks all its ranges and segments for deletion. No orphaned ranges or segments.

6. **ID monotonicity:** IDs only increase. Never reused. Gaps allowed (failed proposals don't consume IDs because the counter only advances on committed apply, not on propose).

7. **Single shard group owns entire subtree:** A topic and all its ranges and segments live in the same shard group. No cross-shard references within the entity hierarchy.

---

## Interaction with Raft Layer

The `CoordinatorStateMachine` is purely passive — it never initiates proposals. All mutations come from applied Raft log entries:

```
RaftCommand variant          CoordinatorStateMachine method
─────────────────────────    ──────────────────────────────
CreateTopic { .. }           apply_create_topic()
SplitRange { .. }            apply_split_range()
MergeRange { .. }            apply_merge_range()
SealSegment { .. }           apply_seal_segment()
DeleteTopic { .. }           apply_delete_topic()
ReassignSegment { .. }       apply_reassign_segment()
```

No `AssignRange` — ranges are created implicitly by `CreateTopic` (initial full-keyspace range) and `SplitRange`/`MergeRange` (lifecycle operations).

Each `apply_*` method:
1. Validates preconditions (correct state, entity exists)
2. Mutates internal HashMaps
3. Returns `Result<ApplyResult, ApplyError>`

Precondition failures in `apply_*` are logged but not fatal — they indicate a bug in the proposal logic, not in the state machine. The state machine is deterministic: given the same log, every replica produces the same state.

### Propose Routing — MultiRaftActor Handles It

No separate "broker" or routing layer. `MultiRaftActor` is the single entry point for all proposals — both client-originated and system-originated.

```
Client                    MultiRaftActor                    Raft
  │                            │                              │
  │── Propose(key, cmd) ──────▶│                              │
  │                            │── ShardGroupId::new(key)     │
  │                            │── groups.get(shard_id)?      │
  │                            │                              │
  │                            │── [found + is_leader]        │
  │                            │       raft.propose(cmd) ────▶│
  │                            │◀──── Ok(log_index) ──────────│
  │                            │   ... replication ...        │
  │                            │◀──── committed ──────────────│
  │◀── Success ────────────────│       apply to SM            │
  │                            │                              │
  │                            │── [found + follower]         │
  │◀── NotLeader(hint) ────────│                              │
  │                            │                              │
  │                            │── [not found]                │
  │◀── NotLeader(None) ────────│   (this node not in group)   │
```

**Why MultiRaftActor, not a separate routing layer:**
- Already has `HashMap<ShardGroupId, Raft>` — can check `is_leader()` directly
- `ShardGroupId::new(key)` is a pure hash — no topology query needed
- Avoids async round-trip to SWIM for routing info
- Internal proposals (AddPeer/RemovePeer from SWIM events) already go through `MultiRaft` directly via `HandleNodeDeath`/`HandleNodeJoin`
- Client handler in `lib.rs` stays thin — just forwards to `MultiRaftActor`

**Current `MultiRaftActorCommand::Propose` takes `shard_group_id: ShardGroupId`.** This changes to `resource_key: Vec<u8>`. MultiRaft computes the ShardGroupId internally. The caller never needs to know shard assignment.

### Who Proposes What?

| Operation | Who decides to propose | Trigger |
|---|---|---|
| CreateTopic | Client request via Propose pathway | External API call |
| SealSegment | Coordinator (shard leader) | Active segment reaches size threshold |
| SplitRange | Coordinator (shard leader) | Range throughput exceeds threshold |
| MergeRange | Coordinator (shard leader) | Adjacent ranges below utilization threshold |
| DeleteTopic | Client request or retention policy | External API call or TTL expiry |
| ReassignSegment | Coordinator (shard leader) | Node failure detected via SWIM |

Client-initiated operations come through `MultiRaftActorCommand::Propose` with a `resource_key`. System-initiated operations (seal, split, merge, reassign) are proposed by the Coordinator itself — it monitors conditions and calls `raft.propose()` directly within `MultiRaft` (no `resource_key` routing needed since the shard group is already known).

---

## Snapshot Representation

For InstallSnapshot (leader → lagging follower), the entire state machine serializes to:

```rust
struct SnapshotData {
    topics:         HashMap<TopicId, TopicMeta>,
    ranges:         HashMap<RangeId, RangeMeta>,
    segments:       HashMap<SegmentId, SegmentMeta>,
    next_topic_id:  u64,
    next_range_id:  u64,
    next_segment_id: u64,
}
```

Includes ID counters — without them, a restored replica would generate colliding IDs.

`topic_name_index` is NOT snapshotted — rebuilt from `topics` on restore.
