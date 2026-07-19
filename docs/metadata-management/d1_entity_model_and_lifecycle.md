# D1 — Metadata Entity Model and Lifecycle

**Goal:** Explain the durable shapes that metadata manages—topics, ranges, and segments—and why their ownership, state transitions, offsets, and lineage are modeled together.

**Depends on:** Nothing. This is the foundation for every later metadata-management topic.

---

## The ownership tree

Metadata is a nested ownership tree rather than a collection of unrelated tables:

```
topic
├── storage and partition policy
├── consumer groups
└── ranges (complete keyspace coverage)
    ├── key bounds and lineage
    └── segments (one active write head, older sealed history)
        ├── replica set
        └── entry-offset bounds
```

This shape makes deletion and validation local. A topic owns every range that can route its keys, and a range owns every segment that can contain its records. There are no back-references whose consistency must be repaired after a partial update.

Identifiers are minted by the nearest owner. Topic identifiers are local to a shard group's metadata state, range identifiers are local to a topic, and segment identifiers are local to a range. A full segment identity therefore includes all three levels.

---

## Topic creation establishes a complete world

A newly created topic is immediately usable. It begins with:

- one active range spanning the full keyspace;
- one active segment beginning at offset zero;
- a chosen data replica set;
- the topic's retention, replication, and partition strategy.

There is no intermediate “topic exists but cannot route” state. This reduces the number of partially initialized states that clients and recovery code must understand.

Topic names are unique within their owning metadata shard. The cluster-wide hash route ensures every node sends the same name to the same shard, so local uniqueness at that shard produces global uniqueness without a central catalog.

---

## Ranges describe routing; segments describe storage epochs

A range owns a half-open slice of the topic keyspace. The active ranges form an ordered, gap-free cover of the entire keyspace. Routing chooses the active range whose bounds contain the record key.

A segment is a storage epoch inside one range. Exactly one segment is active; sealed segments preserve the range's earlier data. Rolling a segment closes the current epoch and opens its successor:

```
segment 0 [0..127] sealed
segment 1 [128..391] sealed
segment 2 [392.. ]  active
```

Entry offsets are continuous across ordinary rolls. The successor starts one position after the sealed end. If failure recovery cannot yet prove the end, metadata may temporarily carry an unknown boundary; the repair protocol later commits the recovered end and moves the successor start forward consistently.

The segment replica set answers where bytes belong. It is independent from the Raft peer set that replicates metadata. A node can participate in one set without participating in the other.

---

## Lifecycle transitions are one-way

Entities move forward through their lifecycle and never become active again after being sealed:

| Entity | Forward path | Why it exists |
|---|---|---|
| Topic | active → sealed → deleted | Stops new routing before logical removal. |
| Range | active → sealed → deleting | Preserves lineage while children or a merge successor take over. |
| Segment | active → sealed → deleting | Separates immutable history from retention reclamation. |

Logical deletion precedes physical byte removal. Metadata first marks data as no longer part of the readable history, then dispatches cleanup to the data replicas. This keeps the replicated decision authoritative even if cleanup messages are retried.

---

## Split and merge preserve lineage

A split seals one parent and creates two children whose adjacent bounds exactly cover the parent's interval. A merge seals two adjacent parents and creates one successor covering their union.

```
split:  [A────────────────Z)  →  [A────M) [M────Z)

merge:  [A────M) [M────Z)    →  [A────────────────Z)
```

Parents remain as sealed history and record their successors; successors record their origins. This lineage lets consumers finish old ranges and discover the ranges that replace them. New ranges start their own offset spaces at zero because a split or merge is a routing discontinuity, not an ordinary continuation of one range's log.

---

## Design guarantees

These state the structural properties enforced by the metadata state machine:

1. **Active ranges cover the keyspace exactly once.** Gaps make keys unroutable; overlaps let the same key have two owners.
2. **Every active range has exactly one active segment.** Zero write heads make the range unavailable; two allow divergent histories.
3. **Known segment boundaries are continuous within a range.** This lets readers advance without guessing whether offsets were lost or duplicated.
4. **Order within replica set is meaningful.** The first data replica is the write leader, so placement decisions must be committed rather than recomputed independently.
5. **Lifecycle state never moves backward.** Immutable history must not silently become writable again.


Continue with [D2](d2_replicated_apply_and_durability.md) for how these transitions become replicated truth.
