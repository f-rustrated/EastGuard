# D2 — Replicated Apply and Durability

**Goal:** Show how metadata commands become durable, deterministic state across a shard group, and why commit, persistence, and apply are distinct boundaries.

**Depends on:** [D1 — Metadata Entity Model and Lifecycle](d1_entity_model_and_lifecycle.md).

---

## One shard, one replicated history

Each metadata shard is an independent Raft group. Its log carries three kinds of entries:

| Payload | Applied effect |
|---|---|
| Metadata transition | Changes topics, ranges, segments, or consumer groups. |
| Membership transition | Changes the shard group's Raft voters. |
| No-op | Establishes a current-term commit point without changing application state. |

The metadata state machine is owned directly by its Raft instance. EastGuard is not exposing a generic consensus library, so an abstraction layer between consensus and the only application state machine would add indirection without adding a real extension point.

---

## Accepted is not committed; committed is not yet applied

The proposal path crosses several boundaries:

```
leader accepts command
        │
        ▼
append to local in-memory log
        │
        ├──────── replicate to followers
        ▼
persist log + hard state
        │
        ▼
majority acknowledgement advances commit
        │
        ▼
apply only through min(committed, durable)
        │
        ▼
metadata transition + completion/event
```

A command is externally successful only after apply reaches its log position. Replying at append time would acknowledge work that a new leader could overwrite. Applying before persistence would expose state that disappears after a crash.

The apply gate is therefore the smaller of:
- the commit index(hightest log entry that a majority has acknowledged)
- stabled index(the highest log entry that this specific node has written to its own local storage)

This ordering is deliberately stricter than “Raft has a majority”: each replica observes a transition only after that replica can recover the entry locally.

---

## Deterministic apply

Every decision that can affect replicated state is carried in the log entry. For example, a segment roll includes the chosen replica order, and a split includes its point and both child placements. Followers do not consult their local hash rings or clocks during apply, because those inputs can differ while gossip converges.

Apply may produce two kinds of output:

- leader-visible effects, such as placing a new active segment or notifying data replicas of a seal;
- deferred metadata proposals, such as an automatically detected split or merge.

The state transition itself happens synchronously at the consensus boundary. External effects are buffered for the actor to flush. Deferred proposals are drained in a later flush round rather than recursively proposed during apply, keeping the transition deterministic and the call graph bounded.

---

## Shared storage, isolated key ranges

All local Raft groups share one key-value store, but every key begins with its shard-group identity:

```
[group prefix][record kind][optional log index]
```

Big-endian numeric fields preserve numeric order under byte sorting. A group's log is one contiguous range, and deleting one group cannot touch another group's records.

Dirty groups flush their log mutations and hard-state changes in one atomic write batch. A successful flush advances each group's durable position; a failed flush advances none. Snapshot key spaces are reserved, but snapshot creation, installation, and log compaction remain future work, so logs currently grow without bound.

---

## Crash recovery boundary

On startup, each hosted group restores its persisted log and hard state. It does not infer application state from gossip or from data-plane files. Raft re-establishes leadership and advances apply from the durable replicated history.

Because applied-index persistence and snapshots are not complete, recovery currently reconstructs metadata by replaying the durable log. This is simple and correct at today's scale, but replay cost and unbounded log growth are the reasons snapshots remain an explicit roadmap item.

---

## Design guarantees

1. **Only committed and locally durable entries apply.** This prevents acknowledged metadata from disappearing after a replica crash.
2. **All replicas apply the same decision inputs.** Local topology, time, or randomness must not alter a committed transition.
3. **Membership changes use the same log discipline.** Two replicas must never calculate different quorums for the same log position.
4. **Storage keys isolate shard groups.** One group's truncation or removal must not corrupt another group's history.
5. **Apply does not recursively propose.** Follow-on policy decisions are deferred so one transition cannot create an unbounded synchronous cascade.

See `.agents/rules/raft.md` and `.agents/rules/storage-layout.md` for exact contracts. [D3](d3_routing_proposals_and_completion.md) follows a client operation through this machinery.
