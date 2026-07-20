# D6 — Range Splitting, Merging, and Load Classification

**Goal:** Explain how committed segment lifecycle events become a low-cost load signal, and how metadata safely turns that signal into split and merge proposals.

**Depends on:** [D1](d1_entity_model_and_lifecycle.md) and [D5](d5_segment_lifecycle_and_repair.md).

---

## Reuse the ordered signal already available

EastGuard does not currently maintain a metrics pipeline or key histogram for range balancing. Segment rolls already pass through the metadata log, so their committed causes provide an ordered, replicated signal:

| Roll cause | Load meaning | Classification effect |
|---|---|---|
| Data pressure | Sustained writes filled the segment | Advance the pressure streak. |
| Idle maintenance | Time, not volume, ended the segment | Reset pressure and classify idle. |
| Replication failure | Availability event | Neutral. |
| Recovery | Repair event | Neutral. |
| Boundary correction | Bookkeeping | Neutral. |

Only committed observations affect classification. A local timeout or failed proposal cannot make replicas disagree about whether a range is hot.

Physical seal time remains useful for retention and operations, but it is not a load-ordering mechanism. Proposal timestamps can be observed in a different order from Raft commits, so load classification follows committed cause while retention continues to follow physical seal time.

---

## Segment rolls and range seals are different

A segment roll seals one storage epoch, creates its successor, and leaves the range active. A range seal occurs during split or merge: it removes the range's write head and records lineage. The latter is a topology transition, not evidence of traffic.

```
active range                              sealed range
    │ segment roll                           │ boundary correction
    ▼                                        ▼
seal active segment                     fill exact segment end
create successor                        create no successor
range remains active                    range remains sealed
may classify load                       never classifies load
```

Only the first successful roll of the current active segment applies its cause. A duplicate finds the segment already sealed and becomes an idempotent no-op or a boundary correction; it cannot count the same physical transition twice.

Retries retain the original cause. Packet loss must not turn a pressure-triggered roll into a neutral recovery observation or make retry timing part of policy.

---

## Pressure produces a split proposal

Consecutive pressure rolls advance a saturating streak. At the threshold, the leader queues a split at the current range midpoint.

```
pressure roll → pressure roll → threshold
                                 │
                                 ▼
                      deferred midpoint split
                                 │
                                 ▼
                   sealed parent + two children
```

The streak saturates so a delayed split cannot grow state indefinitely. The midpoint requires no sampling infrastructure and is deterministic from committed bounds. It may balance poorly under skewed keys; percentile-based splitting remains a future improvement if midpoint behavior proves inadequate.

Fixed-partition topics reject splitting regardless of load.

---

## Idle neighbors produce a merge proposal

An idle roll marks its range idle and resets prior pressure. When two adjacent active ranges are both idle, the leader can queue a merge immediately. A periodic scan is a recovery fallback if the event-driven opportunity was missed.

Split children and merged successors begin unclassified. Absence of traffic is not evidence of idleness; each must first produce an idle observation. This prevents freshly created ranges from collapsing immediately back into a merge.

| Left range | Right range | Merge eligible? |
|---|---|---|
| Unclassified | Idle | No |
| Idle | Pressure | No |
| Pressure | Pressure | No |
| Idle | Idle | Yes |

---

## Apply never proposes recursively

Classification happens while applying a committed roll, but the resulting split or merge is buffered. The actor proposes it during a later flush round:

```
apply roll
├── update segment and load state
├── emit data-plane effects
└── queue policy proposal
        │ later flush
        ▼
     Raft propose
```

This breaks recursion and preserves a clean rule: apply deterministically transforms state; leadership policy originates new work outside that transformation.

---

## Stale proposals are expected

Between classification and commit, another operation may split, merge, seal, or delete the target range. Apply-time preconditions reject stale work without corrupting state. This is preferable to locks or reservations that would serialize unrelated metadata progress around a speculative policy action.

Split and merge commits also rebalance every consumer group for the topic because the active routing units changed. The new generation is part of replicated metadata, covered in [D7](d7_consumer_group_coordination.md).

---

## Edge cases

| Situation | Classification result | Why |
|---|---|---|
| Size and age limits are both eligible | Data pressure wins | A full segment is positive pressure evidence. |
| Roll request is retried | Original cause is retained | Delivery behavior cannot change policy meaning. |
| Failure rolls an active segment | Previous classification remains | Replica health says nothing about demand. |
| A sealed range later reports its exact end | Boundary only; no load change | It is completion of a topology seal, not a new roll. |
| Leadership changes | New leader uses committed classification | No local history reconstruction is needed. |

---

## Retention still uses physical time

Every sealed segment retains its physical seal timestamp. Retention expires an oldest-first prefix by comparing that timestamp with the configured retention period. Split and merge timestamps likewise remain useful for operations.

| Metadata | Ordering authority | Purpose |
|---|---|---|
| Segment seal time | Physical clock | Retention and operational visibility |
| Range-load classification | Raft commit order | Automatic split and merge policy |

No load decision reads creation time, seal time, current time, or retry time.

---

## Design rules

1. **Only committed roll causes classify load.** Local observations are not replicated truth.
2. **Failure and recovery are load-neutral.** Availability events must not masquerade as demand.
3. **Pressure must be consecutive.** An idle roll breaks the evidence of sustained heat.
4. **New ranges begin unclassified.** No observation means unknown, not idle.
5. **Automatic proposals are deferred and revalidated.** This prevents recursive apply and makes races harmless.
6. **Active ranges continue to cover the keyspace exactly once.** Split and merge alter topology without creating gaps or overlaps.
7. **Physical time remains policy-neutral.** Retention needs wall time, while deterministic balancing needs committed event order.

Operational contracts live in `.agents/rules/metadata-state-machine.md`, `.agents/rules/raft-actor.md`, and `.agents/rules/ds-rsm.md`.
