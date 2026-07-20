# D7 — Consumer Group Coordination

**Goal:** Explain how metadata tracks consumer membership and assigns active ranges through committed generations, especially when range topology changes.

**Depends on:** [D1](d1_entity_model_and_lifecycle.md), [D3](d3_routing_proposals_and_completion.md), and [D6](d6_range_splitting_merging_and_load.md).

---

## Why assignments belong in metadata

Consumers read ranges, and ranges can split or merge. Assignment therefore depends on the same strongly consistent topology that routes writes. Keeping membership and assignment in the topic's metadata shard gives every consumer one committed answer to “which active ranges belong to me in this generation?”

Consumer offsets are different: they are high-volume data-plane state and are stored with segment data. Metadata coordinates ownership epochs; the data plane persists progress within those epochs.

---

## Group state

A consumer group records:

- its current generation;
- current members and their session observations;
- a complete mapping from active ranges to members.

Membership actions are proposed through the topic's Raft group. Join, leave, and expiration that change the member set advance the generation and recompute the full assignment. A heartbeat that merely refreshes an existing session does not create a new generation.

```
member sync request
       │
       ▼
topic shard Raft commit
       │
       ├── membership unchanged → refresh only
       │
       └── membership changed → generation + 1
                                  │
                                  ▼
                           full range assignment
```

---

## Complete assignment per generation

When a group has members, every active range is assigned exactly once and every assignee is a current member. When the group is empty, its assignment is empty.

The assignment is recomputed as a whole rather than patched incrementally. Full recomputation is cheap at current range counts and eliminates stale ownership left behind by a member departure or topology change.

Deterministic ordering of members and ranges ensures every Raft replica derives the same result while applying the same command.

---

## Range topology advances the epoch

A split replaces one active range with two; a merge replaces two with one. Either operation changes the units consumers own, so apply recomputes all groups for that topic and emits fresh generation snapshots.

```
generation 8: member A → range 4
                         │ split
                         ▼
generation 9: member A → range 7
              member B → range 8
```

Consumers use lineage to finish sealed parent ranges and the new generation to discover active successors. A stale-generation offset update is rejected so progress from an old ownership epoch cannot be attributed to a new assignment.

---

## Sessions and liveness

Each committed sync includes an observation time and session timeout. Expiration is evaluated through metadata coordination, so removing a member and reassigning its ranges becomes replicated state rather than a local coordinator opinion.

Wall-clock timestamps are a pragmatic choice today. They should not be read as a total ordering across nodes; Raft orders the commands. A future logical-clock design could reduce dependence on clock quality without changing the generation model.

---

## Design guarantees

These restate existing metadata-state contracts.

1. **A non-empty group assigns every active range exactly once.** Missing assignments stall consumption; duplicates create competing owners.
2. **Every assignee is a current member.** Departed or expired members cannot retain ownership.
3. **Membership and topology changes advance the generation.** Consumers need an epoch boundary whenever ownership can change.
4. **Heartbeat-only refreshes preserve the generation.** Liveness traffic must not cause needless rebalances.
5. **Empty groups have empty assignments.** Ownership cannot exist without a consumer.
6. **Offsets are fenced by generation.** Stale owners cannot commit progress into the current epoch.

See `docs/data-plane/d8_consumer_offset_management.md` and `d9_offset_placement_graduation.md` for offset storage and fencing on the data-plane side.
