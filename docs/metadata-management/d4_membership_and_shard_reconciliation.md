# D4 — Membership and Shard Reconciliation

**Goal:** Explain how SWIM's eventually consistent view of live nodes is converted into safe Raft-group membership, including learner catch-up, leader takeover, and capacity return.

**Depends on:** [D2](d2_replicated_apply_and_durability.md) and [D3](d3_routing_proposals_and_completion.md).

---

## Two truths with different jobs

SWIM answers who appears alive across the cluster. Raft answers which voters constitute one shard group's quorum at a particular log position.

```
SWIM observation: node C is dead
             │
             ▼
current shard leader selects replacement D
             │
             ▼
commit removal of C through Raft
             │
             ▼
replicate history to D as non-voting learner
             │
             ▼
commit promotion of caught-up D
```

SWIM never edits the voter set directly. Gossip can arrive in different orders, so direct edits would let replicas disagree about quorum size and potentially commit conflicting histories.

---

## Why additions use learners

Removing one dead voter can be committed by the surviving majority. Adding its replacement directly as a voter is unsafe for availability: if that node has not created the group or cannot receive the log, the new quorum may become unreachable.

The replacement therefore begins as a non-voting learner. It receives log replication but is excluded from elections and commit quorums. Only after it reaches the committed history does the leader propose its promotion. A failed learner can delay its own promotion, but cannot freeze the existing group.

---

## Event handling and reference-back

When SWIM confirms a death, every current shard leader affected by that death can begin reconciliation. But membership events are not a durable work queue. If the leader itself dies, no one may convert that event while the group has no leader.

Every newly elected leader therefore performs a reference-back pass:

1. snapshot the current live set and ring intent;
2. compare them with committed voters and staged learners;
3. remove dead or no-longer-intended voters through the log;
4. stage missing intended members as learners;
5. promote only after catch-up.

Periodic reconciliation repeats the comparison. This handles missed one-shot events and capacity returning after the group had to operate below its configured replication factor.

---

## Ring intent and committed membership can drift

The hash ring changes promptly as nodes join and die. A Raft group changes only through its own committed history. Temporary drift is expected:

| Situation | Ring says | Raft says | Safe behavior |
|---|---|---|---|
| New node joined | New placement includes it | It is not yet a voter | Catch it up as learner. |
| Node confirmed dead | Placement excludes it | It may still be a voter | Leader commits removal. |
| No replacement exists | Smaller live placement | Old replication target | Remove dead voter and run degraded. |
| Capacity returns | Full placement possible | Group still undersized | Stage and promote a replacement. |

Reconciliation closes drift; it does not pretend the two layers change atomically.

---

## Raft peers are not data replicas

The group voter set replicates metadata. A segment replica set places bytes. The same node death can trigger work at both layers, but neither set is derived from the other:

```
node death
├── metadata quorum: remove voter, catch up replacement learner
└── segment bytes: roll active segment or reassign sealed segment
```

A node holding segment files is not authoritative unless the committed segment metadata names it. Conversely, a Raft voter need not hold any segment bytes for the topics whose metadata it replicates.

---

## Limits

If a group loses quorum before a leader can commit a safe removal, Raft cannot repair itself. That requires manual reconfiguration or a future external recovery protocol. The system can automate recovery only while a committed majority remains able to agree.

---

## Design rules

1. **Voter membership changes only through committed log entries.** This keeps quorum calculations deterministic at every index.
2. **A new voter proves catch-up first.** Learners protect availability from phantom or unreachable members.
3. **New leaders reconcile against current cluster facts.** One-shot death events cannot be the only bridge from SWIM to Raft.
4. **Periodic reconciliation restores desired capacity.** A temporary lack of replacements must not become permanent under-replication after nodes return.
5. **Raft membership and segment placement remain separate.** They solve different replication problems and may legitimately drift apart.

See [D2](d2_replicated_apply_and_durability.md) for the consensus boundary and `.agents/rules/raft.md` for exact membership contracts. [D5](d5_segment_lifecycle_and_repair.md) covers the data-replica side of failure handling.
