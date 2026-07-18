# Event-Driven Range Load Classification

**Goal:** Make automatic range split and merge decisions a deterministic result of committed segment-roll causes, while preserving physical seal time for retention and operational visibility.

**Depends on:** segment lifecycle coordination, Raft-backed metadata commands, automatic range split/merge, and segment retention.

**Status:** Proposed by GitHub issue #192. The current implementation still classifies load from a wall-clock seal history; this document describes its replacement.

---

## Why the current signal is insufficient

EastGuard already records every segment roll through Raft. The original range-load policy reused those rolls by storing their proposal-time Unix timestamps:

- several recent seals make a range eligible to split;
- no recent seals make adjacent ranges eligible to merge;
- a split timestamp prevents the children from immediately changing topology again.

This is attractive because it needs no metrics pipeline, but it asks physical time to answer two different questions:

1. **When was this segment sealed?** Physical time is meaningful for retention and operations.
2. **What does this roll say about range load?** This is an ordered policy signal.

Raft commit order is authoritative for the second question. A proposal captures its timestamp before commit, so two proposals may commit in a different order from their timestamps. Sorting the timestamps keeps the history ordered, but the decision still depends on wall time and still treats every roll as evidence of load—even when a roll was caused by failure or recovery.

The replacement keeps the useful “segment rolls are free load observations” idea, but records why each roll happened and applies that cause in commit order.

---

## Range seals and segment seals are different operations

The word “seal” appears at two levels. Conflating them would make topology changes look like traffic.

### Segment roll

A segment roll seals the current active segment, creates its successor, and leaves the owning range active. It can reveal something about the range's workload:

- the segment filled to its size limit;
- the segment reached its age limit without filling;
- replication failed;
- recovery required a new active segment.

Only a successful roll of the current active segment may update range-load state.

### Range seal

A split or merge seals a range itself. The operation also seals the range's active segment, removes its write head, and records lineage. This is a topology transition, not a segment roll and not a load observation.

After a parent range is split, its data leader may still need to report the exact end of the segment that the range seal closed. That message travels through existing segment lifecycle coordination, but metadata applies it only as a sealed-segment boundary correction. It creates no successor and contributes no load signal.

```text
active range                               sealed range
    |                                           |
    | segment roll                              | boundary correction
    v                                           v
seal active segment                        fill exact segment end
create successor                           no successor
range stays active                         range stays sealed
may update load state                      never updates load state
```

This boundary is structural: split and merge remain their own Raft commands. There is no “topology” segment-roll cause.

---

## Roll causes

Every real roll of an active segment carries an explicit cause from its origin through Raft commit.

| Cause | Observation | Effect on range load |
|---|---|---|
| **Data pressure** | The segment reached its configured size limit | Advance consecutive pressure |
| **Idle maintenance** | The segment reached its configured age limit before filling | Mark idle and clear pressure |
| **Replication failure** | The write path could not replicate to the current set | No change |
| **Recovery** | Membership loss or boundary recovery required a new active segment | No change |

“Idle” does not mean that the range received zero writes. It means the active segment survived for the configured age without reaching its size limit: the range is cold enough to be considered for consolidation under this policy.

Failure and recovery are deliberately neutral. They describe cluster health, not demand. Allowing them to advance pressure would split ranges during outages; allowing them to mark idle would merge ranges because replicas failed.

### Retries preserve intent

A retry is another delivery attempt for the same logical roll, not a new reason to roll.

```text
size limit reached
      |
      v
request(Data pressure) -- lost --> retry(Data pressure) --> Raft commit
                                                         pressure + 1
```

The pending request therefore retains its original cause. If a size-triggered request is dropped, its eventual retry still counts as pressure. Network loss and retry timing cannot change classification.

Only the first command that successfully rolls the active segment applies the signal. A later duplicate finds that segment already sealed and becomes an idempotent no-op or, where applicable, a boundary correction; it cannot count the cause twice.

---

## Committed range-load state

Each active range stores one compact classification:

- **Unclassified** — no usable load observation since the range was created.
- **Idle** — the most recent classifying observation was an age-limit roll.
- **Pressure(N)** — `N` consecutive size-limit rolls have committed since the last age-limit roll.

The transition function is:

```text
Unclassified
    | age-limit roll
    v
   Idle <------------------------- age-limit roll
    |                                  |
    | size-limit roll                  |
    v                                  |
Pressure(1) --size--> Pressure(2) --size--> Pressure(threshold)

replication failure / recovery: remain in the current state
range split / merge / boundary correction: do not enter this state machine
```

Pressure is consecutive rather than windowed. An age-limit roll breaks the streak because the active range has demonstrated a full low-pressure interval. Failure and recovery leave the previous observation intact because neither says whether demand increased or decreased.

The pressure count is capped at the split threshold. A split proposal can be delayed, rejected as stale, or temporarily impossible, so further size rolls must not make bookkeeping grow or overflow.

---

## Automatic split

An active range becomes split-eligible when:

1. its committed load state reaches the pressure threshold; and
2. the topic's partition strategy permits splitting.

The leader proposes the split through Raft. The load transition itself happens during deterministic metadata apply, so every replica sees the same state after the same committed log. Only the leader originates the follow-up proposal.

```text
three committed size-limit rolls
              |
              v
       Pressure(threshold)
              |
              v
     leader proposes split
              |
              v
        Raft commits split
```

Existing apply-time checks continue to protect against stale proposals. If the range was already sealed or otherwise changed before the split commits, the proposal cannot corrupt current topology.

---

## Automated merge

The existing leader-side merge timer remains, but its role becomes smaller: it prompts a scan of committed metadata. It does not calculate idleness and its wall-clock value does not affect eligibility.

Two adjacent active ranges are merge-eligible only when both are explicitly idle:

| Left | Right | Merge eligible? |
|---|---|---|
| Unclassified | Idle | No |
| Idle | Pressure | No |
| Pressure | Pressure | No |
| Idle | Idle | Yes |

When an eligible pair is found, the leader proposes the merge through Raft. Existing adjacency, active-state, and partition-strategy checks remain authoritative when the proposal applies.

### Why split children start unclassified

Both children created by a split begin unclassified, not idle. An empty history is absence of evidence; it is not evidence that a child is cold.

This prevents an immediate split-merge loop:

```text
hot parent splits
      |
      +--> left child:  Unclassified --age roll--> Idle
      |
      +--> right child: Unclassified --age roll--> Idle
                                                 |
                                                 v
                                      merge may become eligible
```

Each child must independently survive an age interval without filling before the pair can merge. The explicit idle gate replaces the old wall-clock split cooldown for merge protection. A merged range also starts unclassified because its combined workload has not yet been observed.

---

## Physical time remains for retention

Removing wall time from load classification does not remove segment seal timestamps.

Every sealed segment keeps its physical `sealed_at` value. Retention continues to expire an oldest-first prefix when:

```text
sealed_at + retention period < current time
```

Split and merge timestamps also continue to describe when their segments and ranges were created or sealed. The separation is about use, not collection:

| Metadata | Ordering authority | Purpose |
|---|---|---|
| Segment seal time | Physical clock | Retention and operational visibility |
| Range-load state | Raft commit order | Automatic split and merge policy |

No load-classification decision reads seal time, creation time, current time, or retry time.

---

## Failure and edge cases

### Size and age become eligible together

The data plane assigns the cause at the source. If the segment is already at or above its size limit, data pressure wins; otherwise an age-triggered roll carries idle maintenance. Request deduplication preserves the first logical request that was emitted.

### A request is retried

The retry copies the original cause. Classification therefore depends on why the operation began, not how many packets were lost before it committed.

### Failure rolls an active segment

The successor is created normally, but the range keeps its previous classification. Failure is not demand.

### A range seal needs its exact segment end later

The report is explicitly identified as boundary correction rather than an active segment roll. It can update the already-sealed segment's end and restore offset continuity, but it cannot create a successor or touch range-load state.

### Leadership changes

No load history must be reconstructed. Classification is committed metadata, identical on every replica. The new leader can resume split and merge evaluation from the state it applied through Raft.

---

## Design rules

1. **Only a committed roll of the current active segment may classify range load.** This prevents retries, stale commands, range seals, and boundary correction from counting the same physical transition as new demand.

2. **Roll cause survives every delivery attempt.** Otherwise packet loss could turn pressure into a neutral event—or turn retry timing into a false load signal.

3. **Range topology operations never masquerade as segment load observations.** Split and merge seal ranges for lineage and routing; treating those seals as load would create self-reinforcing topology churn.

4. **Both merge inputs must provide positive idle evidence.** Unclassified means unknown, not cold. Requiring two committed age-limit rolls prevents newly split children from merging immediately.

5. **Physical time is retained but policy-neutral.** Retention needs wall time; deterministic split and merge decisions need committed event order. Keeping those responsibilities separate avoids weakening either one.

---

## Implementation and verification outline

Implementation crosses four conceptual boundaries:

1. Carry the cause from size, age, failure, and recovery triggers through request retries and the committed segment roll.
2. Distinguish active-segment rolls from post-range-seal boundary corrections in lifecycle messages and metadata apply.
3. Replace timestamp history and split cooldown state with the committed unclassified/idle/pressure classification.
4. Make split and merge eligibility read only that committed classification while leaving retention untouched.

Coverage must demonstrate:

- pressure streaks split exactly at the configured threshold;
- an age-limit roll resets pressure and marks the range idle;
- failure and recovery rolls do not change classification;
- reversed physical timestamps cannot change results determined by commit order;
- retries preserve their original cause and duplicate commits cannot count twice;
- split children and merged ranges start unclassified;
- two explicit idle children can merge, while either child being unclassified or pressured blocks merge;
- a range seal and its later boundary correction never update classification;
- segment retention remains based on physical seal time;
- repeated deterministic turmoil scenarios cover size, age, failure, recovery, retry, split, merge, and boundary-correction paths.

The persisted metadata and Raft command formats are serialized. Before implementation, compatibility expectations for existing stores must be decided: migrate old state to unclassified, or explicitly require a fresh store. Historical timestamps cannot reliably be converted into causes.

For the broader architecture, see [Metadata Management — Mental Model](mental-model.md). For the phase history, see [Metadata Management Roadmap](metadata_management_roadmap.md). Operational contracts live in `.agents/rules/metadata-state-machine.md`, `.agents/rules/raft-actor.md`, and `.agents/rules/ds-rsm.md`.
