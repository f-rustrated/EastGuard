# D9: Offset Placement Graduation

**Goal:** Ensure that when a range's ownership moves to new replicas, the new replicas do not
act as authoritative offset sources until they have safely copied the current consumer
checkpoints. This prevents checkpoints from temporarily appearing absent during the move.

**Depends on:** [D8: Consumer Offset Management](d8_consumer_offset_management.md),
[D2: Segment Replication](d2_segment_replication.md), and
[C4: Consumer Offset Commits](../clients/c4_consumer_offset_commits.md).

---

## The Gap (The Problem)

Consumer offsets (checkpoints of where a consumer group is reading) are attached to a "range", not a specific segment. When a segment gets full and we "roll" to a new segment, we might decide to store that new segment on different replica nodes. However, the offsets themselves stay with the range.

Imagine a range's data is placed like this:

```text
old segment:       A  B  C
new segment (roll): A  B  D
```

Before the roll, a checkpoint was saved safely on A, B, and C. During the roll, we tell A, B, and D that they are the new replica set. But we *don't* automatically copy the old checkpoint over to D.

Immediately after this change:

| Node | Status | Does it know it's a replica? | Does it have the old checkpoint? |
|---|---|---|---|
| A | Kept | Yes | Yes |
| B | Kept | Yes | Yes |
| C | Removed | Yes (but stale) | Yes |
| D | Added | Yes | **No (Missing)** |

If a consumer makes a brand new commit, D will get it and be up to date. But if the consumer
is inactive, D never gets the old checkpoint. If A or B later fail, D might be asked to serve
an authoritative offset read, but it is missing the checkpoint. We need a way to bring D up
to date before we trust it.

---

## Placement Is Not Readiness

Just because a node (like D) is told "you are in the replica set", doesn't mean it has all the historical data it needs. Being *placed* in the set is different from being *ready* to serve.

We define explicit stages for a new replica:

```text
              Placement Learned (Told it's a replica)
                     │
                     ▼
                  Joining
             (Accepts incoming historical data,
              but is NOT trusted yet)
                     │
           Durable Bootstrap Complete (Finished copying to disk)
                     │
                     ▼
                   Ready
           (Trusted to serve offset data)
```

This strict graduation process is only for offset authority. Standard segment data continues to use its normal replication rules.

---

## Range-Offset Bootstrap (How we copy the data)

When a roll adds a new replica, an existing, fully "ready" replica takes charge. It transfers
the complete current ledger slice for that range to the new replica: the latest generation
and committed position for every known consumer group, including groups that have not been
active recently. The ledger does not retain the history of every earlier commit.

```text
Control Plane              A (Ready Source)                 D (Joining)
     │                            │                              │
     │── 1. Tells A the new set ─►│                              │
     │── 1. Tells D the new set ────────────────────────────────►│
     │                            │                              │
     │                            │── 2. Sends current offsets ─►│
     │                            │                              │ 3. D saves to disk
     │                            │                              │
     │                            │◄── 4. D says "I am done" ────│
     │                            │                              │
     │                            │                    D becomes ready
```

The transfer (bootstrap) includes:

- **Topic/Range Identity:** So D knows what data this is.
- **New Segment Identity:** So we ensure this data matches the current placement change.
- **Source/Destination:** To ignore wrong or bounced messages.
- **Group Generation & Position:** The actual checkpoint data.

---

## Durable and Monotonic Import (Safe Conflict Resolution)

Copying historical data (bootstrap) is a trusted internal transfer, not a brand new action
from a client. Therefore, we don't apply the normal "is this client generation up to date?"
checks that we do for normal commits.

When the joining replica receives this historical data, it merges it **monotonically** (safely moving forward):

- It only updates its stored generation if the incoming one is *greater*.
- It only updates its stored position if the incoming one is *greater*.
- If it receives the same transfer twice by mistake, nothing changes (harmless).
- If a live consumer makes a brand new commit at the same time, the new commit has a greater position, so it won't be overwritten by the older historical data.

To survive crashes, the imported data and a special "bootstrap-completion marker" must both be written safely to the disk log (WAL). The joining replica only graduates to "ready" *after* this disk save is complete. If the node crashes halfway through, it wakes up, sees the data but no completion marker, and knows it needs to resume the import rather than blindly trusting incomplete data.

---

## Coordination and Retries

Two leaders participate in different parts of graduation:

| Role | Responsibility |
|---|---|
| Metadata Raft leader | Commits the successor placement and re-drives incomplete graduation |
| Range data leader | Serves offset reads and commits, and coordinates transfer from a ready source |

The range data leader knows the active placement used by offset traffic. It only selects
fully ready replicas as bootstrap sources. The metadata leader treats bootstrap as
declarative reconciliation: it can announce the committed placement again after message
loss or leadership change without creating a second transition.

The transfer process is self-healing:

1. The control plane announces the new placement. New replicas become "joining".
2. A "ready" survivor sends the current range-ledger state to the joining replicas.
3. The joining replica saves it and sends back an acknowledgement.
4. If the acknowledgement is lost, the transfer is announced again and the source re-sends
   the data.
5. If either leader changes, the successor reconstructs the work from committed placement
   and durable readiness state rather than relying on the previous leader's memory.

If all ready survivors are unavailable, offset reads fail closed rather than returning
incorrect empty checkpoints. Service can resume when a ready replica restarts or another
trusted copy is recovered and graduated.

---

## Relationship to Segment Recovery

Offset graduation reuses the control pattern already used to repair sealed segment data:

| Segment repair | Offset graduation |
|---|---|
| Announce required placement | Announce required offset placement |
| Select a surviving source | Select a ready surviving source |
| Transfer missing segment bytes | Transfer the current range-ledger slice |
| Verify and durably register the segment | Monotonically merge through the shared WAL |
| Acknowledge durable catch-up | Acknowledge durable graduation |
| Re-drive until confirmed | Re-drive until confirmed |

The payload and completion check remain separate. Segment repair copies immutable bytes and
verifies a committed end; offset graduation copies a mutable map of group generations and
positions while ordinary commits may still arrive. Sharing the reconciliation shape avoids
a second recovery lifecycle without pretending the two kinds of state have the same merge
rules.

---

## Leadership Changes

Bootstrap is fenced by the successor segment identity, which acts as the placement token for
the transition. Every transfer and durable completion marker carries this token. A delayed
message may still contribute a newer checkpoint through monotonic merge, but it cannot mark
a replica ready for a different placement.

### Metadata leader change

A metadata leadership change does not alter the committed range placement. The new leader
re-drives the same placement token. A joining replica resumes its transfer; a replica that
already stored the completion marker answers again without rewriting its state.

```text
old metadata leader fails
          │
new leader reads committed placement
          │
re-announces the same graduation token
          │
joining replica resumes, or ready replica re-acknowledges
```

This keeps completion recoverable without placing transient transfer progress in the Raft
log.

### Range data leader change

Offset leadership follows the first member of the active data replica set. When that leader
fails, recovery rolls the active segment again and places a suitable surviving replica first.
That second roll creates a new successor identity and therefore supersedes the earlier
bootstrap attempt.

```text
S2 placement: A (ready leader), B (ready), D (joining)
A fails
S3 placement: B (ready leader), D (joining), E (joining)
```

B continues serving offsets and bootstraps the joining replicas for S3. A delayed completion
for S2 cannot graduate D for S3. The conservative first implementation bootstraps D again
under the S3 token; a later optimization may reuse D's durable state after proving it covers
the required boundary.

A joining replica must never become authoritative merely because it appears first in a new
placement. Recovery should choose a ready survivor first whenever one exists. If none exists,
offset reads remain unavailable until a replica is safely graduated.

---

## Reads and Commits During Graduation

While the new replicas are catching up, the system keeps working:

- The existing "ready" leader continues serving read requests.
- New offset commits are still accepted. They are sent to the new replicas too, where they safely merge with the historical data being transferred.

**A joining replica CAN:**

- Receive new commits.
- Receive historical bootstrap data and save it.
- Acknowledge that it saved data.

**A joining replica CANNOT:**

- Tell a client that a checkpoint doesn't exist (because it might just not have downloaded it yet).
- Become the leader for reads.
- Act as a source to send data to someone else.
- Graduate to "ready" before the completion marker is safely on disk.

If a client accidentally asks a joining replica for data, the replica will return a "not ready" error, and the client will simply retry until it finds the true leader.

---

## Failure Handling

| What went wrong | How we fix it |
|---|---|
| Transfer message is lost | The source resends it. |
| The "I am done" message is lost | The source resends the data. The joining replica safely ignores the duplicate data. |
| Joining replica crashes before saving to disk | When it restarts, it sees no completion marker and stays in the "joining" state. |
| Joining replica crashes after saving to disk | When it restarts, it sees the completion marker and resumes as "ready". |
| The source replica dies during transfer | Another ready replica takes over and resends. |
| Metadata leader changes | The new leader re-drives the same committed placement token. |
| Range data leader changes | A new roll supersedes the old token and a ready survivor resumes coordination. |
| An old transfer arrives after another roll | Its data may merge monotonically, but it cannot complete the new token. |
| A new commit arrives at the exact same time | The monotonic merge ensures the newer (greater) commit is kept. |
| All ready replicas are unavailable | Reads fail closed until a ready replica returns or trusted state is recovered. |
| A consumer group is completely idle | The bootstrap still copies its old checkpoint safely. |

---

## Scope Boundaries

This process applies whenever a segment roll preserves the range identity but changes the
active replica set. The roll may be triggered normally by sealing or by node-failure
recovery.

It does **not** handle copying offsets when ranges are split into two, or merged together. Those create brand new range identities and require clients to handle the transition. It also doesn't apply to background data repair.

---

## Target Guarantees

These are the strict goals we are aiming for:

1. **No trusting incomplete replicas:** A replacement cannot serve authoritative offset
   reads until its import is safely on disk.
2. **Idle groups are protected:** We copy checkpoints for all groups, not just active ones.
3. **Safe from network weirdness:** Delayed or out-of-order messages won't accidentally roll a checkpoint backward.
4. **Disk durability:** Graduation only happens after the data is physically on the disk.
5. **Only copy from the good ones:** We never use an incomplete replica as a source.
6. **When in doubt, fail closed:** A joining replica must never return an empty checkpoint if it isn't sure.

---

## Implementation Plan

1. Build the logic to let a node enumerate its current range-ledger state and safely merge
   incoming data.
2. Create the messages for transfer, acknowledgement, and the disk completion marker.
3. Track the "joining" and "ready" states.
4. Trigger the transfer whenever a roll adds a new node.
5. Build reconciliation for lost messages, metadata leadership changes, and replacement
   rolls after data-leader failure.
6. Block un-ready nodes from serving reads or acting as sources.
7. Write tests for disk saving, duplicates, out-of-order messages, and recovery.
8. Write a full system test that proves an idle checkpoint survives a replica replacement.
