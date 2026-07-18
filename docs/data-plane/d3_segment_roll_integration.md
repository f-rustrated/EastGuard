# Phase D3: Segment Lifecycle Integration

**Goal:** Connect data plane storage to metadata consensus. Three classes of integration: (1) seal triggers — size, inactivity, replication failure, node death — all converging on a single segment-roll committed through the coordinator, (2) lifecycle event propagation — topic creation, segment roll, range split, and range merge commits turn into segment assignments delivered to the data plane, (3) coordinator routing — the segment leader resolves and reaches the coordinator.

**Depends on:** Phase D2 (segment replication), metadata control plane (shard leader gossip via SWIM).

---

## Coordinator

The **coordinator** is the Raft leader for the shard group that owns the topic. It is not a separate component — it's a role that any vnode member can hold by winning a Raft election. The coordinator proposes metadata changes (topic creation, segment roll, range split, range merge) through Raft and sends lifecycle notifications to the data plane after commit.

**Routing** — how the segment leader locates the coordinator:

```
shard group id            ← cached by the segment from its assignment
    │
    ▼
local topology view        → current leader of that shard group:
                             leader node id + address + election term
    │
    ▼
send the seal request to that leader over the data port
```

The shard leader map is maintained by SWIM gossip (leader hints piggybacked on protocol messages), is term-monotonic (stale entries rejected), and is **not** cleared on node death (it is overwritten by the next election).

**Stale routing is safe.** A deposed leader rejects the proposal as "not leader". The segment leader refreshes its view from the local topology and retries after a seal-request timeout. Convergence may take several retries while SWIM gossip propagates the new leader — how many depends on cluster size and probe interval. There is no correctness risk, only added latency.

---

## Seal Triggers

Four triggers, one response: seal the current segment and open a new one via a segment roll. No retry, no ISR, no reconciliation.

| Trigger | Source | Detection | Initiator |
|---|---|---|---|
| Replication failure | Follower timeout | Sub-second (replication timeout) | Segment leader sends a seal request |
| Segment size | ~1GB threshold | On each flush | Segment leader sends a seal request |
| Segment inactivity | Configurable idle timeout | Periodic ticker (~60s) | Segment leader sends a seal request |
| Node death | SWIM protocol | ~6-7s | Coordinator proposes the roll directly |

### Trigger 1: Replication Failure (D2)

The detection itself was built in D2: a replication timeout fires when any follower fails to ack, and the segment leader raises a seal intent. D2 emitted that intent but left the coordinator target unresolved. D3 completes the flow by resolving the coordinator from the local topology view and carrying the real set of failed nodes in the seal request.

### Trigger 2: Segment Size

When a segment's accumulated size approaches the ~1GB threshold, the segment leader sends a seal request. This is a planned transition — no failed nodes, no uncommitted-tail complications.

```
flush completes
    │
    ├── size threshold reached?
    │       └── yes → seal request:
    │                   this segment,
    │                   failed nodes = none,
    │                   end entry id = last committed entry
    └── no → continue
```

The reported end entry id is the segment's last committed entry, which may lag the write cursor by in-flight entries — those are replayed into the new segment when the seal response arrives. This replay relies on the coordinator keeping the seal requester as the new segment's primary: the requester is alive (it just sent the request) and holds the uncommitted tail. The size check runs after every flush, not on a timer.

### Trigger 3: Segment Inactivity

When a segment has received no newly committed data for the configured idle timeout, the segment leader sends a seal request. This turns inactivity into a committed load signal: a quiet range can later merge with an equally quiet sibling, while occasional committed traffic keeps it active.

```
periodic idle ticker fires (~60s)
    │
    ├── for each active segment this node leads:
    │       time since last committed data beyond idle timeout?
    │           └── yes → seal request (failed nodes = none,
    │                                    end entry id = last committed entry)
    └── no → continue
```

Unlike the size and replication triggers, the inactivity check is driven by a periodic ticker. Successful committed progress refreshes the activity timestamp; stale or duplicate commit notifications do not. The ticker can therefore roll a truly idle segment even when no new write arrives to run a check.

### Trigger 4: SWIM Node Death

SWIM detects node death after ~6-7 seconds. The coordinator scans for active segments with the dead node in their replica set and proposes a roll for each. No seal request is involved — the coordinator has direct access to the metadata state machine. See [Node-Death-Triggered Seals](#node-death-triggered-seals) for the full flow.

---

## Seal Flow: Seal Request → Roll → Seal Response

The full round-trip connecting the data plane to metadata consensus.

### Message Flow

```
Segment Leader (node D)           Coordinator (node A)           Raft [A, B, C]
        │                               │                            │
  trigger fires                         │                            │
        │                               │                            │
  resolve coordinator                   │                            │
  from local topology                   │                            │
        │                               │                            │
        │── seal request ─────────────► │                            │
        │   (segment, failed          receive seal request           │
        │    nodes, end entry id,                                     │
        │    requester)              dedup against in-flight seals    │
        │                            build the roll                   │
        │                            store pending context            │
        │                               │                            │
        │                               │── propose roll ──────────► │
        │                               │                            │ commit
        │                               │◄─── commit notification ───│
        │                               │                            │
        │                          ALWAYS: segment assignment ──► new primary
        │                               │                            │
        │◄── seal response ─────────────│  (best-effort, only if     │
        │   (old segment,               │   pending context still    │
        │    new segment id,            │   exists on this leader)   │
        │    new replica set)           │                            │
        │                               │                            │
  open new segment                      │                            │
  replay uncommitted tail               │                            │
  resume produce (~100ms)               │                            │
```

### Information Carried

The flow moves three pieces of information, each scoped to where it is needed:

- **Seal request** (segment leader → coordinator): the requester's identity, which segment to seal, which nodes failed (empty for size/age seals), the last committed entry id, and the roll intent. The intent distinguishes size pressure, idle maintenance, replication failure, and post-range-seal boundary correction. The requester identity is carried **explicitly** in the request — it is not inferred from the transport connection. Node-death-triggered recovery rolls do not use a seal request at all and therefore have no requester.

- **Roll command** (proposed through Raft): which segment, the seal timestamp, the new replica set, an *optional* end entry id, and the intent. The end entry id is present for segment-leader-initiated seals (it came from the seal request) and absent when recovery does not yet know the committed boundary. Only a successful roll of the active segment applies a load signal; boundary correction fills an already-sealed segment's end without creating a successor.

- **Pending context** (held by the coordinator between propose and commit): the requester and the old segment identity, keyed by the proposal's log position. The coordinator also keeps a dedup set of in-flight seal keys so duplicate seal requests for the same segment are skipped.

### Channel Wiring

```
                          data-port transport (TCP)
                         /            │            \
                inbound messages      │      outbound messages
                        │             │             ▲
                        ▼             │             │
                   data plane         │       outbound channel
                   (OS thread)        │       (coordinator → transport)
                        │             │             │
                forwarding channel    │             │
                (data plane →         │             │
                 coordinator)         │             │
                        ▼             │             │
                   coordinator / Raft layer (tokio)
```

| Channel | Direction | Purpose |
|---|---|---|
| Seal-request forwarding | data plane → coordinator | Forward seal requests for Raft proposal |
| Lifecycle outbound | coordinator → data-port transport | Send seal responses and segment assignments |

Both channels send batched slices rather than individual messages, per the project-wide batching convention (see `code-convention.md`). The sender accumulates during its event loop, then sheds unused capacity before sending.

### Data Plane: Seal-Request Forwarding

On an inbound seal request from the data port, the data plane converts it to a coordinator-bound command, accumulates it in the outgoing batch, and forwards it without blocking — the data plane runs on a dedicated OS thread and must not block on the async runtime. A dropped batch (full channel) is not fatal: the seal-request timeout retries it.

### Coordinator: Seal-Request Handling

```
on a seal request (requester, segment, failed nodes, end entry id, intent):
    if this segment already has an in-flight seal → skip (dedup)
    compute new replica set = current replica set − failed nodes + healthy nodes
    build the roll (segment, seal timestamp, new replica set, end entry id, intent)
    propose through Raft:
        accepted → record the in-flight seal key + pending context
        rejected (not leader) → no reply; the requester retries on timeout
```

### Data Plane: Seal-Request Dispatch and Timeout

**Dispatch:** on a replication timeout, after a flush size check, and on the periodic age ticker, the data plane resolves the coordinator from the local topology view and sends a seal request to it over the data port.

**Timeout (~5s):** when a seal request is sent, the data plane records its failed nodes and intent. If no seal response arrives before the timeout, it refreshes the coordinator from the topology and retries the same logical request with the same intent. For size- and age-triggered seals this is the only recovery path — the replication timeout won't fire, since replication is healthy.

### Applying a Roll

Applying a roll seals the active segment with its end offset set to the reported end entry id, and creates the new segment starting one past that. This aligns the metadata view with the actual data boundary reported by the segment leader.

When the end entry id is absent (node-death seals), the coordinator doesn't know the actual committed offset, so the sealed segment's end offset and the new segment's start offset are left unset until corrected — either by a later seal request from the segment leader (if it is still alive) or by D5 sealed-segment repair.

### Roll Idempotency

Both the write-path timeout (D2) and SWIM node death may fire for the same failure. This is safe: applying a roll first checks that the segment is still active. If it has already been sealed, the duplicate is a no-op rather than an error (see the roll-idempotency invariant in `metadata-state-machine.md`).

**Exception: offset correction.** If a death-triggered roll (with no end entry id) commits first, and the segment leader's seal request arrives later carrying the correct end entry id, an offset-correction step fills in the sealed segment's end offset and the new segment's start offset. It applies only when the current end offset is unset and the incoming end entry id is present; re-application is a no-op, and no duplicate segment assignment is sent.

---

## Lifecycle Event Propagation

After Raft commits a metadata command that creates new segments, the coordinator sends a segment assignment to each new segment's primary. Followers self-authorize from the primary's first replication append (D2).

| Metadata Event | Segments Created | Assignment Target |
|---|---|---|
| Topic created | 1 (initial segment for the initial range) | new segment's primary |
| Segment rolled | 1 (replacing the sealed one) | new segment's primary |
| Range split | 2 (one per child range) | each child's primary |
| Range merged | 1 (for the merged range) | merged range's primary |

### Apply Result

Applying a metadata command yields a result describing what was created — enough, on its own, to construct the corresponding segment assignment(s):

- **Topic created** — the new topic, its initial range and segment, and that segment's replica set.
- **Segment rolled** — the rolled segment's identity, the new replica set, and the end entry id.
- **Range split** — both child ranges, each with its new segment and replica set.
- **Range merged** — the merged range, its new segment, and its replica set.
- **Topic deleted** — no segment created.
- **No-op** — nothing changed (e.g., an idempotent re-applied roll).

The rolled-segment result carries everything needed to build the assignment **independently of the coordinator's pending context**. This is what guarantees the assignment is always sent, even when a coordinator leadership change has lost the pending context.

### Segment Assignment

A segment assignment carries: the segment identity, the shard group id (routing metadata the segment caches for future coordinator resolution), the replica set, and the starting entry id (0 for brand-new segments; one past the previous segment's end for rolls).

### Commit Event

Applying a committed metadata entry can raise zero or more metadata events, each carrying the shard group and log position when it leaves the Raft state machine. **All replicas produce the same events** (apply is deterministic), but **only the leader dispatches** the resulting notifications — followers apply the entry and drop them.

### Dispatch (leader only)

```
segment rolled:
    ALWAYS: segment assignment → new primary (start = end entry id + 1)
    if pending context exists for this roll:
        clear the in-flight seal key
        seal response → requester (best-effort)

topic created:
    segment assignment → primary (start = 0)

range split / range merged:
    segment assignment → each new segment's primary
```

The event stream is produced on all replicas; only the leader dispatches it. For a rolled segment the assignment is always sent (the event carries full context); the roll response is sent only when pending context still exists on this leader. A single entry may also raise independent segment-seal or consumer-group events, so those effects are not hidden inside the roll, split, or merge event.

### Active-Segment Lookup

Node-death handling needs to find every active segment a given node hosts. The metadata state machine answers this with a full scan: topics → active ranges → active segment, filtered by membership in the replica set. Node death is rare and the scan is microseconds for typical segment counts; if counts grow into the thousands, a reverse index can be added later.

---

## Node-Death-Triggered Seals

When SWIM reports a node death, the coordinator does two things for every group it leads: bring the dead node out of the Raft peer set, and seal every active segment the dead node was part of.

```
on node death (dead node):
    for each group this node leads:
        if the dead node is a peer → propose its removal through Raft

        for each active segment where the dead node ∈ replica set:
            new replica set = replace the dead node with a healthy node
            propose a roll with no end entry id
              (the committed offset is unknown — resolved later by
               offset correction or D5 repair)
```

Peer removal flows through the Raft log rather than direct mutation, so every replica agrees on the peer set at the same log position.

**Segment-leader preservation:** when a follower dies, the coordinator keeps the existing leader as the new segment's primary. When the leader itself dies, a surviving follower is promoted. Replacement nodes are chosen from healthy cluster members not already in the set, tie-broken by node id.

---

## Wire Protocol

Updated message catalog (D3 additions in bold):

| Message | Direction | Carries | Phase |
|---|---|---|---|
| Replica append | Leader → followers | segment, replica set, data, record count, entry id | D2 |
| Replica ack | Follower → leader | segment, entry id, sender | D2 |
| Commit advance | Leader → followers | segment, committed entry id | D2 |
| Seal request | Leader → coordinator | segment, failed nodes, end entry id, **requester** | D2 |
| Seal response | Coordinator → leader | old segment, new segment id, new replica set | D2 |
| Segment sealed | Leader → old followers | segment | D2 |
| **Segment assignment** | **Coordinator → leader** | **segment, shard group id, replica set, start entry id** | **D3** |

The seal request now carries the requester's identity explicitly rather than inferring it from the transport connection. The segment assignment message type existed in D2 but was never sent; D3 implements the send path.

---

## Examples

### Topic Creation End-to-End

```
Client                 Coordinator A          Raft [A,B,C]         Broker D
  │                         │                      │                  │
  │── create topic ───────► │                      │                  │
  │                         │── propose ──────────►│                  │
  │                         │                      │ commit           │
  │                         │◄─────────────────────│                  │
  │◄─── ok (topic id) ──────│                      │                  │
  │                         │── segment assignment ──────────────────►│
  │                         │   (segment (0,0,0),  │          create segment
  │                         │    replicas [D,E,F], │          (leader)
  │                         │    start 0)          │     ready for produce
```

### Size-Based Seal

```
Broker D (segment leader)       Coordinator A        Raft [A,B,C]
   │                                  │                    │
   │ size exceeds ~1GB                │                    │
   │── seal request (seg 7,           │                    │
   │   failed [], end 42000) ────────►│                    │
   │                                  │── propose ────────►│
   │                                  │                    │ commit
   │                                  │◄───────────────────│
   │◄─── seal response (seg 8) ───────│                    │
   │                                  │                    │
   │ open seg 8 (start 42001)         │                    │
   │ replay uncommitted tail          │                    │
   │ resume produce                   │                    │
```

---

## Invariants

1. **The coordinator is the sole proposer for segment lifecycle.** Every roll proposal comes from the coordinator — either on behalf of a seal request or directly on SWIM node death. The data plane never proposes Raft commands.

2. **Segment assignments are sent only by the coordinator, only after Raft commit.** No speculative assignments — the segment exists in the metadata state machine before any data node learns about it.

3. **Seal responses are best-effort.** A response is lost if the coordinator changes between propose and commit (the new leader has no pending context). The segment leader detects this via the seal-request timeout and retries.

4. **Apply results are produced on all replicas but dispatched only by the leader.** Followers apply but don't notify. The segment assignment for a roll is always sent (the result carries full context); the seal response is sent only when pending context exists.

5. **Seal-request routing is retryable.** A proposal rejected because the target is no longer leader triggers a retry after refreshing the shard leader view from SWIM gossip. Convergence may take several rounds — safe, affecting only latency.

6. **The end entry id handoff is exact for segment-leader-initiated seals.** It equals the segment's last committed entry, is carried through the roll into the sealed segment's end offset, and the new segment starts one past it.

7. **Node-death seals carry no end entry id.** The coordinator doesn't know the actual offset. It is corrected later — by a seal request from the segment leader (if alive) or by D5 sealed-segment repair. This temporarily violates the metadata state machine's offset-continuity invariant (offset continuity within a range), which holds again after correction.

8. **Size- and inactivity-based seals reuse the failure path.** Same flow, with no failed nodes and the replica set preserved.

9. **Followers need no segment assignment.** They self-authorize from the leader's first replication append (D2).
