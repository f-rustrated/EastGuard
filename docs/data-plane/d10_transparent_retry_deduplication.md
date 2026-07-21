# D10 — Transparent Retry Deduplication (Write Idempotency)

**Goal:** Prevent a transparent producer retry from storing the same logical batch
twice, while preserving EastGuard's direct-to-data-leader write path across crashes,
segment rolls, and range lineage changes.

**Depends on:** [D2: Segment Replication](d2_segment_replication.md),
[D3: Segment Lifecycle Integration](d3_segment_roll_integration.md),
[D5: Crash Recovery](d5_crash_recovery.md),
[D6: Produce/Consume API](d6_produce_consume_api.md),
[D9: Offset Placement Graduation](d9_offset_placement_graduation.md), and
[C2: Producer](../clients/c2_producer.md).

---

## Delivery contract

Within a live producer session:

- the first successful request appends one entry;
- retrying it never appends another entry;
- a retry inside the bounded result window returns its original range and entry id;
- an older retry is still recognized as committed, but its position may have aged out;
- activating a newer producer incarnation fences the older one;
- expired and unknown sessions reject produce requests and are never auto-created.

This is transparent **producer retry deduplication**, not application-level idempotency.
Arbitrary business keys that must deduplicate across producers or ranges need a separate
index with its own routing, conflict, retention, and compaction contract.

It is also not end-to-end exactly-once processing. Applications can create duplicates by
sending the same records under a new request identity or by starting a new session instead
of recovering an ambiguous one. Cross-range transactions and atomic coupling to consumer-
offset commits remain outside this phase.

---

## Two protocol layers

The control plane owns producer authority; the data plane owns append ordering and
durability.

| Layer | State and operations | Frequency |
|---|---|---|
| Topic Raft group | Open session, recover session and increment incarnation, renew lease, expire session, activate fences | Low |
| Range data leader | Check sequence, append entry plus producer identity, replicate, advance producer state, answer duplicates | Every produce |

No produce request enters the Raft log. The existing primary-backup data replication and
all-replica fsync still apply; “no consensus per produce” means no metadata-Raft proposal,
not no cross-node durability.

```
control plane:  session lifecycle ── fence ──► active range leaders

data plane:     client ── produce ──► range leader ── replicate ──► followers
                                      WAL + fsync                    WAL + fsync
```

---

## Request identity and batching

A request is identified by:

| Part | Purpose |
|---|---|
| Producer id | Broker-issued random session identity persisted by the client for recovery |
| Incarnation | Metadata-allocated fencing epoch |
| Range id | Selects the producer-specific ordered stream |
| Sequence | Zero-based contiguous batch number for this producer and range |
| Payload digest | Detects reuse of a recent identity with different content |

Sequence belongs to the immutable broker batch, not to each application record. The
current per-record counter is only a client-side seam and must be replaced.

One client flush can contain records that a refreshed routing snapshot places in different
ranges. Before assigning sequences, the producer divides it into one immutable broker
batch per destination range:

```text
client flush [a, b, c, d]
       │ refresh routing
       ├── range 4: [a, c] → producer/range-4 sequence 17 → node A
       └── range 9: [b, d] → producer/range-9 sequence 17 → node D
```

The two sub-batches may both use sequence 17 because range id is part of their identity.
A stored entry never spans ranges or nodes. Within one range, batch closure allocates the
next sequence atomically and a per-range dispatch queue preserves order. Different ranges
continue concurrently. Once assigned, a retry preserves both the sequence and the exact
serialized bytes.

A segment roll preserves the range and its sequence stream. A split or merge closes the
parent streams and starts sequence zero for each successor range.

---

## The range producer state: one integer plus a small ring

For each live `(producer session, range)`, the range stores only two things:

1. **Committed frontier.** The highest durably committed sequence. This one integer
   decides whether a request is next, duplicate, or unexpectedly ahead.
2. **Recent results.** A fixed-size ring of recent sequence, digest, and committed-
   position tuples. It exists only to return positions for in-flight retries and detect
   recent identity conflicts.

The frontier prevents duplicates for the entire session. The ring does not grow with the
number of requests and must be at least as large as the producer's maximum in-flight
batches per range.

### Range-leader verification pipeline

```text
request(session, incarnation, range, sequence, digest)
│
├── session unknown or expired
│      └── SessionExpired
├── incarnation older than installed fence
│      └── ProducerFenced
├── dedup state or placement still recovering
│      └── DedupStateRecovering
├── sequence == frontier + 1
│      ├── same request already staged ── RequestInFlight
│      └── otherwise ACCEPT
│             └── WAL(entry + producer identity) → replicate → fsync all → advance
├── sequence <= frontier
│      ├── recent + same digest ──────── Duplicate(position)
│      ├── recent + different digest ─── RequestIdentityConflict
│      └── aged out of ring ──────────── DuplicatePositionUnavailable
└── sequence > frontier + 1
       └── SequenceGap { expected: frontier + 1 }
```

An older request outside the ring is rejected as a duplicate regardless of its current
payload. The broker can no longer diagnose a digest conflict or return the exact position,
but it never appends the request again.

The normal producer API keeps unresolved requests inside the result window, so its send
future still completes with a committed position. A low-level historical retry may receive
successful deduplication without one.

---

## Session lifecycle and fencing

A producer session is scoped to one topic, whose metadata already lives in one shard
group. Opening creates a random producer id and incarnation zero. Recovering an existing
session commits the next incarnation through Raft.

After the increment commits, the coordinator broadcasts the new fence to every active
range leader and waits for older in-flight writes to drain or fail. The recovered producer
cannot send until that activation completes. Assignments created by a concurrent roll,
split, or merge install the current fence before accepting writes.

```
restarting producer       metadata coordinator        range leaders
        │                          │                        │
        ├── recover ──────────────►│                        │
        │                          ├── Raft: incarnation+1  │
        │                          ├── install fence ──────►│
        │                          │◄── old writes drained ─┤
        │◄── active incarnation ───┤                        │
```

The client journals its producer id, incarnation, unresolved immutable batches, and the
next sequence for every touched range. Recovery reconciles those journaled ranges with
their durable frontiers before retrying ambiguous batches.

### Inactivity and deletion

Activity renews a coarse session lease, not one metadata record per produce. The metadata
leader proposes expiration after the lease and grace period pass; only committed
expiration invalidates the session.

Range leaders install the expiration fence before producer state becomes garbage-
collectable. Reopening later creates a new random producer id, so sequence zero cannot
collide with a delayed request from the old session. Explicit close may accelerate the
same process, but correctness does not depend on clean client shutdown.

---

## Durability and recovery

The frontier is an in-memory index, but the facts used to rebuild it are durable:

- the producer identity and sequence ride in the existing entry WAL record;
- followers receive and fsync the same identity with the entry;
- the leader advances the frontier only after the entry reaches the normal D2 commit
  boundary;
- recovery loads the consolidated auxiliary-state snapshot and replays later committed
  entries up to the recovered commit boundary.

This adds no second per-produce WAL record or fsync. Consumer offsets and producer
frontiers use the single consolidated end-state snapshot defined with D8/D9: one typed
snapshot, one asynchronous save pipeline, and one auxiliary-state WAL checkpoint
watermark. Producer state is another typed section in that design, not an independent
snapshot lifecycle.

Segment checkpointing remains separate because segment files retain application payload
and sparse seek structure. The auxiliary snapshot retains only current consumer offsets,
placement readiness, producer frontiers, and bounded recent results.

### Leader failure

If an entry becomes durable and its response is lost, a surviving replica rebuilds the
same frontier from the auxiliary snapshot and committed WAL suffix. When recovery changes
the replica set, D9 graduation installs the auxiliary state on joining replicas before
they become authoritative. A retry therefore resolves as a duplicate after leader
failover.

### Segment roll

A roll retains the range identity, so its producer state continues unchanged. If the
replica set changes, producer state uses the same D9 placement trigger, successor-segment
token, ready source, snapshot envelope, durable completion marker, joining/ready states,
and retry machinery as consumer offsets. The transport and readiness framework are
shared; each typed section keeps its own monotonic merge rules.

---

## Split and merge: freeze the parent

When a range splits or merges, closure first settles every staged request against the
committed boundary; its producer state then becomes a complete, read-only frozen snapshot.
Successor ranges start new producer-range streams at sequence zero; they do not copy the
parent frontier.

Retries issued against the parent before it closed continue to target that frozen state:

| Frozen-parent result | Action |
|---|---|
| Sequence at or below the frontier, result retained | Return duplicate with the parent position |
| Sequence at or below the frontier, result aged out | Return duplicate without position |
| Sequence exactly after the frontier | Return `RangeClosedNotCommitted`; the client may repartition into successor batches |
| Frozen state unavailable or incomplete | Return a retriable recovery error; never infer non-commit from missing state |

Routing metadata directs the retry to a replica holding the frozen parent state. The
client may form new child requests only after the complete frozen frontier proves that
the parent request did not commit. This closes the acknowledgment-loss window across a
split or merge.

Frozen parent state remains available until its referenced producer sessions expire. If
repair moves the sealed parent data, its auxiliary-state section moves through the same
D9 graduation mechanism.

---

## Bounds and public API

The session contract fixes:

| Bound | Effect |
|---|---|
| Session lease / maximum retry age | Expired requests receive `SessionExpired`, never a new append |
| Maximum live sessions | Bounds topic- and cluster-wide producer state |
| Maximum touched ranges per session | Bounds the number of frontiers |
| Recent results per producer-range | Bounds exact position and digest history |

The consolidated snapshot contains the same bounded end state. Request identities after
its LSN boundary are replayed from WAL; older WAL can be reclaimed after both segment and
auxiliary checkpoint watermarks permit it.

Produce requests add producer id, incarnation, producer-range sequence, and digest.
Successful responses distinguish newly appended, duplicate with position, and duplicate
without position.

Keep a clearly named raw/at-least-once append only if internal callers need it. The public
producer should default to idempotent sessions only after this protocol is implemented
end to end. Until then EastGuard's public delivery contract remains at-least-once.

---

## Validation

### Sequence and deduplication

- Normal next sequence appends once and advances the frontier after durability.
- Losing an acknowledgment and retrying returns the same position with one stored entry.
- A recent identity with different bytes returns an identity conflict.
- A sequence gap returns the expected sequence without appending.
- An aged-out sequence returns duplicate without position and does not append.
- Concurrent range batches advance independently without false gaps.

### Session lifecycle and fencing

- Activating a newer incarnation drains the boundary and fences every later old write.
- Expired and unknown sessions reject writes and are never implicitly recreated.
- Admission, touched-range, and recent-result limits keep state bounded.

### Failover and recovery

- Crash the write leader after follower fsync but before the client response; the
  successor returns the original position for the unresolved retry.
- Restart all replicas and reconstruct the same frontier and recent window from the
  consolidated snapshot plus committed WAL suffix.
- Migrate a legacy consumer-only snapshot before reclaiming covered WAL.

### Topology transitions

- Retry across a segment roll and changed replica placement without appending twice.
- Retry against a frozen split or merge parent and return duplicate or proven non-commit.
- Move frozen parent state during repair and keep it queryable.

Deterministic data-plane tests should cover sequence decisions and state transfer.
Multi-node turmoil tests should pin RNG and node ids and use size-based rolls, following
the repository's data-plane test rules.

---

## Implementation phases

1. **Storage and WAL framing.** Extend entry identity, generalize the consumer-only
   checkpoint into the typed D9 auxiliary-state snapshot, and support legacy snapshot
   migration.
2. **Local deduplication.** Add the frontier plus recent-result ring and the range-leader
   verification pipeline; change client batching to assign immutable per-range sequences.
3. **Session lifecycle and fencing.** Add Raft-backed open, recover, renew, expire, and
   active-range fence installation; add the durable client recovery journal.
4. **Topology transitions.** Reuse D9 graduation for rolls and repair, and add frozen-
   parent lookup for split and merge retries.

## Design rules

1. **One immutable request identity names one broker batch.** A retry never changes its
   sequence or serialized bytes.
2. **Ordering is per producer and range.** Different ranges progress concurrently without
   creating false gaps.
3. **The durable frontier decides append versus duplicate.** The recent ring only enriches
   duplicate responses; evicting it cannot reopen an append.
4. **Raft owns session authority, not produce throughput.** Produce requests use the data
   replica protocol and never enter the metadata log.
5. **A newer incarnation becomes usable only after fencing completes.** An older process
   cannot write after recovery activates its successor.
6. **Rolls preserve producer state; split and merge freeze it.** Successors start new
   streams while parents remain available for retry decisions.
7. **Missing state never proves non-commit.** Recovery uncertainty delays a retry instead
   of turning it into a new append.
8. **Expired or unknown sessions reject every produce.** They are never auto-created.
9. **Acknowledgment follows full data-replica durability.** Entry and request identity are
   fsynced together before success.

These rules describe the proposed phase, not current behavior. Current production remains
at-least-once until every layer is implemented and validated.
