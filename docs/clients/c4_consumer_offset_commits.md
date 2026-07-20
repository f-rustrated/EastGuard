# Phase C4: Consumer Offset Commits

**Goal:** Make durable consumer progress mean “the application may safely resume after this
record,” while preserving that meaning across batching, rebalances, range lineage, and broker
restarts.

**Depends on:** [C3: Consumer](c3_consumer.md) for record positions and lineage, and
[D8: Consumer Offset Management](../data-plane/d8_consumer_offset_management.md) for
generation fencing, storage, and replication.

---

## The Client-Side Boundary

The broker knows which records are committed to the range log. It does not know whether an
application successfully processed a delivered record. C4 defines the boundary between those
events.

If delivery itself made a record committable in the default mode, an auto-commit could race
ahead of application processing:

```
fetch actor          consumer API          application          commit timer
    │                     │                    │                     │
    │── record ──────────►│                    │                     │
    │                     │── returns ────────►│                     │
    │                     │                    │── processing ...    │
    │                     │                    │                     │── commit
    │                     │                    │── crash             │
```

Restart would resume after a record whose processing never completed. The client therefore
separates delivery, acknowledgement, and durable commit.

---

## Local Progress per Range

For each effectively owned range, the client tracks:

| Position | Meaning | Durable? |
|---|---|---|
| Delivered | Highest record returned to application code | No |
| Committable | Highest record allowed by the selected delivery semantic | No |
| Committed | Highest position successfully acknowledged by the data plane | Yes |

The range cursor separately tracks fetched progress. Fetching can run ahead of delivery and
therefore never makes a position committable by itself.

The durable coordinate contains the entry id, the record's offset within that entry, and its
absolute record offset. This lets resume fetch the containing batch, skip records already
processed inside it, and continue absolute numbering correctly.

All three local positions move monotonically. A commit is skipped when it would not advance
beyond the last successful commit.

---

## Delivery Semantics

| Semantic | When a record becomes committable | Intended tradeoff |
|---|---|---|
| At least once | Application acknowledges it | Processing failures replay rather than silently skip |
| At most once, best effort | Consumer returns it to the application | Inspection stays convenient, but a crash may skip delivered work |

At least once is the SDK default. The explicit best-effort mode is useful for inspection
tools and disposable processing, where displaying the record is itself the useful action.

Application acknowledgement is local and cheap:

```rust
let record = consumer.next_record().await?;
process(&record).await?;
consumer.ack(&record)?;
```

Acknowledgement is a no-op in best-effort at-most-once mode because delivery already advanced
the committable position.

---

## Auto and Manual Commit

Delivery semantics decide **what** may be committed. Commit mode decides **when** current
committable progress is sent to the broker.

| Mode | Behavior |
|---|---|
| Auto | The manager periodically commits committable positions for owned ranges |
| Manual | The application calls commit at a shutdown point, batch boundary, or explicit checkpoint |

Auto-commit is safe in at-least-once mode because the timer flushes acknowledgements; it does
not infer processing success from delivery. Manual commit synchronizes all currently
committable owned ranges and returns only after their data replica sets have durably stored
the positions.

```rust
for record in batch {
    process(&record).await?;
    consumer.ack(&record)?;
}
consumer.commit().await?;
```

The client serializes commit rounds for a group member so overlapping auto and manual commits
cannot race their local committed markers.

---

## Acknowledgement Validation

An acknowledgement is accepted only when:

1. Delivery is not fenced during stale-generation recovery.
2. The range is in the member's current effective ownership.
3. This consumer delivered a record for the range.
4. The acknowledged position does not exceed the highest delivered position.

Older acknowledgements are harmless no-ops. These checks prevent delayed application work
from advancing a range after revocation and prevent fabricated positions from skipping data.

---

## Assignment and Effective Ownership

The control plane assigns active ranges. The client expands that assignment through range
lineage because a sealed predecessor may still need draining before its split children or
merge successor can safely progress.

```
control-plane assignment: child L
effective ownership:      sealed parent P + child L
```

Exactly one child owner receives responsibility for a split parent. A merged successor waits
until the relevant predecessor progress is durable. Ancestor actors start before descendants
so a descendant cannot suppress the only actor capable of draining its predecessor.

Normal rebalances reconcile incrementally:

- revoked actors stop and lose their local offset trackers;
- unchanged actors continue fetching;
- new or previously unprovisioned actors start from durable offsets;
- metadata refresh occurs only when the cached range details cannot describe the assignment.

The client does **not** commit a revoked range using the new generation. Its uncommitted work
must replay at the new owner; relabeling that progress with a newer generation would bypass
the ownership fence.

---

## Stale-Generation Recovery

A commit can be rejected because membership or range topology advanced the group generation.
That signal invalidates the manager's current ownership snapshot.

```
commit rejected as stale
        │
        ▼
fence delivery and stop every range actor
        │
        ▼
request latest assignment + generation
refresh metadata if assigned ranges are unknown
        │
        ▼
install effective lineage ownership
        │
        ▼
retry only positions still effectively owned
        │
        ▼
reload durable offsets and restart owned actors
```

Stopping every actor is deliberately conservative. Until the refreshed assignment and
lineage are known, the manager cannot identify which actor remains valid. Delivery fencing is
immediate, so late records or acknowledgements are rejected while asynchronous stop commands
are still being processed.

Retrying positions still owned preserves acknowledged application work. Positions for
revoked ranges have already been discarded and are not included in the retry.

---

## Durable Commit and Resume

The client sends one commit to the range's data leader. Server-side replication, generation
validation, shared-WAL durability, and leader redirects are described in D8.

When actors start after a rebalance, they read the group's durable positions at the assignment
generation. Each range leader answers only after that generation's ownership seal is durable,
and the client serializes the multi-range lookup with its own commits. The independently routed
reads therefore share one logical transition boundary rather than observing a mixture of old-
and new-generation progress.

For each range:

- If present, fetch begins at the containing entry and filters batch records through the
  committed record offset.
- If absent, the configured earliest/latest start policy applies.
- If the durable read is unavailable or times out, the actor does not start. The failure is
  returned through the consumer's record stream, and a later rebalance retries instead of
  assuming no offset exists. When the lookup is part of explicit stale-commit recovery, the
  failure is returned directly by the commit call.

This fail-closed behavior prevents a temporary broker failure from replaying an entire range
or skipping to its tail under the fallback start policy.

---

## Processing Outcomes

In at-least-once mode, application policy decides when a failed record becomes terminal:

| Outcome | Acknowledgement |
|---|---|
| Processing succeeds | Acknowledge |
| Processing will retry | Do not acknowledge |
| Retry policy is fail-stop | Do not acknowledge |
| Explicit skip policy | Acknowledge only after accepting the loss |
| Dead-letter policy | Acknowledge only after the dead-letter write succeeds |

The pull API does not implement retries or dead-lettering itself. It exposes records and local
acknowledgement; the application owns those policies.

---

## Coverage Contract

The behavior spans fetch actors, application delivery, group generations, range lineage, and
data-plane durability, so both focused state tests and end-to-end tests are required.

| Scenario | Expected behavior | Current coverage |
|---|---|---|
| Delivered but unacknowledged in at-least-once mode | Restart replays the record | End to end |
| Acknowledged and committed | Restart resumes after the record | End to end |
| Best-effort at-most-once delivery | Restart may skip the delivered record | End to end |
| Membership rebalance | Revoked progress is not committed by the old owner | End to end |
| Broker restart during rebalance | Durable progress resumes without replay | End to end |
| Split rebalance | Sealed parent drains before assigned children progress | End to end |
| Direct request to a follower | Redirect; follower does not accept a client commit | Gap: explicit rejection test still needed |
| Replicated commit | Client success waits for every follower's durable acknowledgement | Data-plane state test |

---

## Rules

1. **At-least-once is the SDK default.** Duplicate processing is safer than silent loss for
   applications with durable side effects.
2. **Acknowledgement is local; commit is durable.** The processing hot path stays cheap while
   the checkpoint boundary remains explicit.
3. **Auto-commit flushes only committable progress.** A timer batches decisions; it does not
   create processing decisions.
4. **Only effective ownership may commit.** Active assignment alone is insufficient while a
   sealed predecessor is still draining.
5. **Revoked progress is discarded, not promoted to a new generation.** The new owner must
   recover from the last durable checkpoint.
6. **Offset recovery fails closed.** An unavailable durable-offset read must not be treated as
   an empty checkpoint.
7. **At-most-once behavior is explicit and best effort.** Delivery-time progress can skip
   application work after a crash, so callers must opt in.
