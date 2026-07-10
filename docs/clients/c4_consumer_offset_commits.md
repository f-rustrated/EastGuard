# Phase C4: Consumer Offset Commits

**Goal:** durable consumer offsets represent records the application has decided are safe
to advance past. The consumer SDK must distinguish records it fetched, records it delivered,
and records the application acknowledged, so auto-commit can stay fast without turning
processing failures into skipped records.

**Depends on:** [c3_consumer.md](c3_consumer.md) for record positions and
[d8_consumer_offset_management.md](../data-plane/d8_consumer_offset_management.md) for the
current offset-storage path.

---

## Why C4 Exists

C3 defines how a consumer reads records across ranges and lineage. D8 defines where durable
offsets are stored today: an internal offsets topic used by consumer groups. C4 defines the
missing client-side contract between those two pieces:

> *when is a delivered record eligible to become a durable committed offset?*

That boundary cannot be the broker's job. The broker serves committed log entries, but it
does not know whether application processing succeeded. Only the client side can decide
whether a record should be replayed, skipped, retried, or dead-lettered.

---

## The Bug C4 Prevents

If the SDK marks a record committable when it is merely returned to application code, the
auto-commit timer can write an offset before processing succeeds.

```
fetch worker        SDK consumer API        application          commit timer
    |                      |                    |                     |
    | -- record ---------> |                    |                     |
    |                      | -- returns rec --> |                     |
    |                      | marks committable  |                     |
    |                      |                    | -- processing ...   |
    |                      |                    |                     | -- commits rec
    |                      |                    | -- fails/crashes    |
```

After restart, the consumer resumes after the committed position. The failed record can be
skipped permanently. That is at-most-once relative to application processing, even if the
caller expected the safer at-least-once behavior.

---

## Three Positions

A consumer that writes durable offsets tracks three positions per owned range:

| Position | Meaning | Durable commit source? |
|---|---|---|
| Fetched | Highest record pulled from the broker by the SDK | No |
| Delivered | Highest record returned to application code | Only in at-most-once mode |
| Acknowledged | Highest record the application marked terminal | Yes in at-least-once mode |

The stored coordinate is still the C3 record position: range, entry id, batch offset, and
absolute offset. C4 changes which lifecycle event makes that coordinate committable.

```
at least once: fetched -> delivered -> acknowledged -> committed
at most once:  fetched -> delivered ---------------> committed
```

---

## Delivery Semantics

Delivery semantics are explicit configuration, not an implicit side effect of using the
same consumer API from a different binary.

| Semantic | Commit eligibility | Failure behavior | Default |
|---|---|---|---|
| At least once | Acknowledged records only | Unacknowledged records may replay | SDK default |
| At most once | Delivered records | Delivered records may be skipped if processing fails | CLI default |

The SDK default is at-least-once because application code usually has durable side effects.
If the application crashes before acknowledging a record, replay is safer than silent loss.

The CLI defaults to at-most-once because a consume command is usually an inspection tool:
displaying the record is the useful action. The CLI still exposes a delivery flag for
debugging replay behavior:

```
eg-cli consume --delivery at-least-once
eg-cli consume --delivery at-most-once
```

The SDK should not detect that it is being called by the CLI. The CLI chooses at-most-once
explicitly in its consumer configuration.

---

## Commit Modes

Delivery semantics decide *what* can be committed. Commit mode decides *when* committable
progress is flushed to the broker.

| Mode | Commit source | Flush behavior |
|---|---|---|
| Auto + at least once | Terminal application outcomes | SDK periodically writes acknowledged positions |
| Manual + at least once | Terminal application outcomes | Application calls commit to write acknowledged positions |
| Auto + at most once | Delivered records | SDK periodically writes delivered positions |
| Manual + at most once | Delivered records | Application calls commit to write delivered positions |

Auto-commit remains the normal application default. It is not unsafe by itself; it becomes
unsafe only if the position being flushed is based on delivery rather than processing
outcome. In the default at-least-once mode, acknowledgement is local and cheap, while the
timer batches durable offset writes.

Manual commit means "sync the current committable position now." It is useful for shutdown,
batch boundaries, tests, and operators who need a known checkpoint. It should not be
required for normal correctness, because forcing a broker round trip per record would harm
throughput.

---

## Processing Outcomes

In at-least-once mode, a record becomes committable only after it reaches a terminal
application outcome.

| Outcome | Commit effect |
|---|---|
| Processing succeeds | Acknowledge the record |
| Processing fails but will retry | Do not acknowledge |
| Retries are exhausted and policy is fail-stop | Do not acknowledge; stop or return the error |
| Retries are exhausted and policy is skip | Mark terminal, then acknowledge |
| Retries are exhausted and policy is dead-letter | Acknowledge only after the dead-letter write succeeds |

For the pull API, the application owns retries because the SDK does not run the processing
callback. A later handler-style API could own retries around a callback and acknowledge,
dead-letter, or fail-stop according to policy.

Skipping after retry exhaustion is useful, but it is still application-level data loss
unless the failed record is captured somewhere else. That choice must be explicit.

---

## API Shape

The pull API exposes acknowledgement directly:

```rust
let record = consumer.next_record().await?;
process(&record).await?;
consumer.ack(&record)?;
```

Batch users can acknowledge records locally and flush the durable checkpoint at a batch
boundary:

```rust
for record in batch {
    process(&record).await?;
    consumer.ack(&record)?;
}
consumer.commit().await?;
```

A direct-position acknowledgement is useful for custom batching:

```rust
consumer.ack_position(range, position)?;
```

Acknowledgement APIs are no-ops in at-most-once mode, so shared application code can call
them without branching on delivery semantic.

---

## Validation Rules

Acknowledgements are local state transitions, but they still need guardrails:

1. **Current ownership.** Acknowledgement is accepted only for a range currently owned by
   this consumer member.
2. **Delivered cap.** Acknowledgement cannot move beyond the highest position this
   consumer has delivered for that range.
3. **Monotonicity.** Older acknowledgements are no-ops.
4. **Commit deduplication.** Durable writes are skipped if the committable position is not
   newer than the last successful commit.

These checks keep stale application work from advancing a range after rebalance, and they
prevent accidental commits for records this consumer instance never delivered.

---

## Rebalance Revocation

When a range is revoked, the consumer must flush only the position allowed by the selected
semantic:

1. Stop accepting new acknowledgements for the revoked range.
2. Stop the range fetcher.
3. Flush the highest committable position.
4. Remove local tracking for the range.

In at-least-once mode, delivered but unacknowledged records can replay on the next owner.
In at-most-once mode, delivered records can be skipped after revocation because the caller
explicitly chose delivery-time commits.

The zero-controller group design can still produce short duplicate-delivery windows while
members converge. C4's responsibility is narrower: duplicates are acceptable; premature
commits that skip unprocessed records are not acceptable in at-least-once mode.

---

## Test Coverage

C4 needs e2e tests because the bug lives across multiple async layers: range fetchers,
public consumer delivery, auto-commit, offset-topic storage, and group resume.

| Scenario | Expected behavior |
|---|---|
| At-least-once auto-commit, delivered but unacknowledged | Restart replays the record |
| At-least-once auto-commit, acknowledged | Restart resumes after the record |
| At-least-once manual commit, partial acknowledgement | Restart resumes after the acknowledged subset only |
| At-least-once revocation | New owner replays delivered but unacknowledged records |
| At-most-once auto-commit | Restart skips delivered records even without acknowledgement |
| Stale acknowledgement after revocation | Old owner cannot advance the range |

The key regression test is the first one. It must wait past the auto-commit interval after
delivery but before acknowledgement, then verify that the restarted group receives the same
record again.

---

## Rules

1. **At-least-once is the SDK default.** Most application consumers prefer duplicate work
   over silent loss.
2. **Auto-commit flushes committable progress only.** The timer is a batching mechanism, not
   a processing signal.
3. **Acknowledgement is local; commit is durable.** This keeps the hot path cheap while
   preserving a clear checkpoint operation.
4. **At-most-once is explicit.** It is useful for inspection tools and disposable
   processing, but callers must opt into delivery-time commits.
5. **Revocation obeys the configured semantic.** Rebalance must not upgrade delivered
   records into processed records under at-least-once mode.
