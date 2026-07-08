# Phase C3: Consumer

**Goal:** a consume client that reads a topic in order across its ranges and follows
split/merge lineage as the topic reshapes underneath it. The hard part — *what to read
next* — already exists as a pure state machine (the cursor library); C3 wraps it with
connections, fetching, and retries.

**Depends on:** C1 (routing cache, connection pool, redirect-follow loop) and the
consumer cursor library.

---

## The cursor library does the thinking

The consumer's correctness lives in a **pure, synchronous state machine** that's
already built and tested. It tracks one **cursor per active range** — each holding the
range id, the next offset to read, and the range's keyspace — and it walks lineage as
ranges split and merge. It has no network, no async, no offset storage. Its entire
interface is:

> *feed it the result of a fetch (which range, the next offset, and the in-band
> progress signal the server returned) → get back an action: keep going on this range,
> or transition (these cursors dropped, these added).*

It enforces the invariants that make ordered consumption correct: one cursor per
active range, disjoint keyspaces, and **lineage ordering** — don't start reading a
range until the predecessor that owned its keys has been drained to its end. The
consumer SDK never re-derives any of this; it just supplies I/O and obeys the actions.

```
fetch range R ──> Fetched { entries, next_offset, progress_signal }
                          │
                          v
              cursor library.apply(R, next_offset, signal)
                          │
        ┌─────────────────┴──────────────────┐
        v                                     v
  KeepGoing(R, next)                  Transitioned { dropped, added }
  fetch R again at `next`            stop dropped cursors, start added ones
```

---

## Bootstrapping

A consumer starts from a topic's metadata snapshot (`DescribeTopic`, via C1) plus two
choices the cursor library already models:

- **Where to start** — *latest* (skip sealed history, land on the active ranges) or
  *earliest* (start at the lineage roots and walk forward through every split/merge).
- **Which keys** — all keys, or a sub-span of the keyspace (only the ranges
  overlapping that span get cursors).

From these the library builds the initial cursor set; the SDK then begins fetching
each cursor's range.

---

## The fetch loop

For each active cursor, the consumer fetches and feeds the result back:

1. **Pick a replica.** Map the offset to the segment that holds it — the active tail
   segment, or (reading history) the sealed one covering it — and read from any replica
   in *that* segment's set, nearest/preferred, not necessarily the write leader (a key
   win of all-replica replication: every replica has all committed data). The cache
   carries sealed-segment placement for historical reads (`c1`); C1's redirect loop
   covers a stale choice.
2. **Fetch by id, not name.** Having resolved the topic id once at bootstrap, the
   consumer fetches *by id* — any replica holding the segment serves it directly,
   with no metadata lookup on the data node. A replica that doesn't hold it answers
   "not local," and the consumer re-resolves and picks another.
3. **Feed the signal to the library.** Every fetch response carries a progress
   signal:
   - **Active** → the range is still live; fetch again at the returned next offset.
   - **Sealed { end_offset, transition }** → drain to `end_offset`, then follow the
     transition the library hands back:
     - **Split** → the cursor for the parent is replaced by cursors for its two
       children (the split point partitions the keyspace).
     - **Merged** → once *both* predecessors are drained, a single cursor for the
       merged successor starts (the library parks the merge until both parents finish,
       preserving per-key order).
4. **Apply the action.** `KeepGoing` → fetch the same range again; `Transitioned` →
   stop the dropped cursors and start the added ones.

The consumer loops until its cursors are exhausted (bounded read) or forever (tailing
the active head).

---

## Decoding records

Each entry a fetch returns is an opaque blob the broker never parsed. Before its records
reach the application the consumer reverses what the producer did: read the entry's
**codec tag** — one plaintext byte at the front, ahead of the compressed block —
decompress the remainder with the matching codec (`none` is a no-op), then split it into
records. The codec rides in the payload itself, in the clear (see `c2_producer.md`,
Compression), so decoding needs nothing from the broker — the same blob decodes
identically whether it came from a hot replica or a cold segment file.

---

## Record positions inside batched entries

An entry is the broker's unit of durability, replication, and fetch. The producer may
place several application records into one entry during its linger window, then compress
and publish that batch as one opaque payload. The broker stamps one entry id on the whole
payload; it does not know or index the individual records inside.

That leaves three values in the consumer-facing position:

| Coordinate | Meaning | Advances when |
|---|---|---|
| Entry id | Physical position of a stored entry in a range | A produce batch commits |
| Batch offset | Record position inside that entry | The consumer decodes records from the payload |
| Absolute offset | Logical record progress for the consumer stream | Each application record is delivered |

The entry id and batch offset are the structural coordinate: together they identify the
last delivered record closely enough for a restarted consumer to resume without either
replaying or skipping records. The absolute offset is still important for progress,
diagnostics, and lag, but it is not a storage seek key. Batch sizes vary with linger timing,
byte limits, record limits, compression, and caller concurrency; a logical record counter
therefore cannot be converted into an entry id.

```
entry 0: [record 0, record 1, record 2]
entry 1: [record 0, record 1]
entry 2: [record 0]

committed position: entry 1, batch offset 1, absolute offset 4
resume fetches:      entry 1
resume skips:        record 0 and record 1 inside entry 1
first delivered:     entry 2, record 0, absolute offset 5
```

This split keeps the broker opaque. The data plane stores and serves bytes; the consumer,
which already has to decode the entry before delivery, is the layer that can see the
intra-entry record boundary and attach the correct record-level position.

---

## Lineage, made concrete

The whole reason consume is more than "read a partition" is that ranges reshape live.
The cursor library + the in-band signal handle it without the client polling metadata:

- A **split** seals the parent and the consumer, on draining it, fans out to both
  children — each child covering half the old keyspace, so a key is read by exactly one
  cursor across the transition.
- A **merge** seals two parents into one successor; the consumer must finish *both*
  parents before the successor, or it would read a merged key out of order. The
  library enforces this by parking the merge.

The split point a sealed-split signal carries is the right child's keyspace start; when
the consumer fetches the children's metadata it gets the precise boundaries. Lineage
pointers in `DescribeTopic` (split-into / merged-into / merged-from) let a consumer
reconstruct the DAG from any historical ancestor when starting *earliest*.

---

## Offsets and bounds

`ListOffsets` returns a range's surviving start and committed end — used to (a) bound a
read window, and (b) recover from retention: a consumer that fell behind and fetched a
now-deleted segment (see d7) skips forward to the surviving start.

Durable offset *storage* is a consumer-group concern (see d8), but C3 defines the position
that gets stored because it is the layer that turns entries into application records. A
committed position is a **last delivered** coordinate, not a next-record coordinate:

| Stored field | Why it is stored |
|---|---|
| Range | Identifies which range the position belongs to |
| Entry id | Tells the resumed consumer which physical entry to fetch first |
| Batch offset | Tells the resumed consumer how much of that entry was already processed |
| Absolute offset | Restores the logical record counter after restart and supports lag reporting |

On resume, a saved position overrides the configured start policy for that range. The
consumer fetches the saved entry, drops records with batch offsets less than or equal to
the saved batch offset, then delivers the first unread record with the saved absolute
offset plus one. The skip applies only to the saved entry; once that entry has been
evaluated, later entries stream normally.

If the saved entry has already fallen behind retention, the normal retention path wins:
the cursor moves to the surviving start and the intra-entry skip is discarded because the
physical entry it referred to no longer exists.

---

## Prefetching the next sealed segment

When the segment being fetched is **sealed**, its end and its successor are both known — so
the client can warm the next read *while still finishing the current one*, instead of paying
a cold start (connect + index seek + segment read) at the boundary. The **server does
nothing special**; the client drives it from the per-segment chain and replica sets already
in its cache (`c1`). The "next" is the next link in the range's offset-continuous chain —
which may sit on a **different node** (`d7`) — or, at a range seal, the successor range's
first segment (via lineage).

```
draining sealed S (ends at E)        warm S+1 in parallel (often on other nodes):
  fetch (R, off≤E) ─► records          pre-open conn + speculative fetch (R, E+1)
  S done at E ─────────────────────►   S+1's batch already in hand → no stall
```

It doesn't apply in two places: the **active tail** (no successor yet), and a **roll under
a caught-up reader** — offset continuity makes that invisible (the range stays `Active`, the
new segment continues the offset space, `d4`), so the read crosses by offset into the new
segment's freshly-written **hot** head. No disk cold start there; at most a one-time
re-resolve if the roll placed the segment on other nodes — which prefetch can't pre-empt
anyway, since the successor doesn't exist until the roll commits. Prefetch is the
*behind/cold* reader's win.

Bounded and safe: **one-ahead** (≤ one speculative fetch per cursor); **never load-bearing**
— reads have no server-side effect and position is client-side (`d1`), so a wrong guess is
dropped, and if the client never prefetches at all, offset continuity still carries the read
across; at a **merge**, hold the successor's first fetch until both parents drain (`d4`).

---

## What needs to be done

1. **Consumer over the cursor library** — own a cursor set, drive fetches, feed every
   result's signal back, obey `KeepGoing` / `Transitioned`.
2. **Bootstrap** — build the initial cursor set from `DescribeTopic` + start policy +
   key interest.
3. **Fetch routing** — fetch *by id* from a preferred replica of each range; handle
   "not local" and redirects via C1; pick by nearness/preference.
4. **Lineage following** — apply split/merge transitions from the in-band signal; rely
   on the library's parked-merge ordering.
5. **Retention recovery** — on a fetch to a deleted segment, skip forward via
   `ListOffsets`.
6. **Record decode** — read each entry's codec tag and decompress before surfacing
   records to the application (the producer/consumer contract from `c2_producer.md`).
7. **Record-level positions** — surface entry id, batch offset, and absolute offset with
   each delivered record; use the structural coordinate for resume and the absolute offset
   for logical progress.
8. **Prefetch the next sealed segment** — while draining a sealed segment, pre-open the
   next segment's replica connection and speculatively fetch past the current segment's
   end, so a hand-off to a segment on another node doesn't stall on connect + cold-read
   latency. Driven entirely from cached per-segment placement — no server cooperation;
   off on the active tail.
9. **Tests** — against the simulated cluster: ordered drain of a range, a split mid-
   consume (fan-out to children, each key once), a merge mid-consume (both parents
   drained before successor), read from a non-leader replica, by-id fetch after
   resolving the topic id, skip-forward past a retention-deleted segment, a compressed
   entry decoded by its codec tag, record-level resume from the middle of a batched
   entry, and a historical read crossing a sealed-segment boundary onto a different
   replica, served from a prewarmed next segment (no stall).

## See also

- `c1_routing_and_connections.md` — connection pool, routing cache, redirect loop.
- `d4_consumer_range_tracking.md` — the server side of the in-band progress signal and
  range transitions.
- `d7_retention_gc.md` — retention deletion and the skip-forward recovery.
- `d8_consumer_offset_management.md` — consumer-group offset storage and cooperative
  assignment.
- `d1_storage_engine.md` — the cold-read pool and the within-node hot/cold seam this
  prefetch mirrors, and the opaque payload the consumer decompresses.
- `client_roadmap.md` — consumer-group / durable-offset backlog context.
