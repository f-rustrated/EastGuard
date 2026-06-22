# Phase D4: Consumer Range Tracking

**Goal:** Let a consumer read a topic correctly across range lifecycle changes. Within a range, reads are simple — fetch by offset, served from the commit boundary. The hard part is *transitions*: when a range is sealed by a split or a merge, the consumer must discover its successor range(s) and continue reading them in an order that preserves per-key delivery order. D4 adds the consume read path, the in-band signal that announces a transition, and the consumer-side logic that follows range lineage.

**Depends on:** Phase D3 (segment lifecycle integration — split/merge/seal committed through the coordinator, segment assignments delivered to the data plane).

D4 is single-consumer range tracking. Consumer groups, shared work, and durable offset commits are out of scope (see the roadmap backlog). The full client-facing consume protocol and connection management land in D6 — D4 establishes the tracking semantics that protocol relies on.

---

## Position Model

A consumer's position is **per range**, not per topic:

```
consumer position for a key  =  (range, next offset within that range)
consumer position for a key span  =  one such position per range covering the span
```

There is deliberately no topic-global offset. Offsets are scoped to a range and **reset to zero** when a new range is born from a split or a merge — a clean break, not a continuation. A single monotonic topic offset would be impossible to maintain across these boundaries without rewriting history. Tracking position per range sidesteps the problem: each range has its own independent, gap-free offset space.

To read a key span, a consumer holds one cursor per range that currently covers part of that span, and updates the set of cursors as ranges split and merge underneath it.

---

## Reading Within a Range

This is the common case and it is transparent.

A consumer fetches by *(range, offset)*. The data plane maps the offset to whichever segment within that range holds it and serves the records — from the in-memory tail cache on a hot read, or from the segment file (via the sparse index) on a cold read. Reads are bounded by the **commit boundary**: the highest offset that every replica has acknowledged, and is therefore safe to expose. The leader computes this authoritatively (it collects the acks); each follower knows only the value the leader has told it so far (via the commit-advance from D2), which lags by about one network hop. Either way, a consumer never sees the uncommitted tail.

**Segment rolls are invisible to consumers.** When a segment fills (size), ages out, or is sealed on replica failure, the range stays *active* and a new segment continues the same offset space (offsets are continuous across a range's segment chain — see `metadata-state-machine.md`, offset continuity). The consumer keeps fetching by *(range, offset)*; it never names a segment and takes no action when one rolls.

**Any replica can serve reads.** Every replica holds all committed data for a segment, so a consumer may read from the nearest or fastest replica, not only the leader. A follower serves only up to *its own locally-known* commit boundary, which trails the leader's by about one network hop. This is what keeps a follower from serving a record the leader has not yet confirmed committed.

---

## Range Transitions

A range is sealed only by a **split** or a **merge** — never by an ordinary segment roll. The transition is the one moment a consumer must act, and it is discovered **in-band**: every fetch response reports the range's status.

```
fetch response carries a range status:
    Active                          → keep fetching this range
    Sealed { end offset, transition } → drain to end offset, then follow the transition
```

When the consumer reaches the sealed range's end offset, the response tells it exactly what happened and where to go next — no separate metadata query, no gossip, no polling. The transition describes the successor(s):

- **Split** → two child ranges, with the split point that divides the parent's keyspace.
- **Merge** → one merged range that absorbed this range and a sibling.

### Split

```
Parent range P  keyspace [a, c)   offsets 0 .. end_P
        │  sealed at split point b
        ├──────────────► Child L  keyspace [a, b)   offsets 0 ..
        └──────────────► Child R  keyspace [b, c)   offsets 0 ..

consumer reading [a, c):
    drain P fully (0 .. end_P)
    then read L (from 0) and R (from 0)
```

Each child owns a disjoint sub-keyspace, so any key belongs to exactly one child. For a given key the order to preserve is simply parent-before-child: every record the key had in P precedes any it gets in its child. So a consumer drains P to its end offset, then reads whichever child or children cover the span it wants — both, if it was reading all of [a, c); just the one, if it only cares about a sub-span. The two children are independent of each other and may be read in any interleaving.

### Merge

```
Parent P1  keyspace [a, b)  offsets 0 .. end_P1  ┐
                                                 ├──► Merged M  keyspace [a, c)  offsets 0 ..
Parent P2  keyspace [b, c)  offsets 0 .. end_P2  ┘

consumer reading only [a, b):   drain P1, then read M and keep only [a, b) records
consumer reading all of [a, c): drain P1 and P2, then read M
```

The merged range absorbs two adjacent keyspaces, but the ordering requirement is still per key: for any key, only *one* parent ever held it (P1 for keys in [a, b), P2 for keys in [b, c)), and that parent's records precede the merged range's. So a consumer follows the single parent that covered the span it cares about — drain that parent, then read the merged range, keeping the records whose keys fall in that span. It does **not** need to wait on the sibling parent, and need not even know the sibling exists: that sibling only ever held keys this consumer didn't ask for.

A consumer reading the *whole* merged keyspace is simply tracking both sub-spans at once, so it holds both parent cursors (it was already reading both) and drains both before reading the merged range — not because the rule spans parents, but because it is two independent per-key chains that happen to converge on the same successor.

**Filtering is client-side.** A fetch names a range, a start offset, and a byte budget — it carries no keyspace filter, so the merged range can only be fetched whole. A consumer that cared about just one parent's keyspace therefore transfers all of the merged range and discards the part outside its span. Its pre-merge read cost was zero waste; post-merge it carries a standing read-amplification cost (which compounds if the merged range is itself merged again later). D4 accepts this at its single-consumer, semantics-first altitude — a server-side keyspace filter on fetch is a later optimization, not a correctness concern.

---

## Ordering Guarantees

Per-key order is the guarantee, and it reduces to one rule stated *per key*:

> **For a given key, its successor range is not read until the one predecessor range that owned that key has been drained to its sealed end offset.**

The reason it can be stated per key — and followed with only in-band information — is that, restricted to a single key, range lineage is a simple *chain*, never a branch. A split sends a key to exactly one of the two children; a merge means the key came from exactly one of the two parents. So at every transition a key has exactly one predecessor and one successor, and "drain the predecessor, then read the successor" is unambiguous. The consumer never needs to know about sibling ranges — the child it didn't land in, or the other parent of a merge — because those only ever held *other* keys.

A consumer reading a span of keys is just running several of these chains in parallel, one per sub-keyspace, updating its set of cursors as each chain transitions. This is what "no cross-key ordering" means in practice, and why **different keys progress independently**: a consumer only ever blocks on the predecessor of a key it is actually reading, never on a range full of keys it never asked for.

---

## Where to Start

A consumer first needs the ranges covering the keys it cares about. Topic metadata provides them: every range (active and sealed), each with its keyspace bounds, its state, and its lineage.

- **A key routes to a range by keyspace bounds.** Active ranges tile the whole keyspace with no gaps or overlaps (see `metadata-state-machine.md`, keyspace coverage), so a key maps to exactly one active range at any moment.
- **Read from latest:** route each key to its current active range and start at that range's commit boundary.
- **Read from earliest:** for each sub-keyspace of interest, start at its earliest ancestor range and walk that chain forward, draining each range before its successor. Because sealed ranges and their lineage are retained in metadata, the consumer can reconstruct the full historical order.

---

## Bootstrap

Before its first fetch, a consumer must learn two things from the metadata layer:

1. The set of ranges covering the keyspace it wants to read, with bounds, state, and lineage — what "Where to Start" presupposes.
2. For each range it intends to read, the replica set of the range's current segment — the nodes a fetch can be sent to.

For a *historical* read (`earliest`, or a consumer that has fallen behind into sealed segments), the current segment's replica set is not enough: an old `(range, offset)` lives on a sealed segment whose placement can differ (seal-on-failure, repair, and re-fill move segments independently). Surfacing each range's sealed segments — offset span + replica set — in the metadata response, **gated** so a tail reader doesn't pay for a long history, lets the consumer place any historical offset directly instead of guessing the active segment and leaning on "not local." The metadata group already tracks every segment's `replica_set`, so this is a small **proposed extension** to the query that the client phase (C-series) depends on — not part of D4 as built.

A **topic-metadata query** answers both, and it must reach a node in the topic's metadata shard group — **there is no server-side proxying**. The query is sent to any cluster node the consumer knows (a bootstrap address from configuration, or any node it has previously talked to), with two possible outcomes:

```
Common case (the addressed node owns the topic's metadata):

Consumer                         Cluster node = metadata owner
   │                                   │
   │── topic-metadata (topic) ────────►│
   │◄── range list +           ────────│
   │    per-range active               │
   │    segment + replica set          │

Redirect case (first contact, or owner has changed):

Consumer                         Cluster node             Metadata owner
   │                                   │                        │
   │── topic-metadata (topic) ────────►│                        │
   │◄── redirect: owner is N5  ────────│                        │
   │                                                            │
   │── topic-metadata (topic) ─────────────────────────────────►│
   │◄── range list + ...     ───────────────────────────────────│
```

The consumer caches the (topic → owner) mapping so subsequent queries hit the right node directly. A redirect costs at most one extra round trip on first contact or when the owner has changed (leader re-election, shard group membership change). The same RPC is reused on any fetch that fails with "stale targeting" — both initial discovery and post-roll re-resolution go through the same path.

**Why redirect rather than proxy.** The owner is the authoritative source for the topic's metadata; a proxying intermediary tempts stale caches and obscures who actually answered. And since every subsequent read from the consumer already goes directly to data nodes (the fetch path), the consumer must already be able to address arbitrary cluster nodes — making bootstrap redirect-driven keeps the addressing model uniform end-to-end.

D6 will finalize the full client-facing connection layer (bootstrap address management, handshake, keep-alive). D4 introduces only the metadata-query RPC and its redirect response — the minimum the read path needs to function end-to-end.

---

## Fetch Flow

```
Consumer                         Data node 
   │                                   │
   │── fetch (topic, range, offset) ──►│
   │                              map offset → segment in range
   │                              read committed records
   │                                (tail cache or cold read)
   │                              attach range status
   │◄── records, next offset, ─────────│
   │     range status                  │
   │                                   │
   │ status Active   → fetch next offset from same range
   │ status Sealed   → drain to end offset, then follow transition
```

**Resolving a node to fetch from.** A consumer reads from any replica of the range's current segment. D4 imposes no preferred-replica policy of its own — refining the choice (a tag-based placement and selection policy combining hard constraints from provisioned node labels with soft signals like CPU and I/O) is a separate concern that lives outside D4. Historical (sealed) segments have a fixed, immutable replica set — those reads are stable. Tail reads may need to re-resolve after a segment roll changes the active replica set: if a targeted node no longer hosts the segment, it says so and the consumer re-resolves from metadata and retries. As with coordinator routing in D3, stale targeting costs a retry, never correctness.

---

## What D4 Builds On vs. Introduces

**Already present (reused):**

- The fetch request/response shape, including the in-band range-status signal (active vs. sealed-with-transition) and the offset-listing query.
- The per-segment commit boundary and the leader→follower commit-advance from D2 — the basis for "read only committed data" and "read from any replica."
- Range lineage, keyspace bounds, range/segment state, and the per-range offset model in the metadata state machine.
- Offset continuity within a range (so segment rolls are transparent) and the offset reset to zero on split/merge children.
- The cold read path (sparse index → segment file) for reads that miss the tail cache.

**Introduced by D4:**

- The consume read path that serves committed records for a *(range, offset)* fetch and attaches the current range status — the fetch handler that does not yet exist.
- Resolving a bare *(range, offset)* to the segment that holds it. A fetch names only a range and an offset, but a node currently tracks segments by full identity (topic + range + segment) and holds only the segments it hosts — there is no lookup from a range's offset to its segment. D4 must add that resolution; it's what keeps segment rolls invisible to the consumer.
- Consumer-side range-cursor tracking: holding one cursor per covering range and following lineage on transition.
- The ordering discipline (drain the owning predecessor before its successor) and initial discovery (route keys to ranges, choose a start point).
- A reserved keyspace-bound field on the fetch request — D4 fixes its shape in the wire format so a future consumer-group layer can enable server-side filtering without a protocol break.
- The topic-metadata query RPC and its redirect response — when the addressed node owns the topic's metadata it answers directly; otherwise it returns the address of an owner and the consumer retries (no server-side proxying). What "Where to Start" presupposes; also the path a fetch falls back to on stale targeting.

---

## Wire Protocol

| Message | Direction | Carries | Status |
|---|---|---|---|
| Topic-metadata request | Consumer → any cluster node | topic | Introduced by D4 |
| Topic-metadata response | Cluster node → consumer | either the **metadata payload** (range list with bounds/state/lineage + per-range active segment with replica set; *proposed, gated:* sealed segments' offset spans + replica sets for historical reads) or a **redirect** (address of a member of the owning metadata shard group) | Introduced by D4 |
| Fetch request | Consumer → data node | topic, range, start offset, max bytes, optional keyspace bound | Type extended in D4; handler is D4 |
| Fetch response | Data node → consumer | records, next offset, **range status** | Type exists (send path is D4) |
| Range status (in response) | — | `Active`, or `Sealed { end offset, transition }` | Exists |
| Range transition (in status) | — | `Split { left, right, split point }` or `Merged { merged range }` | Exists |
| Offset-listing request/response | Consumer → data node | range → current offset bounds | Type exists |

The transition signal is the heart of D4: it is the only thing a consumer needs to move from a sealed range to its successors, and it arrives on the same response that delivers the range's final records.

The **optional keyspace bound** on the fetch request is reserved by D4 for use by later phases. When honored, the broker returns only records whose keys fall in the bound; in D4 the handler accepts an omitted bound or one matching the range's full keyspace and rejects anything narrower. The shape lands in the wire format now to avoid a later protocol break.

The motivating case appears whenever a consumer's interest is narrower than the range it must fetch from — typically, after a merge, a consumer reading only one of the pre-merge sub-keyspaces. Without server-side filtering, it pulls the entire merged stream and discards the unrelated share; the cost lasts the life of the merged range and compounds with each successive merge. D4 accepts that amplification in exchange for handler simplicity, and the consumer-group layer can enable filtering without a wire change.

---

## Examples

### Following a Split

```
Consumer                         Data node
   │  reading range P [a,c), at offset 41998
   │── fetch (P, 41998) ──────────►│
   │◄─ records 41998..42000, ──────│  range status: Sealed
   │   next 42001,                 │   end offset 42000
   │   Sealed{end 42000,           │   transition: Split → L [a,b), R [b,c)
   │     Split(L, R, b)}           │
   │                               │
   │ P drained at 42000 → switch to children, each from offset 0
   │── fetch (L, 0) ──────────────►│   (new keyspace [a,b))
   │── fetch (R, 0) ──────────────►│   (new keyspace [b,c))
   │ read L and R independently going forward
```

### Following a Merge

A consumer reading the whole merged keyspace holds both parent cursors and drains both:

```
Consumer                         Data node
   │  reading P1 [a,b) and P2 [b,c)
   │── fetch (P1, ...) ───────────►│◄─ ... Sealed{end_P1, Merged(M)}
   │── fetch (P2, ...) ───────────►│◄─ ... Sealed{end_P2, Merged(M)}
   │                               │
   │ both sub-spans drained → switch to M, from offset 0
   │── fetch (M, 0) ──────────────►│   (keyspace [a,c))
```

A consumer reading only [a, b) runs a single chain and never touches the sibling parent:

```
Consumer                         Data node
   │  reading P1 [a,b) only
   │── fetch (P1, ...) ───────────►│◄─ ... Sealed{end_P1, Merged(M)}
   │                               │
   │ P1 drained → switch to M from offset 0, keep only [a,b) records
   │── fetch (M, 0) ──────────────►│   (fetches all of M; discards [b,c) client-side)
   │ never learns of, or waits on, P2
```

---

## Invariants

1. **A consumer position is *(range, offset)*, never a topic-global offset.** Offsets are per range and reset on split/merge, so a single topic-wide offset cannot exist.

2. **Segment rolls are invisible to consumers; range transitions are not.** Within a range, crossing a segment boundary needs no consumer action (offset continuity). Crossing a range boundary (split/merge) requires following lineage.

3. **For a given key, its successor range is not read until the single predecessor range that owned that key is drained to its sealed end offset.** Restricted to one key, lineage is a chain (a split sends the key to one child; a merge brings it from one parent), so this rule is unambiguous and preserves per-key delivery order. A consumer reading a span runs one such chain per sub-keyspace and never blocks on a sibling range covering keys it didn't request.

4. **Transitions are discovered in-band.** A consumer learns a range was sealed, and where to go next, from the fetch response that delivers the range's final records — not from gossip or a separate poll. This works for merges as well as splits because a consumer follows only its own key-span's chain: the seal signal's forward pointer to the merged range is all it needs, and it never has to discover a sibling parent it wasn't already tracking.

5. **Consumers read only committed data.** Reads are bounded by the commit boundary; the uncommitted tail is never served.

6. **Any replica may serve reads, only up to the commit boundary it has learned.** The leader knows the authoritative commit boundary; a follower knows only the value the leader has told it (lagging by ~one hop) and never serves past it. So a consumer cannot observe a record the leader has not committed, regardless of which replica it reads from.

7. **A key maps to exactly one active range at any moment.** Active ranges tile the keyspace with no gaps or overlaps, so for any key the chain of ranges it has lived in is a total order.

8. **New ranges start at offset zero.** Split children and merged ranges begin a fresh offset space; there is no offset continuation across a range boundary.

9. **Fetch routing is retryable and safe.** Targeting a replica that no longer hosts a segment yields a re-resolve and retry, never lost or duplicated data. Historical (sealed) segment reads are stable because sealed segments are immutable.
