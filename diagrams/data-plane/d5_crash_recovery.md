# Phase D5: Crash Recovery

A node crashes and restarts. Its memory is gone; its disk is not. D5 covers two
things: what the node does with that disk before it rejoins, and how the cluster
puts the surviving data back to use.

**Depends on:** D1 (storage engine) for the WAL and segment files; the repair
flows depend on D2 (replication).

---

## Two facts that shape everything

**1. The durability promise lives in the WAL.** A producer is ACKed only after
its entry is fsynced to the WAL on *every* replica and committed. So anything the
cluster promised is on at least one healthy replica's disk. A single node
recovering never has to reconstruct a promise alone — it only has to make its
*own* data usable again so the cluster can count it. Anything missing or doubtful
can be copied back from a healthy replica.

**2. A restarted node is a new member.** NodeIds are regenerated on startup. SWIM
already saw the old identity die, the cluster sealed every active segment it
touched, and moved on. The restarted node resumes no leadership, no followership,
and no active segment. It joins fresh, with a disk full of data from its previous
life. (`Dead` is terminal in SWIM — a restart is never the old identity revived.)

Together these give recovery one job:

> Make the local data honest, then let the cluster decide what it's worth.

"Honest" means every byte the node later claims must be complete and CRC-verified.
Recovery never has to be *complete*: understating what you hold costs a little
network transfer (repair copies the rest); overstating it costs correctness. So
every rule below errs toward dropping data, never toward claiming it.

Recovery deliberately does **not**:

- resume in-flight produces — un-ACKed entries are the producer's to retry; ACKed
  ones are safe by fact 1,
- warm caches — they refill from traffic,
- track consumer positions — client-owned (D4),
- announce its data — the node stays quiet until the coordinator assigns it
  something, and only then mentions what it already has.

---

## What survives a crash

| On disk | Role | Recovery treatment |
|---|---|---|
| WAL files | Source of truth for anything not yet checkpointed | Replayed forward, then deleted |
| Segment files | Checkpointed prefix of each segment | Kept; replay appends the missing suffix |
| Sparse index | Derived read accelerator | Rebuilt from the segment files every recovery — never trusted across a crash |
| Memory (caches, cursors, trackers, pending replies) | — | Gone; all derived or client-owned |

There is no separate checkpoint file. "What's left in the WAL directory" *is* the
record of what might not have reached segment files: a WAL file is deleted only
once every segment has checkpointed past it.

---

## Single-node recovery

Recovery runs to completion before the node opens any port or joins SWIM:

```
1. Scan the WAL directory      oldest surviving file → newest
2. Replay into segment files   route each record, skip duplicates, append the rest
3. Build the inventory         per segment: highest CRC-verified entry id held
4. Rebuild the sparse index    re-derived from the segment files
5. Delete the old WAL files    everything recovered now lives in segment files
6. Start fresh                 new WAL, new NodeId, join SWIM
```

Steps 1–5 are idempotent: a crash *during* recovery just runs the same sequence
again, and replay skips whatever already reached segment files, so a re-run
converges to the same result.

### WAL replay

The WAL is one shared, interleaved stream — entries from every segment on the
node, in fsync order. Each data record carries a small routing header (topic,
range, segment, entry id). Replay is a single forward scan that splits the stream
back into per-segment files:

```
for each WAL file, oldest → newest:
    for each record:
        CRC check
        route by header to its segment
        skip if the segment file already holds this entry id   (dedup)
        otherwise append
```

**Deduplication.** Surviving WAL files usually overlap with what's already in
segment files (a WAL file is deleted only after all its entries checkpoint). The
rule is per segment: find the last entry id the segment file already holds, append
only WAL entries beyond it. Segment records are bare — no per-record id — so that
last id is *derived*: the segment's base offset (parsed from its filename) plus
the count of CRC-verified records, minus one. Entry ids are contiguous and ordered
within a segment, so this is a single comparison. No log-sequence-number is needed
to know how far the file got: checkpoint appends strictly in id order with no gaps
(D1), so base + verified-count − 1 *is* the checkpoint frontier, and fsync
ordering guarantees the WAL covers the rest.

**The torn tail.** A crash mid-write leaves the last file ending in a partial
batch. Batches are written and fsynced as a unit, ending with an end-of-batch
marker; a missing or corrupt marker means the fsync never completed, so nothing in
that batch was ever ACKed. Replay stops at the last complete batch of the last
file and discards the rest. Safe by construction.

**Corruption elsewhere.** A CRC failure in the *middle* of the stream is real
damage, not a torn write. Replay does not try to be clever: it stops crediting
that segment at the last verified entry and lets repair treat the rest as missing.
Other replicas have healthier copies.

**Surplus for sealed segments.** Some replayed entries may belong to segments the
cluster sealed while the node was down — including entries past the sealed end (a
crashed leader may have fsynced uncommitted entries). Metadata bounds this: once
the node learns a segment's sealed end, anything local past it is surplus and
ignored at read time. The control plane recovers its own Raft log independently
and is the authority on where each sealed segment ends.

### A fresh WAL, not a resumed one

After replay, the old WAL files are deleted and the node starts a brand-new WAL
from sequence one. Reopening the old WAL would require recovering the exact batch
sequence number at the crash point (which the format does not store) and buys
nothing: batch sequence numbers are node-local, the identity has already
restarted, and everything the old WAL held now lives in segment files.

This keeps the WAL's lifecycle trivial — one WAL per node lifetime, born empty,
deleted at the next recovery. A second recovery can't be confused by the sequence
restart: old-WAL deletion finishes before the new WAL is created, and even if a
crash lands in that window, a new lifetime only writes newly assigned segments
(old ones were sealed and never resume), so leftover old files and new files
reference disjoint segments and dedup handles the rest. Segment files persist
across lifetimes on purpose — they are the inventory.

### The inventory

Recovery's output is one in-memory table:

```
(topic, range, segment)  →  highest CRC-verified entry id held locally
```

"Verified" is the load-bearing word: an entry is recorded only after the file's
records pass CRC. A segment damaged past entry N is listed as holding N, not as
holding whatever the file size suggests.

The inventory is **passive** — never broadcast. It is consulted only when the
coordinator later assigns this node to a segment's replica set and the node would
otherwise copy the whole thing (see catch-up). The node doesn't need to know it
used to be in that replica set; it just has files that happen to match, checked on
their own merits. The identity problems that plague stable-NodeId systems
(zombies, stale claims, split identities) don't arise here, because the cluster
never has to decide whether this node *is* the old one. It isn't — it merely holds
some of the old one's bytes.

---

## Rejoining the cluster

The node joins SWIM as a new member and otherwise waits.

**No active segment resumes.** Every segment the node was writing or following was
sealed when SWIM declared the old identity dead (the seal-on-failure model, D2).
Active segments restart fresh on freshly assigned replica sets, with the leader's
uncommitted tail replayed by the *surviving* leader — not by this node. So the
restarted node's local data is only ever relevant to *sealed* segments, which are
immutable. That immutability is exactly why byte-level reuse is safe.

**Work arrives; it is not taken.** The coordinator (the metadata leader for each
shard group) decides which nodes hold which sealed segments. A restarted node is
just another placement candidate — one that gets a zero-transfer shortcut when
it's assigned a segment it already holds.

---

## How the surviving data gets used

A restarted node's disk only pays off if the cluster reassigns it segments it
holds. Three mechanisms make that happen and reliable, plus the catch-up that does
the actual reuse. (The contracts for these live in `.claude/rules/raft-actor.md`
and `.claude/rules/metadata-state-machine.md`.)

### Catch-up: the reuse

When a node is told to hold a sealed segment, it reconciles what it already has
against the segment's sealed end:

```
Coordinator        Healthy peer          Assigned node
    │                   │                       │
    │  hold this sealed segment [start, end] ──►│
    │                   │           check inventory / local store
    │                   │◄─── "I have up to N"──│
    │                   │   stream entries > N─►│
    │                   │                       │  append, verify to end
    │◄──────────────────────────  "have it through end"  (ack)
```

- **Full match** — local data already reaches the sealed end. Verify, ack.
  **Zero bytes transferred.** This is the restarted node's payoff.
- **Partial** — local data reaches N, the segment ends later. Request only entries
  after N from a healthy peer (a non-leader, to keep repair reads off the write
  leader) and append onto the verified prefix.
- **None** — full copy, the default path. Recovery added nothing; nothing breaks.

The assignment is re-driven until the node acks, so a dropped message or a stalled
transfer is retried, not stranded. The ack is what lets the coordinator stop
re-driving.

### Sealed-segment repair on death

When a node dies, the coordinator finds sealed segments whose replica set named
the dead node and reassigns them to replacements chosen by placement. The
reassignment is a single committed metadata change: it swaps the replica set and
leaves the segment sealed — data, offsets, and lineage stay frozen; only *where
the copies live* changes. It then tells each member of the new set to make sure it
holds the segment, via catch-up.

Only segments with a *known* sealed end are repaired this way — catch-up needs an
end to verify against. Segments left with an unknown end by a leader crash are
handled first by boundary recovery (below).

### Capacity-return re-fill

Death-triggered repair has a gap. When a death shrinks a sealed segment to its
survivors and no replacement is available *at that moment* (the cluster is too
small, or the right node is down), the segment is left under-replicated — and the
death scan never revisits it, because it now has no dead member to flag. That
segment would sit one death away from data loss, and a restarted node would never
be offered it back.

The periodic ring check closes this. Alongside repairing dead-member segments, it
re-fills sealed segments that are below the replication factor whenever the ring
can now supply a member — including the restarted node once it has rejoined and
re-entered the ring. This is the capacity-*return* counterpart to death-triggered
repair: when capacity comes back, segments grow back toward full replication, and
the added member catches up — **zero-transfer if it already holds the data.** This
is what makes the restarted-node payoff reliable rather than incidental; it is the
mechanism behind "a zero-transfer shortcut" above. It is scoped to under-filled
segments only (never a rebalance of well-replicated ones), so it adds no churn in
steady state.

### Leader-crash boundary recovery

When the crashed node was a segment's *write leader*, no survivor knows the
committed end — the leader was the one tracking it. Before reassigning such a
segment, the coordinator asks each surviving replica for its durable (fsync'd)
extent and seals at their **minimum** — the highest offset present on *every*
survivor, which is therefore committed (commit requires an all-replica ack). The
successor opens at that end + 1, so the offset chain stays continuous, and the
now-known-end segment is picked up by ordinary repair. The most-complete survivor
becomes the new write leader. Multi-death and total-loss cases fall back to an
unknown end (no worse than before). Full rationale:
[`leader_crash_seal_boundary.md`](leader_crash_seal_boundary.md) and the
boundary-recovery rules in `.claude/rules/raft-actor.md`.

---

## Orphaned data cleanup

A restarted node's fresh identity is in *no* replica set, so at recovery time
**every** inventoried segment is an orphan candidate. Some stay orphaned forever
(the cluster repaired around the old identity, or the topic was deleted); others
get handed back by re-fill or a later death elsewhere, at which point catch-up
reuses the on-disk copy. Classification is implicit — everything is a candidate —
and resolution is lazy.

**The node cannot ask the coordinator "is this segment still mine?"** It knows a
segment only by its ids, but a topic's owning shard group is found by hashing the
topic *name*, which a recovered node doesn't have — so it can't even route such a
query. It doesn't need to. The cluster's decision *reaches* the node anyway, as a
**registration**: when the coordinator reassigns a segment to this node, catch-up
runs and the segment enters the live store. The data replica set is the owning
Raft group's committed decision — which this node, not hosting that group, can't
read directly (see CLAUDE.md "Two replica sets") — but the node observes the
*result* of that decision locally. So "did the cluster make me a replica of this?"
is answered by "did this segment get registered?"

Cleanup is a periodic local sweep. For each segment still in the orphan set:

- **registered** — catch-up brought it into the live store; the reuse happened. It
  is no longer an orphan: drop it from the set, keep the file.
- **mid-catch-up** — a transfer is in flight. Leave it; it is about to register.
- **neither** — the cluster never made this node a replica of it. It is a stray:
  delete the file, drop it from the set.

Deletes are bounded per pass, so a large backlog drains over several sweeps rather
than in one burst.

**Why a delay before deleting.** The sweep's interval doubles as a grace period. A
restarted node's segments only become "registered" *after* re-fill reassigns them
and catch-up completes; deleting before that would throw away a free reuse and
force a full transfer later. The grace lets re-fill and catch-up claim the
reusable segments first; whatever is left unclaimed after it is genuinely a stray.
Getting the timing wrong is bounded: a mistimed delete just means a later catch-up
copies the bytes instead of reusing them — it can never lose data, because a stray
is by definition a segment the cluster does not count on this node for.

**Why local-and-lazy.** An orphaned-but-intact segment is worth keeping — it can
be reused cheaply later, and eager deletion converts a future cheap catch-up into
a full transfer to save nothing but disk. The orphan set is also *bounded*: a node
recovers only what it held before crashing, and the set only shrinks (reused or
deleted), so it can't grow without limit and there's no runaway forcing urgent
reclamation. (Disk-pressure-driven deletion was considered and deferred for this
reason; the design left room to add it.)

The sweep is **self-terminating**: each pass reports whether any orphans remain,
and the timer stops once the set is empty (recovery fills it once; it only
shrinks). A node that recovered nothing sweeps at most once and then goes quiet.

---

## Failure cases during recovery

| Case | Handling |
|---|---|
| Crash during replay | Re-run from step 1; dedup makes replay idempotent. |
| Crash after replay, before WAL deletion | Re-run; everything dedups, files get deleted this time. |
| Sparse index missing or corrupt | Always rebuilt from the segment files — a derived accelerator, never trusted across a crash. |
| Segment file corrupt mid-stream | Inventory credits only up to the last verified entry; repair fills the rest. |
| Entire disk lost | Degenerates to joining as an empty new member; every assignment is a full copy. Correct, just slow. |
| WAL torn tail | Discard back to the last complete batch — never fsync-confirmed, never ACKed. |

The common thread: no recovery failure needs operator intervention to stay
correct. The worst case is always "copy more bytes from a healthy replica."

---

## Invariants

Predicates over on-disk and in-memory state, checkable at any moment (in the
spirit of the control-plane `assert_invariants` methods):

1. **No batch is partially applied.** Every segment file holds exactly a
   contiguous prefix of its entry-id sequence — no gaps. A torn or corrupt WAL
   batch contributes nothing, never a fragment. Without this, a reader could serve
   a hole.

2. **Replay is idempotent.** Running recovery N times over the same disk produces
   byte-identical segment files to running it once. This is what makes a crash
   *during* recovery safe — re-running just converges. Directly property-testable:
   recover, snapshot, recover again, compare.

3. **The inventory never overstates.** For every entry `(segment → N)`, entries up
   to N exist locally and pass CRC; the inventory is always ≤ the verified prefix.
   Overstating is the one unforgivable error — it would have the node claim entries
   it can't serve — so CRC is checked before anything enters the inventory.

4. **The inventory references only sealed segments.** No active segment ever
   appears, because a restarted identity is in no active replica set, so every
   local segment predates the restart and was sealed by the cluster. This is why
   byte-level reuse is safe: sealed segments are immutable.

5. **At most one WAL lifetime exists once the node serves.** After recovery, every
   WAL file on disk was written by the current process. (Transiently false inside
   the deletion window; the disjoint-segment argument above makes that harmless.)

## Rules enforced by construction

These are ordering rules, not state predicates — there is no snapshot in which to
check them, only a sequence that must not be reordered. The better treatment is to
make violating them inexpressible in the code's shape:

- **Metadata before data.** Sealed-segment ends come from the control plane, which
  recovers its Raft log independently. The data plane clips local surplus by those
  ends at read time, so recovery never has to replay "correctly" with respect to
  seals — the clip is in the read path, not in recovery discipline. A segment's
  base offset is in its filename, so recovery turns a verified record *count* into
  absolute entry ids with no metadata lookup at all; only the mutable sealed *end*
  needs the coordinator.

- **Recovery before serving.** Rather than asserting "ports open only after the
  inventory is built," the serving side is constructible only *from* the recovery
  output — the inventory is a required constructor input. No code path can produce
  a serving node without first producing a finished recovery.

- **Destruction is last, and never deletes what the cluster counts on.** Orphan GC
  is the only step that deletes segment data. It deletes a segment only when the
  node is not a data replica of it — judged *locally*, by the segment never having
  been registered through the reassign-and-catch-up path — and only after the grace
  period that lets reuse happen first. The guard is local observation plus the
  grace, not a per-delete cluster round-trip (which a fresh identity couldn't even
  route).

---
